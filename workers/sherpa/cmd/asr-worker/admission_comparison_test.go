package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const admissionShortSHA = "35d512d4a5d5a3a705f53361d4edffc0e0f49edb54a42a5ba6fe591e42019e2a"

// admissionArm 是运行前固定的实验槽位，不补场；未执行者保留 not_run。
type admissionArm struct {
	Name              string                   `json:"name"`
	Strategy          string                   `json:"strategy"`
	Kind              string                   `json:"kind"`
	State             string                   `json:"state"`
	Failure           string                   `json:"failure,omitempty"`
	TriggerNS         *int64                   `json:"trigger_ns"`
	TriggerSnapshot   *gateway.GatewaySnapshot `json:"trigger_snapshot"`
	TriggerCreated    [2]int                   `json:"trigger_created"`
	TriggerClosed     [2]int                   `json:"trigger_closed"`
	BeforeModels      [2]int                   `json:"before_models"`
	AfterModels       [2]int                   `json:"after_models"`
	CleanupNS         *int64                   `json:"cleanup_ns"`
	LocalPoolReusable *bool                    `json:"local_pool_reusable"` // 旧策略无本地容量，保持 null。
	Cases             []string                 `json:"cases"`
}

func admissionPlan() []admissionArm {
	arms := []admissionArm{{Name: "warmup", Strategy: "capacity", Kind: "warmup", State: "not_run", Cases: []string{"WA", "WB"}}}
	for p, strategies := range [][]string{{"old", "capacity"}, {"capacity", "old"}, {"old", "capacity"}} {
		for _, strategy := range strategies {
			arms = append(arms, admissionArm{Name: fmt.Sprintf("p%d_%s", p+1, strategy), Strategy: strategy, Kind: "staggered", State: "not_run", Cases: []string{"L", "S", "N"}})
		}
	}
	for i := 1; i <= 3; i++ {
		arms = append(arms, admissionArm{Name: fmt.Sprintf("overload%d", i), Strategy: "capacity", Kind: "overload", State: "not_run", Cases: []string{"L", "S", "N"}})
	}
	return arms
}

// admissionCase 的时间以同一驱动起点计，不含远程时钟相减。
// Report 中 cleanup 字段只供旧夹具兼容，本实验清理只在 Arm 中判定。
type admissionCase struct {
	Name       string               `json:"name"`
	StartNS    int64                `json:"start_ns"`
	ReturnNS   int64                `json:"return_ns"`
	EntryCode  string               `json:"entry_code"`
	Assignment *admissionAssignment `json:"assignment"`
	ModelID    *int                 `json:"model_id"`
	Report     gatewayCaseReport    `json:"report"`
}
type admissionCall struct {
	done  chan struct{}
	value admissionCase
	err   error
}

// launchAdmissionCase 不在 goroutine 中调用 t.Fatal；文件与消费者只属于该场。
func launchAdmissionCase(ctx context.Context, g *gateway.Gateway, r *admissionRouter, url, out, name string, pcm []byte) *admissionCall {
	call := &admissionCall{done: make(chan struct{})}
	r.setPending(name)
	go func() {
		defer close(call.done)
		file, err := os.OpenFile(filepath.Join(out, name+".jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			call.err = err
			return
		}
		defer file.Close()
		sum := sha256.Sum256(pcm)
		report := gatewayCaseReport{Name: name, InputBytes: len(pcm), InputSHA: hex.EncodeToString(sum[:])}
		source := &gatewayPCMSource{reader: bytes.NewReader(pcm)}
		var consumer gatewayTranscript
		var start time.Time
		client, err := wsclient.NewV2(wsclient.V2Config{URL: url, ChunkBytes: 3200, Realtime: true, OnResult: func(ctx context.Context, msg wsprotocol.SequencedResultMessage, at time.Time) error {
			read := source.read.Load()
			if err := consumer.apply(ctx, msg, at.Sub(start).Nanoseconds(), read, file); err != nil {
				return err
			}
			if report.FirstTextNS == nil && strings.TrimSpace(msg.Text) != "" {
				ns := at.Sub(start).Nanoseconds()
				report.FirstTextNS = &ns
				report.FirstTextBeforeEnd = read < int64(len(pcm))
			}
			return nil
		}})
		if err != nil {
			call.err = err
			return
		}
		start = time.Now()
		source.start = start
		call.value.Name = name
		call.value.StartNS = start.Sub(r.start).Nanoseconds()
		report.Client, err = client.Run(ctx, source)
		returned := time.Now()
		call.value.ReturnNS = returned.Sub(r.start).Nanoseconds()
		report.RunNS = returned.Sub(start).Nanoseconds()
		report.ActiveAtReturn = g.Snapshot().ActiveSessions
		if err != nil {
			report.Error = err.Error()
		}
		var rejected *wsclient.V2EntryError
		if errors.As(err, &rejected) {
			call.value.EntryCode = string(rejected.Code)
		}
		report.ReadBytes, report.SourceCloses = source.read.Load(), source.closed.Load()
		if eof := source.eofNS.Load(); eof != 0 {
			report.SourceEOFNS = &eof
			tail := report.RunNS - eof
			report.TailNS = &tail
		}
		report.CallbackUpdates = len(consumer.updates)
		report.Segments = len(consumer.segments)
		report.Transcript = consumer.segments["1"].Text
		for _, event := range consumer.updates {
			if event.Message.IsFinal {
				report.Finals++
			}
		}
		if err := file.Close(); err != nil {
			call.err = err
		}
		call.value.Report = report
		data, writeErr := json.MarshalIndent(call.value, "", "  ")
		if writeErr == nil {
			writeErr = os.WriteFile(filepath.Join(out, name+"-return.json"), append(data, '\n'), 0600)
		}
		if writeErr != nil {
			call.err = errors.Join(call.err, writeErr)
		}
	}()
	return call
}

// awaitAdmission 不猜延时，等待因果条件；ctx 期限只作实验失败上限。
func awaitAdmission(ctx context.Context, predicate func() (bool, error)) error {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		ok, err := predicate()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
func callReturned(c *admissionCall) bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
func awaitAssignment(ctx context.Context, r *admissionRouter, name string, call *admissionCall) error {
	return awaitAdmission(ctx, func() (bool, error) {
		if a, ok := r.assignment(name); ok && a.Trace != nil {
			return true, nil
		}
		if callReturned(call) {
			return false, fmt.Errorf("%s returned before Worker RPC opened", name)
		}
		return false, nil
	})
}

func admissionConfig() gateway.Config {
	return gateway.Config{
		MaxSessions: 2, MaxMessageBytes: 3208, StartTimeout: 10 * time.Second, InputIdleTimeout: 30 * time.Second,
		WorkerSendTimeout: 2 * time.Second, TailTimeout: 15 * time.Second, ResultWriteTimeout: 2 * time.Second, MaxPendingAudioBytes: 0,
		V2: &gateway.V2Config{MaxHandshakes: 64, EntryTimeout: 10 * time.Second, InputProgressTimeout: 30 * time.Second, ResumeWindow: 10 * time.Second, ResultRetentionTimeout: 30 * time.Second, WorkerStatusTimeout: 2 * time.Second, MaxAudioBytes: 1 << 20, MaxAudioChunks: 256, MaxResultBytes: 1 << 20, MaxResults: 256},
	}
}

func finalizeAdmissionCase(t *testing.T, out string, r *admissionRouter, call *admissionCall, modelID *int) admissionCase {
	t.Helper()
	<-call.done
	value := call.value
	if a, ok := r.assignment(value.Name); ok {
		value.Assignment = &a
		if a.Trace != nil {
			value.Report.RPC = a.Trace.snapshot()
		}
	}
	value.ModelID = modelID
	writeGatewayJSON(t, filepath.Join(out, value.Name+"-case.json"), value)
	if call.err != nil {
		t.Fatal(call.err)
	}
	return value
}
func checkAdmissionCase(c admissionCase, outcome string) error {
	r := c.Report
	if r.SourceCloses != 1 || r.Client.ResumeAttempts != 0 || r.Client.ResumedConnections != 0 {
		return errors.New("source or recovery mismatch")
	}
	if outcome == "session_limit" {
		if c.EntryCode != "session_limit" || c.Assignment != nil || r.Error == "" || r.Client.Completed || r.Client.CompletionAckWritten || r.Finals != 0 {
			return errors.New("entry rejection mismatch")
		}
		return nil
	}
	if c.Assignment == nil || c.Assignment.Trace == nil || c.Assignment.RPCEndNS == 0 {
		return errors.New("RPC association or terminal missing")
	}
	if outcome == "busy" {
		if r.RPC.Terminal != "ResourceExhausted" || c.Assignment.TerminalMessage != "worker is busy" || r.Error == "" || r.Client.Completed || r.Client.CompletionAckWritten || r.Finals != 0 || c.ModelID != nil {
			return errors.New("busy rejection mismatch")
		}
		return nil
	}
	if r.Error != "" || !r.Client.Completed || !r.Client.CompletionAckWritten || r.Client.FinalOffset != uint64(r.InputBytes) || r.Client.AppliedSeq != uint64(r.CallbackUpdates) || r.ReadBytes != int64(r.InputBytes) || r.SourceEOFNS == nil || r.Finals != 1 || r.Segments != 1 || r.RPC.AudioBytes != r.InputBytes || r.RPC.AudioSHA != r.InputSHA || !r.RPC.HalfClosed || r.RPC.Terminal != "EOF" || r.FirstTextNS == nil || !r.FirstTextBeforeEnd || c.ModelID == nil {
		return errors.New("normal completion mismatch")
	}
	if len(r.RPC.Results) != r.CallbackUpdates || len(r.RPC.Chunks) == 0 || r.RPC.Chunks[len(r.RPC.Chunks)-1] != (r.InputBytes-1)%3200+1 {
		return errors.New("snapshot count or short audio tail mismatch")
	}
	return nil
}

// runAdmissionArm 仅在批次边界要求 Gateway=0；每臂共享进程及 gRPC 连接。
func runAdmissionArm(t *testing.T, arm *admissionArm, out string, clients [2]asrv1.ASRServiceClient, models [2]*backendMonitor, sampler *admissionSampler, long, short []byte) error {
	t.Helper()
	dir := filepath.Join(out, arm.Name)
	if err := os.Mkdir(dir, 0700); err != nil {
		return err
	}
	eventFile, err := os.Create(filepath.Join(dir, "routing.jsonl"))
	if err != nil {
		return err
	}
	defer eventFile.Close()
	router := &admissionRouter{start: sampler.start, calls: make(map[string]*admissionAssignment), events: eventFile}
	workers := make([]workerpool.Worker, 2)
	for i, client := range clients {
		workers[i] = workerpool.Worker{ID: string(rune('A' + i)), Client: &admissionClient{client: client, router: router, worker: string(rune('A' + i))}}
	}
	if arm.Strategy == "old" {
		router.roundRobin, err = workerpool.NewRoundRobin(workers)
	} else {
		router.pool, err = workerpool.NewCapacityRoundRobin([]workerpool.WorkerCapacity{{Worker: workers[0], Limit: 1}, {Worker: workers[1], Limit: 1}})
	}
	if err != nil {
		return err
	}
	life, cancelLife := context.WithCancel(context.Background())
	g, err := gateway.NewWithAllocator(life, router, admissionConfig())
	if err != nil {
		cancelLife()
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/asr", g.ServeV2HTTP)
	server := httptest.NewServer(mux)
	sampler.setArm(arm.Name, g)
	// 不等 caller 的 t.Cleanup 才关闭当前臂，避免积累历史全文和 HTTP 连接。
	defer func() {
		g.StopAccepting()
		cancelLife()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Wait(ctx)
		server.Close()
		sampler.setArm("between", nil)
	}()
	ctx, cancel := context.WithTimeout(life, 45*time.Second)
	defer cancel()
	var calls []*admissionCall
	defer func() {
		cancel()
		for _, call := range calls {
			<-call.done
		}
	}()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/asr"
	for i, m := range models {
		created, closed, err := modelCounts(m)
		if err != nil {
			return err
		}
		if created != closed {
			return errors.New("previous model not naturally released")
		}
		arm.BeforeModels[i] = created
	}
	launch := func(name string, pcm []byte) *admissionCall {
		call := launchAdmissionCase(ctx, g, router, url, dir, name, pcm)
		calls = append(calls, call)
		return call
	}
	waitCall := func(call *admissionCall) error {
		select {
		case <-call.done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if arm.Kind == "warmup" {
		for i, name := range arm.Cases {
			call := launch(name, long)
			if err := awaitAssignment(ctx, router, name, call); err != nil {
				return err
			}
			if err := waitCall(call); err != nil {
				return err
			}
			budget, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := awaitAdmission(budget, func() (bool, error) {
				created, closed, err := modelCounts(models[i])
				return g.Snapshot().ActiveSessions == 0 && created == closed && created == arm.BeforeModels[i]+1, err
			})
			cancel()
			if err != nil {
				return err
			}
		}
	} else {
		l := launch("L", long)
		if err := awaitAssignment(ctx, router, "L", l); err != nil {
			return err
		}
		input := short
		if arm.Kind == "overload" {
			input = long
		}
		s := launch("S", input)
		if err := awaitAssignment(ctx, router, "S", s); err != nil {
			return err
		}
		if arm.Kind == "staggered" {
			if err := waitCall(s); err != nil {
				return err
			}
			budget, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := awaitAdmission(budget, func() (bool, error) {
				created, closed, err := modelCounts(models[1])
				return g.Snapshot().ActiveSessions == 1 && created == closed && closed == arm.BeforeModels[1]+1, err
			})
			cancel()
			if err != nil {
				return err
			}
			created, closed, err := modelCounts(models[0])
			if err != nil {
				return err
			}
			if callReturned(l) || created != arm.BeforeModels[0]+1 || closed != arm.BeforeModels[0] {
				return errors.New("staggered overlap condition not met; no replacement run")
			}
		} else {
			if err := awaitAdmission(ctx, func() (bool, error) {
				for i, m := range models {
					created, _, err := modelCounts(m)
					if err != nil {
						return false, err
					}
					if created != arm.BeforeModels[i]+1 {
						return false, nil
					}
				}
				return g.Snapshot().ActiveSessions == 2, nil
			}); err != nil {
				return err
			}
			if callReturned(l) || callReturned(s) {
				return errors.New("overload overlap condition not met")
			}
		}
		snapshot := g.Snapshot()
		arm.TriggerSnapshot = &snapshot
		for i, m := range models {
			created, closed, err := modelCounts(m)
			if err != nil {
				return err
			}
			arm.TriggerCreated[i], arm.TriggerClosed[i] = created, closed
		}
		trigger := time.Since(sampler.start).Nanoseconds()
		arm.TriggerNS = &trigger
		n := launch("N", short)
		if err := waitCall(n); err != nil {
			return err
		}
		if err := waitCall(l); err != nil {
			return err
		}
		if err := waitCall(s); err != nil {
			return err
		}
	}
	// 批次末同时核对本地会话与两原生进程；这不是固定 sleep 猜测释放。
	budget, cancelCleanup := context.WithTimeout(ctx, 5*time.Second)
	err = awaitAdmission(budget, func() (bool, error) {
		if g.Snapshot().ActiveSessions != 0 {
			return false, nil
		}
		for _, m := range models {
			created, closed, err := modelCounts(m)
			if err != nil {
				return false, err
			}
			if created != closed {
				return false, nil
			}
		}
		return true, nil
	})
	cancelCleanup()
	if err != nil {
		return err
	}
	cleanup := time.Since(sampler.start).Nanoseconds()
	arm.CleanupNS = &cleanup
	if router.pool != nil {
		a, e1 := router.pool.TryAcquire()
		b, e2 := router.pool.TryAcquire()
		good := e1 == nil && e2 == nil && a.Worker().ID != b.Worker().ID
		if a != nil {
			a.Release()
		}
		if b != nil {
			b.Release()
		}
		arm.LocalPoolReusable = &good
		if !good {
			return errors.New("local reservations did not become reusable")
		}
	}
	next := arm.BeforeModels
	for _, call := range calls {
		name := call.value.Name
		outcome := "normal"
		if name == "N" {
			if arm.Kind == "overload" {
				outcome = "session_limit"
			} else if arm.Strategy == "old" {
				outcome = "busy"
			}
		}
		var modelID *int
		if outcome == "normal" {
			a, ok := router.assignment(name)
			if !ok {
				return errors.New("missing normal assignment")
			}
			index := int(a.Worker[0] - 'A')
			next[index]++
			id := next[index]
			modelID = &id
		}
		value := finalizeAdmissionCase(t, dir, router, call, modelID)
		if err := checkAdmissionCase(value, outcome); err != nil {
			return fmt.Errorf("%s/%s: %w", arm.Name, name, err)
		}
		if outcome == "normal" {
			events := models[int(value.Assignment.Worker[0]-'A')].modelEvents(*modelID)
			if len(events) != 2 || events[0].ObservedNS < value.Assignment.RPCStartNS || events[0].ObservedNS > value.ReturnNS {
				return errors.New("native model identity/time mismatch")
			}
		}
	}
	for i, m := range models {
		created, _, err := modelCounts(m)
		if err != nil {
			return err
		}
		arm.AfterModels[i] = created
		if created != next[i] {
			return errors.New("unexpected model creation count")
		}
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if router.err != nil {
		return router.err
	}
	return nil
}

// TestRealWorkerAdmissionComparison 显式启用固定两 Worker 配对实验，不修改业务参数。
func TestRealWorkerAdmissionComparison(t *testing.T) {
	out := os.Getenv("TIDE_SHERPA_ADMISSION_EVIDENCE_DIR")
	if out == "" {
		t.Skip("explicit admission evidence directory required")
	}
	bin, modelDir := os.Getenv("TIDE_SHERPA_WORKER_BIN"), os.Getenv("TIDE_SHERPA_MODEL_DIR")
	if bin == "" || modelDir == "" {
		t.Fatal("Worker binary and model directory required")
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				t.Fatal("use a non-race driver")
			}
		}
	}
	long, err := readTestPCM(filepath.Join(modelDir, "test_wavs", "0.wav"))
	if err != nil {
		t.Fatal(err)
	}
	short, err := readTestPCM(filepath.Join(modelDir, "test_wavs", "1.wav"))
	if err != nil {
		t.Fatal(err)
	}
	modelFiles, err := freezeBackendAssets(modelDir, long)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(short)
	if len(short) != 163200 || hex.EncodeToString(sum[:]) != admissionShortSHA {
		t.Fatal("short input identity mismatch")
	}
	workerSHA, err := backendFileSHA(bin)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	arms := admissionPlan()
	writeGatewayJSON(t, filepath.Join(out, "arms.json"), arms)
	writeGatewayJSON(t, filepath.Join(out, "conditions.json"), map[string]any{"schema": "tide.admission-comparison.v1", "gateway": admissionConfig(), "input_long_bytes": len(long), "input_long_sha256": backendInputSHA, "input_short_bytes": len(short), "input_short_sha256": admissionShortSHA, "model_files": modelFiles, "worker_sha256": workerSHA, "threads_per_worker": 2, "worker_limit": 1, "sample_interval_ns": int64(500 * time.Millisecond), "max_sample_gap_ns": int64(3 * time.Second), "between_idle_ns": int64(2 * time.Second), "client_realtime": true, "client_resume": false, "forced_gc": false, "sdk": "sherpa-onnx-go-macos@v1.13.8", "decoding": "greedy_search", "arm_budget_ns": int64(45 * time.Second), "cleanup_budget_ns": int64(5 * time.Second)})
	var processes [2]*realWorkerProcess
	var models [2]*backendMonitor
	var clients [2]asrv1.ASRServiceClient
	start := time.Now()
	for i := range 2 {
		dir := filepath.Join(out, "worker"+string(rune('A'+i)))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		processes[i] = startRealWorkerProcess(t, bin, modelDir, dir)
		conn, err := grpc.NewClient("passthrough:///"+processes[i].address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		clients[i] = asrv1.NewASRServiceClient(conn)
		models[i], err = newBackendMonitor(dir, processes[i].cmd.Process.Pid, nil)
		if err != nil {
			t.Fatal(err)
		}
		models[i].start = start
		models[i].maxEvents = 64
	}
	sampler := &admissionSampler{start: start, models: models, pids: [2]int{processes[0].cmd.Process.Pid, processes[1].cmd.Process.Pid}, arm: "startup"}
	if err := sampler.startSampling(out); err != nil {
		t.Fatal(err)
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			if err := sampler.stop(); err != nil {
				t.Error(err)
			}
			for _, m := range models {
				if err := m.stop(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	t.Cleanup(stop)
	writeGatewayJSON(t, filepath.Join(out, "process-start.json"), map[string]any{"worker_pids": sampler.pids, "driver_pid": os.Getpid(), "started_at": start.Format(time.RFC3339Nano)})
	for i := range arms {
		arms[i].State = "running"
		writeGatewayJSON(t, filepath.Join(out, "arms.json"), arms)
		if err := runAdmissionArm(t, &arms[i], out, clients, models, sampler, long, short); err != nil {
			arms[i].State = "failed"
			arms[i].Failure = err.Error()
			writeGatewayJSON(t, filepath.Join(out, "arms.json"), arms)
			t.Fatal(err)
		}
		arms[i].State = "passed"
		writeGatewayJSON(t, filepath.Join(out, "arms.json"), arms)
		t.Logf("arm=%s state=passed models=%v->%v", arms[i].Name, arms[i].BeforeModels, arms[i].AfterModels)
		time.Sleep(2 * time.Second)
	}
	stop()
	for _, p := range processes {
		p.stop(t)
	}
	writeGatewayJSON(t, filepath.Join(out, "process.json"), map[string]any{"process_exit": [2]int{0, 0}, "worker_pids": sampler.pids, "driver_pid": os.Getpid(), "models_created": [2]int{models[0].created, models[1].created}, "models_closed": [2]int{models[0].closed, models[1].closed}})
}
