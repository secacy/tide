package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// backlogExperimentReport 区分业务结果与实验验证结果。缺失时间用 nil，
// 逐块发送、计量和结果记录用于复核，不能将未完成转录算作延迟改善。
type backlogExperimentReport struct {
	Case                string           `json:"case"`
	Budget              uint64           `json:"budget_bytes"`
	Validated           bool             `json:"validated"`
	Outcome             string           `json:"outcome"`
	RunError            string           `json:"run_error,omitempty"`
	ProcessingMS        float64          `json:"processing_ms"`
	PauseMS             float64          `json:"pause_ms"`
	PlannedChunks       int              `json:"planned_chunks"`
	ClientWrittenBytes  int              `json:"client_written_bytes"`
	SendSucceededBytes  int              `json:"send_succeeded_bytes"`
	WorkerReceivedBytes int              `json:"worker_received_bytes"`
	WorkerProgressSent  uint64           `json:"worker_progress_sent_bytes"`
	WorkerError         string           `json:"worker_error,omitempty"`
	FinalProgress       progressSample   `json:"final_progress"`
	PeakPending         uint64           `json:"observed_peak_pending_bytes"`
	MaxSampleGapMS      float64          `json:"max_sample_gap_ms"`
	MaxSnapshotMS       float64          `json:"max_snapshot_call_ms"`
	EndMS               *float64         `json:"end_ms"`
	FinalMS             *float64         `json:"final_ms"`
	TailMS              *float64         `json:"tail_ms"`
	CloseMS             float64          `json:"close_ms"`
	CloseCode           int              `json:"close_code"`
	CloseReason         string           `json:"close_reason"`
	RunReturnedMS       float64          `json:"run_returned_ms"`
	HandlerReturnedMS   float64          `json:"handler_returned_ms"`
	WorkerReturnedMS    float64          `json:"worker_returned_ms"`
	ActiveAfterCleanup  int              `json:"active_after_cleanup"`
	Reused              bool             `json:"reused"`
	Writes              []baselineWrite  `json:"writes"`
	Sends               []baselineWrite  `json:"sends"`
	Partials            []pausePartial   `json:"partials"`
	Samples             []progressSample `json:"samples"`
}

type backlogMeasuredExit struct {
	baselineWorkerExit
	progress uint64
	at       time.Time
}

// backlogMeasuredWorker 验证收到的合成音频顺序，并记录成功发出的累计进度。
// 进度发送成功不代表网关一定收到了确认；这两个观测点分开记录。
type backlogMeasuredWorker struct {
	asrv1.UnimplementedASRServiceServer
	worker *mockasr.Worker
	exits  chan backlogMeasuredExit
}

func (w *backlogMeasuredWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	observed := &backlogMeasuredStream{baselineStream: &baselineStream{BidiStreamingServer: s}}
	err := w.worker.StreamingRecognize(observed)
	w.exits <- backlogMeasuredExit{baselineWorkerExit{observed.chunks, observed.bytes, err}, observed.progress, time.Now()}
	return err
}

type backlogMeasuredStream struct {
	*baselineStream
	progress uint64
}

func (s *backlogMeasuredStream) Send(r *asrv1.StreamingRecognizeResponse) error {
	err := s.BidiStreamingServer.Send(r)
	if err == nil && r.Progress != nil {
		s.progress = r.Progress.ProcessedAudioBytes
	}
	return err
}

// TestAudioBacklogExperimentProbe 快速覆盖实验夹具的完成与超限分支。
// 预算不足以容纳首块时必然失败，不依赖机器调度或处理速度的偶然差异。
func TestAudioBacklogExperimentProbe(t *testing.T) {
	t.Run("completed", func(t *testing.T) { runAudioBacklogExperiment(t, "probe_completed", 0, time.Millisecond, 0, 6, false) })
	t.Run("exceeded", func(t *testing.T) { runAudioBacklogExperiment(t, "probe_exceeded", 3199, time.Millisecond, 0, 6, true) })
}

// TestAudioBacklogComparisonExperiment 每轮执行三场景的关闭/启用配对。
// 正式 -count=3 共 18 个主会话；额外短会话验证清理后的名额复用。
func TestAudioBacklogComparisonExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_BACKLOG_COMPARISON") != "1" {
		t.Skip("set TIDE_RUN_BACKLOG_COMPARISON=1")
	}
	for _, c := range []struct {
		name         string
		delay, pause time.Duration
	}{
		{"normal", 50 * time.Millisecond, 0}, {"pause_500ms", 50 * time.Millisecond, 500 * time.Millisecond}, {"slow_200ms", 200 * time.Millisecond, 0},
	} {
		for _, budget := range []uint64{0, 32000} {
			t.Run(fmt.Sprintf("%s/budget_%d", c.name, budget), func(t *testing.T) {
				runAudioBacklogExperiment(t, c.name, budget, c.delay, c.pause, 100, budget > 0 && c.name == "slow_200ms")
			})
		}
	}
}

// runAudioBacklogExperiment 使用生产 session.run，测试接入壳暴露计量器。
// 主会话的接入壳不用于证明 Gateway.ServeHTTP 吞吐；复用会话调用真实 handler。
func runAudioBacklogExperiment(t *testing.T, name string, budget uint64, delay, pause time.Duration, chunks int, wantExceeded bool) {
	t.Helper()
	report := backlogExperimentReport{Case: name, Budget: budget, Outcome: "incomplete", ProcessingMS: baselineMS(delay), PauseMS: baselineMS(pause), PlannedChunks: chunks}
	defer func() {
		data, err := json.Marshal(report)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("BACKLOG_COMPARISON_JSON %s", data)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	texts := make([]string, chunks/5)
	for i := range texts {
		texts[i] = fmt.Sprintf("p%d", i+1)
	}
	worker := &backlogMeasuredWorker{worker: mockasr.New(mockasr.Config{ProcessingDelay: delay, PauseAfterChunks: 20, PauseDuration: pause, PartialEvery: 500 * time.Millisecond, PartialTexts: texts, FinalText: "final"}), exits: make(chan backlogMeasuredExit, 2)}
	probe := &pauseSendProbe{}
	workerClient := &pauseClient{ASRServiceClient: newBaselineTCPWorkerClient(t, worker), probe: probe}
	appCtx, stop := context.WithCancel(context.Background())
	defer stop()
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1, MaxPendingAudioBytes: int64(budget)})
	if err != nil {
		t.Fatal(err)
	}
	type exit struct {
		at  time.Time
		err error
	}
	ready := make(chan *session, 1)
	runDone := make(chan exit, 1)
	handlerDone := make(chan time.Time, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { handlerDone <- time.Now() }()
		if r.URL.Path == "/reuse" {
			g.ServeHTTP(w, r)
			return
		}
		if err := g.tracker.tryEnter(); err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		defer g.tracker.leave()
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(g.cfg.MaxMessageBytes)
		s := newSession(ws, g.worker, g.cfg.StartTimeout, g.cfg.InputIdleTimeout, g.cfg.WorkerSendTimeout, g.cfg.TailTimeout, g.cfg.ResultWriteTimeout, uint64(g.cfg.MaxPendingAudioBytes))
		ready <- s
		err = s.run(g.ctx)
		runDone <- exit{time.Now(), err}
	}))
	var clients []*websocket.Conn
	defer func() {
		stop()
		for _, c := range clients {
			_ = c.CloseNow()
		}
		server.Close()
		g.StopAccepting()
		cleanup, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
		if err := g.Wait(cleanup); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	client, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, client)
	var s *session
	select {
	case s = <-ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	origin := time.Now()
	ms := func(at time.Time) float64 { return baselineMS(at.Sub(origin)) }
	probe.mu.Lock()
	probe.origin = origin
	probe.mu.Unlock()
	received := make(chan pauseReception, 1)
	receiverDone := make(chan struct{})
	go func() { defer close(receiverDone); received <- receivePause(ctx, client, origin) }()
	sampleCtx, stopSampling := context.WithCancel(context.Background())
	sampled := make(chan []progressSample, 1)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		rows := []progressSample{takeProgressSample(s, origin)}
		for {
			select {
			case <-ticker.C:
				rows = append(rows, takeProgressSample(s, origin))
			case <-sampleCtx.Done():
				rows = append(rows, takeProgressSample(s, origin))
				sampled <- rows
				return
			}
		}
	}()
	joinedSampler := false
	defer func() {
		stopSampling()
		cancel()
		if !joinedSampler {
			select {
			case report.Samples = <-sampled:
			case <-time.After(5 * time.Second):
				t.Error("sampler cleanup timed out")
			}
		}
		select {
		case <-receiverDone:
		case <-time.After(5 * time.Second):
			t.Error("receiver cleanup timed out")
		}
	}()
	// Read 检测到关闭会唤醒节拍等待；不能等到原定 100 块全写完才处理超限。
upload:
	for i := 0; i < chunks; i++ {
		timer := time.NewTimer(max(time.Until(origin.Add(time.Duration(i)*100*time.Millisecond)), 0))
		select {
		case <-timer.C:
		case <-receiverDone:
			timer.Stop()
			break upload
		case <-ctx.Done():
			timer.Stop()
			t.Fatal(ctx.Err())
		}
		select {
		case <-receiverDone:
			break upload
		default:
		}
		began := time.Now()
		err := client.Write(ctx, websocket.MessageBinary, bytes.Repeat([]byte{byte(i)}, audio.ChunkBytesDefault))
		row := baselineWrite{Index: i, PlannedMS: float64(i) * 100, StartedMS: ms(began), ReturnedMS: ms(time.Now())}
		if err != nil {
			row.Error = err.Error()
		} else {
			report.ClientWrittenBytes += audio.ChunkBytesDefault
		}
		report.Writes = append(report.Writes, row)
		if err != nil {
			break
		}
	}
	if report.ClientWrittenBytes == chunks*audio.ChunkBytesDefault {
		at := ms(time.Now())
		report.EndMS = &at
		if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
			t.Fatal(err)
		}
	}
	var reception pauseReception
	select {
	case reception = <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	report.Partials = reception.rows
	report.CloseCode = reception.closeCode
	report.CloseMS = ms(reception.closed)
	var closeErr websocket.CloseError
	if errors.As(reception.err, &closeErr) {
		report.CloseReason = closeErr.Reason
	}
	if !reception.final.IsZero() {
		at := ms(reception.final)
		report.FinalMS = &at
		if report.EndMS != nil {
			tail := at - *report.EndMS
			report.TailMS = &tail
		}
	}
	var run exit
	select {
	case run = <-runDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	report.RunReturnedMS = ms(run.at)
	if run.err != nil {
		report.RunError = run.err.Error()
	}
	select {
	case at := <-handlerDone:
		report.HandlerReturnedMS = ms(at)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stopSampling()
	select {
	case report.Samples = <-sampled:
		joinedSampler = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var workerExit backlogMeasuredExit
	select {
	case workerExit = <-worker.exits:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	report.WorkerReceivedBytes = workerExit.bytes
	report.WorkerProgressSent = workerExit.progress
	report.WorkerReturnedMS = ms(workerExit.at)
	if workerExit.err != nil {
		report.WorkerError = workerExit.err.Error()
	}
	report.FinalProgress = takeProgressSample(s, origin)
	probe.mu.Lock()
	report.Sends = append([]baselineWrite(nil), probe.rows...)
	probe.mu.Unlock()
	for _, row := range report.Sends {
		if row.Error != "" {
			t.Fatalf("unexpected Send failure: %s", row.Error)
		}
		report.SendSucceededBytes += audio.ChunkBytesDefault
	}
	for i, row := range report.Samples {
		if row.Processed > row.Received || row.Pending != row.Received-row.Processed {
			t.Fatalf("invalid counters: %+v", row)
		}
		if i > 0 {
			prev := report.Samples[i-1]
			if row.Received < prev.Received || row.Processed < prev.Processed {
				t.Fatal("counter regression")
			}
			report.MaxSampleGapMS = max(report.MaxSampleGapMS, row.AtMS-prev.AtMS)
		}
		report.PeakPending = max(report.PeakPending, row.Pending)
		report.MaxSnapshotMS = max(report.MaxSnapshotMS, row.AtMS-row.BeginMS)
	}
	g.tracker.mu.Lock()
	report.ActiveAfterCleanup = g.tracker.active
	g.tracker.mu.Unlock()
	if report.ActiveAfterCleanup != 0 || appCtx.Err() != nil {
		t.Fatal("session cleanup required external cancellation")
	}
	if wantExceeded {
		report.Outcome = "audio_backlog_exceeded"
		if !errors.Is(run.err, ErrAudioBacklogExceeded) || closeErr.Code != websocket.StatusInternalError || closeErr.Reason != "audio backlog exceeded" {
			t.Fatalf("wrong failure: run=%v receive=%v", run.err, reception.err)
		}
		if status.Code(workerExit.err) != codes.Canceled {
			t.Fatalf("Worker did not cancel: %v", workerExit.err)
		}
		if reception.finals != 0 || report.EndMS != nil {
			t.Fatal("overload unexpectedly finished input or produced final")
		}
		if report.FinalProgress.Received != uint64(report.SendSucceededBytes+audio.ChunkBytesDefault) {
			t.Fatal("over-limit block not accounted for or was sent")
		}
		if report.PeakPending > budget+audio.ChunkBytesDefault {
			t.Fatal("sample exceeded budget plus one chunk")
		}
	} else {
		report.Outcome = "completed"
		if run.err != nil || reception.err != nil || workerExit.err != nil {
			t.Fatalf("unexpected failure: %v / %v / %v", run.err, reception.err, workerExit.err)
		}
		if reception.finals != 1 || len(reception.rows) != chunks/5 || reception.closeCode != 1000 {
			t.Fatal("missing results or normal closure")
		}
		total := chunks * audio.ChunkBytesDefault
		if report.SendSucceededBytes != total || workerExit.bytes != total || report.FinalProgress.Received != uint64(total) || report.FinalProgress.Processed != uint64(total) || report.FinalProgress.Pending != 0 {
			t.Fatal("incomplete audio processing")
		}
	}
	// 已关闭主会话不依赖 StopAccepting/应用取消；同一个 Gateway 仍可接受新连接。
	// 预算小于标准块的快速探针只检查重新接入；正式配置验证完整的单块会话。
	reuse, _, err := websocket.Dial(ctx, url+"/reuse", nil)
	if err != nil {
		t.Fatal(err)
	}
	clients = append(clients, reuse)
	if budget > 0 && budget < audio.ChunkBytesDefault {
		// 探针保持合成内容校验所需 3200 字节块，因此该配置本身不允许正常复用音频。
		// 只验证新连接被接纳并在主动关闭后清理，正式配置则验证完整正常转录。
		_ = reuse.CloseNow()
	} else {
		if err := finishAdmission(ctx, reuse); err != nil {
			t.Fatalf("reuse: %v", err)
		}
		select {
		case exit := <-worker.exits:
			if exit.err != nil || exit.bytes != audio.ChunkBytesDefault {
				t.Fatalf("reuse Worker: %+v", exit)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-handlerDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	g.tracker.mu.Lock()
	active := g.tracker.active
	g.tracker.mu.Unlock()
	if active != 0 || appCtx.Err() != nil {
		t.Fatal("reuse cleanup failed")
	}
	report.Reused = true
	report.Validated = true
}
