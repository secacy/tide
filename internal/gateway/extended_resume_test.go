package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// extendedTestWriter 的故障与屏障由测试设置，关闭次数仅在 finish 后读取。
type extendedTestWriter struct {
	bytes.Buffer
	writeErr, closeErr error
	short              bool
	entered, release   chan struct{}
	once               sync.Once
	closes             int
}

func (w *extendedTestWriter) Write(p []byte) (int, error) {
	if w.entered != nil {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.short {
		return 0, nil
	}
	return w.Buffer.Write(p)
}
func (w *extendedTestWriter) Close() error { w.closes++; return w.closeErr }

func TestExtendedResumeLog(t *testing.T) {
	t.Run("immutable_events_and_close", func(t *testing.T) {
		w := &extendedTestWriter{}
		origin := time.Now()
		var canceled atomic.Int32
		log, err := newExtendedLog(w, origin, func() { canceled.Add(1) }, extendedLogLimits{1024, 8192, 65536})
		if err != nil {
			t.Fatal(err)
		}
		payload := map[string]int{"n": 1}
		if !log.emit("source", "read", origin.Add(time.Millisecond), payload) {
			t.Fatal(log.err())
		}
		payload["n"] = 2
		if !log.emit("source", "read", origin.Add(2*time.Millisecond), payload) {
			t.Fatal(log.err())
		}
		if err := log.finish(); err != nil {
			t.Fatal(err)
		}
		if err := log.finish(); err != nil {
			t.Fatal(err)
		}
		if canceled.Load() != 0 || w.closes != 1 || log.events.Load() != 2 {
			t.Fatal("incorrect ownership")
		}
		scanner := bufio.NewScanner(bytes.NewReader(w.Bytes()))
		var i int
		for scanner.Scan() {
			var row extendedEvent
			if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
				t.Fatal(err)
			}
			i++
			var value map[string]int
			json.Unmarshal(row.Data, &value)
			if row.Seq != uint64(i) || value["n"] != i || row.ElapsedNS != int64(i)*int64(time.Millisecond) {
				t.Fatal("event changed")
			}
		}
		if i != 2 || scanner.Err() != nil {
			t.Fatal("missing events")
		}
		if log.emit("source", "late", time.Now(), nil) || !errors.Is(log.err(), errExtendedEvidence) {
			t.Fatal("late event accepted")
		}
	})
	t.Run("queue_full_is_nonblocking", func(t *testing.T) {
		w := &extendedTestWriter{entered: make(chan struct{}), release: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log, err := newExtendedLog(w, time.Now(), cancel, extendedLogLimits{1, 8192, 1})
		if err != nil {
			t.Fatal(err)
		}
		defer log.finish()
		defer close(w.release)
		log.emit("run", "one", time.Now(), nil)
		<-w.entered
		if !log.emit("run", "two", time.Now(), nil) {
			t.Fatal("queue slot not available")
		}
		if log.emit("run", "three", time.Now(), nil) {
			t.Fatal("overflow accepted")
		}
		if ctx.Err() != context.Canceled || !errors.Is(log.err(), errExtendedEvidence) {
			t.Fatal("failure not propagated independently")
		}
	})
	for _, name := range []string{"write", "short_write", "flush", "close", "oversize", "encoding", "unknown_component", "negative_time"} {
		t.Run(name, func(t *testing.T) {
			w := &extendedTestWriter{}
			sentinel := errors.New("disk error")
			limits := extendedLogLimits{8, 8192, 1}
			switch name {
			case "write":
				w.writeErr = sentinel
			case "short_write":
				w.short = true
			case "flush":
				w.writeErr = sentinel
				limits.Buffer = 65536
			case "close":
				w.closeErr = sentinel
			}
			var cancels atomic.Int32
			log, err := newExtendedLog(w, time.Now(), func() { cancels.Add(1) }, limits)
			if err != nil {
				t.Fatal(err)
			}
			component := "run"
			at := time.Now()
			var payload any = map[string]int{"n": 1}
			switch name {
			case "oversize":
				payload = strings.Repeat("x", 8192)
			case "encoding":
				payload = make(chan int)
			case "unknown_component":
				component = "unbounded-component"
			case "negative_time":
				at = log.origin.Add(-time.Second)
			}
			log.emit(component, "event", at, payload)
			if !errors.Is(log.finish(), errExtendedEvidence) || cancels.Load() != 1 || w.closes != 1 {
				t.Fatal("evidence error was not retained")
			}
		})
	}
	t.Run("concurrent_emit_and_close", func(t *testing.T) {
		w := &extendedTestWriter{}
		log, err := newExtendedLog(w, time.Now(), func() {}, extendedLogLimits{1024, 8192, 65536})
		if err != nil {
			t.Fatal(err)
		}
		var tasks sync.WaitGroup
		for i := 0; i < 32; i++ {
			tasks.Add(1)
			go func() { defer tasks.Done(); log.emit("run", "concurrent", time.Now(), nil) }()
		}
		tasks.Add(1)
		go func() { defer tasks.Done(); log.finish() }()
		tasks.Wait()
		log.finish()
		if w.closes != 1 {
			t.Fatal("duplicate writer close")
		}
	})
	t.Run("invalid_limits", func(t *testing.T) {
		w := &extendedTestWriter{}
		_, err := newExtendedLog(w, time.Now(), func() {}, extendedLogLimits{0, 8192, 65536})
		if err == nil || w.closes != 0 {
			t.Fatal("invalid constructor took ownership")
		}
	})
}

func TestExtendedResumePatternAndConfiguration(t *testing.T) {
	for _, tc := range []struct {
		index uint64
		sha   string
	}{
		{0, "7866853710afa96ddb54d04dc6035b03ee3caf66ff2a44b8e155dd8507e50c09"},
		{1, "ac9cb6729022829f79973847cda97d0c87afc9e23d1fc4cd124ad2b800bbb715"},
		{85, "9d4cda177c97951e44ea86ac886c5666ded5168ad0d39877f2e7dccdf4d92139"},
		{300, "4a6fea4f93f473b45f4500fc0e76d5a54d2a2ec627b94b52a1145cb3d99bf5b3"},
		{5999, "46a1e92a1c8b753a64eceef3f75d2938c00f106ded3799185906a28c5dccc049"},
	} {
		if block := extendedPattern(tc.index); len(block) != 3200 || extendedSHA(block) != tc.sha {
			t.Fatal("independent Python known vector mismatch")
		}
	}
	for _, probe := range []bool{true, false} {
		for _, cfg := range extendedCases(probe) {
			if err := cfg.validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	cfg := extendedCases(true)[1]
	cfg.FaultChunks[1] = cfg.FaultChunks[0]
	if cfg.validate() == nil {
		t.Fatal("duplicate fault accepted")
	}
	if extendedCases(true)[1].FaultChunks[1] != 25 {
		t.Fatal("configuration alias leaked")
	}
}

// TestExtendedResumeWorkerRejectsBadInput 用真实 RPC 验证在线字节校验，不只检查生成器。
func TestExtendedResumeWorkerRejectsBadInput(t *testing.T) {
	for _, name := range []string{"corrupt", "wrong_block", "short_block"} {
		t.Run(name, func(t *testing.T) {
			writer := &extendedTestWriter{}
			log, err := newExtendedLog(writer, time.Now(), func() {}, extendedLogLimits{1024, 8192, 65536})
			if err != nil {
				t.Fatal(err)
			}
			defer log.finish()
			worker := &extendedWorker{log: log, cfg: extendedCases(true)[0], digest: sha256.New(), done: make(chan struct{})}
			client := newBaselineTCPWorkerClient(t, worker)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			stream, err := client.StreamingRecognize(ctx)
			if err != nil {
				t.Fatal(err)
			}
			block := extendedPattern(0)
			switch name {
			case "corrupt":
				block[31] ^= 1
			case "wrong_block":
				block = extendedPattern(1)
			case "short_block":
				block = block[:3199]
			}
			if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: block}); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Recv(); err == nil || err == io.EOF {
				t.Fatal("invalid block accepted")
			}
			select {
			case <-worker.done:
			case <-ctx.Done():
				t.Fatal("Worker did not exit")
			}
			if !errors.Is(log.finish(), errExtendedEvidence) || worker.active.Load() != 0 || worker.calls.Load() != 1 {
				t.Fatal("bad input not recorded")
			}
			worker.mu.Lock()
			received := worker.received
			worker.mu.Unlock()
			if received != 0 {
				t.Fatal("bad block treated as received prefix")
			}
		})
	}
}

// TestExtendedResumeForcedCleanup 区分自然清理和人为取消，使用更短的独立故障配置。
func TestExtendedResumeForcedCleanup(t *testing.T) {
	cfg := extendedConfig{Case: "forced_probe", Chunks: 20, FaultChunks: []int{1, 3, 5, 7, 9, 11}, Attempts: 2, Outage: 20 * time.Millisecond, Processing: time.Millisecond, RunTimeout: 5 * time.Second, CleanupTimeout: time.Millisecond, SamplePeriod: time.Millisecond}
	r := runExtendedProbe(t, cfg, t.TempDir())
	if !r.Forced || r.NaturalIdle || r.LogError || r.ErrorClass != "recovery_exhausted" || r.SourceCloses != 1 {
		t.Fatalf("forced cleanup concealed: %+v", r)
	}
}

// decodeExtendedEvents 校验完整行、固定组件、连续序号和最后的生产者退出标记。
// 只用于 probe 结束后核对，正式独立分析器后续单独实现。
func decodeExtendedEvents(input io.Reader) ([]extendedEvent, error) {
	reader := bufio.NewReaderSize(input, 8192)
	sequences := map[string]uint64{"source": 0, "worker": 0, "proxy": 0, "sample": 0, "run": 0}
	var rows []extendedEvent
	for {
		line, err := reader.ReadSlice('\n')
		if err == io.EOF && len(line) == 0 {
			break
		}
		if err != nil {
			return nil, errors.New("incomplete or oversized event line")
		}
		var row extendedEvent
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, errors.New("invalid event JSON")
		}
		seq, known := sequences[row.Component]
		if !known || row.Version != 1 || row.Kind == "" || row.ElapsedNS < 0 || row.Seq != seq+1 {
			return nil, errors.New("invalid event envelope")
		}
		sequences[row.Component] = row.Seq
		rows = append(rows, row)
	}
	if len(rows) == 0 || rows[len(rows)-1].Component != "run" || rows[len(rows)-1].Kind != "joined" {
		return nil, errors.New("missing terminal join")
	}
	return rows, nil
}

func readExtendedEvents(t *testing.T, path string) []extendedEvent {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := decodeExtendedEvents(file)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestExtendedResumeProbe(t *testing.T) {
	for _, cfg := range extendedCases(true) {
		t.Run(cfg.Case, func(t *testing.T) {
			dir := t.TempDir()
			// 指定目录时保留完整 probe 证据；正式采集器会创建逐场独立目录。
			if root := os.Getenv("TIDE_EXTENDED_PROBE_OUTPUT"); root != "" {
				dir = filepath.Join(root, cfg.Case)
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			r := runExtendedProbe(t, cfg, dir)
			if r.LogError || r.Forced || !r.NaturalIdle || r.SourceCloses != 1 || r.WorkerCalls != 1 || r.Picks != 1 {
				t.Fatalf("ownership/evidence: %+v", r)
			}
			complete := cfg.Case != "C_budget_5"
			if r.Result.Completed != complete || r.Result.CompletionAckWritten != complete {
				t.Fatal("incorrect business outcome")
			}
			faults := len(cfg.FaultChunks)
			wantResume := faults
			if !complete {
				wantResume = 5
			}
			if r.Faults != faults || r.Result.ResumeAttempts != wantResume || r.Result.ResumedConnections != wantResume || r.Connections != wantResume+1 {
				t.Fatalf("faults/attempts: %+v", r)
			}
			if complete {
				if r.ErrorClass != "" || r.Captured != 384000 || r.Received != r.Captured || r.Processed != r.Captured || r.SourceSHA != r.WorkerSHA || r.Result.AppliedSeq != 121 {
					t.Fatal("complete input/result mismatch")
				}
			} else if r.ErrorClass != "recovery_exhausted" || r.Received != 85*3200 || r.Processed != r.Received || r.Captured < 86*3200 || r.Result.AppliedSeq != 85 {
				t.Fatal("budget failure concealed gap")
			}
			rows := readExtendedEvents(t, filepath.Join(dir, "events.jsonl"))
			faultNS, openNS := map[int]int64{}, map[int]int64{}
			reads := map[uint64]int64{}
			var applications int
			var joined, idle bool
			var forwarded int
			var sourceHash, workerHash = sha256.New(), sha256.New()
			for _, row := range rows {
				if bytes.Contains(row.Data, []byte("resumeToken")) || bytes.Contains(row.Data, []byte("SessionID")) {
					t.Fatal("credentials in event")
				}
				var v struct {
					Round              int `json:"round"`
					Begin, End, Offset uint64
					SHA                string `json:"sha256"`
					Final              bool   `json:"final"`
					AppliedNS          int64  `json:"applied_ns"`
				}
				if err := json.Unmarshal(row.Data, &v); err != nil {
					t.Fatal(err)
				}
				switch row.Component + "/" + row.Kind {
				case "proxy/fault":
					if _, ok := faultNS[v.Round]; ok {
						t.Fatal("replay retriggered fault")
					}
					faultNS[v.Round] = row.ElapsedNS
				case "proxy/opened":
					openNS[v.Round] = row.ElapsedNS
				case "proxy/handshake_forwarded":
					forwarded++
				case "source/read":
					block := extendedPattern(v.Begin / 3200)
					if v.End-v.Begin != 3200 || v.SHA != extendedSHA(block) {
						t.Fatal("source range/digest mismatch")
					}
					reads[v.End] = row.ElapsedNS
					sourceHash.Write(block)
				case "worker/received":
					block := extendedPattern(v.Begin / 3200)
					if v.SHA != extendedSHA(block) {
						t.Fatal("Worker digest mismatch")
					}
					workerHash.Write(block)
				case "run/application":
					applications++
				case "run/natural_idle":
					idle = true
				case "run/joined":
					joined = true
				}
			}
			if !joined || !idle || len(faultNS) != faults || len(openNS) != faults || applications != int(r.Result.AppliedSeq) || forwarded != wantResume+1 || hex.EncodeToString(sourceHash.Sum(nil)) != r.SourceSHA || hex.EncodeToString(workerHash.Sum(nil)) != r.WorkerSHA || uint64(len(rows)) != r.Events {
				t.Fatal("event history contradicts terminal summary")
			}
			for round, cut := range faultNS {
				if openNS[round]-cut < int64(cfg.Outage) {
					t.Fatal("opening occurred before configured duration")
				}
				if round <= wantResume {
					during := false
					for _, at := range reads {
						if at > cut && at < openNS[round] {
							during = true
						}
					}
					if !during {
						t.Fatal("no ongoing capture during outage")
					}
				}
			}
			t.Logf("case=%s faults=%d resume=%d/%d completed=%t captured=%d processed=%d events=%d queue_observed_peak=%d natural_idle=%t", cfg.Case, r.Faults, r.Result.ResumedConnections, r.Result.ResumeAttempts, r.Result.Completed, r.Captured, r.Processed, r.Events, r.QueuePeak, r.NaturalIdle)
		})
	}
}

func TestExtendedResumeEvidenceReader(t *testing.T) {
	first := extendedEvent{1, "run", 1, "configuration", 0, json.RawMessage(`{}`)}
	last := extendedEvent{1, "run", 2, "joined", 1, json.RawMessage(`{}`)}
	encode := func(rows ...extendedEvent) []byte {
		var data []byte
		for _, row := range rows {
			line, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, line...)
			data = append(data, '\n')
		}
		return data
	}
	valid := encode(first, last)
	if rows, err := decodeExtendedEvents(bytes.NewReader(valid)); err != nil || len(rows) != 2 {
		t.Fatal("valid evidence rejected")
	}
	for _, name := range []string{"truncated_json", "missing_newline", "missing_join", "gap", "duplicate", "unknown_component", "oversized", "invalid_version", "negative_time"} {
		t.Run(name, func(t *testing.T) {
			data := bytes.Clone(valid)
			row := last
			switch name {
			case "truncated_json":
				data = data[:len(data)-4]
			case "missing_newline":
				data = data[:len(data)-1]
			case "missing_join":
				data = encode(first)
			case "gap":
				row.Seq = 3
				data = encode(first, row)
			case "duplicate":
				row.Seq = 1
				data = encode(first, row)
			case "unknown_component":
				row.Component = "new-unbounded-component"
				data = encode(first, row)
			case "oversized":
				data = []byte(strings.Repeat("x", 8192) + "\n")
			case "invalid_version":
				row.Version = 2
				data = encode(first, row)
			case "negative_time":
				row.ElapsedNS = -1
				data = encode(first, row)
			}
			if _, err := decodeExtendedEvents(bytes.NewReader(data)); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}
