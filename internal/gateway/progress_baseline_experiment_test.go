package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/mockasr"
)

// progressSample 保存 snapshot 调用前后时刻；实际取值发生在这段时间内。
// Pending 是未确认处理量，既不是物理队列大小，也不是实际等待时长。
type progressSample struct {
	BeginMS   float64 `json:"begin_ms"`
	AtMS      float64 `json:"at_ms"`
	Received  uint64  `json:"received_bytes"`
	Processed uint64  `json:"processed_bytes"`
	Pending   uint64  `json:"pending_bytes"`
}

type progressBaselineReport struct {
	Case                     string           `json:"case"`
	Outcome                  string           `json:"outcome"`
	Error                    string           `json:"error,omitempty"`
	Chunks                   int              `json:"chunks"`
	ProcessingMS             float64          `json:"processing_ms"`
	PauseMS                  float64          `json:"pause_ms"`
	SamplePeriodMS           float64          `json:"sample_period_ms"`
	ObservedPeakPendingBytes uint64           `json:"observed_peak_pending_bytes"`
	ObservedPeakAudioMS      float64          `json:"observed_peak_audio_ms"`
	PeakAtMS                 float64          `json:"peak_at_ms"`
	MaxSampleGapMS           float64          `json:"max_sample_gap_ms"`
	MaxSnapshotCallMS        float64          `json:"max_snapshot_call_ms"`
	EndStartedMS             *float64         `json:"end_started_ms"`
	EndReturnedMS            *float64         `json:"end_returned_ms"`
	AtEndWriteReturn         *progressSample  `json:"at_end_write_return"`
	FinalMS                  *float64         `json:"final_ms"`
	TailMS                   *float64         `json:"tail_ms"`
	CloseMS                  *float64         `json:"close_ms"`
	SessionReturnedMS        *float64         `json:"session_returned_ms"`
	PauseBoundaryMS          *float64         `json:"pause_boundary_ms"`
	ResumeMS                 *float64         `json:"resume_ms"`
	FirstLowAfterResumeMS    *float64         `json:"first_low_after_resume_ms"`
	FirstLowDelayMS          *float64         `json:"first_low_delay_ms"`
	MaxSendMS                float64          `json:"max_send_ms"`
	MaxWriteMS               float64          `json:"max_write_ms"`
	MaxInputScheduleLagMS    float64          `json:"max_input_schedule_lag_ms"`
	CloseCode                int              `json:"close_code"`
	WorkerChunks             int              `json:"worker_chunks"`
	WorkerBytes              int              `json:"worker_bytes"`
	FinalProgress            *progressSample  `json:"final_progress"`
	ActiveAfterCleanup       *int             `json:"active_after_cleanup"`
	Writes                   []baselineWrite  `json:"writes"`
	Sends                    []baselineWrite  `json:"sends"`
	Partials                 []pausePartial   `json:"partials"`
	Samples                  []progressSample `json:"samples"`
}

// takeProgressSample 的耗时也被记录，便于评估采样锁和调度影响。
func takeProgressSample(s *session, origin time.Time) progressSample {
	begin := time.Now()
	p := s.progress.snapshot()
	return progressSample{BeginMS: baselineMS(begin.Sub(origin)), AtMS: baselineMS(time.Since(origin)),
		Received: p.receivedBytes, Processed: p.processedBytes, Pending: p.pendingBytes}
}

// TestProgressBacklogProbe 用短输入验证实验夹具，日常回归无需运行完整负载。
func TestProgressBacklogProbe(t *testing.T) {
	runProgressBacklogBaseline(t, "probe_only", time.Millisecond, 0, 6)
}

// TestProgressBacklogBaselineExperiment 是限流改造前的基线，正式数据不启用 race。
// 开启后一次执行三个单会话场景，-count=3 获得每个场景三次记录。
func TestProgressBacklogBaselineExperiment(t *testing.T) {
	if os.Getenv("TIDE_RUN_PROGRESS_BACKLOG_EXPERIMENT") != "1" {
		t.Skip("set TIDE_RUN_PROGRESS_BACKLOG_EXPERIMENT=1")
	}
	t.Logf("environment go=%s os=%s arch=%s cpu=%d GOMAXPROCS=%d transport=loopback-TCP same-process handler=instrumented-session-wrapper", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0))
	for _, tc := range []struct {
		name         string
		delay, pause time.Duration
	}{
		{"normal", 50 * time.Millisecond, 0},
		{"pause_500ms", 50 * time.Millisecond, 500 * time.Millisecond},
		{"slow_200ms", 200 * time.Millisecond, 0},
	} {
		t.Run(tc.name, func(t *testing.T) { runProgressBacklogBaseline(t, tc.name, tc.delay, tc.pause, 100) })
	}
}

// runProgressBacklogBaseline 复用生产 session.run 与 tracker，接入壳额外暴露 session
// 给采样者，并保留 run 返回值；不直接调用 Gateway.ServeHTTP，不用于容量或准入结论。
func runProgressBacklogBaseline(t *testing.T, name string, delay, pause time.Duration, chunks int) {
	t.Helper()
	const samplePeriod = 5 * time.Millisecond
	report := progressBaselineReport{Case: name, Outcome: "incomplete", Chunks: chunks,
		ProcessingMS: baselineMS(delay), PauseMS: baselineMS(pause), SamplePeriodMS: baselineMS(samplePeriod)}
	defer func() {
		data, err := json.Marshal(report)
		if err != nil {
			t.Errorf("encode progress baseline: %v", err)
			return
		}
		t.Logf("PROGRESS_BASELINE_JSON %s", data)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	fail := func(err error) {
		report.Outcome, report.Error = "failed", err.Error()
		if ctx.Err() != nil {
			report.Outcome = "experiment_timeout"
		}
		t.Fatal(err)
	}
	texts := make([]string, chunks/5)
	for i := range texts {
		texts[i] = fmt.Sprintf("p%d", i+1)
	}
	worker := &pauseWorker{worker: mockasr.New(mockasr.Config{
		ProcessingDelay: delay, PartialEvery: 500 * time.Millisecond, PartialTexts: texts, FinalText: "final",
		PauseAfterChunks: 20, PauseDuration: pause,
	}), done: make(chan pauseWorkerExit, 1)}
	probe := &pauseSendProbe{}
	workerClient := &pauseClient{ASRServiceClient: newBaselineTCPWorkerClient(t, worker), probe: probe}
	appCtx, stop := context.WithCancel(context.Background())
	defer stop()
	g, err := New(appCtx, workerClient, Config{MaxSessions: 1})
	if err != nil {
		fail(err)
	}
	ready := make(chan *session, 1)
	type sessionExit struct {
		at  time.Time
		err error
	}
	runExit := make(chan sessionExit, 1)
	handlerDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		if err := g.tracker.tryEnter(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer g.tracker.leave()
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(g.cfg.MaxMessageBytes)
		s := newSession(ws, g.worker, g.cfg.StartTimeout, g.cfg.InputIdleTimeout, g.cfg.WorkerSendTimeout, g.cfg.TailTimeout, g.cfg.ResultWriteTimeout)
		ready <- s
		err = s.run(g.ctx)
		runExit <- sessionExit{time.Now(), err}
	}))
	var client *websocket.Conn
	defer func() {
		g.StopAccepting()
		stop()
		if client != nil {
			_ = client.CloseNow()
		}
		server.Close()
		waitCtx, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
		if err := g.Wait(waitCtx); err != nil {
			t.Errorf("experiment cleanup: %v", err)
		}
	}()
	client, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		fail(err)
	}
	var s *session
	select {
	case s = <-ready:
	case <-handlerDone:
		fail(fmt.Errorf("handler exited before initialization"))
	case <-ctx.Done():
		fail(ctx.Err())
	}
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		fail(err)
	}
	origin := time.Now()
	probe.mu.Lock()
	probe.origin = origin
	probe.mu.Unlock()
	ms := func(at time.Time) *float64 {
		if at.IsZero() {
			return nil
		}
		v := baselineMS(at.Sub(origin))
		return &v
	}
	// 两个观察者独立收集记录，通过 channel 交付，避免测量自身的数据竞争。
	received := make(chan pauseReception, 1)
	go func() { received <- receivePause(ctx, client, origin) }()
	sampleCtx, stopSampling := context.WithCancel(context.Background())
	sampled := make(chan []progressSample, 1)
	go func() {
		ticker := time.NewTicker(samplePeriod)
		defer ticker.Stop()
		rows := make([]progressSample, 0, 4500)
		rows = append(rows, takeProgressSample(s, origin))
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
	var reception pauseReception
	joinedReceiver, joinedSampler := false, false
	defer func() {
		stopSampling()
		cancel()
		if !joinedSampler {
			select {
			case report.Samples = <-sampled:
			case <-time.After(5 * time.Second):
				t.Error("sampler did not exit")
			}
		}
		if !joinedReceiver {
			select {
			case reception = <-received:
			case <-time.After(5 * time.Second):
				t.Error("receiver did not exit")
			}
		}
		report.Partials, report.CloseCode = reception.rows, reception.closeCode
		probe.mu.Lock()
		report.Sends = append([]baselineWrite(nil), probe.rows...)
		probe.mu.Unlock()
	}()
	for i := 0; i < chunks; i++ {
		planned := origin.Add(time.Duration(i) * 100 * time.Millisecond)
		if d := time.Until(planned); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				fail(ctx.Err())
			}
		}
		began := time.Now()
		err := client.Write(ctx, websocket.MessageBinary, bytes.Repeat([]byte{byte(i)}, audio.ChunkBytesDefault))
		row := baselineWrite{Index: i, PlannedMS: float64(i) * 100, StartedMS: baselineMS(began.Sub(origin)), ReturnedMS: baselineMS(time.Since(origin))}
		if err != nil {
			row.Error = err.Error()
		}
		report.Writes = append(report.Writes, row)
		report.MaxWriteMS = max(report.MaxWriteMS, row.ReturnedMS-row.StartedMS)
		report.MaxInputScheduleLagMS = max(report.MaxInputScheduleLagMS, row.StartedMS-row.PlannedMS)
		if err != nil {
			fail(err)
		}
	}
	report.EndStartedMS = ms(time.Now())
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
		fail(err)
	}
	report.EndReturnedMS = ms(time.Now())
	atEnd := takeProgressSample(s, origin)
	report.AtEndWriteReturn = &atEnd // 不是 Gateway 已处理 end 的通知。
	select {
	case reception = <-received:
		joinedReceiver = true
	case <-ctx.Done():
		fail(ctx.Err())
	}
	report.FinalMS, report.CloseMS = ms(reception.final), ms(reception.closed)
	if report.FinalMS != nil {
		tail := *report.FinalMS - *report.EndStartedMS
		report.TailMS = &tail
	}
	if reception.err != nil {
		fail(reception.err)
	}
	if reception.finals != 1 || len(reception.rows) != chunks/5 {
		fail(fmt.Errorf("finals=%d partials=%d", reception.finals, len(reception.rows)))
	}
	select {
	case exit := <-runExit:
		report.SessionReturnedMS = ms(exit.at)
		if exit.err != nil {
			fail(exit.err)
		}
	case <-ctx.Done():
		fail(ctx.Err())
	}
	stopSampling()
	select {
	case report.Samples = <-sampled:
		joinedSampler = true
	case <-ctx.Done():
		fail(ctx.Err())
	}
	select {
	case exit := <-worker.done:
		report.WorkerChunks, report.WorkerBytes = exit.chunks, exit.bytes
		if pause > 0 {
			report.PauseBoundaryMS, report.ResumeMS = ms(exit.boundary), ms(exit.resume)
		}
		if exit.err != nil {
			fail(exit.err)
		}
		if exit.chunks != chunks || exit.bytes != chunks*audio.ChunkBytesDefault {
			fail(fmt.Errorf("worker received chunks=%d bytes=%d", exit.chunks, exit.bytes))
		}
	case <-ctx.Done():
		fail(ctx.Err())
	}
	for i, row := range report.Samples {
		if row.Processed > row.Received || row.Pending != row.Received-row.Processed {
			fail(fmt.Errorf("invalid sampled counters: %+v", row))
		}
		if i > 0 {
			previous := report.Samples[i-1]
			if row.Received < previous.Received || row.Processed < previous.Processed {
				fail(fmt.Errorf("sampled counters regressed"))
			}
			report.MaxSampleGapMS = max(report.MaxSampleGapMS, row.AtMS-previous.AtMS)
		}
		report.MaxSnapshotCallMS = max(report.MaxSnapshotCallMS, row.AtMS-row.BeginMS)
		if row.Pending > report.ObservedPeakPendingBytes {
			report.ObservedPeakPendingBytes, report.PeakAtMS = row.Pending, row.AtMS
		}
		if report.ResumeMS != nil && row.BeginMS >= *report.ResumeMS && row.Pending <= audio.ChunkBytesDefault && report.FirstLowAfterResumeMS == nil {
			at, elapsed := row.AtMS, row.AtMS-*report.ResumeMS
			report.FirstLowAfterResumeMS, report.FirstLowDelayMS = &at, &elapsed
		}
	}
	report.ObservedPeakAudioMS = float64(report.ObservedPeakPendingBytes) * 1000 / float64(audio.BytesPerSecond)
	probe.mu.Lock()
	sendRows := append([]baselineWrite(nil), probe.rows...)
	probe.mu.Unlock()
	if len(sendRows) != chunks {
		fail(fmt.Errorf("Gateway sends=%d, want %d", len(sendRows), chunks))
	}
	for _, row := range sendRows {
		if row.Error != "" {
			fail(fmt.Errorf("Gateway Send failed: %s", row.Error))
		}
		report.MaxSendMS = max(report.MaxSendMS, row.ReturnedMS-row.StartedMS)
	}
	final := takeProgressSample(s, origin)
	report.FinalProgress = &final
	if final.Received != uint64(chunks*audio.ChunkBytesDefault) || final.Processed != final.Received || final.Pending != 0 {
		fail(fmt.Errorf("unexpected final counters: %+v", final))
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		fail(err)
	}
	select {
	case <-handlerDone:
	case <-ctx.Done():
		fail(ctx.Err())
	}
	g.tracker.mu.Lock()
	active := g.tracker.active
	g.tracker.mu.Unlock()
	report.ActiveAfterCleanup = &active
	if active != 0 || appCtx.Err() != nil {
		fail(fmt.Errorf("cleanup depended on service cancellation or left active sessions"))
	}
	report.Outcome = "completed"
}
