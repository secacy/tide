//go:build tide_recovery

package wsclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// catchupEvent uses one monotonic epoch. A source overlay observes the actual
// coordinator's transitions; it does not substitute a second recovery policy.
type catchupEvent struct {
	Kind       string  `json:"kind"`
	MS         float64 `json:"ms"`
	Attempt    string  `json:"attempt,omitempty"`
	Checkpoint uint64  `json:"checkpoint"`
	Captured   uint64  `json:"captured"`
	DeadlineMS float64 `json:"deadline_ms,omitempty"`
	Detail     string  `json:"detail,omitempty"`
}
type catchupKey struct{}
type catchupTrace struct {
	mu     sync.Mutex
	epoch  time.Time
	events []catchupEvent
}

var catchupOverlayEnabled bool

func catchupRecord(ctx context.Context, kind, attempt string, checkpoint, captured uint64, deadline time.Time, detail string) {
	trace, ok := ctx.Value(catchupKey{}).(*catchupTrace)
	if !ok {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	e := catchupEvent{Kind: kind, MS: float64(time.Since(trace.epoch)) / float64(time.Millisecond), Attempt: attempt, Checkpoint: checkpoint, Captured: captured, Detail: detail}
	if !deadline.IsZero() {
		e.DeadlineMS = float64(deadline.Sub(trace.epoch)) / float64(time.Millisecond)
	}
	trace.events = append(trace.events, e)
}

// catchupWorker has independent restartable synthetic segments and a fixed
// wall-clock service time per 100 ms audio block. It does not model ASR quality.
type catchupWorker struct {
	asrv1.UnimplementedASRServiceServer
	ctx              context.Context
	delay            time.Duration
	fault            bool
	attempt          atomic.Int32
	active           atomic.Int32
	unavailableUntil atomic.Int64
}

func (w *catchupWorker) RecoverableRecognize(s asrv1.ASRService_RecoverableRecognizeServer) error {
	nth := w.attempt.Add(1)
	w.active.Add(1)
	defer w.active.Add(-1)
	first, err := s.Recv()
	if err != nil {
		return err
	}
	start := first.RecoveryStart
	if start == nil {
		return status.Error(codes.InvalidArgument, "missing Start")
	}
	catchupRecord(w.ctx, "worker_open", start.AttemptId, start.FromSample, 0, time.Time{}, "")
	defer catchupRecord(w.ctx, "worker_exit", start.AttemptId, 0, 0, time.Time{}, "")
	if err := s.Send(&asrv1.StreamingRecognizeResponse{Ready: true}); err != nil {
		return err
	}
	from, next := start.FromSample, start.FromSample
	var seq uint64
	emit := func() error {
		if from == next {
			return nil
		}
		err := s.Send(&asrv1.StreamingRecognizeResponse{Checkpoint: &asrv1.RecoveryCheckpoint{FromSample: from, ThroughSample: next, Text: fmt.Sprintf("[%d,%d)", from, next)}})
		from = next
		return err
	}
	for {
		req, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return emit()
		}
		if err != nil {
			return err
		}
		if req.StartSample != next || req.AudioSeq != seq+1 || len(req.Data) != 3200 {
			return status.Error(codes.InvalidArgument, "invalid replay chunk")
		}
		for i := 0; i < len(req.Data); i += 2 {
			if binary.LittleEndian.Uint16(req.Data[i:]) != uint16(next+uint64(i/2)) {
				return status.Error(codes.InvalidArgument, "changed audio")
			}
		}
		delay := w.delay
		if nth == 1 && w.fault {
			delay = 20 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-s.Context().Done():
			timer.Stop()
			return s.Context().Err()
		}
		next += 1600
		seq++
		if nth == 1 && w.fault && seq == 20 {
			w.unavailableUntil.Store(time.Now().Add(2 * time.Second).UnixNano())
			catchupRecord(w.ctx, "fault", start.AttemptId, from, next, time.Time{}, "Worker unavailable; new admission unavailable for 2s")
			return status.Error(codes.Unavailable, "injected failure before checkpoint")
		}
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: seq}}); err != nil {
			return err
		}
		if next-from >= 8000 {
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

func TestExperimentRecoveryCatchup(t *testing.T) {
	if os.Getenv("TIDE_RUN_CATCHUP") != "1" {
		t.Skip("set TIDE_RUN_CATCHUP=1")
	}
	if !catchupOverlayEnabled {
		t.Fatal("run with observation overlay via experiment script")
	}
	for _, tc := range []struct {
		name  string
		fault bool
		delay time.Duration
	}{
		{"healthy_40ms", false, 40 * time.Millisecond},
		{"recover_40ms", true, 40 * time.Millisecond},
		{"recover_80ms", true, 80 * time.Millisecond},
		{"recover_100ms", true, 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) { runCatchupCase(t, tc.name, tc.fault, tc.delay) })
	}
}
func runCatchupCase(t *testing.T, name string, fault bool, delay time.Duration) {
	trace := &catchupTrace{epoch: time.Now()}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), catchupKey{}, trace), 23*time.Second)
	defer cancel()
	worker := &catchupWorker{ctx: ctx, delay: delay, fault: fault}
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	asrv1.RegisterASRServiceServer(server, worker)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	defer lis.Close()
	cc, err := grpc.NewClient("passthrough:///catchup", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	var logs bytes.Buffer
	g, err := gateway.New(ctx, asrv1.NewASRServiceClient(cc), slog.New(slog.NewJSONHandler(&logs, nil)), gateway.Config{MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	var rejected atomic.Int32
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if time.Now().UnixNano() < worker.unavailableUntil.Load() {
			rejected.Add(1)
			http.Error(w, "injected admission outage", 503)
			return
		}
		g.ServeHTTP(w, r)
	}))
	defer hs.Close()
	// Teardown cancels every transport before waiting even if an assertion fails.
	defer func() {
		cancel()
		g.StopAccepting()
		g.Abort()
		waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_ = g.Wait(waitCtx)
	}()
	var maxLag uint64
	c, err := New(Config{URL: "ws" + strings.TrimPrefix(hs.URL, "http"), ChunkBytes: 3200, Realtime: true, Recovery: RecoveryConfig{Observe: func(cp wsprotocol.RecoveryMessage) {
		catchupRecord(ctx, "checkpoint", cp.AttemptID, cp.ThroughSample, 0, time.Time{}, "")
	}}})
	if err != nil {
		t.Fatal(err)
	}
	// 16s input remains active beyond the first fault + 10s recovery budget.
	r, runErr := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(1600))})
	catchupRecord(ctx, "returned", "", 0, r.CapturedThrough, time.Time{}, fmt.Sprint(runErr))
	g.StopAccepting()
	waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := g.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	for worker.active.Load() != 0 && waitCtx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if worker.active.Load() != 0 {
		t.Fatal("Worker not released")
	}
	requireCoverage(t, r)
	if r.CachePeakBytes > 480000 {
		t.Fatal("PCM limit exceeded")
	}
	if errors.Is(runErr, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatalf("outer watchdog fired: %v", runErr)
	}
	if !fault && (!r.Complete || runErr != nil) {
		t.Fatalf("healthy control failed: %v", runErr)
	}
	if runErr != nil && !errors.Is(runErr, ErrRecoveryBudget) && !errors.Is(runErr, ErrIncomplete) {
		t.Fatalf("unexpected terminal error: %v", runErr)
	}
	trace.mu.Lock()
	events := append([]catchupEvent(nil), trace.events...)
	trace.mu.Unlock()
	var recoveryStart, deadline, caughtUp float64
	for _, e := range events {
		if e.Kind == "recovery_start" {
			if recoveryStart != 0 {
				t.Fatal("unexpected renewed recovery episode")
			}
			recoveryStart = e.MS
			deadline = e.DeadlineMS
		}
		if e.Kind == "caught_up" && caughtUp == 0 {
			caughtUp = e.MS
		}
		if e.Kind == "applied" {
			if e.Captured < e.Checkpoint {
				t.Fatal("invalid observed frontier")
			}
			maxLag = max(maxLag, e.Captured-e.Checkpoint)
		}
	}
	if fault && recoveryStart == 0 {
		t.Fatal("missing recovery observation")
	}
	if errors.Is(runErr, ErrRecoveryBudget) && caughtUp != 0 {
		t.Fatal("budget exhausted after observed recovery")
	}
	if caughtUp > deadline && deadline != 0 {
		t.Fatal("late checkpoint renewed budget")
	}
	if fault && r.Complete && caughtUp == 0 {
		t.Fatal("completed without observing live catchup")
	}
	errText := ""
	if runErr != nil {
		errText = runErr.Error()
	}
	sample := map[string]any{"experiment": "EXP-015", "case": name, "worker_block_ms": delay.Milliseconds(), "audio_block_ms": 100, "checkpoint_audio_ms": 500, "input_ms": 16000, "admission_outage_ms": 2000, "buffer_limit_bytes": 480000, "recovery_budget_ms": 10000, "heartbeat_interval_ms": 2000, "heartbeat_timeout_ms": 3000, "complete": r.Complete, "ended": r.Ended, "error": errText, "attempts": r.Attempts, "rejected": rejected.Load(), "captured_through": r.CapturedThrough, "cache_peak_bytes": r.CachePeakBytes, "max_applied_lag_ms": float64(maxLag) / 16, "events": events, "segments": r.Segments, "gaps": r.Gaps, "gateway_logs": logs.String(), "gateway_drained": true, "worker_active_after_cleanup": worker.active.Load(), "quality_pass": true}
	data, err := json.Marshal(sample)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("CATCHUP_SAMPLE " + string(data))
}
