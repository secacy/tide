//go:build tide_recovery

package wsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

// EXP-014 tests the implemented recovery path. Short budgets accelerate failure
// semantics; these samples are neither an ASR benchmark nor default-budget SLOs.
func TestExperimentProductionRecovery(t *testing.T) {
	if os.Getenv("TIDE_RUN_PRODUCTION_RECOVERY") != "1" {
		t.Skip("set TIDE_RUN_PRODUCTION_RECOVERY=1")
	}
	for _, test := range []struct {
		name, mode     string
		blocks         int
		realtime       bool
		buffer, budget time.Duration
		complete       bool
		wantErr        error
	}{
		{"normal", "normal", 10, false, 0, time.Second, true, nil},
		{"worker_before_checkpoint", "fail_before_checkpoint", 10, false, 0, time.Second, true, nil},
		{"worker_after_checkpoint", "fail_after_checkpoint", 10, false, 0, time.Second, true, nil},
		{"websocket_checkpoint_lost", "normal", 10, false, 0, time.Second, true, nil},
		{"end_closure_lost", "fail_after_end", 10, false, 0, time.Second, true, nil},
		{"buffer_gap_continue", "no_checkpoint_first", 35, true, 40 * time.Millisecond, time.Second, false, ErrIncomplete},
		{"end_tail_budget", "persistent_tail", 5, false, 0, 150 * time.Millisecond, false, ErrRecoveryBudget},
		{"fallback_budget", "never_checkpoint", 1000, true, 40 * time.Millisecond, 180 * time.Millisecond, false, ErrRecoveryBudget},
		{"unsupported_worker", "unsupported", 5, false, 0, time.Second, false, nil},
		{"invalid_checkpoint", "invalid", 5, false, 0, time.Second, false, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := &recoveryFaultWorker{mode: test.mode}
			c, g := recoveryHarnessWithGateway(t, w, RecoveryConfig{BufferDuration: test.buffer, Budget: test.budget, RetryDelay: 10 * time.Millisecond}, test.realtime)
			if test.name == "websocket_checkpoint_lost" {
				c.cfg.URL, _ = checkpointLossRelay(t, c.cfg.URL)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			started := time.Now()
			r, err := c.RunRecoverable(ctx, ReaderSource{bytes.NewReader(testPCM(test.blocks))})
			elapsed := time.Since(started)
			if r.Complete != test.complete || (test.complete && err != nil) || (!test.complete && err == nil) || (test.wantErr != nil && !errors.Is(err, test.wantErr)) {
				t.Fatalf("report=%+v err=%v", r, err)
			}
			requireCoverage(t, r)
			if r.CachePeakBytes > int(c.cfg.Recovery.BufferDuration*32000/time.Second) {
				t.Fatal("PCM budget exceeded")
			}
			g.StopAccepting()
			if err := g.Wait(ctx); err != nil {
				t.Fatal("Gateway failed to drain:", err)
			}
			for w.active.Load() != 0 && ctx.Err() == nil {
				time.Sleep(time.Millisecond)
			}
			if w.active.Load() != 0 {
				t.Fatal("Worker RPC remains active")
			}
			w.mu.Lock()
			starts := make([]uint64, 0, len(w.starts))
			for _, s := range w.starts {
				starts = append(starts, s.FromSample)
			}
			w.mu.Unlock()
			if test.mode == "persistent_tail" {
				for _, s := range starts {
					if s != 0 {
						t.Fatal("End skipped retained tail")
					}
				}
			}
			if test.name == "websocket_checkpoint_lost" && (len(starts) < 2 || starts[1] != 160) {
				t.Fatalf("wrong client replay point: %v", starts)
			}
			errText := ""
			if err != nil {
				errText = err.Error()
			}
			sample := map[string]any{"experiment": "EXP-014", "case": test.name, "complete": r.Complete, "ended": r.Ended, "attempts": r.Attempts, "captured_through": r.CapturedThrough, "segments": r.Segments, "gaps": r.Gaps, "cache_peak_bytes": r.CachePeakBytes, "cache_limit_bytes": int(c.cfg.Recovery.BufferDuration * 32000 / time.Second), "budget_ms": test.budget.Milliseconds(), "run_elapsed_ms": float64(elapsed.Microseconds()) / 1000, "retry_from_samples": starts, "worker_active_after_cleanup": w.active.Load(), "gateway_drained": true, "error": errText, "quality_pass": true}
			b, err := json.Marshal(sample)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("RECOVERY_SAMPLE " + string(b))
		})
	}
}
