//go:build tide_load

package gateway

import (
	"context"
	"testing"
	"time"
)

// Positive controls distinguish a policy blind spot from a broken experiment
// timer. These backdated states validate cancellation wiring, not real latency.
func TestDeadlineExperimentWatchdogControls(t *testing.T) {
	cases := []struct {
		name, mode, reason string
		prepare            func(*deadlineTrace)
	}{
		{"queued_audio", "local", "queue_age", func(m *deadlineTrace) { m.arrivals = []time.Time{time.Now().Add(-4 * time.Second)} }},
		{"blocked_send", "local", "send_stall", func(m *deadlineTrace) { m.sendStarted = time.Now().Add(-4 * time.Second) }},
		{"unprocessed_audio", "progress", "unprocessed_age", func(m *deadlineTrace) { m.arrivals = []time.Time{time.Now().Add(-4 * time.Second)}; m.dequeued = 1 }},
		{"end_without_eof", "progress", "end_timeout", func(m *deadlineTrace) { m.ended = time.Now().Add(-6 * time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &deadlineTrace{mode: tc.mode}
			tc.prepare(m)
			deadlineObserver.Store(m)
			defer deadlineObserver.Store(nil)
			s := newSession(context.Background(), "experiment-control", nil)
			defer s.cancel(nil)
			stop := deadlineWatch(s)
			defer stop()
			select {
			case <-s.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("experimental watchdog failed to cancel")
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.reason != tc.reason {
				t.Fatalf("reason=%q, want %q", m.reason, tc.reason)
			}
			if context.Cause(s.ctx) == nil {
				t.Fatal("missing cancellation cause")
			}
		})
	}
}
