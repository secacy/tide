package wsheartbeat

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestNormalize(t *testing.T) {
	got, err := (Config{}).Normalize()
	if err != nil || got.Interval != 2*time.Second || got.Timeout != 3*time.Second || got.Disabled {
		t.Fatalf("defaults: %+v %v", got, err)
	}
	for _, cfg := range []Config{{Interval: -1}, {Timeout: -1, Disabled: true}} {
		if _, err := cfg.Normalize(); err == nil {
			t.Fatal("negative budget accepted")
		}
	}
	cfg := Config{Interval: 5 * time.Second, Timeout: time.Second, Disabled: true}
	if got, err := cfg.Normalize(); got != cfg || err != nil {
		t.Fatalf("override: %+v %v", got, err)
	}
}

// 虚拟时间验证单探测、完整预算及停止语义，不用这些时间作性能证据。
func TestMonitorTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls, failures atomic.Int32
		start := time.Now()
		m := Start(t.Context(), Config{Interval: 2 * time.Second, Timeout: 3 * time.Second}, func(ctx context.Context) error {
			calls.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}, func(err error) {
			if !errors.Is(err, ErrFailed) || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("failure = %v", err)
			}
			failures.Add(1)
		})
		if err := m.Wait(); !errors.Is(err, ErrFailed) {
			t.Fatal(err)
		}
		if calls.Load() != 1 || failures.Load() != 1 || time.Since(start) != 5*time.Second {
			t.Fatalf("calls=%d failures=%d elapsed=%v", calls.Load(), failures.Load(), time.Since(start))
		}
		m.Stop()
		m.Stop()
	})
}

func TestMonitorStopPreservesInflightPing(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan context.Context, 1), make(chan struct{})
			var calls, failures atomic.Int32
			m := Start(ctx, Config{Interval: time.Second, Timeout: 3 * time.Second}, func(probe context.Context) error {
				calls.Add(1)
				entered <- probe
				<-release
				return probe.Err()
			}, func(error) { failures.Add(1) })
			probe := <-entered
			if cancelParent {
				cancel()
			} else {
				m.Stop()
				m.Stop()
			}
			synctest.Wait()
			if probe.Err() != nil {
				t.Fatal("stopping canceled an active Ping")
			}
			close(release)
			if err := m.Wait(); err != nil || failures.Load() != 0 || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d failures=%d", err, calls.Load(), failures.Load())
			}
		})
	}
}

func TestMonitorDisabledAndStoppedBeforeFirstProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, disabled := range []bool{false, true} {
			m := Start(t.Context(), Config{Disabled: disabled, Interval: time.Second, Timeout: time.Second}, func(context.Context) error {
				t.Error("unexpected probe")
				return nil
			}, func(error) { t.Error("unexpected failure") })
			m.Stop()
			if err := m.Wait(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
