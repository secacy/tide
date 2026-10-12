package gateway

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// 可控剩余值模拟两种时钟的差异；推进虚拟时间，不修改系统时钟。
func TestGroupResumeReleaseClock(t *testing.T) {
	for _, mode := range []string{"normal", "wall_lags_after_wakeup", "forward_wall_keeps_monotonic_floor", "repeated_wall_retreat", "cancel", "deadline_bounds_retreat"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "deadline_bounds_retreat" {
					ctx, cancel = context.WithTimeout(ctx, 3*time.Second)
					defer cancel()
				}
				if mode == "cancel" {
					go func() { time.Sleep(time.Second); cancel() }()
				}
				began := time.Now()
				calls := 0
				err := waitGroupRelease(ctx, func() time.Duration {
					calls++
					elapsed := time.Since(began)
					mono := 2*time.Second - elapsed
					wall := mono
					switch mode {
					case "wall_lags_after_wakeup":
						if elapsed >= 2*time.Second {
							wall += 4 * time.Millisecond
						}
					case "forward_wall_keeps_monotonic_floor":
						wall -= time.Second
					case "repeated_wall_retreat":
						if calls == 2 {
							wall = 4 * time.Millisecond
						} else if calls == 3 {
							wall = 2 * time.Millisecond
						}
					case "deadline_bounds_retreat":
						wall = time.Second
					}
					return max(mono, wall)
				})
				want, wantErr := 2*time.Second, error(nil)
				switch mode {
				case "wall_lags_after_wakeup":
					want += 4 * time.Millisecond
				case "repeated_wall_retreat":
					want += 6 * time.Millisecond
				case "cancel":
					want, wantErr = time.Second, context.Canceled
				case "deadline_bounds_retreat":
					want, wantErr = 3*time.Second, context.DeadlineExceeded
				}
				if !errors.Is(err, wantErr) || time.Since(began) != want {
					t.Fatalf("elapsed=%v error=%v want=%v/%v calls=%d", time.Since(began), err, want, wantErr, calls)
				}
			})
		})
	}
}
