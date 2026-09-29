package audio_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
)

// TestPacerPlanUsesSuccessfulBytes 验证读取计划无副作用，按实际字节推进且保留时钟信息。
func TestPacerPlanUsesSuccessfulBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		origin := time.Now()
		p := audio.NewPacer()
		if p.NextSendAt() != origin {
			t.Fatal("first plan differs from construction time")
		}
		var elapsed time.Duration
		for _, step := range []struct {
			bytes    int
			duration time.Duration
		}{
			{3200, 100 * time.Millisecond}, {1600, 50 * time.Millisecond}, {2, 62500 * time.Nanosecond},
		} {
			p.Advance(step.bytes)
			elapsed += step.duration
			for range 3 {
				if p.NextSendAt() != origin.Add(elapsed) {
					t.Fatalf("plan = %v, want origin + %v", p.NextSendAt(), elapsed)
				}
			}
			time.Sleep(time.Second)
			if p.NextSendAt() != origin.Add(elapsed) {
				t.Fatal("plan drifted with wall time")
			}
		}
	})
}

// TestPacerWaitUsesExposedPlan 用虚拟时间核对等待与取消，不依赖真实调度误差。
func TestPacerWaitUsesExposedPlan(t *testing.T) {
	for _, mode := range []string{"future", "overdue", "canceled", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := audio.NewPacer()
				p.Advance(6400)
				target := p.NextSendAt()
				ctx := context.Background()
				var wantErr error
				wantWait := 200 * time.Millisecond
				switch mode {
				case "overdue":
					time.Sleep(300 * time.Millisecond)
					wantWait = 0
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					wantWait, wantErr = 0, context.Canceled
				case "deadline":
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
					defer cancel()
					wantWait, wantErr = 50*time.Millisecond, context.DeadlineExceeded
				}
				before := time.Now()
				if err := p.WaitBeforeSend(ctx); !errors.Is(err, wantErr) {
					t.Fatalf("wait = %v, want %v", err, wantErr)
				}
				if got := time.Since(before); got != wantWait {
					t.Fatalf("wait duration = %v, want %v", got, wantWait)
				}
				if p.NextSendAt() != target {
					t.Fatal("waiting changed plan")
				}
			})
		})
	}
}
