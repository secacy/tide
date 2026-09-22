package gateway

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestTailWaitStartsOnlyAfterEnd 使用虚拟时间跨过长时间问诊，再验证完整尾部预算。
// 不用墙上时间的容差判断，防止调度噪声掩盖提前计时或重复计时。
func TestTailWaitStartsOnlyAfterEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events := make(chan sessionResult, 1)
		ended := make(chan struct{})
		done := make(chan sessionResult, 1)
		const budget = 2 * time.Second
		go func() { done <- waitSessionEvent(ctx, events, ended, budget) }()
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()
		assertTailStillWaiting(t, done)

		close(ended)
		synctest.Wait() // 确保通知已被处理且计时器已启动，再推进虚拟时间。
		assertTailStillWaiting(t, done)
		time.Sleep(budget - time.Nanosecond)
		synctest.Wait()
		assertTailStillWaiting(t, done)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		got := readTailResult(t, done)
		if got.kind != resultTailTimeout || !errors.Is(got.err, ErrTailTimeout) {
			t.Fatalf("tail result: %+v", got)
		}
	})
}

// TestTailWaitForwardsExitEvents 验证 end 前后都能立即返回退出事件。
// 该辅助函数不负责判断 completed 是否有合法 end；此责任仍属于 waitSessionResult。
func TestTailWaitForwardsExitEvents(t *testing.T) {
	workerErr := errors.New("worker failed for test")
	clientErr := errors.New("client disconnected for test")
	for _, afterEnd := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			want sessionResult
		}{
			{"completed", sessionResult{kind: resultCompleted}},
			{"worker_failed", sessionResult{kind: resultWorkerFailed, err: workerErr}},
			{"client_disconnected", sessionResult{kind: resultClientDisconnected, err: clientErr}},
		} {
			phase := "before_end"
			if afterEnd {
				phase = "after_end"
			}
			t.Run(phase+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					events := make(chan sessionResult, 1)
					ended := make(chan struct{})
					done := make(chan sessionResult, 1)
					go func() { done <- waitSessionEvent(ctx, events, ended, time.Hour) }()
					synctest.Wait()
					if afterEnd {
						close(ended)
						synctest.Wait()
					}
					events <- tc.want
					synctest.Wait()
					got := readTailResult(t, done)
					if got.kind != tc.want.kind || got.err != tc.want.err {
						t.Fatalf("got %+v, want %+v", got, tc.want)
					}
				})
			})
		}
	}
}

// TestTailWaitServiceCancellation 验证服务取消在 end 前后都能结束等待。
// 不把同时就绪的多个事件谁先被 select 选中写成确定性要求。
func TestTailWaitServiceCancellation(t *testing.T) {
	for _, afterEnd := range []bool{false, true} {
		name := "before_end"
		if afterEnd {
			name = "after_end"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ended := make(chan struct{})
				done := make(chan sessionResult, 1)
				go func() { done <- waitSessionEvent(ctx, make(chan sessionResult), ended, time.Hour) }()
				synctest.Wait()
				if afterEnd {
					close(ended)
					synctest.Wait()
				}
				cancel()
				synctest.Wait()
				got := readTailResult(t, done)
				if got.kind != resultServerStopping || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("cancel result: %+v", got)
				}
			})
		})
	}
}

// TestTailWaitAlreadyEnded 验证先发生 end、后开始等待时也只启动一次预算。
func TestTailWaitAlreadyEnded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ended := make(chan struct{})
		close(ended)
		start := time.Now()
		got := waitSessionEvent(ctx, make(chan sessionResult), ended, time.Second)
		if got.kind != resultTailTimeout || !errors.Is(got.err, ErrTailTimeout) || time.Since(start) != time.Second {
			t.Fatalf("result=%+v elapsed=%v", got, time.Since(start))
		}
	})
}

func assertTailStillWaiting(t *testing.T, done <-chan sessionResult) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("returned before expected event: %+v", got)
	default:
	}
}

// readTailResult 必须在 synctest.Wait 后使用，确保待测 goroutine 已有机会处理事件。
func readTailResult(t *testing.T, done <-chan sessionResult) sessionResult {
	t.Helper()
	select {
	case got := <-done:
		return got
	default:
		t.Fatal("waiter did not return after event")
		return sessionResult{}
	}
}

// TestTailSessionResult 验证接入协调者后的超时分类和正常完成合法性。
func TestTailSessionResult(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ended := make(chan struct{})
			close(ended)
			s := &session{tailTimeout: time.Second}
			got := s.waitSessionResult(context.Background(), context.Background(), make(chan sessionResult), ended)
			if got.kind != resultTailTimeout || !errors.Is(got.err, ErrTailTimeout) {
				t.Fatalf("timeout classification: %+v", got)
			}
		})
	})
	for _, ended := range []bool{false, true} {
		name := "worker_completed_before_end"
		if ended {
			name = "worker_completed_after_end"
		}
		t.Run(name, func(t *testing.T) {
			inputEnded := make(chan struct{})
			if ended {
				close(inputEnded)
			}
			events := make(chan sessionResult, 1)
			events <- sessionResult{kind: resultCompleted}
			s := &session{tailTimeout: time.Hour}
			got := s.waitSessionResult(context.Background(), context.Background(), events, inputEnded)
			if ended {
				if got.kind != resultCompleted || got.err != nil {
					t.Fatalf("legal completion: %+v", got)
				}
			} else if got.kind != resultWorkerFailed || got.err == nil {
				t.Fatalf("premature completion accepted: %+v", got)
			}
		})
	}
}

// TestTailEventKeepsExistingPriorities 给定确定的尾部超时事件，验证已有原因不会被覆盖。
func TestTailEventKeepsExistingPriorities(t *testing.T) {
	for _, tc := range []struct {
		name             string
		stop, send, idle bool
		want             sessionResultKind
		wantErr          error
	}{
		{"service_stop", true, true, true, resultServerStopping, context.Canceled},
		{"send_timeout", false, true, true, resultWorkerSendTimeout, ErrWorkerSendTimeout},
		{"input_timeout", false, false, true, resultInputIdleTimeout, ErrInputIdleTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rpcCtx, cancelRPC := context.WithCancelCause(ctx)
			defer cancelRPC(nil)
			if tc.send {
				cancelRPC(ErrWorkerSendTimeout)
			}
			if tc.stop {
				cancel()
			}
			s := &session{tailTimeout: time.Second}
			if tc.idle {
				readCtx, stopRead := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stopRead()
				s.inputReadCtx = readCtx
			}
			events := make(chan sessionResult, 1)
			events <- sessionResult{kind: resultTailTimeout, err: ErrTailTimeout}
			got := s.waitSessionResult(ctx, rpcCtx, events, make(chan struct{}))
			if got.kind != tc.want || !errors.Is(got.err, tc.wantErr) {
				t.Fatalf("got %+v want kind=%v err=%v", got, tc.want, tc.wantErr)
			}
		})
	}
}
