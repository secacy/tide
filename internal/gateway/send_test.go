package gateway

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// sendTestStream 将发送行为交给测试控制；辅助函数不应操作接收或半关闭方向。
type sendTestStream struct {
	send func(*asrv1.StreamingRecognizeRequest) error
}

func (s *sendTestStream) Send(req *asrv1.StreamingRecognizeRequest) error { return s.send(req) }
func (*sendTestStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	panic("sendWithTimeout must not receive")
}
func (*sendTestStream) CloseSend() error { panic("sendWithTimeout must not half-close") }

// TestSendWithTimeoutPreservesResult 验证正常发送原样保留结果，并停止旧计时器。
func TestSendWithTimeoutPreservesResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"success", nil}, {"eof", io.EOF}, {"worker_error", errors.New("worker unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				req := &asrv1.StreamingRecognizeRequest{Data: []byte{1, 2}}
				calls := 0
				stream := &sendTestStream{send: func(got *asrv1.StreamingRecognizeRequest) error {
					calls++
					if got != req {
						t.Error("request was replaced")
					}
					return tc.err
				}}
				if err := sendWithTimeout(ctx, cancel, stream, req, time.Second); err != tc.err {
					t.Fatalf("error=%v, want %v", err, tc.err)
				}
				time.Sleep(2 * time.Second)
				synctest.Wait()
				if calls != 1 || context.Cause(ctx) != nil {
					t.Fatalf("calls=%d cause=%v", calls, context.Cause(ctx))
				}
			})
		})
	}
}

// TestSendWithTimeoutDoesNotCancelNextSend 让第二次发送跨过第一次的原定期限，
// 验证旧计时器不会取消后续发送；每次 Send 仍在调用者 goroutine 内同步完成。
func TestSendWithTimeoutDoesNotCancelNextSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		calls := 0
		stream := &sendTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
			calls++
			if calls == 2 {
				time.Sleep(750 * time.Millisecond)
			}
			return ctx.Err()
		}}
		req := &asrv1.StreamingRecognizeRequest{}
		if err := sendWithTimeout(ctx, cancel, stream, req, time.Second); err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		if err := sendWithTimeout(ctx, cancel, stream, req, time.Second); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if calls != 2 || context.Cause(ctx) != nil {
			t.Fatalf("calls=%d cause=%v", calls, context.Cause(ctx))
		}
	})
}

// TestSendWithTimeoutCancelsAndWaits 验证期限取消了 RPC，且函数等待底层 Send 真正返回。
// RPC 取消引发的 EOF 或 nil 都不应掩盖发送超时原因。
func TestSendWithTimeoutCancelsAndWaits(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{"eof", io.EOF}, {"nil", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				returned := false
				stream := &sendTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
					<-ctx.Done()
					time.Sleep(10 * time.Millisecond) // 模拟取消后底层收尾仍需时间。
					returned = true
					return tc.err
				}}
				start := time.Now()
				err := sendWithTimeout(ctx, cancel, stream, &asrv1.StreamingRecognizeRequest{}, time.Second)
				if !errors.Is(err, ErrWorkerSendTimeout) || !errors.Is(context.Cause(ctx), ErrWorkerSendTimeout) || !returned {
					t.Fatalf("error=%v cause=%v Send returned=%v", err, context.Cause(ctx), returned)
				}
				if elapsed := time.Since(start); elapsed != 1010*time.Millisecond {
					t.Fatalf("elapsed=%v, want 1.01s of virtual time", elapsed)
				}
			})
		})
	}
}

// TestSendWithTimeoutWaitsForCallback 暂停已经触发的取消回调，稳定构造 Stop 返回 false
// 但回调尚未完成的竞争，验证辅助函数不会带着未完成回调返回。
func TestSendWithTimeoutWaitsForCallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		release := make(chan struct{})
		defer close(release)
		callbackStarted, sendReturned := false, false
		observedCancel := func(cause error) {
			cancel(cause)
			callbackStarted = true
			<-release // 只在测试中暂停回调，模拟取消后尚未执行完成通知。
		}
		stream := &sendTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
			<-ctx.Done()
			sendReturned = true
			return io.EOF
		}}
		done := make(chan error, 1)
		go func() {
			done <- sendWithTimeout(ctx, observedCancel, stream, &asrv1.StreamingRecognizeRequest{}, time.Second)
		}()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if !callbackStarted || !sendReturned {
			t.Fatal("did not reach callback/Send boundary")
		}
		select {
		case err := <-done:
			t.Fatalf("returned before callback completed: %v", err)
		default:
		}
		// 使用发送唤醒一次等待，defer 关闭只用于失败兜底，避免重复 close。
		release <- struct{}{}
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, ErrWorkerSendTimeout) {
				t.Fatalf("error=%v", err)
			}
		default:
			t.Fatal("helper did not return after callback completed")
		}
	})
}

// TestSendWithTimeoutExternalCancellation 验证进入前和发送中的外部取消保留其原因。
func TestSendWithTimeoutExternalCancellation(t *testing.T) {
	for _, before := range []bool{true, false} {
		name := "during_send"
		if before {
			name = "before_send"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, stop := context.WithCancelCause(context.Background())
				defer stop(nil)
				ctx, cancel := context.WithCancelCause(parent)
				defer cancel(nil)
				cause := errors.New("service stopping")
				calls := 0
				stream := &sendTestStream{send: func(*asrv1.StreamingRecognizeRequest) error {
					calls++
					<-ctx.Done()
					return io.EOF
				}}
				if before {
					stop(cause)
				} else {
					time.AfterFunc(10*time.Millisecond, func() { stop(cause) })
				}
				err := sendWithTimeout(ctx, cancel, stream, &asrv1.StreamingRecognizeRequest{}, time.Second)
				if err != cause {
					t.Fatalf("error=%v, want external cause", err)
				}
				want := 1
				if before {
					want = 0
				}
				if calls != want {
					t.Fatalf("Send calls=%d, want %d", calls, want)
				}
				time.Sleep(2 * time.Second)
				if context.Cause(ctx) != cause {
					t.Fatalf("cause changed: %v", context.Cause(ctx))
				}
			})
		})
	}
}

// TestSendWithTimeoutInvalidDuration 区分配置错误与真实发送超时；非法参数不触发 I/O 或取消。
func TestSendWithTimeoutInvalidDuration(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				calls := 0
				stream := &sendTestStream{send: func(*asrv1.StreamingRecognizeRequest) error { calls++; return nil }}
				err := sendWithTimeout(ctx, cancel, stream, &asrv1.StreamingRecognizeRequest{}, timeout)
				if err == nil {
					t.Fatal("invalid timeout accepted")
				}
				if errors.Is(err, ErrWorkerSendTimeout) {
					t.Errorf("configuration error incorrectly classified as send timeout: %v", err)
				}
				time.Sleep(2 * time.Second)
				if calls != 0 || context.Cause(ctx) != nil {
					t.Fatalf("calls=%d cause=%v", calls, context.Cause(ctx))
				}
			})
		})
	}
}
