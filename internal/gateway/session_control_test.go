package gateway

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// startTestSessionControl 启动唯一控制循环，测试结束时取消并等待它退出。
// 在 synctest bubble 内调用；状态断言须在 controlDone 之后进行。
func startTestSessionControl(t *testing.T, s *resumableSession, now func() time.Time) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go s.runControl(ctx, now)
	t.Cleanup(func() {
		cancel()
		<-s.controlDone
	})
	return cancel
}

// callTestSessionControl 经公共内部入口调用，覆盖各薄包装的结果转换。
func callTestSessionControl(s *resumableSession, ctx context.Context, kind sessionControlKind) sessionControlResult {
	switch kind {
	case controlResume:
		generation, err := s.requestResume(ctx)
		return sessionControlResult{generation: generation, err: err}
	case controlDetach:
		detached, err := s.reportDetach(ctx, 1)
		return sessionControlResult{detached: detached, err: err}
	default:
		return sessionControlResult{err: s.requestClose(ctx)}
	}
}

func TestSessionControlChannelsAreIndependent(t *testing.T) {
	a := newTestResumableSession(t, identityTestMaterial())
	b := newTestResumableSession(t, identityTestMaterial())
	if a.commands == nil || a.controlDone == nil || cap(a.commands) != 0 || a.commands == b.commands || a.controlDone == b.controlDone {
		t.Fatal("each session must have its own unbuffered commands and done channel")
	}
	select {
	case <-a.controlDone:
		t.Fatal("construction must not start or finish the control loop")
	default:
	}
}

func TestSessionControlUndeliveredRequestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		generation, err := s.requestResume(ctx)
		if generation != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("undelivered request = (%d, %v)", generation, err)
		}
		// 没有启动循环，无人接收；虚拟期限到达后返回且状态未变。
		assertResumeState(t, s.resume, resumeAttached, 1, time.Time{})
	})
}

func TestSessionControlPreCanceledRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind sessionControlKind
	}{{"resume", controlResume}, {"detach", controlDetach}, {"close", controlClose}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				now := time.Now()
				s.resume.detach(1, now)
				startTestSessionControl(t, s, time.Now)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				result := callTestSessionControl(s, ctx, tc.kind)
				if !errors.Is(result.err, context.Canceled) || result.generation != 0 || result.detached {
					t.Fatalf("pre-canceled result = %+v", result)
				}
				if generation, err := s.requestResume(context.Background()); generation != 2 || err != nil {
					t.Fatalf("canceled request changed state: (%d, %v)", generation, err)
				}
			})
		})
	}
}

func TestSessionControlRejectsCanceledDeliveredCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind sessionControlKind
	}{{"resume", controlResume}, {"detach", controlDetach}, {"close", controlClose}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				now := time.Now()
				s.resume.detach(1, now)
				startTestSessionControl(t, s, time.Now)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				// 直接交付，独立检查协调者的取消校验，绕过 submit 的预检查。
				reply := make(chan sessionControlResult, 1)
				s.commands <- sessionControlCommand{kind: tc.kind, ctx: ctx, generation: 1, reply: reply}
				if result := <-reply; !errors.Is(result.err, context.Canceled) || result.generation != 0 || result.detached {
					t.Fatalf("delivered canceled result = %+v", result)
				}
				if generation, err := s.requestResume(context.Background()); generation != 2 || err != nil {
					t.Fatalf("loop did not remain usable: (%d, %v)", generation, err)
				}
				if err := s.requestClose(context.Background()); err != nil {
					t.Fatal(err)
				}
				if len(reply) != 0 {
					t.Fatal("command received more than one reply")
				}
			})
		})
	}
}

func TestSessionControlLifecycleAndUnknownCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		startTestSessionControl(t, s, time.Now)
		ctx := context.Background()
		if generation, err := s.requestResume(ctx); generation != 0 || !errors.Is(err, errResumeAlreadyAttached) {
			t.Fatalf("initial resume = (%d, %v)", generation, err)
		}
		if result := s.submitControl(ctx, sessionControlKind(255), 0); !errors.Is(result.err, errInvalidSessionControlCommand) {
			t.Fatalf("unknown command = %+v", result)
		}
		if detached, err := s.reportDetach(ctx, 0); detached || err != nil {
			t.Fatal("unmatched generation changed state")
		}
		for _, generation := range []uint64{1, 2} {
			if detached, err := s.reportDetach(ctx, generation); !detached || err != nil {
				t.Fatalf("current detach = (%v, %v)", detached, err)
			}
			if detached, err := s.reportDetach(ctx, generation); detached || err != nil {
				t.Fatal("duplicate detach must be ignored")
			}
			if next, err := s.requestResume(ctx); next != generation+1 || err != nil {
				t.Fatalf("resume = (%d, %v)", next, err)
			}
			if detached, err := s.reportDetach(ctx, generation); detached || err != nil {
				t.Fatal("old generation affected new connection")
			}
		}
		if err := s.requestClose(ctx); err != nil {
			t.Fatal(err)
		}
		assertResumeState(t, s.resume, resumeClosed, 3, time.Time{})
		if generation, err := s.requestResume(ctx); generation != 0 || !errors.Is(err, errResumeClosed) {
			t.Fatalf("resume closed = (%d, %v)", generation, err)
		}
		if detached, err := s.reportDetach(ctx, 3); detached || !errors.Is(err, errResumeClosed) {
			t.Fatalf("detach closed = (%v, %v)", detached, err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := s.requestClose(canceled); err != nil {
			t.Fatalf("already closed must be idempotent: %v", err)
		}
	})
}

func TestSessionControlConcurrentResumeHasOneWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		now := time.Now()
		s.resume.detach(1, now)
		startTestSessionControl(t, s, time.Now)
		start := make(chan struct{})
		results := make(chan sessionControlResult, 32)
		for range 32 {
			go func() {
				<-start
				results <- callTestSessionControl(s, context.Background(), controlResume)
			}()
		}
		close(start)
		wins := 0
		for range 32 {
			result := <-results
			if result.err == nil && result.generation == 2 {
				wins++
			} else if result.generation != 0 || !errors.Is(result.err, errResumeAlreadyAttached) {
				t.Fatalf("unexpected competing resume: %+v", result)
			}
		}
		if wins != 1 {
			t.Fatalf("successful resumes = %d, want 1", wins)
		}
	})
}

func TestSessionControlCancellationAfterProcessingCheckKeepsResult(t *testing.T) {
	for _, which := range []string{"request", "lifecycle"} {
		t.Run(which, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				now := time.Now()
				s.resume.detach(1, now)
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				clockCalls := 0 // 仅控制循环访问；首次调用用于启动期安排 Timer。
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				cancelLifecycle := startTestSessionControl(t, s, func() time.Time {
					clockCalls++
					if clockCalls == 2 { // 第二次调用属于已接收的 resume 命令。
						close(entered)
						<-release
					}
					return time.Now()
				})
				ctx, cancelRequest := context.WithCancel(context.Background())
				defer cancelRequest()
				result := make(chan sessionControlResult, 1)
				go func() { result <- callTestSessionControl(s, ctx, controlResume) }()
				<-entered // 已收到命令并通过取消检查，暂停在测试时钟中。
				if which == "request" {
					cancelRequest()
				} else {
					cancelLifecycle()
				}
				synctest.Wait()
				select {
				case got := <-result:
					t.Fatalf("accepted caller returned before reply: %+v", got)
				default:
				}
				unblock()
				if got := <-result; got.err != nil || got.generation != 2 {
					t.Fatalf("committed result was lost: %+v", got)
				}
				if which == "request" {
					// 成功结果仍须处理：用有效控制 ctx 报告刚接回的连接已不可用。
					if detached, err := s.reportDetach(context.Background(), 2); !detached || err != nil {
						t.Fatalf("post-cancel connection handoff failed: (%v, %v)", detached, err)
					}
				} else {
					<-s.controlDone
					assertResumeState(t, s.resume, resumeClosed, 2, time.Time{})
				}
			})
		})
	}
}

func TestSessionControlCancelWhileWaitingToDeliver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		entered, release := make(chan struct{}), make(chan struct{})
		var gateOnce, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		startTestSessionControl(t, s, func() time.Time {
			gateOnce.Do(func() { close(entered); <-release })
			return time.Now()
		})
		first := make(chan sessionControlResult, 1)
		go func() { first <- callTestSessionControl(s, context.Background(), controlDetach) }()
		<-entered
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		second := make(chan sessionControlResult, 1)
		go func() { second <- callTestSessionControl(s, ctx, controlResume) }()
		synctest.Wait() // 第二个请求确定阻塞在交付阶段。
		cancel()
		if got := <-second; !errors.Is(got.err, context.Canceled) || got.generation != 0 {
			t.Fatalf("waiting request = %+v", got)
		}
		unblock()
		if got := <-first; !got.detached || got.err != nil {
			t.Fatalf("first command = %+v", got)
		}
		if generation, err := s.requestResume(context.Background()); generation != 2 || err != nil {
			t.Fatalf("undelivered request changed state: (%d, %v)", generation, err)
		}
	})
}

func TestSessionControlCloseWaitsForLoopExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		returned := make(chan error, 1)
		go func() { returned <- s.requestClose(ctx) }()
		// 协议夹具充当唯一接收者，分开“已回复”与“循环已退出”两个时刻。
		cmd := <-s.commands
		if cmd.kind != controlClose || cap(cmd.reply) != 1 {
			t.Fatal("invalid close request or reply capacity")
		}
		cmd.reply <- sessionControlResult{}
		cancel()
		synctest.Wait()
		select {
		case err := <-returned:
			t.Fatalf("close returned before loop exit: %v", err)
		default:
		}
		close(s.controlDone)
		if err := <-returned; err != nil {
			t.Fatal(err)
		}
	})
}

func TestSessionControlConcurrentShutdownReturnsAllCallers(t *testing.T) {
	for _, cause := range []string{"close_commands", "lifecycle_cancellation"} {
		t.Run(cause, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				now := time.Now()
				s.resume.detach(1, now)
				cancel := startTestSessionControl(t, s, time.Now)
				start := make(chan struct{})
				resumes := make(chan sessionControlResult, 32)
				closes := make(chan error, 8)
				for range 32 {
					go func() {
						<-start
						resumes <- callTestSessionControl(s, context.Background(), controlResume)
					}()
				}
				for range 8 {
					go func() { <-start; closes <- s.requestClose(context.Background()) }()
				}
				if cause == "lifecycle_cancellation" {
					go func() { <-start; cancel() }()
				}
				close(start)
				wins := 0
				for range 32 {
					got := <-resumes
					if got.err == nil && got.generation == 2 {
						wins++
					} else if got.generation != 0 || (!errors.Is(got.err, errResumeAlreadyAttached) && !errors.Is(got.err, errResumeClosed)) {
						t.Fatalf("unexpected shutdown result: %+v", got)
					}
				}
				for range 8 {
					if err := <-closes; err != nil {
						t.Fatalf("competing close: %v", err)
					}
				}
				if wins > 1 {
					t.Fatal("more than one resume succeeded")
				}
				<-s.controlDone
				assertResumeState(t, s.resume, resumeClosed, uint64(1+wins), time.Time{})
			})
		})
	}
}

func TestSessionControlLifecycleCancellationAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		cancel := startTestSessionControl(t, s, time.Now)
		cancel()
		<-s.controlDone
		assertResumeState(t, s.resume, resumeClosed, 1, time.Time{})
		if generation, err := s.requestResume(context.Background()); generation != 0 || !errors.Is(err, errResumeClosed) {
			t.Fatalf("resume stopped loop = (%d, %v)", generation, err)
		}
	})
}

func TestSessionControlExpiredResumeRepliesBeforeExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		now := time.Now()
		s.resume.detach(1, now)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		clockCalls := 0
		startTestSessionControl(t, s, func() time.Time {
			clockCalls++
			if clockCalls == 2 { // 在 resume 已交付后暂停，Timer 通知尚不能被处理。
				close(entered)
				<-release
			}
			return time.Now()
		})
		result := make(chan sessionControlResult, 1)
		go func() { result <- callTestSessionControl(s, context.Background(), controlResume) }()
		<-entered
		time.Sleep(10 * time.Second) // synctest 虚拟时间：推进到真实的业务截止时间。
		unblock()
		if got := <-result; got.generation != 0 || !errors.Is(got.err, errResumeExpired) {
			t.Fatalf("expiry result lost to loop exit: %+v", got)
		}
		<-s.controlDone
		assertResumeState(t, s.resume, resumeClosed, 1, time.Time{})
	})
}

func TestSessionControlGenerationExhaustionCanStillClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.generation = math.MaxUint64
		s.resume.detach(math.MaxUint64, time.Now())
		startTestSessionControl(t, s, time.Now)
		if generation, err := s.requestResume(context.Background()); generation != 0 || !errors.Is(err, errResumeGenerationExhausted) {
			t.Fatalf("exhausted generation = (%d, %v)", generation, err)
		}
		if err := s.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertResumeState(t, s.resume, resumeClosed, math.MaxUint64, time.Time{})
	})
}
