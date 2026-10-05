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

// assertControlAlive 等待其他协程稳定后确认循环仍运行，不读取其可变状态。
func assertControlAlive(t *testing.T, s *resumableSession) {
	t.Helper()
	synctest.Wait()
	select {
	case <-s.controlDone:
		t.Fatal("control loop ended before the current expiry")
	default:
	}
}

// assertControlExpired 检查当前虚拟时刻已经退出，不通过等待推进时间掩盖迟到。
// done 提供状态读取的同步边界；generation 是预期保留的连接代次。
func assertControlExpired(t *testing.T, s *resumableSession, generation uint64) {
	t.Helper()
	synctest.Wait()
	select {
	case <-s.controlDone:
		assertResumeState(t, s.resume, resumeClosed, generation, time.Time{})
	default:
		t.Fatal("control loop did not end at the current expiry")
	}
}

func TestSessionControlAttachedDoesNotExpire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		startTestSessionControl(t, s, time.Now)
		synctest.Wait()
		time.Sleep(30 * time.Second) // 虚拟时间，覆盖三个恢复窗口。
		assertControlAlive(t, s)
		if err := s.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertResumeState(t, s.resume, resumeClosed, 1, time.Time{})
	})
}

func TestSessionControlDetachedExpiresWithoutRequests(t *testing.T) {
	for _, initial := range []bool{false, true} {
		name := "detach_command"
		if initial {
			name = "initial_detached"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				if initial {
					s.resume.detach(1, time.Now())
				}
				startTestSessionControl(t, s, time.Now)
				if !initial {
					if detached, err := s.reportDetach(context.Background(), 1); !detached || err != nil {
						t.Fatalf("detach = (%v, %v)", detached, err)
					}
				}
				synctest.Wait() // 确认 Timer 已安排，之后不再发出命令。
				time.Sleep(10*time.Second - time.Nanosecond)
				assertControlAlive(t, s)
				time.Sleep(time.Nanosecond)
				assertControlExpired(t, s, 1)
			})
		})
	}
}

func TestSessionControlIgnoredCommandsDoNotRenewExpiry(t *testing.T) {
	for _, name := range []string{"duplicate_detach", "stale_detach", "canceled_command", "unknown_command"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				startTestSessionControl(t, s, time.Now)
				if detached, err := s.reportDetach(context.Background(), 1); !detached || err != nil {
					t.Fatalf("initial detach = (%v, %v)", detached, err)
				}
				time.Sleep(9 * time.Second)
				switch name {
				case "duplicate_detach", "stale_detach":
					generation := uint64(1)
					if name == "stale_detach" {
						generation = 0
					}
					if detached, err := s.reportDetach(context.Background(), generation); detached || err != nil {
						t.Fatalf("ignored detach = (%v, %v)", detached, err)
					}
				case "canceled_command":
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					reply := make(chan sessionControlResult, 1)
					s.commands <- sessionControlCommand{kind: controlResume, ctx: ctx, reply: reply}
					if got := <-reply; !errors.Is(got.err, context.Canceled) {
						t.Fatalf("canceled command = %+v", got)
					}
				case "unknown_command":
					if got := s.submitControl(context.Background(), 255, 0); !errors.Is(got.err, errInvalidSessionControlCommand) {
						t.Fatalf("unknown command = %+v", got)
					}
				}
				assertControlAlive(t, s)
				time.Sleep(time.Second)
				assertControlExpired(t, s, 1) // 原 t=10 截止，没有变成 t=19。
			})
		})
	}
}

func TestSessionControlResumeStopsOldExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.detach(1, time.Now())
		startTestSessionControl(t, s, time.Now)
		synctest.Wait()
		time.Sleep(9 * time.Second)
		if generation, err := s.requestResume(context.Background()); generation != 2 || err != nil {
			t.Fatalf("resume before expiry = (%d, %v)", generation, err)
		}
		time.Sleep(21 * time.Second)
		assertControlAlive(t, s)
		if generation, err := s.requestResume(context.Background()); generation != 0 || !errors.Is(err, errResumeAlreadyAttached) {
			t.Fatalf("resumed connection no longer attached: (%d, %v)", generation, err)
		}
		if err := s.requestClose(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertResumeState(t, s.resume, resumeClosed, 2, time.Time{})
	})
}

func TestSessionControlNewDetachUsesNewExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.detach(1, time.Now()) // 第一轮期限 t=10。
		startTestSessionControl(t, s, time.Now)
		synctest.Wait()
		time.Sleep(3 * time.Second)
		if generation, err := s.requestResume(context.Background()); generation != 2 || err != nil {
			t.Fatalf("resume = (%d, %v)", generation, err)
		}
		time.Sleep(2 * time.Second)
		if detached, err := s.reportDetach(context.Background(), 2); !detached || err != nil {
			t.Fatalf("new detach = (%v, %v)", detached, err)
		}
		time.Sleep(5 * time.Second)
		assertControlAlive(t, s) // t=10 旧期限不再有效，新期限为 t=15。
		time.Sleep(5*time.Second - time.Nanosecond)
		assertControlAlive(t, s)
		time.Sleep(time.Nanosecond)
		assertControlExpired(t, s, 2)
	})
}

func TestSessionControlShutdownDuringExpiryWait(t *testing.T) {
	for _, name := range []string{"explicit_close", "lifecycle_cancel"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := newTestResumableSession(t, identityTestMaterial())
				s.resume.detach(1, time.Now())
				cancel := startTestSessionControl(t, s, time.Now)
				synctest.Wait()
				time.Sleep(5 * time.Second)
				before := time.Now()
				if name == "explicit_close" {
					if err := s.requestClose(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				assertControlExpired(t, s, 1)
				if !time.Now().Equal(before) {
					t.Fatal("shutdown waited for the expiry instead of terminating immediately")
				}
			})
		})
	}
}

func TestSessionControlExpiryCompetitionReturnsAllCallers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.detach(1, time.Now().Add(-10*time.Second)) // 截止恰好为当前时刻。
		entered, release := make(chan struct{}), make(chan struct{})
		var gateOnce, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		startTestSessionControl(t, s, func() time.Time {
			gateOnce.Do(func() { close(entered); <-release })
			return time.Now()
		})
		<-entered // 暂停在初次 Timer 安排，使恢复请求先等待交付。
		results := make(chan sessionControlResult, 32)
		for range 32 {
			go func() { results <- callTestSessionControl(s, context.Background(), controlResume) }()
		}
		synctest.Wait()
		unblock() // Timer(0) 和命令现在均可参与 select，不规定选择顺序。
		for range 32 {
			got := <-results
			if got.generation != 0 || (!errors.Is(got.err, errResumeExpired) && !errors.Is(got.err, errResumeClosed)) {
				t.Fatalf("expired session resumed or lost result: %+v", got)
			}
		}
		assertControlExpired(t, s, 1)
	})
}

func TestSessionControlGenerationExhaustionKeepsExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.generation = math.MaxUint64
		s.resume.detach(math.MaxUint64, time.Now())
		startTestSessionControl(t, s, time.Now)
		synctest.Wait()
		time.Sleep(9 * time.Second)
		if generation, err := s.requestResume(context.Background()); generation != 0 || !errors.Is(err, errResumeGenerationExhausted) {
			t.Fatalf("exhausted generation = (%d, %v)", generation, err)
		}
		assertControlAlive(t, s)
		time.Sleep(time.Second)
		assertControlExpired(t, s, math.MaxUint64)
	})
}

func TestSessionControlOverdueAtStartupExpiresImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestResumableSession(t, identityTestMaterial())
		s.resume.detach(1, time.Now().Add(-11*time.Second))
		before := time.Now()
		startTestSessionControl(t, s, time.Now)
		assertControlExpired(t, s, 1)
		if !time.Now().Equal(before) {
			t.Fatal("overdue startup created a new recovery window")
		}
	})
}
