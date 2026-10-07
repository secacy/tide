package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func testEntryGate(t *testing.T, sessions, handshakes int) *entryGate {
	t.Helper()
	g, err := newEntryGate(newSessionTracker(sessions), handshakes)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func entryHandshakeCount(g *entryGate) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.activeHandshakes
}

func requireEntryNotification(t *testing.T, ch <-chan struct{}, closed bool) {
	t.Helper()
	select {
	case <-ch:
		if !closed {
			t.Fatal("notification closed before cleanup")
		}
	default:
		if closed {
			t.Fatal("cleanup notification not closed")
		}
	}
}

func TestEntryGateConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		tracker *sessionTracker
		limit   int
	}{
		{"nil_tracker", nil, 1},
		{"zero_limit", newSessionTracker(1), 0},
		{"negative_limit", newSessionTracker(1), -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := newEntryGate(tc.tracker, tc.limit)
			if g != nil || !errors.Is(err, errInvalidEntryGateConfig) {
				t.Fatalf("gate=%v err=%v", g, err)
			}
		})
	}
	t.Run("preserves_shared_tracker", func(t *testing.T) {
		tracker := newSessionTracker(2)
		if err := tracker.tryEnter(); err != nil {
			t.Fatal(err)
		}
		defer tracker.leave()
		g, err := newEntryGate(tracker, 3)
		if err != nil || g.tracker != tracker || tracker.snapshot().ActiveSessions != 1 || entryHandshakeCount(g) != 0 {
			t.Fatalf("constructor changed shared budget: gate=%v err=%v", g, err)
		}
		requireEntryNotification(t, g.handshakesDrained, false)
	})
}

// 保留成功申请直到所有竞争者返回，验证真实的同时占位上限。
func TestEntryGateConcurrentBudgets(t *testing.T) {
	const attempts, limit = 64, 4
	for _, kind := range []string{"handshakes", "mixed_v1_v2_sessions"} {
		t.Run(kind, func(t *testing.T) {
			handshakeLimit := limit
			if kind != "handshakes" {
				handshakeLimit = attempts / 2
			}
			g := testEntryGate(t, limit, handshakeLimit)
			if kind != "handshakes" {
				// 每个模拟 v2 请求各持有自己的握手名额。
				for range attempts / 2 {
					if err := g.tryEnterHandshake(); err != nil {
						t.Fatal(err)
					}
					defer g.leaveHandshake()
				}
			}
			start := make(chan struct{})
			results := make(chan error, attempts)
			for i := range attempts {
				go func() {
					<-start
					if kind == "handshakes" {
						results <- g.tryEnterHandshake()
					} else if i%2 == 0 {
						results <- g.tracker.tryEnter() // 现有 v1 直接占用同一 tracker。
					} else {
						results <- g.tryEnterSession()
					}
				}()
			}
			close(start)
			accepted := 0
			wantErr := errSessionLimit
			if kind == "handshakes" {
				wantErr = errHandshakeLimit
			}
			for range attempts {
				if err := <-results; err == nil {
					accepted++
				} else if !errors.Is(err, wantErr) {
					t.Errorf("unexpected rejection: %v", err)
				}
			}
			if accepted != limit {
				t.Fatalf("accepted=%d want=%d", accepted, limit)
			}
			if kind == "handshakes" {
				if entryHandshakeCount(g) != limit || g.tracker.snapshot().ActiveSessions != 0 {
					t.Fatal("handshake budget changed logical session budget")
				}
				for range accepted {
					g.leaveHandshake()
				}
				if err := g.tryEnterHandshake(); err != nil {
					t.Fatalf("released slot not reusable: %v", err)
				}
				g.leaveHandshake()
			} else {
				if entryHandshakeCount(g) != attempts/2 || g.tracker.snapshot().ActiveSessions != limit {
					t.Fatal("v1/v2 did not share one session budget")
				}
				for range accepted {
					g.tracker.leave()
				}
			}
			requireEntryNotification(t, g.handshakesDrained, false)
		})
	}
}

// 这里只模拟接回已有会话的本地提交，不宣称真实恢复入口已经可用。
func TestEntryGateFullSessionsPermitSimulatedResume(t *testing.T) {
	g := testEntryGate(t, 1, 1)
	if err := g.tracker.tryEnter(); err != nil {
		t.Fatal(err)
	}
	defer g.tracker.leave()
	if err := g.tryEnterHandshake(); err != nil {
		t.Fatal(err)
	}
	defer g.leaveHandshake()
	committed := false
	if err := g.withCommit(context.Background(), func() error { committed = true; return nil }); err != nil || !committed {
		t.Fatalf("simulated resume rejected at full budget: %v", err)
	}
	if err := g.tryEnterSession(); !errors.Is(err, errSessionLimit) {
		t.Fatalf("new session at full budget: %v", err)
	}
	if g.tracker.snapshot().ActiveSessions != 1 || entryHandshakeCount(g) != 1 {
		t.Fatal("resume or rejected start consumed an extra slot")
	}
}

func TestEntryGateStopAccepting(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		name := "empty"
		if occupied {
			name = "occupied"
		}
		t.Run(name, func(t *testing.T) {
			g := testEntryGate(t, 1, 1)
			if occupied {
				if err := g.tryEnterHandshake(); err != nil {
					t.Fatal(err)
				}
				if err := g.tryEnterSession(); err != nil {
					t.Fatal(err)
				}
			}
			g.stopAccepting()
			g.stopAccepting()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for _, err := range []error{
				g.tryEnterHandshake(), g.tryEnterSession(), g.tracker.tryEnter(),
				g.withCommit(ctx, func() error { t.Fatal("commit executed after stop"); return nil }),
			} {
				if !errors.Is(err, errGatewayStopping) {
					t.Fatalf("stop must take priority: %v", err)
				}
			}
			requireEntryNotification(t, g.handshakesDrained, !occupied)
			requireEntryNotification(t, g.tracker.drained, !occupied)
			if occupied {
				if entryHandshakeCount(g) != 1 || g.tracker.snapshot().ActiveSessions != 1 {
					t.Fatal("stop released resources before cleanup")
				}
				g.leaveHandshake()
				g.tracker.leave()
			}
			if err := g.wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEntryGateCommitContextAndOutcome(t *testing.T) {
	cause := errors.New("entry canceled")
	callbackErr := errors.New("local validation failed")
	for _, name := range []string{"canceled", "deadline", "callback_error", "success_then_cancel", "error_then_cancel"} {
		t.Run(name, func(t *testing.T) {
			g := testEntryGate(t, 1, 1)
			if err := g.tryEnterHandshake(); err != nil {
				t.Fatal(err)
			}
			defer g.leaveHandshake()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			want := error(nil)
			if name == "canceled" {
				cancel(cause)
				want = cause
			} else if name == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				want = context.DeadlineExceeded
			} else if name == "callback_error" || name == "error_then_cancel" {
				want = callbackErr
			}
			calls := 0
			err := g.withCommit(ctx, func() error {
				calls++
				if name == "success_then_cancel" || name == "error_then_cancel" {
					cancel(cause)
				}
				if name == "callback_error" || name == "error_then_cancel" {
					return callbackErr
				}
				return nil
			})
			wantCalls := 1
			if name == "canceled" || name == "deadline" {
				wantCalls = 0
			}
			if err != want || calls != wantCalls {
				t.Fatalf("err=%v calls=%d; want err=%v calls=%d", err, calls, want, wantCalls)
			}
		})
	}
}

// Mutex 等待不是 synctest 的持久阻塞点，此处用真实 goroutine 和通道设定顺序。
// 测试专用回调暂停只为观察临界区，生产回调禁止等待任务。
func TestEntryGateCommitSerializesStop(t *testing.T) {
	g := testEntryGate(t, 1, 1)
	if err := g.tryEnterHandshake(); err != nil {
		t.Fatal(err)
	}
	defer g.leaveHandshake()
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	committed := make(chan error, 1)
	go func() {
		committed <- g.withCommit(context.Background(), func() error {
			close(entered)
			<-release
			select {
			case <-stopped:
				return errors.New("stop returned inside active commit")
			default:
				return nil
			}
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("commit did not enter")
	}
	if g.mu.TryLock() {
		g.mu.Unlock()
		t.Fatal("commit callback did not hold the stop mutex")
	}
	stopAttempted := make(chan struct{})
	go func() { close(stopAttempted); g.stopAccepting(); close(stopped) }()
	<-stopAttempted
	requireEntryNotification(t, stopped, false)
	unblock()
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("commit did not exit")
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("stop did not exit after commit")
	}
	if err := g.withCommit(context.Background(), func() error { t.Error("late commit executed"); return nil }); err != errGatewayStopping {
		t.Fatalf("late commit: %v", err)
	}
}

func TestEntryGateCommitChecksCancellationAfterLock(t *testing.T) {
	g := testEntryGate(t, 1, 1)
	if err := g.tryEnterHandshake(); err != nil {
		t.Fatal(err)
	}
	defer g.leaveHandshake()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("canceled before acquiring commit lock")
	g.mu.Lock()
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- g.withCommit(ctx, func() error { t.Error("canceled commit executed"); return nil })
	}()
	<-started
	cancel(cause)
	g.mu.Unlock()
	select {
	case err := <-result:
		if err != cause {
			t.Fatalf("cancellation cause lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not return")
	}
}

func TestEntryGateWaitBothResources(t *testing.T) {
	for _, first := range []string{"handshake", "session"} {
		t.Run(first+"_first", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := testEntryGate(t, 1, 1)
				if err := g.tryEnterHandshake(); err != nil {
					t.Fatal(err)
				}
				if err := g.tryEnterSession(); err != nil {
					t.Fatal(err)
				}
				g.stopAccepting()
				done := make(chan error, 3)
				for range 3 {
					go func() { done <- g.wait(context.Background()) }()
				}
				synctest.Wait()
				if first == "handshake" {
					g.leaveHandshake()
				} else {
					g.tracker.leave()
				}
				synctest.Wait()
				if len(done) != 0 {
					t.Fatal("wait completed while one resource class remained")
				}
				if first == "handshake" {
					g.tracker.leave()
				} else {
					g.leaveHandshake()
				}
				for range 3 {
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				requireEntryNotification(t, g.handshakesDrained, true)
				requireEntryNotification(t, g.tracker.drained, true)
			})
		})
	}
}

func TestEntryGateWaitBudgetAndOwnership(t *testing.T) {
	for _, name := range []string{"handshake_timeout", "session_timeout", "shared_deadline"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := testEntryGate(t, 1, 1)
				if err := g.tryEnterHandshake(); err != nil {
					t.Fatal(err)
				}
				if err := g.tryEnterSession(); err != nil {
					t.Fatal(err)
				}
				g.stopAccepting()
				if name == "session_timeout" {
					g.leaveHandshake()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- g.wait(ctx) }()
				synctest.Wait()
				if name == "shared_deadline" {
					time.Sleep(4 * time.Second)
					g.leaveHandshake()
					synctest.Wait()
					time.Sleep(time.Second)
				} else {
					time.Sleep(5 * time.Second)
				}
				synctest.Wait()
				select {
				case err := <-done:
					if err != context.DeadlineExceeded {
						t.Fatalf("wait timeout: %v", err)
					}
				default:
					t.Fatal("wait renewed its deadline or did not return")
				}
				wantHandshakes := 0
				if name == "handshake_timeout" {
					wantHandshakes = 1
				}
				if entryHandshakeCount(g) != wantHandshakes || g.tracker.snapshot().ActiveSessions != 1 {
					t.Fatal("stop or waiting timeout altered caller-owned resources")
				}
				if wantHandshakes != 0 {
					g.leaveHandshake()
				}
				g.tracker.leave()
				if err := g.wait(context.Background()); err != nil {
					t.Fatalf("retry wait after actual cleanup: %v", err)
				}
			})
		})
	}
}

func TestEntryGateWaitDoesNotStopAdmission(t *testing.T) {
	g := testEntryGate(t, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.wait(ctx); err != context.Canceled {
		t.Fatalf("wait without stopping: %v", err)
	}
	if err := g.tryEnterHandshake(); err != nil {
		t.Fatalf("wait stopped admission: %v", err)
	}
	g.leaveHandshake()
	g.stopAccepting()
	if err := g.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEntryGateProgrammingErrors(t *testing.T) {
	for _, name := range []string{"unpaired_leave", "nil_context", "nil_commit", "callback_panic"} {
		t.Run(name, func(t *testing.T) {
			g := testEntryGate(t, 1, 1)
			func() {
				defer func() {
					if recover() == nil {
						t.Error("programming error did not panic")
					}
				}()
				switch name {
				case "unpaired_leave":
					g.leaveHandshake()
				case "nil_context":
					g.withCommit(nil, func() error { return nil })
				case "nil_commit":
					g.withCommit(context.Background(), nil)
				case "callback_panic":
					g.withCommit(context.Background(), func() error { panic("callback bug") })
				}
			}()
			// panic 不应留下锁，也不应破坏计数；不等待以免坏实现卡住测试。
			if !g.mu.TryLock() {
				t.Fatal("panic left entry gate locked")
			}
			g.mu.Unlock()
			if entryHandshakeCount(g) != 0 {
				t.Fatal("programming error corrupted handshake count")
			}
		})
	}
}
