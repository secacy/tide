package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// admissionActive 在 tracker 锁内读取计数，避免测试观察本身引入数据竞争。
func admissionActive(tracker *sessionTracker) int {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.active
}

// TestAdmissionConfig 验证配置边界及配置实际传入 tracker。
func TestAdmissionConfig(t *testing.T) {
	for _, tc := range []struct {
		name             string
		configured, want int
	}{
		{"default", 0, 64},
		{"explicit", 4, 4},
		{"negative", -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{MaxSessions: tc.configured})
			if tc.configured < 0 {
				if err == nil || g != nil {
					t.Fatalf("negative limit: gateway=%v err=%v", g, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if g.cfg.MaxSessions != tc.want || g.tracker.maxActive != tc.want {
				t.Fatalf("limit: config=%d tracker=%d want=%d", g.cfg.MaxSessions, g.tracker.maxActive, tc.want)
			}
		})
	}
}

// TestAdmissionConcurrentReservations 保持成功登记的名额，直到全部接入尝试结束。
// 这样可以确定性验证上限，而不会因提前归还名额使成功次数超过上限。
func TestAdmissionConcurrentReservations(t *testing.T) {
	const limit, attempts = 4, 100
	tracker := newSessionTracker(limit)
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- tracker.tryEnter()
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	accepted, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, errSessionLimit):
			rejected++
		default:
			t.Errorf("unexpected admission error: %v", err)
		}
	}
	if accepted != limit || rejected != attempts-limit || admissionActive(tracker) != limit {
		t.Fatalf("accepted=%d rejected=%d active=%d", accepted, rejected, admissionActive(tracker))
	}
	tracker.leave()
	if err := tracker.tryEnter(); err != nil {
		t.Fatalf("reuse released slot: %v", err)
	}
	if err := tracker.tryEnter(); !errors.Is(err, errSessionLimit) {
		t.Fatalf("full again: %v", err)
	}
	for range limit {
		tracker.leave()
	}
	if got := admissionActive(tracker); got != 0 {
		t.Fatalf("active after cleanup=%d", got)
	}
}

// TestAdmissionStopWithOutstandingReservation 验证停止优先，以及清理完成才通知 drained。
func TestAdmissionStopWithOutstandingReservation(t *testing.T) {
	tracker := newSessionTracker(1)
	if err := tracker.tryEnter(); err != nil {
		t.Fatal(err)
	}
	tracker.stopAccepting()
	if err := tracker.tryEnter(); !errors.Is(err, errGatewayStopping) {
		t.Fatalf("stopping while full: %v", err)
	}
	select {
	case <-tracker.drained:
		t.Fatal("drained closed with an outstanding reservation")
	default:
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tracker.wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait: %v", err)
	}
	if got := admissionActive(tracker); got != 1 {
		t.Fatalf("wait cancellation changed active to %d", got)
	}
	tracker.leave()
	select {
	case <-tracker.drained:
	default:
		t.Fatal("drained not closed after final cleanup")
	}
	tracker.stopAccepting() // 重复停止不能重复关闭通知。
	if err := tracker.tryEnter(); !errors.Is(err, errGatewayStopping) {
		t.Fatalf("stopping with free slot: %v", err)
	}
}

// TestAdmissionRejectedRequestPreservesReservations 使用已有登记模拟名额已满。
// HTTP 503 之外还检查计数，防止拒绝请求误调用 leave、释放别人的名额。
func TestAdmissionRejectedRequestPreservesReservations(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		name := "full"
		if stopping {
			name = "stopping"
		}
		t.Run(name, func(t *testing.T) {
			worker := &recordingWorker{}
			g, err := New(context.Background(), worker, Config{MaxSessions: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.tracker.tryEnter(); err != nil {
				t.Fatal(err)
			}
			if stopping {
				g.StopAccepting()
			}
			for range 3 {
				recorder := httptest.NewRecorder()
				g.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
				if recorder.Code != http.StatusServiceUnavailable {
					t.Fatalf("HTTP status=%d", recorder.Code)
				}
				if !strings.Contains(recorder.Body.String(), map[bool]string{false: "session limit", true: "service is stopping"}[stopping]) {
					t.Fatalf("rejection body=%q", recorder.Body.String())
				}
				if got := admissionActive(g.tracker); got != 1 {
					t.Fatalf("rejected request changed active: got=%d want=1", got)
				}
				if worker.called.Load() {
					t.Fatal("rejected request created a Worker stream")
				}
			}
			g.tracker.leave()
			g.StopAccepting()
		})
	}
}

// TestAdmissionUpgradeFailureReturnsSlot 验证登记成功但升级失败时也归还名额。
// 重复无效握手不能逐渐占满 Gateway。
func TestAdmissionUpgradeFailureReturnsSlot(t *testing.T) {
	worker := &recordingWorker{}
	g, err := New(context.Background(), worker, Config{MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		recorder := httptest.NewRecorder()
		g.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		if recorder.Code < 400 || recorder.Code == http.StatusServiceUnavailable {
			t.Fatalf("upgrade failure status=%d", recorder.Code)
		}
		if got := admissionActive(g.tracker); got != 0 {
			t.Fatalf("failed upgrade retained %d reservations", got)
		}
	}
	if worker.called.Load() {
		t.Fatal("failed upgrade created a Worker stream")
	}
	g.StopAccepting()
}
