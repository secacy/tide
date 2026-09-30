package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
)

// newSnapshotGateway 使用正式构造路径，避免快照测试绕过默认配置。
func newSnapshotGateway(t *testing.T, limit int) *Gateway {
	t.Helper()
	g, err := New(context.Background(), singleWorkerPool(t, &recordingWorker{}), Config{MaxSessions: limit})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// assertGatewaySnapshot 通过公开入口比较整个快照，不直接读取 tracker 字段。
func assertGatewaySnapshot(t *testing.T, g *Gateway, want GatewaySnapshot) {
	t.Helper()
	if got := g.Snapshot(); got != want {
		t.Fatalf("snapshot=%+v, want %+v", got, want)
	}
}

// TestGatewaySnapshotLifecycle 验证读取不会影响登记、拒绝、停止与 drained 通知。
func TestGatewaySnapshotLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name              string
		configured, limit int
	}{{"default", 0, 64}, {"explicit", 2, 2}} {
		t.Run(tc.name, func(t *testing.T) {
			g := newSnapshotGateway(t, tc.configured)
			assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: tc.limit})
			for n := 1; n <= tc.limit; n++ {
				if err := g.tracker.tryEnter(); err != nil {
					t.Fatal(err)
				}
				assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: n, MaxSessions: tc.limit})
			}
			if err := g.tracker.tryEnter(); !errors.Is(err, errSessionLimit) {
				t.Fatalf("full admission: %v", err)
			}
			assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: tc.limit, MaxSessions: tc.limit})
			g.StopAccepting()
			g.StopAccepting()
			if err := g.tracker.tryEnter(); !errors.Is(err, errGatewayStopping) {
				t.Fatalf("stopped admission: %v", err)
			}
			for n := tc.limit; n > 0; n-- {
				assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: n, MaxSessions: tc.limit, Stopping: true})
				select {
				case <-g.tracker.drained:
					t.Fatal("snapshot/drain released outstanding sessions")
				default:
				}
				g.tracker.leave()
			}
			assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: tc.limit, Stopping: true})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := g.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestGatewaySnapshotIsolation 验证返回值、历史快照和不同 Gateway 互不污染。
func TestGatewaySnapshotIsolation(t *testing.T) {
	g, other := newSnapshotGateway(t, 2), newSnapshotGateway(t, 3)
	old := g.Snapshot()
	modified := old
	modified.ActiveSessions, modified.MaxSessions, modified.Stopping = 99, 99, true
	assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: 2})
	if err := g.tracker.tryEnter(); err != nil {
		t.Fatal(err)
	}
	g.StopAccepting()
	assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: 1, MaxSessions: 2, Stopping: true})
	assertGatewaySnapshot(t, other, GatewaySnapshot{MaxSessions: 3})
	g.tracker.leave()
	if old != (GatewaySnapshot{MaxSessions: 2}) || modified.ActiveSessions != 99 {
		t.Fatalf("saved values changed: old=%+v modified=%+v", old, modified)
	}
	assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: 2, Stopping: true})
}

// TestGatewaySnapshotConcurrent 验证观察与登记/释放并发，及停止后的状态不回退。
// 操作次数仅为竞争测试夹具，不是吞吐或容量测量。
func TestGatewaySnapshotConcurrent(t *testing.T) {
	const writers, iterations = 8, 200
	g := newSnapshotGateway(t, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			<-start
			for range iterations {
				if err := g.tracker.tryEnter(); err != nil {
					t.Errorf("admission: %v", err)
					return
				}
				g.tracker.leave()
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			<-start
			for range 1000 {
				s := g.Snapshot()
				if s.MaxSessions != writers || s.ActiveSessions < 0 || s.ActiveSessions > writers || s.Stopping {
					t.Errorf("invalid running snapshot: %+v", s)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: writers})
	for range writers {
		if err := g.tracker.tryEnter(); err != nil {
			t.Fatal(err)
		}
	}
	start = make(chan struct{})
	wg.Go(func() { <-start; g.StopAccepting() })
	wg.Go(func() {
		<-start
		for range writers {
			g.tracker.leave()
		}
	})
	for range 4 {
		wg.Go(func() {
			<-start
			last, stopped := writers, false
			for range 1000 {
				s := g.Snapshot()
				if s.MaxSessions != writers || s.ActiveSessions < 0 || s.ActiveSessions > last || (stopped && !s.Stopping) {
					t.Errorf("invalid draining snapshot: %+v after active=%d stopping=%v", s, last, stopped)
					return
				}
				last, stopped = s.ActiveSessions, s.Stopping
			}
		})
	}
	close(start)
	wg.Wait()
	assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: writers, Stopping: true})
}

// snapshotUpgradeRecorder 在升级失败写 HTTP 响应时观察已登记的名额。
type snapshotUpgradeRecorder struct {
	*httptest.ResponseRecorder
	g        *Gateway
	observed GatewaySnapshot
}

func (w *snapshotUpgradeRecorder) WriteHeader(code int) {
	w.observed = w.g.Snapshot()
	w.ResponseRecorder.WriteHeader(code)
}

// TestGatewaySnapshotUpgradeFailure 验证登记早于升级，升级失败后归还名额。
func TestGatewaySnapshotUpgradeFailure(t *testing.T) {
	g := newSnapshotGateway(t, 1)
	w := &snapshotUpgradeRecorder{ResponseRecorder: httptest.NewRecorder(), g: g}
	g.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/asr", nil))
	if w.Code < 400 || w.observed != (GatewaySnapshot{ActiveSessions: 1, MaxSessions: 1}) {
		t.Fatalf("upgrade response=%d snapshot=%+v", w.Code, w.observed)
	}
	assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: 1})
}

// TestGatewaySnapshotConnections 验证真实 WebSocket 等待 start 时占位，拒绝不加计数，
// 正常尾部完成或客户端断开后，在 handler 清理结束时归零。
func TestGatewaySnapshotConnections(t *testing.T) {
	for _, mode := range []string{"complete_after_stop", "disconnect_before_start"} {
		t.Run(mode, func(t *testing.T) {
			worker := newBaselineTCPWorkerClient(t, mustMockWorker(t, mockasr.Config{FinalText: "final"}))
			app, stop := context.WithCancel(context.Background())
			defer stop()
			g, err := New(app, singleWorkerPool(t, worker), Config{MaxSessions: 1})
			if err != nil {
				t.Fatal(err)
			}
			returned := make(chan struct{}, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { returned <- struct{}{} }()
				g.ServeHTTP(w, r)
			}))
			var conn *websocket.Conn
			t.Cleanup(func() {
				stop()
				if conn != nil {
					_ = conn.CloseNow()
				}
				server.Close()
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			waitHandler := func() {
				select {
				case <-returned:
				case <-ctx.Done():
					t.Fatal("handler did not finish")
				}
			}
			url := "ws" + strings.TrimPrefix(server.URL, "http")
			conn, _, err = websocket.Dial(ctx, url, nil)
			if err != nil {
				t.Fatal(err)
			}
			assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: 1, MaxSessions: 1})
			rejected, resp, err := websocket.Dial(ctx, url, nil)
			if rejected != nil {
				_ = rejected.CloseNow()
			}
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("capacity rejection: response=%v err=%v", resp, err)
			}
			waitHandler()
			assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: 1, MaxSessions: 1})
			stopping := mode == "complete_after_stop"
			if stopping {
				g.StopAccepting()
				assertGatewaySnapshot(t, g, GatewaySnapshot{ActiveSessions: 1, MaxSessions: 1, Stopping: true})
				if err := finishAdmission(ctx, conn); err != nil {
					t.Fatal(err)
				}
			} else {
				_ = conn.CloseNow()
			}
			waitHandler()
			assertGatewaySnapshot(t, g, GatewaySnapshot{MaxSessions: 1, Stopping: stopping})
			if app.Err() != nil {
				t.Fatal("application canceled before cleanup assertion")
			}
		})
	}
}
