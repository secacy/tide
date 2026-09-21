package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cleanupListener 统计测试 HTTP 服务实际接受、尚未关闭的底层连接。
// WebSocket 升级后的连接仍被跟踪，避免把 HTTP hijack 误当成连接已经释放。
type cleanupListener struct {
	net.Listener
	accepted atomic.Int64 // 已成功接受的连接总数。
	open     atomic.Int64 // 尚未调用底层 Close 的连接数。
}

// Accept 为每条成功接入的连接增加关闭计数包装。
func (l *cleanupListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.accepted.Add(1)
	l.open.Add(1)
	return &cleanupConn{Conn: conn, open: &l.open}, nil
}

// cleanupConn 保留真实连接行为；重复 Close 只减少一次存活计数。
type cleanupConn struct {
	net.Conn
	open *atomic.Int64 // 所属监听器的存活连接计数。
	once sync.Once     // 保证关闭计数只更新一次。
}

// Close 先关闭底层连接，再更新测试计数。
func (c *cleanupConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { c.open.Add(-1) })
	return err
}

// cleanupScenario 保存一条测试会话的预期结束方式和完成通知。
// 所有实例在预热前创建，避免测试夹具自身增长干扰堆内存观测。
type cleanupScenario struct {
	id          string                   // 同时用于请求路径和音频前缀。
	mode        string                   // normal、disconnect 或 worker_error。
	handlerDone chan struct{}            // Gateway handler 返回后关闭。
	workerExit  chan isolationWorkerExit // 容量为 1，记录 Worker 退出。
	stopWorker  chan error               // 容量为 1，仅用于主动报错的场景。
}

// TestGatewayRepeatedSessionCleanup 在同一 Gateway 和 gRPC 连接上反复运行混合会话。
// 每轮检查会话、Worker 和真实连接全部退出；goroutine 与堆内存作为辅助观测。
// 这是清理回归测试，不是容量或吞吐量基准测试。
func TestGatewayRepeatedSessionCleanup(t *testing.T) {
	const rounds = 20
	const perRound = 6
	worker := &isolationWorker{
		exits: make(map[string]chan isolationWorkerExit), stopAfterFirstResult: make(map[string]<-chan error),
	}
	batches := make([][]cleanupScenario, rounds+1) // 第 0 轮仅用于预热。
	handlers := make(map[string]chan struct{})
	for round := range batches {
		for i := 0; i < perRound; i++ {
			mode := []string{"normal", "disconnect", "worker_error"}[i%3]
			scenario := cleanupScenario{
				id: fmt.Sprintf("round-%02d-%s-%d", round, mode, i), mode: mode,
				handlerDone: make(chan struct{}), workerExit: make(chan isolationWorkerExit, 1),
			}
			if mode == "worker_error" {
				scenario.stopWorker = make(chan error, 1)
				worker.stopAfterFirstResult[scenario.id] = scenario.stopWorker
			}
			worker.exits[scenario.id] = scenario.workerExit
			handlers[scenario.id] = scenario.handlerDone
			batches[round] = append(batches[round], scenario)
		}
	}
	workerClient := newTestWorkerClient(t, worker)
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)
	g, err := New(sessionCtx, workerClient, Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		done, ok := handlers[strings.TrimPrefix(r.URL.Path, "/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		defer close(done)
		g.ServeHTTP(w, r)
	}))
	listener := &cleanupListener{Listener: server.Listener}
	server.Listener = listener
	server.Start()
	t.Cleanup(func() {
		g.StopAccepting()
		cancelSessions()
		server.Close()
	})

	runCleanupBatch(t, server.URL, batches[0])
	assertCleanupCounts(t, g, listener, perRound)
	baseline := logCleanupResources(t, "after warmup")
	for round := 1; round <= rounds; round++ {
		runCleanupBatch(t, server.URL, batches[round])
		assertCleanupCounts(t, g, listener, int64((round+1)*perRound))
		if sessionCtx.Err() != nil {
			t.Fatalf("service unexpectedly stopped in round %d: %v", round, sessionCtx.Err())
		}
		if round%5 == 0 {
			logCleanupResources(t, fmt.Sprintf("after round %d", round))
		}
	}

	// Go 和 gRPC 会保留少量后台 goroutine，不能机械要求与基线完全相等。
	// 8 个的余量用于降低环境噪声；不能据此声称绝无任何 goroutine 泄漏。
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for runtime.NumGoroutine() > baseline+8 {
		select {
		case <-ticker.C:
		case <-deadline.C:
			var stacks bytes.Buffer
			_ = pprof.Lookup("goroutine").WriteTo(&stacks, 2)
			t.Fatalf("goroutines did not settle: baseline=%d current=%d\n%s", baseline, runtime.NumGoroutine(), stacks.String())
		}
	}
	t.Logf("completed %d measured sessions plus %d warmup sessions; goroutines baseline=%d final=%d",
		rounds*perRound, perRound, baseline, runtime.NumGoroutine())
	g.StopAccepting()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := g.Wait(waitCtx); err != nil {
		t.Fatalf("final session wait failed: %v", err)
	}
}

// runCleanupBatch 先建立一轮全部会话，再混合触发结束，最后等待各自的清理通知。
// 本函数返回前完成断言；客户端 CloseNow 的 defer 仅用于失败兜底。
func runCleanupBatch(t *testing.T, serverURL string, scenarios []cleanupScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clients := make(map[string]*websocket.Conn)
	defer func() {
		for _, conn := range clients {
			_ = conn.CloseNow()
		}
	}()
	for _, scenario := range scenarios {
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverURL, "http")+"/"+scenario.id, nil)
		if err != nil {
			t.Fatalf("connect %s: %v", scenario.id, err)
		}
		clients[scenario.id] = conn
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
			t.Fatalf("start %s: %v", scenario.id, err)
		}
		if err := conn.Write(ctx, websocket.MessageBinary, []byte(scenario.id+":audio")); err != nil {
			t.Fatalf("send audio %s: %v", scenario.id, err)
		}
		assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{
			Type: wsprotocol.MessageTypeResult, SegmentID: scenario.id, Text: scenario.id + ":audio",
		})
	}
	for _, scenario := range scenarios {
		conn := clients[scenario.id]
		switch scenario.mode {
		case "normal":
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
				t.Fatalf("send end %s: %v", scenario.id, err)
			}
		case "disconnect":
			if err := conn.CloseNow(); err != nil {
				t.Fatalf("disconnect %s: %v", scenario.id, err)
			}
		case "worker_error":
			scenario.stopWorker <- status.Error(codes.Unavailable, "injected worker failure")
		}
	}
	for _, scenario := range scenarios {
		conn := clients[scenario.id]
		if scenario.mode != "disconnect" {
			wantClose := websocket.StatusInternalError
			if scenario.mode == "normal" {
				assertRecognitionResult(t, ctx, conn, wsprotocol.ResultMessage{
					Type: wsprotocol.MessageTypeResult, SegmentID: scenario.id, Text: scenario.id + ":tail", IsFinal: true,
				})
				wantClose = websocket.StatusNormalClosure
			}
			_, _, err := conn.Read(ctx)
			if websocket.CloseStatus(err) != wantClose {
				t.Fatalf("close %s: %v, want %v", scenario.id, err, wantClose)
			}
		}
		select {
		case exit := <-scenario.workerExit:
			switch scenario.mode {
			case "normal":
				if exit.err != nil || exit.contextErr != nil {
					t.Fatalf("normal worker %s failed: %+v", scenario.id, exit)
				}
			case "disconnect":
				if exit.err == nil || !errors.Is(exit.contextErr, context.Canceled) {
					t.Fatalf("worker %s did not observe cancellation: %+v", scenario.id, exit)
				}
			case "worker_error":
				if status.Code(exit.err) != codes.Unavailable || exit.contextErr != nil {
					t.Fatalf("worker %s did not report injected failure: %+v", scenario.id, exit)
				}
			}
		case <-ctx.Done():
			t.Fatalf("worker %s did not exit", scenario.id)
		}
		select {
		case <-scenario.handlerDone:
		case <-ctx.Done():
			t.Fatalf("handler %s did not exit", scenario.id)
		}
	}
}

// assertCleanupCounts 在每轮所有 handler 返回后，检查会话及底层连接计数。
func assertCleanupCounts(t *testing.T, g *Gateway, listener *cleanupListener, accepted int64) {
	t.Helper()
	g.tracker.mu.Lock()
	active, stopping := g.tracker.active, g.tracker.stopping
	g.tracker.mu.Unlock()
	if active != 0 || stopping {
		t.Fatalf("after batch: active=%d stopping=%v, want zero sessions and still accepting", active, stopping)
	}
	if open := listener.open.Load(); open != 0 {
		t.Fatalf("server connections still open after batch: %d", open)
	}
	if got := listener.accepted.Load(); got != accepted {
		t.Fatalf("accepted connections=%d, want %d", got, accepted)
	}
}

// logCleanupResources 在 GC 后记录辅助指标，减少尚未回收对象对堆内存观测的影响。
// 堆分配量受运行时缓存影响，只记录，不使用固定字节阈值判断泄漏。
// 重复创建整套夹具时，bufconn 的关闭期限计时器可能暂时保留缓冲区；
// 比较跨次运行的堆内存时，还需在全部夹具关闭、计时器结束后观察回收。
func logCleanupResources(t *testing.T, label string) int {
	t.Helper()
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	count := runtime.NumGoroutine()
	t.Logf("%s: goroutines=%d heap_alloc=%d KiB heap_objects=%d", label, count, memory.HeapAlloc/1024, memory.HeapObjects)
	return count
}
