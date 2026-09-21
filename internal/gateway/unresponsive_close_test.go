package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// TestGatewayShutdownWithoutCloseReply 验证客户端不再读取、不会回复关闭帧时，
// RPC 仍能及时取消且会话最终退出，并记录实际收尾耗时。
// 8 秒是覆盖当前库内置握手期限的测试上限，不是产品的关闭预算。
func TestGatewayShutdownWithoutCloseReply(t *testing.T) {
	worker := &shutdownWorker{
		received: make(chan []byte, 1),
		finished: make(chan error, 1),
	}
	workerClient := newTestWorkerClient(t, worker)
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)
	g, err := New(sessionCtx, workerClient, Config{})
	if err != nil {
		t.Fatal(err)
	}
	handlerDone := make(chan struct{}) // 在 Gateway 完成清理和退出计数后关闭。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		g.ServeHTTP(w, r)
	}))
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		cancelSessions()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelClient()
	conn, _, err = websocket.Dial(clientCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatalf("send start: %v", err)
	}
	if err := conn.Write(clientCtx, websocket.MessageBinary, []byte("audio")); err != nil {
		t.Fatalf("send audio: %v", err)
	}
	assertRecognitionResult(t, clientCtx, conn, wsprotocol.ResultMessage{
		Type: wsprotocol.MessageTypeResult, SegmentID: "segment-1", Text: "正在识别",
	})

	// 从此不再调用客户端 Read、Close 或 CloseNow，确保客户端不参与关闭握手。
	g.StopAccepting()
	started := time.Now()
	cancelSessions()
	rpcWait, cancelRPCWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelRPCWait()
	select {
	case err := <-worker.finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("worker did not observe cancellation: %v", err)
		}
		t.Logf("Worker canceled after %v", time.Since(started))
	case <-rpcWait.Done():
		t.Fatal("Worker cancellation was held up by the WebSocket close handshake")
	}

	// 短等待用于观测“等待返回”与“会话释放”的区别，不要求会话一定慢于此期限。
	shortWait, cancelShortWait := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = g.Wait(shortWait)
	cancelShortWait()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected wait error: %v", err)
	}
	g.tracker.mu.Lock()
	active := g.tracker.active
	g.tracker.mu.Unlock()
	t.Logf("short wait returned %v; active sessions=%d", err, active)

	// 即使短等待超时，也继续观察真实资源释放，而不是由测试提前断开客户端。
	finalWait, cancelFinalWait := context.WithDeadline(context.Background(), started.Add(8*time.Second))
	defer cancelFinalWait()
	if err := g.Wait(finalWait); err != nil {
		t.Fatalf("session did not finish closing without a client reply: %v", err)
	}
	select {
	case <-handlerDone:
	case <-finalWait.Done():
		t.Fatal("Gateway handler did not return")
	}
	t.Logf("session fully released after %v", time.Since(started))
}
