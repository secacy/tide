package gateway

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestGatewayShutdownBeforeStart 验证连接后未发送 start 的会话也会响应服务取消，
// 完成资源清理和退出登记，且不会创建 Worker stream。
func TestGatewayShutdownBeforeStart(t *testing.T) {
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)

	worker := &recordingWorker{}
	g, err := New(sessionCtx, singleWorkerPool(t, worker), Config{})
	if err != nil {
		t.Fatalf("create gateway: %v", err)
	}

	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		// 仅在断言结束或失败后兜底清理，不能提前替服务端断开连接。
		g.StopAccepting()
		cancelSessions()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})

	// 客户端使用独立的 context，避免服务取消时客户端主动断开掩盖问题。
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	conn, _, err = websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	// 握手成功说明连接已被接纳；不发送 start，也不主动关闭客户端。

	g.StopAccepting()
	cancelSessions()

	// 等待期限只约束测试，不负责取消会话。
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := g.Wait(waitCtx); err != nil {
		t.Fatalf("wait for session cleanup: %v", err)
	}
	if worker.called.Load() {
		t.Fatal("worker stream was created before start")
	}

	// 确认连接实际结束；此场景只检查释放行为，不限定关闭码。
	readCtx, cancelRead := context.WithTimeout(context.Background(), time.Second)
	defer cancelRead()
	_, _, err = conn.Read(readCtx)
	if err == nil {
		t.Fatal("expected websocket to be closed")
	}
	if readCtx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("websocket read timed out instead of observing server closure: %v", err)
	}
}
