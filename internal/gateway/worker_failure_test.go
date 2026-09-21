package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGatewayWorkerFailureIsolated 验证 Worker 主动报错和合法 end 前的正常 EOF
// 均被视为会话失败，同时不影响共享 Gateway 和 gRPC 连接的健康会话。
func TestGatewayWorkerFailureIsolated(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error // nil 模拟 Worker 提前正常返回；非 nil 模拟显式 gRPC 失败。
	}{
		{name: "grpc_error", err: status.Error(codes.Unavailable, "injected worker failure")},
		{name: "eof_before_end"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stopA := make(chan error, 1) // 两个会话均完成双向传输后，才允许 A 的 Worker 退出。
			worker := &isolationWorker{
				exits: map[string]chan isolationWorkerExit{
					"a": make(chan isolationWorkerExit, 1), "b": make(chan isolationWorkerExit, 1),
				},
				stopAfterFirstResult: map[string]<-chan error{"a": stopA},
			}
			workerClient := newTestWorkerClient(t, worker)
			sessionCtx, cancelSessions := context.WithCancel(context.Background())
			t.Cleanup(cancelSessions)
			g, err := New(sessionCtx, workerClient, Config{})
			if err != nil {
				t.Fatal(err)
			}
			// handler 返回发生在 tracker.leave 之后，可验证 A 已独立完成清理。
			handlerDone := map[string]chan struct{}{"a": make(chan struct{}), "b": make(chan struct{})}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				done, ok := handlerDone[strings.TrimPrefix(r.URL.Path, "/")]
				if !ok {
					http.NotFound(w, r)
					return
				}
				defer close(done)
				g.ServeHTTP(w, r)
			}))
			clients := make(map[string]*websocket.Conn)
			t.Cleanup(func() {
				// 断言前不关闭客户端、不取消服务、不停止 gRPC 服务端。
				g.StopAccepting()
				cancelSessions()
				for _, conn := range clients {
					_ = conn.CloseNow()
				}
				server.Close()
			})
			clientCtx, cancelClient := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelClient()
			for _, id := range []string{"a", "b"} {
				conn, _, err := websocket.Dial(clientCtx, "ws"+strings.TrimPrefix(server.URL, "http")+"/"+id, nil)
				if err != nil {
					t.Fatalf("connect client %s: %v", id, err)
				}
				clients[id] = conn
				if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
					t.Fatalf("send start for %s: %v", id, err)
				}
				if err := conn.Write(clientCtx, websocket.MessageBinary, []byte(id+":first")); err != nil {
					t.Fatalf("send first audio for %s: %v", id, err)
				}
				assertRecognitionResult(t, clientCtx, conn, wsprotocol.ResultMessage{
					Type: wsprotocol.MessageTypeResult, SegmentID: id, Text: id + ":first",
				})
			}

			// A 没有发送 end；Worker 的两种退出都必须转化为异常关闭，而非 1000。
			stopA <- tc.err
			failureCtx, cancelFailure := context.WithTimeout(context.Background(), time.Second)
			defer cancelFailure()
			_, _, err = clients["a"].Read(failureCtx)
			if websocket.CloseStatus(err) != websocket.StatusInternalError {
				t.Fatalf("expected close 1011 for worker failure, got: %v", err)
			}
			select {
			case exit := <-worker.exits["a"]:
				if status.Code(exit.err) != status.Code(tc.err) || exit.contextErr != nil {
					t.Fatalf("unexpected worker exit: %+v, injected error: %v", exit, tc.err)
				}
			case <-failureCtx.Done():
				t.Fatal("worker a did not exit")
			}
			select {
			case <-handlerDone["a"]:
			case <-failureCtx.Done():
				t.Fatal("session a did not finish cleanup")
			}
			g.tracker.mu.Lock()
			active := g.tracker.active
			g.tracker.mu.Unlock()
			if active != 1 {
				t.Fatalf("active sessions after worker failure = %d, want 1", active)
			}
			if sessionCtx.Err() != nil {
				t.Fatalf("worker failure canceled the service: %v", sessionCtx.Err())
			}

			// A 清理完成后才继续 B，证明健康会话仍可传输和正常结束。
			b := clients["b"]
			if err := b.Write(clientCtx, websocket.MessageBinary, []byte("b:second")); err != nil {
				t.Fatalf("send audio on surviving session: %v", err)
			}
			assertRecognitionResult(t, clientCtx, b, wsprotocol.ResultMessage{
				Type: wsprotocol.MessageTypeResult, SegmentID: "b", Text: "b:second",
			})
			if err := b.Write(clientCtx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
				t.Fatalf("send end on surviving session: %v", err)
			}
			assertRecognitionResult(t, clientCtx, b, wsprotocol.ResultMessage{
				Type: wsprotocol.MessageTypeResult, SegmentID: "b", Text: "b:tail", IsFinal: true,
			})
			_, _, err = b.Read(clientCtx)
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				t.Fatalf("expected normal close for b, got: %v", err)
			}
			select {
			case exit := <-worker.exits["b"]:
				if exit.err != nil || exit.contextErr != nil {
					t.Fatalf("worker b did not finish normally: %+v", exit)
				}
			case <-clientCtx.Done():
				t.Fatal("worker b did not exit")
			}
			g.StopAccepting()
			if err := g.Wait(clientCtx); err != nil {
				t.Fatalf("sessions did not finish cleanup: %v", err)
			}
		})
	}
}
