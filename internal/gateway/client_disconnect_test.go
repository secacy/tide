package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// isolationWorkerExit 保存 Worker 退出时的结果及 context 状态。
// 分别检查它们，避免把正常输入 EOF 当成 RPC 取消。
type isolationWorkerExit struct {
	err        error // 识别方法的返回错误。
	contextErr error // 识别方法返回前的 RPC context 状态。
}

// isolationWorker 用测试音频中的前缀区分两个会话，并回显各自的数据。
// exits 在启动前初始化，之后只读；每个通道容量为 1，对应一个会话。
type isolationWorker struct {
	asrv1.UnimplementedASRServiceServer
	exits map[string]chan isolationWorkerExit // 测试观测通道，不属于生产会话注册表。
	// stopAfterFirstResult 可选地在首次响应后等待退出指令。
	// 通道收到 nil 表示提前正常 EOF，非 nil 表示 Worker 主动报错；map 初始化后只读。
	stopAfterFirstResult map[string]<-chan error
}

// StreamingRecognize 持续读取音频；正常输入 EOF 后返回尾部结果，断开时返回 RPC 错误。
func (w *isolationWorker) StreamingRecognize(
	stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse],
) (err error) {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	id, _, ok := strings.Cut(string(request.GetData()), ":")
	exit, exists := w.exits[id]
	if !ok || !exists {
		return fmt.Errorf("unknown test session: %q", request.GetData())
	}
	defer func() { exit <- isolationWorkerExit{err: err, contextErr: stream.Context().Err()} }()
	for {
		if !strings.HasPrefix(string(request.GetData()), id+":") {
			return fmt.Errorf("audio crossed session boundary: %q", request.GetData())
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{
			SegmentId: id, Text: string(request.GetData()),
		}); err != nil {
			return err
		}
		if stop := w.stopAfterFirstResult[id]; stop != nil {
			select {
			case err := <-stop:
				return err
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		}
		request, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.Send(&asrv1.StreamingRecognizeResponse{
				SegmentId: id, Text: id + ":tail", IsFinal: true,
			})
		}
		if err != nil {
			return err
		}
	}
}

// TestGatewayClientDisconnectIsolated 验证 A 直接断开后，其 RPC 和会话退出，
// 同一 Gateway、同一 gRPC 客户端连接上的 B 仍可继续识别并正常结束。
func TestGatewayClientDisconnectIsolated(t *testing.T) {
	worker := &isolationWorker{exits: map[string]chan isolationWorkerExit{
		"a": make(chan isolationWorkerExit, 1),
		"b": make(chan isolationWorkerExit, 1),
	}}
	workerClient := newTestWorkerClient(t, worker)
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)
	g, err := New(sessionCtx, workerClient, Config{})
	if err != nil {
		t.Fatal(err)
	}
	// handler 返回晚于会话清理和 tracker.leave，可作为清理完成的同步点。
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

	// A 不发送 end 或关闭帧，直接关闭传输连接；服务及 B 均保持运行。
	if err := clients["a"].CloseNow(); err != nil {
		t.Fatalf("disconnect client a: %v", err)
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
	defer cancelCleanup()
	select {
	case exit := <-worker.exits["a"]:
		if exit.err == nil || !errors.Is(exit.contextErr, context.Canceled) {
			t.Fatalf("worker a did not observe RPC cancellation: %+v", exit)
		}
	case <-cleanupCtx.Done():
		t.Fatal("worker a did not exit after client disconnect")
	}
	select {
	case <-handlerDone["a"]:
	case <-cleanupCtx.Done():
		t.Fatal("session a did not finish cleanup")
	}
	g.tracker.mu.Lock()
	active := g.tracker.active
	g.tracker.mu.Unlock()
	if active != 1 {
		t.Fatalf("active sessions after a disconnected = %d, want 1", active)
	}
	if sessionCtx.Err() != nil {
		t.Fatalf("client disconnect canceled the service context: %v", sessionCtx.Err())
	}

	// 清理 A 完成后，B 仍能完成一轮新的双向传输，结果不得串入 A 的数据。
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
}
