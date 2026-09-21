package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// shutdownWorker 为单会话测试提供可观察的真实 gRPC 服务端。
// 收到一块音频后返回一条中间结果，可选地确认输入 EOF，然后等待 RPC 被取消。
type shutdownWorker struct {
	asrv1.UnimplementedASRServiceServer
	received   chan []byte   // 容量为 1，记录收到的音频，不阻塞 Worker。
	finished   chan error    // 容量为 1，在识别方法退出时记录退出原因。
	inputEnded chan struct{} // 非 nil 时，等待输入 EOF 并关闭此通道通知测试；响应流仍保持打开。
}

// StreamingRecognize 保持响应流打开，确保会话因服务取消而非正常 EOF 结束。
func (w *shutdownWorker) StreamingRecognize(
	stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse],
) (err error) {
	defer func() { w.finished <- err }()

	request, err := stream.Recv()
	if err != nil {
		return err
	}
	w.received <- request.GetData()
	if err := stream.Send(&asrv1.StreamingRecognizeResponse{
		SegmentId: "segment-1",
		Text:      "正在识别",
		IsFinal:   false,
	}); err != nil {
		return err
	}

	if w.inputEnded != nil {
		// 请求流 EOF 只说明音频输入结束，不能让识别方法立即返回 nil。
		// 保持响应流打开，模拟 Worker 仍在生成尾部结果。
		_, err := stream.Recv()
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("expected input EOF after one audio chunk, got: %v", err)
		}
		if err := stream.Context().Err(); err != nil {
			return err
		}
		close(w.inputEnded)
	}

	<-stream.Context().Done()
	return stream.Context().Err()
}

// TestGatewayShutdownDuringStreaming 验证双向传输建立后停止服务，
// Worker 能收到 RPC 取消，客户端收到 1001，且会话完成退出登记。
func TestGatewayShutdownDuringStreaming(t *testing.T) {
	testGatewayShutdown(t, false)
}

// TestGatewayShutdownAfterEnd 验证 Worker 已收到输入 EOF、但尚未返回尾部结果时，
// 服务取消仍能终止 RPC 和 WebSocket，并完成会话退出登记。
func TestGatewayShutdownAfterEnd(t *testing.T) {
	testGatewayShutdown(t, true)
}

// testGatewayShutdown 复用连接建立、双向传输和清理断言。
// afterEnd 为 true 时，在取消前发送 end，并等待 Worker 确认输入 EOF。
func testGatewayShutdown(t *testing.T, afterEnd bool) {
	t.Helper()
	worker := &shutdownWorker{
		received: make(chan []byte, 1),
		finished: make(chan error, 1),
	}
	if afterEnd {
		worker.inputEnded = make(chan struct{})
	}

	workerClient := newTestWorkerClient(t, worker)

	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)
	g, err := New(sessionCtx, workerClient, Config{})
	if err != nil {
		t.Fatalf("create gateway: %v", err)
	}
	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		// 仅在断言完成或失败后执行，避免主动断开客户端掩盖取消失效。
		g.StopAccepting()
		cancelSessions()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})

	// 客户端不继承 sessionCtx，始终由服务端发起本次关闭。
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelClient()
	conn, _, err = websocket.Dial(clientCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatalf("send start: %v", err)
	}
	audio := []byte{1, 2, 3, 4}
	if err := conn.Write(clientCtx, websocket.MessageBinary, audio); err != nil {
		t.Fatalf("send audio: %v", err)
	}

	// 收到中间结果作为同步点，证明音频和结果已完成双向转发，无需 Sleep。
	messageType, data, err := conn.Read(clientCtx)
	if err != nil {
		t.Fatalf("read recognition result: %v", err)
	}
	var result wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode recognition result: %v", err)
	}
	if messageType != websocket.MessageText || result.Type != wsprotocol.MessageTypeResult ||
		result.SegmentID != "segment-1" || result.Text != "正在识别" || result.IsFinal {
		t.Fatalf("unexpected recognition result: type=%v result=%+v", messageType, result)
	}
	select {
	case received := <-worker.received:
		if !bytes.Equal(received, audio) {
			t.Fatalf("worker received audio %v, want %v", received, audio)
		}
	default:
		t.Fatal("worker returned a result without recording audio")
	}

	if afterEnd {
		if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
			t.Fatalf("send end: %v", err)
		}
		// 写入 end 成功不等于 Gateway 已经执行 CloseSend。
		// 必须等到 Worker 观察到 EOF，才能确定取消发生在等待尾部结果的阶段。
		select {
		case <-worker.inputEnded:
		case err := <-worker.finished:
			t.Fatalf("worker exited before confirming input EOF: %v", err)
		case <-clientCtx.Done():
			t.Fatalf("worker did not observe input EOF: %v", clientCtx.Err())
		}
	}

	// 两个场景中 Worker 都不主动完成，让服务取消成为唯一的停止原因。
	g.StopAccepting()
	cancelSessions()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	// 先读取关闭帧，客户端库会回复关闭握手，服务端才能完成正常收尾。
	_, _, err = conn.Read(shutdownCtx)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("expected websocket close 1001, got: %v", err)
	}
	if err := g.Wait(shutdownCtx); err != nil {
		t.Fatalf("wait for session cleanup: %v", err)
	}
	select {
	case err := <-worker.finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected Worker context cancellation, got: %v", err)
		}
	case <-shutdownCtx.Done():
		t.Fatal("worker did not exit after service cancellation")
	}
}

// newTestWorkerClient 用内存监听器运行真实 gRPC 服务，保留半关闭、EOF 和取消语义。
// 客户端连接和服务端仅在测试清理时关闭，避免替被测会话提前释放 RPC。
func newTestWorkerClient(t *testing.T, worker asrv1.ASRServiceServer) asrv1.ASRServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	rpcServer := grpc.NewServer()
	asrv1.RegisterASRServiceServer(rpcServer, worker)
	serveDone := make(chan struct{}) // 通知服务的 Serve goroutine 已退出。
	go func() {
		defer close(serveDone)
		_ = rpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		rpcServer.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	rpcConn, err := grpc.NewClient("passthrough:///test-worker",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("create worker client: %v", err)
	}
	t.Cleanup(func() { _ = rpcConn.Close() })
	return asrv1.NewASRServiceClient(rpcConn)
}
