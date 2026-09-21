package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// normalEndWorker 在输入结束前返回片段结果，在输入 EOF 后按测试指令返回尾部结果。
// 它只服务一个会话，确保测试能准确控制“输入结束”和“识别结束”的先后关系。
type normalEndWorker struct {
	asrv1.UnimplementedASRServiceServer
	inputEnded  chan struct{} // Worker 观察到请求流 EOF 后关闭。
	releaseTail chan struct{} // 测试关闭此通道后，Worker 才发送尾部结果。
	finished    chan error    // 容量为 1，报告识别方法的退出原因。
}

// StreamingRecognize 接收两块音频，并在尾部结果全部发送后正常返回。
func (w *normalEndWorker) StreamingRecognize(
	stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse],
) (err error) {
	defer func() { w.finished <- err }()
	for i, response := range []*asrv1.StreamingRecognizeResponse{
		{SegmentId: "segment-1", Text: "第一段定稿", IsFinal: true},
		{SegmentId: "segment-2", Text: "第二段中间结果", IsFinal: false},
	} {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(request.GetData()) != fmt.Sprintf("audio-%d", i+1) {
			return fmt.Errorf("unexpected audio chunk %d: %q", i+1, request.GetData())
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected input EOF after two chunks, got: %v", err)
	}
	close(w.inputEnded)
	select {
	case <-w.releaseTail:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, response := range []*asrv1.StreamingRecognizeResponse{
		{SegmentId: "segment-2", Text: "第二段定稿", IsFinal: true},
		{SegmentId: "segment-3", Text: "尾部补充结果", IsFinal: true},
	} {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

// TestGatewayNormalEndPreservesTail 验证片段定稿不会结束会话，合法 end 只半关闭输入，
// 尾部结果完整有序返回后才正常关闭；延迟场景还验证输入空闲期限不作用于收尾阶段。
func TestGatewayNormalEndPreservesTail(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		name := "immediate_tail"
		if delayed {
			name = "tail_after_input_idle_deadline"
		}
		t.Run(name, func(t *testing.T) {
			worker := &normalEndWorker{
				inputEnded: make(chan struct{}), releaseTail: make(chan struct{}), finished: make(chan error, 1),
			}
			workerClient := newTestWorkerClient(t, worker)
			sessionCtx, cancelSessions := context.WithCancel(context.Background())
			t.Cleanup(cancelSessions)
			const idleTimeout = 250 * time.Millisecond
			// 尾部延迟 500ms 也超过发送期限，验证该期限不会误变成整条 RPC 的期限。
			g, err := New(sessionCtx, workerClient, Config{InputIdleTimeout: idleTimeout, WorkerSendTimeout: 100 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(g)
			var conn *websocket.Conn
			t.Cleanup(func() {
				// 清理仅在断言完成或失败后执行，正常结束不能依赖主动取消。
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
			for i, expected := range []wsprotocol.ResultMessage{
				{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-1", Text: "第一段定稿", IsFinal: true},
				{Type: wsprotocol.MessageTypeResult, SegmentID: "segment-2", Text: "第二段中间结果", IsFinal: false},
			} {
				// 第二块音频在第一段定稿后才发送，验证 is_final 不代表整场结束。
				if err := conn.Write(clientCtx, websocket.MessageBinary, []byte(fmt.Sprintf("audio-%d", i+1))); err != nil {
					t.Fatalf("send audio %d: %v", i+1, err)
				}
				assertRecognitionResult(t, clientCtx, conn, expected)
			}
			if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
				t.Fatalf("send end: %v", err)
			}
			select {
			case <-worker.inputEnded:
			case err := <-worker.finished:
				t.Fatalf("worker exited before input EOF: %v", err)
			case <-clientCtx.Done():
				t.Fatal("worker did not observe input EOF")
			}
			if delayed {
				// 从确认 EOF 开始跨过完整空闲期限，避免 end 尚未处理就开始计时。
				timer := time.NewTimer(2 * idleTimeout)
				defer timer.Stop()
				select {
				case <-timer.C:
				case err := <-worker.finished:
					t.Fatalf("worker exited while waiting to send tail: %v", err)
				case <-clientCtx.Done():
					t.Fatal("test timed out while delaying tail")
				}
			}
			close(worker.releaseTail)
			assertRecognitionResult(t, clientCtx, conn, wsprotocol.ResultMessage{
				Type: wsprotocol.MessageTypeResult, SegmentID: "segment-2", Text: "第二段定稿", IsFinal: true,
			})
			assertRecognitionResult(t, clientCtx, conn, wsprotocol.ResultMessage{
				Type: wsprotocol.MessageTypeResult, SegmentID: "segment-3", Text: "尾部补充结果", IsFinal: true,
			})
			// 下一条必须是正常关闭；多余结果、提前断开或异常关闭都不能通过。
			_, _, err = conn.Read(clientCtx)
			if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
				t.Fatalf("expected close 1000 after all results, got: %v", err)
			}
			select {
			case err := <-worker.finished:
				if err != nil {
					t.Fatalf("worker did not finish normally: %v", err)
				}
			case <-clientCtx.Done():
				t.Fatal("worker did not finish")
			}
			// StopAccepting 不取消已有会话，仅让 Wait 能观察最终计数归零。
			g.StopAccepting()
			if err := g.Wait(clientCtx); err != nil {
				t.Fatalf("session cleanup did not finish: %v", err)
			}
			if sessionCtx.Err() != nil {
				t.Fatal("normal completion depended on service cancellation")
			}
		})
	}
}

// assertRecognitionResult 验证完整结果及其消息类型，连续调用同时验证返回顺序。
func assertRecognitionResult(t *testing.T, ctx context.Context, conn *websocket.Conn, want wsprotocol.ResultMessage) {
	t.Helper()
	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read result for %s: %v", want.SegmentID, err)
	}
	var got wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if typ != websocket.MessageText || got != want {
		t.Fatalf("result type=%v body=%+v, want %+v", typ, got, want)
	}
}
