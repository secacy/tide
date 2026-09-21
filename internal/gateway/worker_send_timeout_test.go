package gateway

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWorkerSendTimeoutConfig 验证默认、自定义及非法配置的语义。
func TestWorkerSendTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value, want time.Duration
		invalid     bool
	}{
		{"default", 0, 2 * time.Second, false},
		{"custom", 75 * time.Millisecond, 75 * time.Millisecond, false},
		{"negative", -time.Second, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{WorkerSendTimeout: tc.value})
			if tc.invalid {
				if err == nil || errors.Is(err, ErrWorkerSendTimeout) {
					t.Fatalf("expected configuration error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if g.cfg.WorkerSendTimeout != tc.want {
				t.Fatalf("timeout=%v want %v", g.cfg.WorkerSendTimeout, tc.want)
			}
		})
	}
}

// TestWorkerSendTimeoutResultPriority 固定事件顺序，不靠调度偶然触发竞争。
// 即使 download 的取消错误先到达，也应从 RPC cause 恢复发送超时；服务停止仍优先。
func TestWorkerSendTimeoutResultPriority(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		sendTimeout, stopService, inputExpired bool
		want                                   sessionResultKind
	}{
		{"download_first", true, false, false, resultWorkerSendTimeout},
		{"service_stop_first", true, true, false, resultServerStopping},
		{"send_before_input", true, false, true, resultWorkerSendTimeout},
		{"worker_error", false, false, false, resultWorkerFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			rpcCtx, cancelRPC := context.WithCancelCause(ctx)
			defer cancelRPC(nil)
			if tc.sendTimeout {
				cancelRPC(ErrWorkerSendTimeout)
			}
			if tc.stopService {
				stop()
			}
			s := &session{}
			if tc.inputExpired {
				readCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				s.inputReadCtx = readCtx
			}
			workerErr := status.Error(codes.Canceled, "RPC canceled")
			events := make(chan sessionResult, 1)
			events <- sessionResult{kind: resultWorkerFailed, err: workerErr}
			got := s.waitSessionResult(ctx, rpcCtx, events, make(chan struct{}))
			if got.kind != tc.want {
				t.Fatalf("kind=%v want %v", got.kind, tc.want)
			}
			wantErr := workerErr
			if tc.want == resultWorkerSendTimeout {
				wantErr = ErrWorkerSendTimeout
			}
			if tc.want == resultServerStopping {
				wantErr = context.Canceled
			}
			if !errors.Is(got.err, wantErr) {
				t.Fatalf("error=%v want %v", got.err, wantErr)
			}
		})
	}
}

// blockedSendClient 提供响应取消的流，稳定复现首个 Send 阻塞；真实流控另由 TCP 实验验证。
type blockedSendClient struct {
	ctxReady chan context.Context
	sendDone chan struct{}
}

func (c *blockedSendClient) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	c.ctxReady <- ctx
	return &blockedSendStream{ctx: ctx, sendDone: c.sendDone}, nil
}

type blockedSendStream struct {
	grpc.ClientStream
	ctx      context.Context
	sendDone chan struct{}
}

func (s *blockedSendStream) Send(*asrv1.StreamingRecognizeRequest) error {
	<-s.ctx.Done()
	close(s.sendDone)
	return io.EOF // 符合 Send 可只返回 EOF 的情形，必须保留本地取消原因。
}
func (s *blockedSendStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	<-s.ctx.Done()
	return nil, status.FromContextError(s.ctx.Err()).Err()
}
func (*blockedSendStream) CloseSend() error { return nil }

// TestGatewayWorkerSendTimeout 验证配置实际传到会话、取消原因、1011 关闭和清理。
// 客户端保持连接与结果读取，测试断言前不主动取消服务，也不关闭客户端。
func TestGatewayWorkerSendTimeout(t *testing.T) {
	worker := &blockedSendClient{ctxReady: make(chan context.Context, 1), sendDone: make(chan struct{})}
	serviceCtx, cancelService := context.WithCancel(context.Background())
	g, err := New(serviceCtx, worker, Config{WorkerSendTimeout: 40 * time.Millisecond})
	if err != nil {
		cancelService()
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		g.StopAccepting()
		cancelService()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.Read(ctx)
	var closeErr websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.StatusInternalError || closeErr.Reason != "worker send timeout" {
		t.Fatalf("unexpected close: %v", err)
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case rpcCtx := <-worker.ctxReady:
		if !errors.Is(context.Cause(rpcCtx), ErrWorkerSendTimeout) {
			t.Fatalf("RPC cause=%v", context.Cause(rpcCtx))
		}
	default:
		t.Fatal("RPC was not created")
	}
	select {
	case <-worker.sendDone:
	default:
		t.Fatal("Send did not exit")
	}
	if serviceCtx.Err() != nil {
		t.Fatal("single stream timeout canceled the service")
	}
}

// TestUploadWorkerSendTimeout 验证 upload 本身报告独立超时结果，不进入 Send EOF 等待路径。
func TestUploadWorkerSendTimeout(t *testing.T) {
	result := make(chan sessionResult, 1)
	conn := dialInputReader(t, context.Background(), time.Second, func(wsCtx context.Context, s *session) {
		rpcCtx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		s.workerSendTimeout = 30 * time.Millisecond
		stream := &blockedSendStream{ctx: rpcCtx, sendDone: make(chan struct{})}
		result <- s.upload(wsCtx, rpcCtx, cancel, stream, make(chan struct{}))
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.kind != resultWorkerSendTimeout || !errors.Is(got.err, ErrWorkerSendTimeout) {
			t.Fatalf("result=%+v", got)
		}
	case <-ctx.Done():
		t.Fatal("upload did not return send timeout")
	}
}
