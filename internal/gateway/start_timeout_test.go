package gateway

import (
	"context"
	"encoding/json"
	"errors"
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

// TestGatewayStartTimeoutConfig 验证启动期限的默认值、自定义值和无效值。
func TestGatewayStartTimeoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "default", want: 10 * time.Second},
		{name: "custom", input: 2 * time.Second, want: 2 * time.Second},
		{name: "negative", input: -time.Second, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{StartTimeout: tc.input})
			if tc.wantErr {
				if err == nil {
					t.Fatal("negative start timeout was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("create gateway: %v", err)
			}
			if g.cfg.StartTimeout != tc.want {
				t.Fatalf("start timeout = %v, want %v", g.cfg.StartTimeout, tc.want)
			}
		})
	}
}

// TestGatewayStartTimeoutReleasesSession 验证服务未取消时，未发送 start 的连接
// 能自行超时、关闭并退出登记；StopAccepting 只用于允许 Wait 观察计数归零。
func TestGatewayStartTimeoutReleasesSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	worker := &recordingWorker{}
	g, err := New(ctx, worker, Config{StartTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	var conn *websocket.Conn
	t.Cleanup(func() {
		cancel()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClient()
	conn, _, err = websocket.Dial(clientCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	g.StopAccepting()
	if err := g.Wait(clientCtx); err != nil {
		t.Fatalf("session did not exit on start timeout: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("service context was canceled before the timeout assertion")
	}
	if worker.called.Load() {
		t.Fatal("worker was called without start")
	}
	_, _, err = conn.Read(clientCtx)
	if err == nil || clientCtx.Err() != nil {
		t.Fatalf("expected server closure before client deadline, got: %v", err)
	}
}

// dialTestSession 通过真实 WebSocket 运行单个 session，并暴露 run 的返回值。
// result 容量为 1，确保测试失败时 handler 不会阻塞在发送结果上。
// parent 决定服务生命周期；客户端使用独立 context。
func dialTestSession(t *testing.T, parent context.Context, worker asrv1.ASRServiceClient, timeout time.Duration) (*websocket.Conn, <-chan error) {
	t.Helper()
	return dialSessionWithTimeouts(t, parent, worker, timeout, defaultInputIdleTimeout)
}

// dialSessionWithTimeouts 允许分别设置启动期限和输入空闲期限，其他行为与 dialTestSession 相同。
func dialSessionWithTimeouts(t *testing.T, parent context.Context, worker asrv1.ASRServiceClient, startTimeout, inputIdleTimeout time.Duration) (*websocket.Conn, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	result := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			result <- err
			return
		}
		defer ws.CloseNow()
		result <- newSession(ws, worker, startTimeout, inputIdleTimeout, defaultWorkerSendTimeout, defaultTailTimeout, defaultResultWriteTimeout, 0).run(ctx)
	}))
	var conn *websocket.Conn
	t.Cleanup(func() {
		cancel()
		if conn != nil {
			_ = conn.CloseNow()
		}
		server.Close()
	})
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDial()
	var err error
	conn, _, err = websocket.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("connect websocket: %v", err)
	}
	return conn, result
}

// TestSessionStartTimeoutCause 区分启动期限到期、服务主动取消和服务期限到期。
func TestSessionStartTimeoutCause(t *testing.T) {
	for _, name := range []string{"start_timeout", "parent_canceled", "parent_deadline"} {
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			startTimeout := 50 * time.Millisecond
			want := ErrStartTimeout
			if name == "parent_canceled" {
				startTimeout = time.Second
				want = context.Canceled
			}
			if name == "parent_deadline" {
				var cancelDeadline context.CancelFunc
				parent, cancelDeadline = context.WithTimeout(parent, 50*time.Millisecond)
				defer cancelDeadline()
				startTimeout = time.Second
				want = context.DeadlineExceeded
			}
			worker := &recordingWorker{}
			_, result := dialTestSession(t, parent, worker, startTimeout)
			if name == "parent_canceled" {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Errorf("session error = %v, want %v", err, want)
				}
				if name != "start_timeout" && errors.Is(err, ErrStartTimeout) {
					t.Error("parent context termination was misclassified as start timeout")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("session did not exit")
			}
			if worker.called.Load() {
				t.Error("worker was called without start")
			}
		})
	}
}

// startProbeWorker 暴露建流时的 context，并将一块音频回显为识别结果。
// 用于验证 start 读取的期限没有传递给后续 RPC。
type startProbeWorker struct {
	opened chan context.Context // 容量为 1，通知测试合法 start 已处理且 RPC 已创建。
}

// StreamingRecognize 保存 RPC context，返回仅实现本测试所用操作的 stream。
func (w *startProbeWorker) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	w.opened <- ctx
	return &startProbeStream{ctx: ctx, responses: make(chan *asrv1.StreamingRecognizeResponse, 1)}, nil
}

// startProbeStream 模拟单次音频转发及其响应，不需要真实 ASR 计算。
type startProbeStream struct {
	grpc.ClientStream                                        // 未使用的元数据操作不属于当前测试范围。
	ctx               context.Context                        // 实际建流 context，控制 Send 和 Recv 的退出。
	responses         chan *asrv1.StreamingRecognizeResponse // 容量为 1，传递回显响应。
}

// Send 回显音频内容，供客户端验证启动期限之后仍能完成双向转发。
func (s *startProbeStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	select {
	case s.responses <- &asrv1.StreamingRecognizeResponse{Text: string(req.GetData())}:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Recv 等待回显响应或 RPC 取消。
func (s *startProbeStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	select {
	case response := <-s.responses:
		return response, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// CloseSend 满足接口；此测试通过服务取消结束，不发送 end。
func (s *startProbeStream) CloseSend() error { return nil }

// TestSessionStartTimeoutDoesNotLimitStreaming 验证合法 start 后，超过原启动期限
// 仍可发送音频并接收结果；最后通过服务取消收尾。
func TestSessionStartTimeoutDoesNotLimitStreaming(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	worker := &startProbeWorker{opened: make(chan context.Context, 1)}
	const startTimeout = 100 * time.Millisecond
	conn, result := dialTestSession(t, parent, worker, startTimeout)
	clientCtx, cancelClient := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClient()
	if err := conn.Write(clientCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatalf("send start: %v", err)
	}
	var rpcCtx context.Context
	select {
	case rpcCtx = <-worker.opened:
	case <-clientCtx.Done():
		t.Fatal("worker stream was not opened")
	}
	// 这里刻意跨过启动期限，是测试目标本身；建流就绪由 opened 通知保证。
	timer := time.NewTimer(2 * startTimeout)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-rpcCtx.Done():
		t.Fatalf("start timeout canceled the RPC: %v", rpcCtx.Err())
	}
	if rpcCtx.Err() != nil {
		t.Fatalf("RPC is no longer active: %v", rpcCtx.Err())
	}
	if err := conn.Write(clientCtx, websocket.MessageBinary, []byte("audio-after-start-deadline")); err != nil {
		t.Fatalf("send audio after start deadline: %v", err)
	}
	_, data, err := conn.Read(clientCtx)
	if err != nil {
		t.Fatalf("read result after start deadline: %v", err)
	}
	var message wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &message); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if message.Type != wsprotocol.MessageTypeResult || message.Text != "audio-after-start-deadline" {
		t.Fatalf("unexpected recognition result: %+v", message)
	}
	cancel()
	_, _, err = conn.Read(clientCtx)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("expected websocket close 1001, got: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected service cancellation, got: %v", err)
		}
	case <-clientCtx.Done():
		t.Fatal("session did not finish cleanup")
	}
}
