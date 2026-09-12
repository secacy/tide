package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestGatewayRejectsInvalidCapacity(t *testing.T) {
	worker := &unusedGatewayWorker{}
	for _, limit := range []int{0, -1} {
		g, err := New(context.Background(), worker, nil, Config{MaxSessions: limit})
		if g != nil || !errors.Is(err, errInvalidMaxSessions) {
			t.Fatalf("New(MaxSessions=%d) = (%v, %v), want invalid capacity", limit, g, err)
		}
	}
}

// 在升级失败响应写出时检查登记，证明失败的升级流程也受到管理。
func TestGatewayRegistersBeforeUpgradeAndReleasesFailure(t *testing.T) {
	worker := &unusedGatewayWorker{}
	g, err := New(context.Background(), worker, nil, Config{MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	ids := make(map[string]bool)
	for range 2 {
		observed := false
		var registered *session
		response := &observedGatewayResponse{
			ResponseRecorder: httptest.NewRecorder(),
			beforeHeader: func() {
				observed = true
				sessions := g.registry.snapshot()
				if len(sessions) != 1 || sessions[0].id == "" {
					t.Fatalf("upgrade response must have one registered session, got %v", sessions)
				}
				if sessions[0].ws != nil {
					t.Fatal("failed upgrade unexpectedly bound a WebSocket")
				}
				registered = sessions[0]
				if ids[sessions[0].id] {
					t.Fatal("successive requests reused a session ID")
				}
				ids[sessions[0].id] = true
			},
		}
		// 普通 GET 缺少 WebSocket 握手头，Accept 应拒绝升级。
		g.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/asr", nil))
		if !observed || response.Code < 400 || response.Code >= 500 {
			t.Fatalf("upgrade rejection = %d, observed registration = %v", response.Code, observed)
		}
		assertRegistrySessions(t, g.registry)
		if registered == nil || registered.ctx.Err() == nil {
			t.Fatal("failed upgrade did not release the Session Context")
		}
	}
	if worker.calls.Load() != 0 {
		t.Fatal("failed upgrade opened a Worker stream")
	}
}

// 未发送 start 的连接也占用名额；拒绝新请求不能影响已有连接。
func TestGatewayWaitingForStartConsumesCapacity(t *testing.T) {
	worker := &unusedGatewayWorker{}
	h := newGatewayHarness(t, worker, 1)
	first := h.mustDial(t)
	firstID := h.onlySessionID(t)
	h.expectHTTPStatus(t, http.StatusServiceUnavailable)
	h.waitHandlers(t, 1) // 被拒绝的请求已返回，已有连接仍在等待 start。
	if got := h.onlySessionID(t); got != firstID {
		t.Fatalf("rejected request changed active session: %q -> %q", firstID, got)
	}
	if worker.calls.Load() != 0 {
		t.Fatal("Worker stream opened before start")
	}

	_ = first.CloseNow()
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
	replacement := h.mustDial(t)
	if got := h.onlySessionID(t); got == firstID {
		t.Fatal("replacement connection reused the previous session ID")
	}
	_ = replacement.CloseNow()
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

// 真实 Mock Worker 的 final 必须先到达，连接结束后名额才能重新使用。
func TestGatewayNormalCompletionReleasesCapacity(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{
		PartialEvery: 100 * time.Millisecond,
		PartialTexts: []string{"partial"},
		FinalText:    "final",
	}))
	h := newGatewayHarness(t, worker, 1)
	for range 2 {
		conn := h.mustDial(t)
		h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
		h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
		h.expectResult(t, conn, "partial", false)
		h.onlySessionID(t)
		h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
		h.expectResult(t, conn, "final", true)
		h.expectClose(t, conn, websocket.StatusNormalClosure)
		h.waitHandlers(t, 1)
		assertRegistrySessions(t, h.gateway.registry)
	}
}

func TestGatewayInvalidStartReleasesCapacity(t *testing.T) {
	worker := &unusedGatewayWorker{}
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"unsupported"}`))
	h.expectClose(t, conn, websocket.StatusPolicyViolation)
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
	if worker.calls.Load() != 0 {
		t.Fatal("invalid start opened a Worker stream")
	}
}

// 在传音频和等待尾部结果两个阶段断开客户端，都应取消 RPC 并释放登记。
func TestGatewayClientDisconnectCancelsWorker(t *testing.T) {
	for _, afterEnd := range []bool{false, true} {
		name := "streaming"
		if afterEnd {
			name = "waiting_for_final"
		}
		t.Run(name, func(t *testing.T) {
			ready, workerDone := make(chan struct{}), make(chan struct{})
			worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
				defer close(workerDone)
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if afterEnd {
					if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
						return status.Errorf(codes.Internal, "expected input EOF, got %v", err)
					}
				}
				close(ready)
				<-stream.Context().Done()
				return stream.Context().Err()
			}})
			h := newGatewayHarness(t, worker, 1)
			conn := h.mustDial(t)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
			if afterEnd {
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
			}
			awaitGatewaySignal(t, h.ctx, ready, "Worker reached test stage")
			h.onlySessionID(t)
			_ = conn.CloseNow()
			h.waitHandlers(t, 1)
			awaitGatewaySignal(t, h.ctx, workerDone, "Worker canceled")
			assertRegistrySessions(t, h.gateway.registry)
		})
	}
}

func TestGatewayWorkerFailureReleasesCapacity(t *testing.T) {
	workerDone := make(chan struct{})
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		defer close(workerDone)
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "injected Worker failure")
	}})
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	h.expectClose(t, conn, websocket.StatusInternalError)
	h.waitHandlers(t, 1)
	awaitGatewaySignal(t, h.ctx, workerDone, "Worker exited")
	assertRegistrySessions(t, h.gateway.registry)
}

// 停止接入只影响后续登记，已经接纳的连接仍能完成正常识别。
func TestGatewayStopAcceptingPreservesExistingSession(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: "final"}))
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	id := h.onlySessionID(t)
	h.gateway.StopAccepting()
	h.gateway.StopAccepting() // 重复停止不会影响已有会话。
	h.expectHTTPStatus(t, http.StatusServiceUnavailable)
	h.waitHandlers(t, 1)
	if got := h.onlySessionID(t); got != id {
		t.Fatalf("stopAccepting replaced session %q with %q", id, got)
	}
	assertRegistryNotDrained(t, h.gateway.registry)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectResult(t, conn, "final", true)
	h.expectClose(t, conn, websocket.StatusNormalClosure)
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
	if err := h.gateway.Wait(h.ctx); err != nil {
		t.Fatalf("Wait after session cleanup: %v", err)
	}
}

// 取消或耗尽等待预算后，原有连接仍应继续接收识别结果并正常完成。
func TestGatewayWaitCancellationPreservesStreamingSession(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline_exceeded"
		}
		t.Run(name, func(t *testing.T) {
			worker := startGatewayWorker(t, mockasr.New(mockasr.Config{
				PartialEvery: 100 * time.Millisecond,
				PartialTexts: []string{"first", "second"},
				FinalText:    "final",
			}))
			h := newGatewayHarness(t, worker, 1)
			conn := h.mustDial(t)
			id := h.onlySessionID(t)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
			h.expectResult(t, conn, "first", false)
			h.gateway.StopAccepting()

			// 显式取消或使用已过期的期限，无需通过 sleep 猜测超时发生的时刻。
			waitCtx, cancel := context.WithCancel(h.ctx)
			want := context.Canceled
			if deadline {
				cancel()
				waitCtx, cancel = context.WithDeadline(h.ctx, time.Now().Add(-time.Second))
				want = context.DeadlineExceeded
			} else {
				cancel()
			}
			defer cancel()
			if err := h.gateway.Wait(waitCtx); !errors.Is(err, want) {
				t.Fatalf("Wait = %v, want %v while session remains active", err, want)
			}
			if got := h.onlySessionID(t); got != id {
				t.Fatalf("Wait changed active session: %q -> %q", id, got)
			}

			// 不仅检查登记仍在，还确认取消等待后真实音频链路继续工作。
			h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
			h.expectResult(t, conn, "second", false)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
			h.expectResult(t, conn, "final", true)
			h.expectClose(t, conn, websocket.StatusNormalClosure)
			h.waitHandlers(t, 1)
			if err := h.gateway.Wait(h.ctx); err != nil {
				t.Fatalf("Wait with fresh budget after cleanup: %v", err)
			}
			assertRegistrySessions(t, h.gateway.registry)
		})
	}
}

// 多个等待者必须等待全部会话注销，不能在第一个会话结束时完成排空。
func TestGatewayWaitersCompleteAfterLastSession(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: "final"}))
	h := newGatewayHarness(t, worker, 2)
	first, last := h.mustDial(t), h.mustDial(t)
	h.gateway.StopAccepting()

	const waiters = 8
	results := make(chan error, waiters)
	for range waiters {
		go func() { results <- h.gateway.Wait(h.ctx) }()
	}

	h.write(t, first, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, first, websocket.MessageBinary, []byte{0, 0})
	h.write(t, first, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectResult(t, first, "final", true)
	h.expectClose(t, first, websocket.StatusNormalClosure)
	h.waitHandlers(t, 1)
	h.onlySessionID(t) // 第二个会话仍在等待 start。
	assertRegistryNotDrained(t, h.gateway.registry)
	select {
	case err := <-results:
		t.Fatalf("Wait returned before last session completed: %v", err)
	default:
	}

	h.write(t, last, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, last, websocket.MessageBinary, []byte{0, 0})
	h.write(t, last, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectResult(t, last, "final", true)
	h.expectClose(t, last, websocket.StatusNormalClosure)
	for range waiters {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("Wait after final session: %v", err)
			}
			// Wait 的返回本身应保证注销已完成，不依赖 handler 完成通知。
			assertRegistrySessions(t, h.gateway.registry)
		case <-h.ctx.Done():
			t.Fatal("waiters did not complete after final session")
		}
	}
	h.waitHandlers(t, 1)
}

// 同时发起真实 WebSocket 握手；全部尝试完成前保留成功连接，防止名额复用干扰断言。
func TestGatewayConcurrentAdmissionLimit(t *testing.T) {
	const limit, attempts = 4, 24
	worker := &unusedGatewayWorker{}
	h := newGatewayHarness(t, worker, limit)
	type dialResult struct {
		conn   *websocket.Conn
		status int
		err    error
	}
	results := make(chan dialResult, attempts)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			conn, response, err := h.dial()
			code := 0
			if response != nil {
				code = response.StatusCode
				if err != nil {
					_ = response.Body.Close()
				}
			}
			results <- dialResult{conn: conn, status: code, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var connected []*websocket.Conn
	for result := range results {
		if result.conn != nil {
			conn := result.conn
			t.Cleanup(func() { _ = conn.CloseNow() })
			connected = append(connected, conn)
			if result.err != nil {
				t.Errorf("successful connection also returned an error: %v", result.err)
			}
		} else if result.err == nil || result.status != http.StatusServiceUnavailable {
			t.Errorf("rejected handshake: status=%d error=%v, want 503", result.status, result.err)
		}
	}
	if len(connected) != limit {
		t.Fatalf("connected = %d, want %d", len(connected), limit)
	}
	if got := len(h.gateway.registry.snapshot()); got != limit {
		t.Fatalf("registered = %d, want %d", got, limit)
	}
	if worker.calls.Load() != 0 {
		t.Fatal("pending clients opened Worker streams before start")
	}
	for _, conn := range connected {
		_ = conn.CloseNow()
	}
	h.waitHandlers(t, attempts)
	assertRegistrySessions(t, h.gateway.registry)
}

// unusedGatewayWorker 用于不应进入识别阶段的测试，记录意外建流次数。
type unusedGatewayWorker struct{ calls atomic.Int32 }

func (w *unusedGatewayWorker) StreamingRecognize(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	w.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "unexpected Worker stream")
}

// observedGatewayResponse 在升级失败响应写出前观察注册表。
type observedGatewayResponse struct {
	*httptest.ResponseRecorder
	beforeHeader func()
}

func (w *observedGatewayResponse) WriteHeader(code int) {
	w.beforeHeader()
	w.ResponseRecorder.WriteHeader(code)
}

// gatewayWorkerServer 使用真实 gRPC 传输执行可控的 Worker 行为。
type gatewayWorkerServer struct {
	asrv1.UnimplementedASRServiceServer
	run func(asrv1.ASRService_StreamingRecognizeServer) error
}

func (w gatewayWorkerServer) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	return w.run(stream)
}

func startGatewayWorker(t *testing.T, worker asrv1.ASRServiceServer) asrv1.ASRServiceClient {
	t.Helper()
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer()
	asrv1.RegisterASRServiceServer(server, worker)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///gateway-worker-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return asrv1.NewASRServiceClient(conn)
}

// gatewayHarness 使用内存连接承载 HTTP/WebSocket，避免依赖外部服务或固定端口。
// finished 在 ServeHTTP 返回后通知测试，确保断言发生在全部 defer 执行之后。
type gatewayHarness struct {
	gateway  *Gateway
	ctx      context.Context
	client   *http.Client
	finished chan struct{}
}

func newGatewayHarness(t *testing.T, worker asrv1.ASRServiceClient, capacity int) *gatewayHarness {
	t.Helper()
	return newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: capacity})
}

func newGatewayHarnessWithConfig(t *testing.T, worker asrv1.ASRServiceClient, cfg Config) *gatewayHarness {
	t.Helper()
	return newGatewayHarnessFactory(t, func(ctx context.Context, logger *slog.Logger) (*Gateway, error) {
		return New(ctx, worker, logger, cfg)
	})
}

func newGatewayHarnessWithPool(t *testing.T, pool *WorkerPool, cfg Config) *gatewayHarness {
	t.Helper()
	return newGatewayHarnessFactory(t, func(ctx context.Context, logger *slog.Logger) (*Gateway, error) {
		return NewWithPool(ctx, pool, logger, cfg)
	})
}

func newGatewayHarnessFactory(t *testing.T, build func(context.Context, *slog.Logger) (*Gateway, error)) *gatewayHarness {
	t.Helper()
	appCtx, cancelApp := context.WithCancel(context.Background())
	t.Cleanup(cancelApp)
	// 生命周期测试显式注入静默日志器，避免依赖全局日志配置。
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	g, err := build(appCtx, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	h := &gatewayHarness{gateway: g, ctx: ctx, finished: make(chan struct{}, 128)}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { h.finished <- struct{}{} }()
		g.ServeHTTP(w, r)
	})}
	listener := bufconn.Listen(64 * 1024)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		_ = server.Serve(listener)
	}()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}}
	h.client = &http.Client{Transport: transport}
	t.Cleanup(func() {
		g.StopAccepting()
		cancelApp()
		_ = server.Close()
		_ = listener.Close()
		transport.CloseIdleConnections()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		awaitGatewaySignal(t, cleanupCtx, serverDone, "HTTP server exited")
		if err := g.Wait(cleanupCtx); err != nil {
			t.Errorf("Gateway cleanup left registered sessions: %v", err)
		}
		for _, worker := range g.pool.Snapshot() {
			if worker.Reserved != 0 {
				t.Errorf("Worker reservation leaked: %+v", worker)
			}
		}
	})
	return h
}

func (h *gatewayHarness) dial() (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(h.ctx, "ws://gateway.test/v1/asr", &websocket.DialOptions{HTTPClient: h.client})
}

func (h *gatewayHarness) mustDial(t *testing.T) *websocket.Conn {
	t.Helper()
	conn, response, err := h.dial()
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		t.Fatalf("dial Gateway: %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

// onlySessionID 仅读取创建后不再修改的 ID，避免并发观察正在绑定的 ws 字段。
func (h *gatewayHarness) onlySessionID(t *testing.T) string {
	t.Helper()
	sessions := h.gateway.registry.snapshot()
	if len(sessions) != 1 || sessions[0].id == "" {
		t.Fatalf("want one identified session, got %v", sessions)
	}
	return sessions[0].id
}

func (h *gatewayHarness) waitHandlers(t *testing.T, count int) {
	t.Helper()
	for range count {
		awaitGatewaySignal(t, h.ctx, h.finished, "Gateway handler returned")
	}
}

func (h *gatewayHarness) expectHTTPStatus(t *testing.T, want int) {
	t.Helper()
	request, err := http.NewRequestWithContext(h.ctx, http.MethodGet, "http://gateway.test/v1/asr", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != want {
		t.Fatalf("HTTP status = %d, want %d", response.StatusCode, want)
	}
}

func (h *gatewayHarness) write(t *testing.T, conn *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	if err := conn.Write(h.ctx, typ, data); err != nil {
		t.Fatalf("write client message: %v", err)
	}
}

func (h *gatewayHarness) expectResult(t *testing.T, conn *websocket.Conn, text string, final bool) {
	t.Helper()
	typ, data, err := conn.Read(h.ctx)
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	var result wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText || result.Type != wsprotocol.MessageTypeResult ||
		result.SegmentID != "1" || result.Text != text || result.IsFinal != final {
		t.Fatalf("unexpected result: %s", data)
	}
}

func (h *gatewayHarness) expectClose(t *testing.T, conn *websocket.Conn, want websocket.StatusCode) {
	t.Helper()
	_, _, err := conn.Read(h.ctx)
	if got := websocket.CloseStatus(err); got != want {
		t.Fatalf("close status = %d, want %d: %v", got, want, err)
	}
}

// awaitGatewaySignal 使用事件同步；超时只用于防止回归错误使测试永久阻塞。
func awaitGatewaySignal(t *testing.T, ctx context.Context, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", description, ctx.Err())
	}
}
