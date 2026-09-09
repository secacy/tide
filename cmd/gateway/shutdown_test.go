package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// 真实 WebSocket 和 gRPC 验证退出通知不会提前取消正在自然排空的会话。
func TestServeSignalAllowsSessionToFinish(t *testing.T) {
	h := newShutdownHarness(t, shutdownConfig{DrainTimeout: time.Second, CleanupTimeout: time.Second})
	conn := h.dial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
	h.result(t, conn, "first", false)
	h.stop()
	waitShutdownSignal(t, h.ctx, h.sessions.stopped, "admission stopped")
	if err := h.sessionCtx.Err(); err != nil {
		t.Fatalf("stop signal canceled session parent: %v", err)
	}
	// HTTP 服务此时可已退出，WebSocket 会话仍须能继续识别并返回 final。
	h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
	h.result(t, conn, "second", false)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.result(t, conn, "final", true)
	_, _, err := conn.Read(h.ctx)
	if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("expected normal session close, got %v", err)
	}
	if err := h.wait(t); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	if h.sessions.aborts.Load() != 0 {
		t.Fatal("completed session required Abort")
	}
	if err := h.gateway.Wait(h.ctx); err != nil {
		t.Fatalf("serve returned before session drained: %v", err)
	}
}

func TestServeSignalWithNoSessions(t *testing.T) {
	h := newShutdownHarness(t, shutdownConfig{DrainTimeout: time.Second, CleanupTimeout: time.Second})
	// 包含通知早于 Serve goroutine 完成启动的交错。
	h.stop()
	if err := h.wait(t); err != nil {
		t.Fatal(err)
	}
	if h.sessions.aborts.Load() != 0 {
		t.Fatal("empty gateway required Abort")
	}
}

// 等待 start 的客户端不会自然结束，期限耗尽后应强制关闭并成功退出。
func TestServeSignalForcesIdleSession(t *testing.T) {
	h := newShutdownHarness(t, shutdownConfig{DrainTimeout: 20 * time.Millisecond, CleanupTimeout: time.Second})
	conn := h.dial(t)
	h.stop()
	if err := h.wait(t); err != nil {
		t.Fatalf("forced cleanup should recover drain timeout: %v", err)
	}
	if got := h.sessions.aborts.Load(); got != 1 {
		t.Fatalf("Abort calls = %d, want 1", got)
	}
	if _, _, err := conn.Read(h.ctx); err == nil {
		t.Fatal("forced session still readable")
	}
	if err := h.gateway.Wait(h.ctx); err != nil {
		t.Fatalf("serve returned before forced cleanup: %v", err)
	}
}

// 强制停止也不能保证任意代码都响应取消；必须报告最终等待失败。
func TestShutdownReportsCleanupTimeout(t *testing.T) {
	sessions := &shutdownLifecycleStub{wait: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	err := shutdownServer(&http.Server{}, sessions, shutdownTestLogger(), shutdownConfig{
		DrainTimeout: 5 * time.Millisecond, CleanupTimeout: 5 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "wait for session cleanup") {
		t.Fatalf("expected cleanup deadline error, got %v", err)
	}
	if sessions.stops != 1 || sessions.aborts != 1 || sessions.waits != 2 {
		t.Fatalf("unexpected lifecycle calls: %+v", sessions)
	}
}

// 只有本阶段期限耗尽才是可恢复的升级条件，其他错误不能因最终清理成功而消失。
func TestShutdownPreservesUnexpectedDrainErrors(t *testing.T) {
	for _, want := range []error{errors.New("registry wait failed"), context.Canceled, context.DeadlineExceeded} {
		t.Run(want.Error(), func(t *testing.T) {
			calls := 0
			sessions := &shutdownLifecycleStub{wait: func(ctx context.Context) error {
				calls++
				if calls == 1 {
					if ctx.Err() != nil {
						t.Error("drain context expired before injected error")
					}
					return want
				}
				return nil
			}}
			err := shutdownServer(&http.Server{}, sessions, shutdownTestLogger(), shutdownConfig{
				DrainTimeout: time.Second, CleanupTimeout: time.Second,
			})
			if !errors.Is(err, want) {
				t.Fatalf("unexpected drain error was lost: %v", err)
			}
			if sessions.aborts != 1 || sessions.waits != 2 {
				t.Fatal("error skipped forced cleanup")
			}
		})
	}
}

// 普通 HTTP 请求耗尽共享的排空预算后，会话不能再获得第二份自然排空时间。
// Server.Close 应取消该请求；强制清理的 Wait 必须获得新的有效 Context。
func TestShutdownSharesDrainBudgetAndClosesHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entered, handlerDone := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(handlerDone)
	})}
	listener := bufconn.Listen(64 * 1024)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer server.Close()
	defer listener.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://shutdown.test/blocked", nil)
	if err != nil {
		t.Fatal(err)
	}
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := (&http.Client{Transport: transport}).Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	waitShutdownSignal(t, ctx, entered, "HTTP handler entered")
	var firstCtx context.Context
	sessions := &shutdownLifecycleStub{wait: func(waitCtx context.Context) error {
		if firstCtx == nil {
			firstCtx = waitCtx
			if !errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				t.Error("HTTP and session drain did not share the same exhausted budget")
			}
			return waitCtx.Err()
		}
		if waitCtx == firstCtx || waitCtx.Err() != nil {
			t.Error("forced cleanup reused exhausted context")
		}
		select {
		case <-handlerDone:
			return nil
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}}
	if err := shutdownServer(server, sessions, shutdownTestLogger(), shutdownConfig{
		DrainTimeout: 20 * time.Millisecond, CleanupTimeout: time.Second,
	}); err != nil {
		t.Fatalf("forced HTTP cleanup: %v", err)
	}
	if sessions.aborts != 1 || sessions.waits != 2 {
		t.Fatal("shared budget did not trigger forced phase")
	}
	waitShutdownSignal(t, ctx, requestDone, "HTTP client exited")
	select {
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("HTTP Serve did not return")
	}
}

func TestServePreservesListenerAndCleanupErrors(t *testing.T) {
	for _, cleanupFails := range []bool{false, true} {
		name := "serve_error"
		if cleanupFails {
			name = "serve_and_cleanup_errors"
		}
		t.Run(name, func(t *testing.T) {
			serveFailure, cleanupFailure := errors.New("listener failed"), errors.New("cleanup failed")
			listener := &failedShutdownListener{err: serveFailure}
			sessions := &shutdownLifecycleStub{wait: func(context.Context) error {
				if cleanupFails {
					return cleanupFailure
				}
				return nil
			}}
			// 没有退出通知，Serve 自身的失败也必须启动关闭编排。
			err := serve(context.Background(), &http.Server{}, listener, sessions, shutdownTestLogger(), shutdownConfig{
				DrainTimeout: time.Second, CleanupTimeout: time.Second,
			})
			if !errors.Is(err, serveFailure) || (cleanupFails && !errors.Is(err, cleanupFailure)) {
				t.Fatalf("lost Serve or cleanup error: %v", err)
			}
			if sessions.stops != 1 || sessions.waits == 0 {
				t.Fatal("Serve error skipped session cleanup")
			}
		})
	}
}

// Shutdown 自己关闭 Listener 时发生的错误也要跨阶段保留。
func TestShutdownPreservesHTTPShutdownError(t *testing.T) {
	want := errors.New("listener close failed")
	listener := &closeErrorListener{Listener: bufconn.Listen(1024), entered: make(chan struct{}), err: want}
	defer listener.Close()
	server := &http.Server{}
	defer server.Close()
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waitShutdownSignal(t, ctx, listener.entered, "listener registered by HTTP server")
	sessions := &shutdownLifecycleStub{}
	err := shutdownServer(server, sessions, shutdownTestLogger(), shutdownConfig{
		DrainTimeout: time.Second, CleanupTimeout: time.Second,
	})
	if !errors.Is(err, want) || sessions.aborts != 1 || sessions.waits != 2 {
		t.Fatalf("HTTP shutdown error lost or cleanup skipped: %v, calls=%+v", err, sessions)
	}
	select {
	case <-served:
	case <-ctx.Done():
		t.Fatal("Serve remained running")
	}
}

func TestServeRejectsInvalidShutdownBudgets(t *testing.T) {
	for _, cfg := range []shutdownConfig{
		{DrainTimeout: 0, CleanupTimeout: time.Second},
		{DrainTimeout: -time.Second, CleanupTimeout: time.Second},
		{DrainTimeout: time.Second, CleanupTimeout: 0},
		{DrainTimeout: time.Second, CleanupTimeout: -time.Second},
	} {
		listener := &failedShutdownListener{err: errors.New("unexpected Accept")}
		sessions := &shutdownLifecycleStub{}
		if err := serve(context.Background(), &http.Server{}, listener, sessions, shutdownTestLogger(), cfg); err == nil {
			t.Fatalf("accepted invalid config %+v", cfg)
		}
		if listener.accepts.Load() != 0 || sessions.stops != 0 {
			t.Fatal("invalid config started serving or changed session lifecycle")
		}
	}
}

// shutdownLifecycleStub 控制排空结果；调用计数只在关闭流程返回后读取。
type shutdownLifecycleStub struct {
	stops, aborts, waits int
	wait                 func(context.Context) error
}

func (s *shutdownLifecycleStub) StopAccepting() { s.stops++ }
func (s *shutdownLifecycleStub) Abort()         { s.aborts++ }
func (s *shutdownLifecycleStub) Wait(ctx context.Context) error {
	s.waits++
	if s.wait != nil {
		return s.wait(ctx)
	}
	return nil
}

// failedShutdownListener 将确定的监听错误交给真实 HTTP Serve。
type failedShutdownListener struct {
	err     error
	accepts atomic.Int32
}

func (l *failedShutdownListener) Accept() (net.Conn, error) {
	l.accepts.Add(1)
	return nil, l.err
}
func (l *failedShutdownListener) Close() error   { return nil }
func (l *failedShutdownListener) Addr() net.Addr { return &net.TCPAddr{} }

// closeErrorListener 实际关闭内存 Listener，同时模拟关闭返回错误。
type closeErrorListener struct {
	net.Listener
	entered chan struct{}
	once    sync.Once
	err     error
}

func (l *closeErrorListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	return l.Listener.Accept()
}
func (l *closeErrorListener) Close() error {
	_ = l.Listener.Close()
	return l.err
}

// shutdownLifecycleProbe 观察真实 Gateway 的停止阶段，不改变其生命周期行为。
type shutdownLifecycleProbe struct {
	gatewayLifecycle
	stopped chan struct{}
	once    sync.Once
	aborts  atomic.Int32
}

func (s *shutdownLifecycleProbe) StopAccepting() {
	s.gatewayLifecycle.StopAccepting()
	s.once.Do(func() { close(s.stopped) })
}
func (s *shutdownLifecycleProbe) Abort() {
	s.aborts.Add(1)
	s.gatewayLifecycle.Abort()
}

// shutdownHarness 通过内存连接运行真实关闭编排、Gateway 和 Mock Worker。
// 测试通过 stop 模拟信号通知，避免向测试进程发送系统信号。
type shutdownHarness struct {
	ctx, sessionCtx context.Context
	stop            context.CancelFunc
	gateway         *gateway.Gateway
	sessions        *shutdownLifecycleProbe
	client          *http.Client
	finished        chan error
	returned        bool
}

func newShutdownHarness(t *testing.T, cfg shutdownConfig) *shutdownHarness {
	t.Helper()
	workerListener := bufconn.Listen(64 * 1024)
	workerServer := grpc.NewServer()
	asrv1.RegisterASRServiceServer(workerServer, mockasr.New(mockasr.Config{
		PartialEvery: 100 * time.Millisecond,
		PartialTexts: []string{"first", "second"},
		FinalText:    "final",
	}))
	go func() { _ = workerServer.Serve(workerListener) }()
	t.Cleanup(func() { workerServer.Stop(); _ = workerListener.Close() })
	workerConn, err := grpc.NewClient("passthrough:///shutdown-worker",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return workerListener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workerConn.Close() })
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	t.Cleanup(cancelSessions)
	g, err := gateway.New(sessionCtx, asrv1.NewASRServiceClient(workerConn), shutdownTestLogger(), gateway.Config{MaxSessions: 4})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelTest := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancelTest)
	stopCtx, stop := context.WithCancel(context.Background())
	sessions := &shutdownLifecycleProbe{gatewayLifecycle: g, stopped: make(chan struct{})}
	h := &shutdownHarness{ctx: ctx, sessionCtx: sessionCtx, stop: stop, gateway: g, sessions: sessions, finished: make(chan error, 1)}
	listener := bufconn.Listen(64 * 1024)
	server := &http.Server{Handler: routes(g)}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}}
	h.client = &http.Client{Transport: transport}
	go func() { h.finished <- serve(stopCtx, server, listener, sessions, shutdownTestLogger(), cfg) }()
	t.Cleanup(func() {
		stop()
		g.Abort()
		_ = server.Close()
		_ = listener.Close()
		transport.CloseIdleConnections()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if !h.returned {
			select {
			case <-h.finished:
			case <-cleanupCtx.Done():
				t.Error("shutdown coordinator leaked after test cleanup")
			}
		}
		if err := g.Wait(cleanupCtx); err != nil {
			t.Errorf("sessions remained after cleanup: %v", err)
		}
	})
	return h
}

func (h *shutdownHarness) dial(t *testing.T) *websocket.Conn {
	t.Helper()
	conn, response, err := websocket.Dial(h.ctx, "ws://shutdown.test/v1/asr", &websocket.DialOptions{HTTPClient: h.client})
	if err != nil {
		if response != nil {
			_ = response.Body.Close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func (h *shutdownHarness) write(t *testing.T, conn *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	if err := conn.Write(h.ctx, typ, data); err != nil {
		t.Fatal(err)
	}
}

func (h *shutdownHarness) result(t *testing.T, conn *websocket.Conn, want string, final bool) {
	t.Helper()
	typ, data, err := conn.Read(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var result wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &result); err != nil || typ != websocket.MessageText ||
		result.Type != wsprotocol.MessageTypeResult || result.Text != want || result.IsFinal != final {
		t.Fatalf("unexpected result %s: %v", data, err)
	}
}

func (h *shutdownHarness) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.finished:
		h.returned = true
		return err
	case <-h.ctx.Done():
		t.Fatal("shutdown coordinator did not return")
		return h.ctx.Err()
	}
}

func shutdownTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// waitShutdownSignal 用事件同步测试；期限仅用于避免回归导致永久等待。
func waitShutdownSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", description, ctx.Err())
	}
}
