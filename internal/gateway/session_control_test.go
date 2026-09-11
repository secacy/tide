package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// 两种确定顺序以及同时竞争都不能丢失停止请求；重复停止只执行一次关闭。
func TestSessionAbortAndAttachTransport(t *testing.T) {
	for _, order := range []string{"abort_first", "attach_first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			for range 100 {
				s := newSession(context.Background(), "session", nil)
				conn := &observedCloseConn{}
				switch order {
				case "abort_first":
					s.abort()
					s.attachTransport(conn)
				case "attach_first":
					s.attachTransport(conn)
					s.abort()
				case "concurrent":
					start := make(chan struct{})
					var wg sync.WaitGroup
					wg.Add(9)
					go func() { defer wg.Done(); <-start; s.attachTransport(conn) }()
					for range 8 {
						go func() { defer wg.Done(); <-start; s.abort() }()
					}
					close(start)
					wg.Wait()
				}
				s.abort()
				if got := conn.closes.Load(); got != 1 {
					t.Fatalf("transport closed %d times, want 1", got)
				}
				if !errors.Is(context.Cause(s.ctx), errSessionAborted) {
					t.Fatalf("cause = %v", context.Cause(s.ctx))
				}
			}
		})
	}
}

// 用可重入的关闭回调确定性验证两个关闭入口均已释放控制锁。
func TestSessionClosesTransportOutsideControlLock(t *testing.T) {
	for _, abortFirst := range []bool{false, true} {
		s := newSession(context.Background(), "session", nil)
		conn := &observedCloseConn{onClose: func() {
			if !s.controlMu.TryLock() {
				t.Error("Close called while controlMu is locked")
				return
			}
			s.controlMu.Unlock()
			s.abort() // 重复停止不得递归关闭或等待当前的 Close。
		}}
		if abortFirst {
			s.abort()
		}
		s.attachTransport(conn)
		s.abort()
		if got := conn.closes.Load(); got != 1 {
			t.Fatalf("transport closed %d times", got)
		}
	}
}

// 强制停止只发出中断，注册表必须等待接入流程执行最终注销。
func TestSessionAbortDoesNotUnregister(t *testing.T) {
	r, err := newSessionRegistry(1)
	if err != nil {
		t.Fatal(err)
	}
	s := newSession(context.Background(), "session", nil)
	if err := r.register(s); err != nil {
		t.Fatal(err)
	}
	r.stopAccepting()
	s.abort()
	assertRegistrySessions(t, r, s)
	assertRegistryNotDrained(t, r)
	r.unregister(s)
	assertRegistryWaitCompleted(t, r)
}

func TestSessionCancellationCause(t *testing.T) {
	parentCause := errors.New("parent stopped")
	for _, abortFirst := range []bool{false, true} {
		parent, cancelParent := context.WithCancelCause(context.Background())
		s := newSession(parent, "session", nil)
		if _, canceled := cancellationResult(s.ctx); canceled {
			t.Fatal("new session already canceled")
		}
		wantKind, wantCause := resultServerStopping, parentCause
		if abortFirst {
			s.abort()
			wantKind, wantCause = resultAborted, errSessionAborted
		}
		cancelParent(parentCause)
		s.abort()
		s.cancel(nil) // 清理不能覆盖已记录的原因。
		result, canceled := cancellationResult(s.ctx)
		if !canceled || result.kind != wantKind || !errors.Is(result.err, wantCause) {
			t.Fatalf("cancellation result = %+v, canceled=%v", result, canceled)
		}
		// 已经取消时 run 不得读取尚未绑定的 ws，也不得尝试建流。
		if err := s.run(Config{ProcessingTimeout: 3 * time.Second, EndTimeout: 5 * time.Second, MaxUnprocessedChunks: 4096, AudioQueueMaxBytes: 64_000, AudioQueueMaxChunks: 128, ResultWriteTimeout: 2 * time.Second}); !errors.Is(err, wantCause) {
			t.Fatalf("run after cancellation = %v, want %v", err, wantCause)
		}
	}
}

// 包装链中的普通响应仍经由最外层写入器，Hijack 成功后才绑定传输连接。
func TestSessionResponseWriterHijack(t *testing.T) {
	for _, abortDuringHijack := range []bool{false, true} {
		s := newSession(context.Background(), "session", nil)
		t.Cleanup(func() { s.cancel(nil) })
		conn := &observedCloseConn{}
		buffer := bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(nil)), bufio.NewWriter(io.Discard))
		base := &hijackTestWriter{
			ResponseRecorder: httptest.NewRecorder(),
			hijack: func() (net.Conn, *bufio.ReadWriter, error) {
				if abortDuringHijack {
					s.abort()
				}
				return conn, buffer, nil
			},
		}
		outer := &unwrapTestWriter{ResponseWriter: &unwrapTestWriter{ResponseWriter: base}}
		wrapped := wrapSessionResponseWriter(outer, s)
		wrapped.Header().Set("X-Test", "preserved")
		wrapped.WriteHeader(http.StatusSwitchingProtocols)
		if outer.headers != 1 || base.Header().Get("X-Test") != "preserved" {
			t.Fatal("wrapper bypassed ordinary HTTP response behavior")
		}
		gotConn, gotBuffer, err := http.NewResponseController(wrapped).Hijack()
		if err != nil || gotConn != conn || gotBuffer != buffer || s.transport != conn {
			t.Fatalf("Hijack did not preserve and attach transport: %v", err)
		}
		if abortDuringHijack && conn.closes.Load() != 1 {
			t.Fatal("late transport escaped an earlier abort")
		}
		if !abortDuringHijack && conn.closes.Load() != 0 {
			t.Fatal("active transport unexpectedly closed")
		}
		s.abort()
	}
}

func TestSessionResponseWriterUnsupportedAndFailedHijack(t *testing.T) {
	s := newSession(context.Background(), "session", nil)
	defer s.cancel(nil)
	unsupported := &unwrapTestWriter{ResponseWriter: httptest.NewRecorder()}
	if got := wrapSessionResponseWriter(unsupported, s); got != unsupported {
		t.Fatal("unsupported writer gained a Hijacker implementation")
	}
	want := errors.New("hijack failed")
	writer := &hijackTestWriter{
		ResponseRecorder: httptest.NewRecorder(),
		hijack:           func() (net.Conn, *bufio.ReadWriter, error) { return nil, nil, want },
	}
	_, _, err := http.NewResponseController(wrapSessionResponseWriter(writer, s)).Hijack()
	if !errors.Is(err, want) || s.transport != nil {
		t.Fatalf("failed Hijack bound a connection or lost error: %v", err)
	}
}

// 使用真实 WebSocket 写入和无缓冲 net.Pipe，明确停在尚未完成的网络写入上。
func TestSessionAbortUnblocksWebSocketWrite(t *testing.T) {
	s := newSession(context.Background(), "session", nil)
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		s.cancel(nil)
		if s.ws != nil {
			_ = s.ws.CloseNow()
		}
	})
	writer := &hijackTestWriter{
		ResponseRecorder: httptest.NewRecorder(),
		hijack: func() (net.Conn, *bufio.ReadWriter, error) {
			return server, bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)), nil
		},
	}
	request := websocketTestRequest()
	var err error
	s.ws, err = websocket.Accept(wrapSessionResponseWriter(writer, s), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = client.SetDeadline(deadline)
	written := make(chan error, 1)
	go func() { written <- s.ws.Write(context.Background(), websocket.MessageBinary, make([]byte, 8192)) }()
	// 只消费帧头的一部分，确认服务端已进入写入，其余内容保持阻塞。
	if _, err := io.ReadFull(client, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	s.abort()
	select {
	case err := <-written:
		if err == nil {
			t.Fatal("incomplete write unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("abort did not unblock WebSocket write")
	}
}

// observedCloseConn 记录 Close，并允许检查调用 Close 时的锁状态。
type observedCloseConn struct {
	net.Conn
	closes  atomic.Int32
	onClose func()
}

func (c *observedCloseConn) Close() error {
	c.closes.Add(1)
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

// hijackTestWriter 在测试中提供可控的 HTTP 接管结果。
type hijackTestWriter struct {
	*httptest.ResponseRecorder
	hijack func() (net.Conn, *bufio.ReadWriter, error)
}

func (w *hijackTestWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) { return w.hijack() }

// unwrapTestWriter 模拟通过 Unwrap 暴露底层能力的 HTTP 中间件。
type unwrapTestWriter struct {
	http.ResponseWriter
	headers int
}

func (w *unwrapTestWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *unwrapTestWriter) WriteHeader(code int) {
	w.headers++
	w.ResponseWriter.WriteHeader(code)
}

// websocketTestRequest 构造合法升级请求，供内存传输测试复用。
func websocketTestRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://gateway.test/v1/asr", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	return r
}
