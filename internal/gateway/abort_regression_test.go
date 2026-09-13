package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// 检查 run 的实际返回值，不能只检查 Session Context 中仍保留着取消原因。
func TestSessionRunPreservesEarlyCancellationCause(t *testing.T) {
	for _, stage := range []string{"waiting_start", "opening_worker"} {
		for _, abort := range []bool{false, true} {
			name := "parent_cancel"
			if abort {
				name = "abort"
			}
			t.Run(stage+"/"+name, func(t *testing.T) {
				parent, cancelParent := context.WithCancelCause(context.Background())
				defer cancelParent(nil)
				opening := make(chan struct{})
				worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
					close(opening)
					<-ctx.Done()
					return nil, ctx.Err()
				}}
				s, peer, reading := newDirectSession(t, parent, worker)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				deadline, _ := ctx.Deadline()
				_ = peer.SetDeadline(deadline)
				finished := make(chan error, 1)
				go func() {
					finished <- s.run(Config{Heartbeat: wsheartbeat.Config{Interval: 2 * time.Second, Timeout: 3 * time.Second}, ProcessingTimeout: 3 * time.Second, EndTimeout: 5 * time.Second, MaxUnprocessedChunks: 4096, AudioQueueMaxBytes: 64_000, AudioQueueMaxChunks: 128, ResultWriteTimeout: 2 * time.Second})
				}()
				awaitGatewaySignal(t, ctx, reading, "Session entered initial read")
				if stage == "opening_worker" {
					writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
					awaitGatewaySignal(t, ctx, opening, "Worker opening entered")
				}

				want := errors.New("application stopped for test")
				if abort {
					want = errSessionAborted
					s.abort()
				} else {
					cancelParent(want)
					// 建流期间保持 WS reader，读取并回应服务端的 1001 关闭帧。
					if stage == "opening_worker" {
						opcode, payload := readRawServerFrame(t, peer)
						if opcode != 8 {
							t.Fatalf("expected close frame, got opcode %d", opcode)
						}
						writeRawClientFrame(t, peer, 8, payload)
					}
				}
				select {
				case err := <-finished:
					if !errors.Is(err, want) {
						t.Fatalf("run returned %v; want original cancellation cause %v", err, want)
					}
				case <-ctx.Done():
					t.Fatal("run did not exit after cancellation")
				}
			})
		}
	}
}

// Worker 故障已经确定后，收尾中的 Abort 应中断握手，但不能覆盖原始故障。
func TestSessionAbortDuringFailureCleanupPreservesWorkerError(t *testing.T) {
	want := errors.New("worker rejected stream")
	worker := &controlledWorkerClient{open: func(context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return nil, want
	}}
	s, peer, _ := newDirectSession(t, context.Background(), worker)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	_ = peer.SetDeadline(deadline)
	finished := make(chan error, 1)
	go func() {
		finished <- s.run(Config{Heartbeat: wsheartbeat.Config{Interval: 2 * time.Second, Timeout: 3 * time.Second}, ProcessingTimeout: 3 * time.Second, EndTimeout: 5 * time.Second, MaxUnprocessedChunks: 4096, AudioQueueMaxBytes: 64_000, AudioQueueMaxChunks: 128, ResultWriteTimeout: 2 * time.Second})
	}()
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
	opcode, _ := readRawServerFrame(t, peer)
	if opcode != 8 {
		t.Fatalf("expected worker failure close frame, got opcode %d", opcode)
	}
	// 不响应关闭握手，待结果已确定后再强制停止。
	s.abort()
	select {
	case err := <-finished:
		if !errors.Is(err, want) {
			t.Fatalf("cleanup replaced worker error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("abort did not interrupt failure cleanup")
	}
}

// 多个调用方同时批量停止，覆盖等待 start 与正在识别的真实连接。
func TestGatewayConcurrentAbortStopsAllSessions(t *testing.T) {
	const sessions, callers = 12, 8
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{
		PartialEvery: 100 * time.Millisecond,
		PartialTexts: []string{"partial"},
	}))
	h := newGatewayHarness(t, worker, sessions)
	for i := range sessions {
		conn := h.mustDial(t)
		if i%2 == 0 {
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			h.write(t, conn, websocket.MessageBinary, make([]byte, 3200))
			h.expectResult(t, conn, "partial", false)
		}
	}
	active := h.gateway.registry.snapshot()
	if len(active) != sessions {
		t.Fatalf("registered %d sessions, want %d", len(active), sessions)
	}
	start, returned := make(chan struct{}), make(chan struct{}, callers)
	for range callers {
		go func() {
			<-start
			h.gateway.Abort()
			returned <- struct{}{}
		}()
	}
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()
	close(start)
	for range callers {
		awaitGatewaySignal(t, ctx, returned, "Gateway Abort returned")
	}
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatalf("Wait after batch Abort: %v", err)
	}
	assertRegistrySessions(t, h.gateway.registry)
	for _, s := range active {
		if !errors.Is(context.Cause(s.ctx), errSessionAborted) {
			t.Errorf("session %q missed Abort: %v", s.id, context.Cause(s.ctx))
		}
	}
	h.waitHandlers(t, sessions)
	h.expectHTTPStatus(t, http.StatusServiceUnavailable)
	h.waitHandlers(t, 1)
	h.gateway.Abort() // 全部注销后再次调用仍安全。
}

// 接入已登记但 Hijack 尚未返回时执行批量停止，迟到的连接也必须被关闭。
func TestGatewayAbortDuringHijackClosesLateTransport(t *testing.T) {
	worker := &unusedGatewayWorker{}
	g, err := New(context.Background(), worker, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	server, peer := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = peer.Close() })
	var accepted *session
	writer := &hijackTestWriter{
		ResponseRecorder: httptest.NewRecorder(),
		hijack: func() (net.Conn, *bufio.ReadWriter, error) {
			active := g.registry.snapshot()
			if len(active) != 1 {
				t.Fatalf("Hijack must already be registered, got %d sessions", len(active))
			}
			accepted = active[0]
			g.Abort()
			assertRegistrySessions(t, g.registry, accepted)
			assertRegistryNotDrained(t, g.registry)
			return server, bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server)), nil
		},
	}
	g.ServeHTTP(writer, websocketTestRequest())
	if accepted == nil || !errors.Is(context.Cause(accepted.ctx), errSessionAborted) {
		t.Fatal("late upgrade lost the earlier abort")
	}
	if worker.calls.Load() != 0 {
		t.Fatal("late upgrade opened a Worker stream after Abort")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Wait(ctx); err != nil {
		t.Fatalf("late upgrade did not drain: %v", err)
	}
	assertRegistrySessions(t, g.registry)
	deadline, _ := ctx.Deadline()
	_ = peer.SetReadDeadline(deadline)
	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("late transport not closed: %v", err)
	}
}

// newDirectSession 使用真实 WebSocket 协议，但由测试直接观察 run 的返回值。
// reading 通知底层首次 Read 已进入，避免使用 sleep 猜测会话阶段。
func newDirectSession(t *testing.T, parent context.Context, worker asrv1.ASRServiceClient) (*session, net.Conn, <-chan struct{}) {
	t.Helper()
	s := newSession(parent, "direct-session", worker)
	server, peer := net.Pipe()
	reading := make(chan struct{})
	transport := &readSignalConn{Conn: server, reading: reading}
	t.Cleanup(func() {
		s.abort()
		_ = peer.Close()
		_ = server.Close()
		if s.ws != nil {
			_ = s.ws.CloseNow()
		}
	})
	writer := &hijackTestWriter{
		ResponseRecorder: httptest.NewRecorder(),
		hijack: func() (net.Conn, *bufio.ReadWriter, error) {
			return transport, bufio.NewReadWriter(bufio.NewReader(transport), bufio.NewWriter(transport)), nil
		},
	}
	var err error
	s.ws, err = websocket.Accept(wrapSessionResponseWriter(writer, s), websocketTestRequest(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, peer, reading
}

// readSignalConn 在首次读取时通知测试，其他行为委托给真实连接。
type readSignalConn struct {
	net.Conn
	reading chan struct{}
	once    sync.Once
}

func (c *readSignalConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.reading) })
	return c.Conn.Read(p)
}
