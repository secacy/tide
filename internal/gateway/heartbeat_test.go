package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// 实际 Client.Run 与 Gateway/Mock 配套，覆盖双方同时启用心跳的完整业务链路。
func TestGatewayHeartbeatEndToEnd(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{PartialEvery: 100 * time.Millisecond, FinalText: "final"}))
	cfg := wsheartbeat.Config{Interval: 10 * time.Millisecond, Timeout: time.Second}
	g, err := New(t.Context(), worker, nil, Config{MaxSessions: 1, Heartbeat: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Abort()
	server := httptest.NewServer(g)
	defer server.Close()
	client, err := wsclient.New(wsclient.Config{URL: "ws" + server.URL[4:], Realtime: true, Heartbeat: cfg})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := client.Run(ctx, bytes.NewReader(make([]byte, 16_000))); err != nil {
		t.Fatal(err)
	}
	g.StopAccepting()
	if err := g.Wait(ctx); err != nil || g.pool.Snapshot()[0].Reserved != 0 {
		t.Fatalf("cleanup: %v %+v", err, g.pool.Snapshot())
	}
}

// 未处理控制帧的对端模拟不可完成往返，不靠 Worker 期限或外部 Abort 退出。
func TestGatewayHeartbeatFailureStages(t *testing.T) {
	for _, stage := range []string{"waiting_start", "opening_worker", "idle", "send_stalled"} {
		t.Run(stage, func(t *testing.T) {
			opened, entered, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				close(opened)
				if stage == "opening_worker" {
					<-ctx.Done()
					close(canceled)
					return nil, ctx.Err()
				}
				return &controlledWorkerStream{ctx: ctx,
					send: func(*asrv1.StreamingRecognizeRequest) error {
						close(entered)
						<-ctx.Done()
						return ctx.Err()
					},
					recv: func() (*asrv1.StreamingRecognizeResponse, error) {
						<-ctx.Done()
						close(canceled)
						return nil, ctx.Err()
					}}, nil
			}}
			h := newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: 1, ProcessingTimeout: 5 * time.Second,
				Heartbeat: wsheartbeat.Config{Interval: 20 * time.Millisecond, Timeout: 100 * time.Millisecond}})
			conn := h.mustDial(t)
			s := gatewayOnlySession(t, h)
			if stage != "waiting_start" {
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
				awaitGatewaySignal(t, h.ctx, opened, "Worker open entered")
			}
			if stage == "send_stalled" {
				h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
				awaitGatewaySignal(t, h.ctx, entered, "Send stalled")
			}
			deadline, cancel := context.WithTimeout(h.ctx, time.Second)
			defer cancel()
			awaitGatewaySignal(t, deadline, s.ctx.Done(), "heartbeat canceled Session")
			if !errors.Is(context.Cause(s.ctx), wsheartbeat.ErrFailed) {
				t.Fatalf("cause = %v", context.Cause(s.ctx))
			}
			if stage != "waiting_start" {
				awaitGatewaySignal(t, deadline, canceled, "Worker operation exited")
			}
			awaitGatewaySignal(t, deadline, h.finished, "handler joined loops and released lease")
			assertRegistrySessions(t, h.gateway.registry)
			if got := h.gateway.pool.Snapshot()[0].Reserved; got != 0 {
				t.Fatalf("reserved = %d", got)
			}
		})
	}
}

// 建流尚未返回时，音频仍受处理期限约束；不能因迁移建流位置而失去背压。
func TestGatewayOpeningWorkerProcessingDeadline(t *testing.T) {
	opened, canceled := make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		close(opened)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}}
	h := newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: 1, ProcessingTimeout: 100 * time.Millisecond,
		Heartbeat: wsheartbeat.Config{Interval: 10 * time.Millisecond, Timeout: time.Second}})
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	awaitGatewaySignal(t, h.ctx, opened, "Worker opening")
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	// Read 在期待关闭时同时处理服务端 Ping，排除心跳作为失败来源。
	h.expectClose(t, conn, websocket.StatusTryAgainLater)
	awaitGatewaySignal(t, h.ctx, canceled, "opening canceled by processing deadline")
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

// 处理期限先到、在途 Ping 随后超时：保持业务原因，但不再等待关闭握手。
func TestGatewayHeartbeatFailureDuringBusinessCleanup(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	h := newGatewayHarnessWithConfig(t, stalledSendWorker(entered, canceled), Config{MaxSessions: 1,
		ProcessingTimeout: 100 * time.Millisecond,
		Heartbeat:         wsheartbeat.Config{Interval: 20 * time.Millisecond, Timeout: 200 * time.Millisecond}})
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	ctx, cancel := context.WithTimeout(h.ctx, 700*time.Millisecond)
	defer cancel()
	awaitGatewaySignal(t, ctx, canceled, "processing canceled RPC")
	// 对端不 Read，不消费 Ping/Pong 或关闭帧。必须由生产收尾自行结束。
	awaitGatewaySignal(t, ctx, h.finished, "failed Ping bypassed graceful handshake")
	assertRegistrySessions(t, h.gateway.registry)
}

// 同一 Reader 从 Start 交接到音频阶段；慢建流和 End 均不能阻止它处理 Pong。
func TestGatewayHeartbeatHealthyPreparationAndEnd(t *testing.T) {
	actual := startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: "final"}))
	opened, release := make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		close(opened)
		select {
		case <-release:
			return actual.StreamingRecognize(ctx)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	h := newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: 1,
		Heartbeat: wsheartbeat.Config{Interval: 10 * time.Millisecond, Timeout: time.Second}})
	pings := make(chan struct{}, 100)
	conn, _, err := websocket.Dial(h.ctx, "ws://gateway.test/v1/asr", &websocket.DialOptions{HTTPClient: h.client,
		OnPingReceived: func(context.Context, []byte) bool { pings <- struct{}{}; return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	done := make(chan error, 1)
	final := false // Reader writes; read only after done.
	go func() {
		for {
			_, data, err := conn.Read(h.ctx)
			if err != nil {
				done <- err
				return
			}
			var message wsprotocol.ResultMessage
			if json.Unmarshal(data, &message) == nil && message.Text == "final" && message.IsFinal {
				final = true
			}
		}
	}()
	for range 3 {
		awaitGatewaySignal(t, h.ctx, pings, "Ping while waiting Start")
	}
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	awaitGatewaySignal(t, h.ctx, opened, "Worker opening")
	// 这是客户端主动的 Ping，证明慢建流时 Gateway 的 Reader 也在推进。
	if err := conn.Ping(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.gateway.StopAccepting() // 排空期已有连接继续探测。
	for range 3 {
		awaitGatewaySignal(t, h.ctx, pings, "Ping after End during slow open")
	}
	close(release)
	select {
	case err := <-done:
		if websocket.CloseStatus(err) != websocket.StatusNormalClosure || !final {
			t.Fatalf("final=%v close=%v", final, err)
		}
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	if err := h.gateway.Wait(h.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayHeartbeatJoinDoesNotDelayProcessingCancellation(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	s, peer, _ := newDirectSession(t, t.Context(), stalledSendWorker(entered, canceled))
	finished := runDirectStreamingTest(t, s, Config{ProcessingTimeout: 100 * time.Millisecond, EndTimeout: time.Second,
		MaxUnprocessedChunks: 16, AudioQueueMaxBytes: 32, AudioQueueMaxChunks: 4, ResultWriteTimeout: time.Second,
		Heartbeat: wsheartbeat.Config{Interval: 10 * time.Millisecond, Timeout: time.Second}})
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
	writeRawClientFrame(t, peer, 2, []byte{0, 0})
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	awaitGatewaySignal(t, ctx, entered, "Send entered")
	// 对端尚未读取 Ping，它仍阻塞于写入；RPC 必须先因处理期限取消。
	awaitGatewaySignal(t, ctx, canceled, "processing canceled before heartbeat budget")
	opcode, payload := readRawServerFrame(t, peer)
	if opcode != 9 {
		t.Fatalf("expected in-flight Ping, got %d", opcode)
	}
	writeRawClientFrame(t, peer, 10, payload)
	opcode, payload = readRawServerFrame(t, peer)
	if opcode != 8 {
		t.Fatalf("expected close, got %d", opcode)
	}
	writeRawClientFrame(t, peer, 8, payload)
	if err := <-finished; !errors.Is(err, errProcessingTimeout) {
		t.Fatalf("lost processing failure: %v", err)
	}
}

// heartbeatWriteSignal 只用于确定在途 Ping 已进入真实阻塞 Write。
type heartbeatWriteSignal struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
}

func (c *heartbeatWriteSignal) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func TestGatewayHeartbeatStopDuringControlWrite(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal_stop", true: "abort"}[abort], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			s, peer, _ := newDirectSession(t, ctx, &unusedGatewayWorker{})
			entered := make(chan struct{})
			transport := s.transport.(*readSignalConn)
			transport.Conn = &heartbeatWriteSignal{Conn: transport.Conn, entered: entered}
			read := make(chan error, 1)
			go func() { _, _, err := s.ws.Read(ctx); read <- err }()
			m := wsheartbeat.Start(ctx, wsheartbeat.Config{Interval: time.Millisecond, Timeout: time.Second}, s.ws.Ping, s.abortWithCause)
			awaitGatewaySignal(t, ctx, entered, "Ping write entered")
			m.Stop()
			if abort {
				s.abort()
				_ = m.Wait()
				if !errors.Is(context.Cause(s.ctx), errSessionAborted) || <-read == nil {
					t.Fatal("Abort did not terminate I/O with its own cause")
				}
				return
			}
			opcode, payload := readRawServerFrame(t, peer)
			if opcode != 9 {
				t.Fatalf("expected Ping, got %d", opcode)
			}
			writeRawClientFrame(t, peer, 10, payload)
			if err := m.Wait(); err != nil {
				t.Fatal(err)
			}
			writeRawClientFrame(t, peer, 1, []byte("still open"))
			if err := <-read; err != nil {
				t.Fatal(err)
			}
		})
	}
}
