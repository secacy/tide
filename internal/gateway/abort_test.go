package gateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func TestGatewayAbortWaitingForStart(t *testing.T) {
	worker := &unusedGatewayWorker{}
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	s := gatewayOnlySession(t, h)
	s.abort()
	s.abort()
	assertGatewayAborted(t, h, s)
	if _, _, err := conn.Read(h.ctx); err == nil {
		t.Fatal("aborted connection still readable")
	}
	if worker.calls.Load() != 0 {
		t.Fatal("aborted waiting session opened a Worker stream")
	}
}

// 用可控建流/Send/Recv 阻塞，避免依赖真实 gRPC 流控窗口大小。
func TestGatewayAbortBlockedWorkerOperations(t *testing.T) {
	for _, stage := range []string{"opening", "sending", "receiving_after_end"} {
		t.Run(stage, func(t *testing.T) {
			ready, sendDone, recvDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
				if stage == "opening" {
					close(ready)
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &controlledWorkerStream{
					ctx: ctx,
					send: func(*asrv1.StreamingRecognizeRequest) error {
						if stage == "sending" {
							defer close(sendDone)
							close(ready)
							<-ctx.Done()
							return ctx.Err()
						}
						return nil
					},
					recv: func() (*asrv1.StreamingRecognizeResponse, error) {
						defer close(recvDone)
						<-ctx.Done()
						return nil, ctx.Err()
					},
					closeSend: func() error {
						if stage == "receiving_after_end" {
							close(ready)
						}
						return nil
					},
				}, nil
			}}
			h := newGatewayHarness(t, worker, 1)
			conn := h.mustDial(t)
			s := gatewayOnlySession(t, h)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			if stage != "opening" {
				h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
			}
			if stage == "receiving_after_end" {
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
			}
			awaitGatewaySignal(t, h.ctx, ready, "Worker reached blocked stage")
			s.abort()
			assertGatewayAborted(t, h, s)
			if stage == "sending" {
				awaitGatewaySignal(t, h.ctx, sendDone, "Worker Send exited")
			}
			if stage != "opening" {
				awaitGatewaySignal(t, h.ctx, recvDone, "Worker Recv exited")
			}
		})
	}
}

// 真实内存 gRPC 传输验证取消会传播到 Worker 服务端。
func TestGatewayAbortCancelsRemoteWorker(t *testing.T) {
	ready, done := make(chan struct{}), make(chan struct{})
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		defer close(done)
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(ready)
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	s := gatewayOnlySession(t, h)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	awaitGatewaySignal(t, h.ctx, ready, "remote Worker received audio")
	s.abort()
	assertGatewayAborted(t, h, s)
	awaitGatewaySignal(t, h.ctx, done, "remote Worker canceled")
}

// 取消已经生效，但 Send 的清理尚未完成时，会话必须继续占用名额。
func TestGatewayAbortWaitsForIOExitBeforeUnregister(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &controlledWorkerStream{
			ctx: ctx,
			send: func(*asrv1.StreamingRecognizeRequest) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release // 模拟取消后的收尾过程尚未完成。
				return ctx.Err()
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}, nil
	}}
	h := newGatewayHarness(t, worker, 1)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	conn := h.mustDial(t)
	s := gatewayOnlySession(t, h)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	awaitGatewaySignal(t, h.ctx, entered, "Send entered")
	h.gateway.StopAccepting()
	s.abort()
	awaitGatewaySignal(t, h.ctx, canceled, "Send observed cancellation")
	waitCtx, cancelWait := context.WithCancel(h.ctx)
	cancelWait()
	if err := h.gateway.Wait(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait reported drain before Send exited: %v", err)
	}
	if got := gatewayOnlySession(t, h); got != s {
		t.Fatal("session removed before I/O exited")
	}
	assertRegistryNotDrained(t, h.gateway.registry)
	releaseOnce.Do(func() { close(release) })
	assertGatewayAborted(t, h, s)
}

// 原始客户端读取关闭帧后不回送确认，确定性停在服务端的关闭握手中。
func TestGatewayAbortInterruptsCloseHandshake(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{FinalText: "final"}))
	h := newGatewayHarness(t, worker, 1)
	transport := h.client.Transport.(*http.Transport)
	conn, err := transport.DialContext(h.ctx, "tcp", "gateway.test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	deadline, _ := h.ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if err := websocketTestRequest().Write(conn); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols {
		_ = response.Body.Close()
		t.Fatalf("upgrade = %d", response.StatusCode)
	}
	s := gatewayOnlySession(t, h)
	writeRawClientFrame(t, conn, 1, []byte(`{"type":"start","version":"v1"}`))
	writeRawClientFrame(t, conn, 2, []byte{0, 0})
	writeRawClientFrame(t, conn, 1, []byte(`{"type":"end"}`))
	opcode, data := readRawServerFrame(t, reader)
	var result wsprotocol.ResultMessage
	if err := json.Unmarshal(data, &result); err != nil || opcode != 1 || !result.IsFinal || result.Text != "final" {
		t.Fatalf("expected final result before closing, opcode=%d payload=%q err=%v", opcode, data, err)
	}
	opcode, data = readRawServerFrame(t, reader)
	if opcode != 8 || len(data) < 2 || binary.BigEndian.Uint16(data) != uint16(websocket.StatusNormalClosure) {
		t.Fatalf("expected normal close frame, opcode=%d payload=%v", opcode, data)
	}
	// 不发关闭确认。此时仍应登记，直到强制关闭唤醒握手和剩余 I/O。
	if got := gatewayOnlySession(t, h); got != s {
		t.Fatal("closing session was removed early")
	}
	s.abort()
	assertGatewayAborted(t, h, s)
}

// gatewayOnlySession 仅获取注册表引用，不并发读取 handler 正在绑定的 ws。
func gatewayOnlySession(t *testing.T, h *gatewayHarness) *session {
	t.Helper()
	sessions := h.gateway.registry.snapshot()
	if len(sessions) != 1 {
		t.Fatalf("want one session, got %d", len(sessions))
	}
	return sessions[0]
}

// assertGatewayAborted 的时间预算短于 WebSocket 自身的关闭握手超时。
func assertGatewayAborted(t *testing.T, h *gatewayHarness, s *session) {
	t.Helper()
	h.gateway.StopAccepting()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatalf("aborted session did not finish cleanup: %v", err)
	}
	if !errors.Is(context.Cause(s.ctx), errSessionAborted) {
		t.Fatalf("session lost abort cause during cleanup: %v", context.Cause(s.ctx))
	}
	assertRegistrySessions(t, h.gateway.registry)
	h.waitHandlers(t, 1)
}

// controlledWorkerClient 通过回调控制建流何时返回。
type controlledWorkerClient struct {
	open func(context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error)
}

func (w *controlledWorkerClient) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	return w.open(ctx)
}

// controlledWorkerStream 只实现 Gateway 实际调用的流操作，其余接口方法不应被使用。
type controlledWorkerStream struct {
	grpc.ClientStream
	ctx       context.Context
	send      func(*asrv1.StreamingRecognizeRequest) error
	recv      func() (*asrv1.StreamingRecognizeResponse, error)
	closeSend func() error
}

func (s *controlledWorkerStream) Context() context.Context                         { return s.ctx }
func (s *controlledWorkerStream) Send(req *asrv1.StreamingRecognizeRequest) error  { return s.send(req) }
func (s *controlledWorkerStream) Recv() (*asrv1.StreamingRecognizeResponse, error) { return s.recv() }
func (s *controlledWorkerStream) CloseSend() error                                 { return s.closeSend() }

// writeRawClientFrame 发送单个短帧，包含客户端必须提供的掩码。
func writeRawClientFrame(t *testing.T, conn net.Conn, opcode byte, payload []byte) {
	t.Helper()
	if len(payload) > 125 {
		t.Fatal("test helper only supports short client frames")
	}
	mask := [4]byte{1, 2, 3, 4}
	frame := []byte{0x80 | opcode, 0x80 | byte(len(payload)), mask[0], mask[1], mask[2], mask[3]}
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
}

// readRawServerFrame 读取完整短帧，不自动响应关闭握手。
func readRawServerFrame(t *testing.T, reader io.Reader) (byte, []byte) {
	t.Helper()
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		t.Fatal(err)
	}
	if header[0]&0x80 == 0 || header[1]&0x80 != 0 || header[1] > 125 {
		t.Fatalf("unexpected server frame header %v", header)
	}
	payload := make([]byte, int(header[1]))
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatal(err)
	}
	return header[0] & 0xf, payload
}
