package gateway

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EXP-001 的回归条件：无需 Abort，客户端关闭就能取消仍在 Send 中的 RPC。
func TestGatewayStalledSendDisconnectCancelsRPC(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	worker := stalledSendWorker(entered, canceled)
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{0, 0})
	awaitGatewaySignal(t, h.ctx, entered, "stalled Send entered")
	_ = conn.CloseNow()
	ctx, cancel := context.WithTimeout(h.ctx, time.Second)
	defer cancel()
	awaitGatewaySignal(t, ctx, canceled, "RPC canceled by disconnect")
	awaitGatewaySignal(t, ctx, h.finished, "handler cleaned up without Abort")
	assertRegistrySessions(t, h.gateway.registry)
}

func TestGatewayQueueOverloadCancelsStalledSend(t *testing.T) {
	for _, cfg := range []Config{
		{MaxSessions: 1, AudioQueueMaxBytes: 2, AudioQueueMaxChunks: 10},
		{MaxSessions: 1, AudioQueueMaxBytes: 100, AudioQueueMaxChunks: 1},
	} {
		entered, canceled := make(chan struct{}), make(chan struct{})
		h := newGatewayHarnessWithConfig(t, stalledSendWorker(entered, canceled), cfg)
		conn := h.mustDial(t)
		h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
		h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
		awaitGatewaySignal(t, h.ctx, entered, "first chunk is in flight")
		h.write(t, conn, websocket.MessageBinary, []byte{2, 0}) // 只有这一块占用队列。
		h.write(t, conn, websocket.MessageBinary, []byte{3, 0})
		ctx, cancel := context.WithTimeout(h.ctx, time.Second)
		awaitGatewaySignal(t, ctx, canceled, "overload canceled RPC before close handshake")
		cancel()
		h.expectClose(t, conn, websocket.StatusTryAgainLater)
		h.waitHandlers(t, 1)
		assertRegistrySessions(t, h.gateway.registry)
	}
}

// 小队列内已有音频且 Send 阻塞时，Reader 仍能消费 End；Sender 释放后严格按序排空。
func TestSessionEndDrainsBeforeHalfClose(t *testing.T) {
	entered, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var sends atomic.Int32
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		firstResult := true
		return &controlledWorkerStream{
			ctx: ctx,
			send: func(req *asrv1.StreamingRecognizeRequest) error {
				n := sends.Add(1)
				if len(req.Data) != 2 || req.Data[0] != byte(n) {
					return errors.New("audio order changed")
				}
				if n == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
			closeSend: func() error {
				if sends.Load() != 3 {
					return errors.New("half-close before audio drained")
				}
				close(halfClosed)
				return nil
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				select {
				case <-halfClosed:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if firstResult {
					firstResult = false
					return &asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: "final", IsFinal: true}, nil
				}
				return nil, io.EOF
			},
		}, nil
	}}
	s, peer, _ := newDirectSession(t, context.Background(), worker)
	finished := runDirectStreamingTest(t, s, Config{AudioQueueMaxBytes: 4, AudioQueueMaxChunks: 2, ResultWriteTimeout: time.Second})
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
	writeRawClientFrame(t, peer, 2, []byte{1, 0})
	awaitGatewaySignal(t, t.Context(), entered, "first Send entered")
	writeRawClientFrame(t, peer, 2, []byte{2, 0})
	writeRawClientFrame(t, peer, 2, []byte{3, 0})
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"end"}`))
	// net.Pipe 无写缓冲；End 已被读取，其前面的两块音频已经入队。
	select {
	case <-halfClosed:
		t.Fatal("half-close while Send remains blocked")
	default:
	}
	close(release)
	_, payload := readRawServerFrame(t, peer)
	if !strings.Contains(string(payload), `"isFinal":true`) {
		t.Fatalf("missing final result: %s", payload)
	}
	opcode, payload := readRawServerFrame(t, peer)
	if opcode != 8 || len(payload) < 2 || payload[0] != 3 || payload[1] != 232 {
		t.Fatalf("missing normal closure: %d %v", opcode, payload)
	}
	writeRawClientFrame(t, peer, 8, payload)
	if err := <-finished; err != nil {
		t.Fatalf("normal drain failed: %v", err)
	}
}

// 输入已 End 但最后的 Send 尚未完成，Worker 的正常 EOF 仍应判定为提前结束。
func TestGatewayPrematureEOFWithPendingAudioFails(t *testing.T) {
	entered, endWorker := make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &controlledWorkerStream{
			ctx: ctx,
			send: func(*asrv1.StreamingRecognizeRequest) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				select {
				case <-endWorker:
					return nil, io.EOF
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}, nil
	}}
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
	awaitGatewaySignal(t, h.ctx, entered, "Send entered")
	h.write(t, conn, websocket.MessageBinary, []byte{2, 0})
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	close(endWorker)
	h.expectClose(t, conn, websocket.StatusInternalError)
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

func TestSessionCompletionEventOrdering(t *testing.T) {
	workerErr := status.Error(codes.Unavailable, "worker rejected audio")
	for _, tc := range []struct {
		name   string
		events []sessionResult
		want   sessionResultKind
		err    error
	}{
		{"input_then_worker", []sessionResult{{kind: resultInputSent}, {kind: resultCompleted}}, resultCompleted, nil},
		{"worker_then_input", []sessionResult{{kind: resultCompleted}, {kind: resultInputSent}}, resultCompleted, nil},
		{"send_eof_then_failure", []sessionResult{{kind: resultSendStopped, err: io.EOF}, {kind: resultWorkerFailed, err: workerErr}}, resultWorkerFailed, workerErr},
		{"close_failed_then_eof", []sessionResult{{kind: resultSendStopped, err: io.EOF}, {kind: resultCompleted}}, resultWorkerFailed, io.EOF},
		{"eof_then_close_failed", []sessionResult{{kind: resultCompleted}, {kind: resultSendStopped, err: io.EOF}}, resultWorkerFailed, io.EOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closing := make(chan struct{})
			close(closing)
			events := make(chan sessionResult, len(tc.events))
			for _, event := range tc.events {
				events <- event
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			got := waitSessionResult(ctx, events, closing)
			if got.kind != tc.want || !errors.Is(got.err, tc.err) {
				t.Fatalf("result = %+v, want %v / %v", got, tc.want, tc.err)
			}
		})
	}
}

// Worker EOF 不能绕过尚未返回的 CloseSend；取消仍能解除协调者等待。
func TestSessionCompletionWaitsForSender(t *testing.T) {
	closing := make(chan struct{})
	close(closing)
	events := make(chan sessionResult, 1)
	events <- sessionResult{kind: resultCompleted}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	got := waitSessionResult(ctx, events, closing)
	if got.kind != resultServerStopping || !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatalf("completed before Sender returned: %+v", got)
	}
}

func TestSenderPreservesTerminalStatusBoundaries(t *testing.T) {
	wantErr := errors.New("injected transport failure")
	for _, tc := range []struct {
		name        string
		sendErr     error
		closeErr    error
		want        sessionResultKind
		wantClosing bool
	}{
		{"send_eof", io.EOF, nil, resultSendStopped, false},
		{"send_error", wantErr, nil, resultWorkerFailed, false},
		{"close_error", nil, wantErr, resultSendStopped, true},
		{"normal", nil, nil, resultInputSent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := mustAudioQueue(t, 4, 2)
			mustQueuePush(t, q, []byte{1, 0})
			mustQueuePush(t, q, []byte{2, 0})
			q.closeInput()
			closing := make(chan struct{})
			sends, closes := 0, 0
			stream := &controlledWorkerStream{
				send:      func(*asrv1.StreamingRecognizeRequest) error { sends++; return tc.sendErr },
				closeSend: func() error { closes++; return tc.closeErr },
			}
			got := sendAudio(t.Context(), stream, q, closing)
			want := tc.sendErr
			if want == nil {
				want = tc.closeErr
			}
			if got.kind != tc.want || !errors.Is(got.err, want) {
				t.Fatalf("sender = %+v, want %v / %v", got, tc.want, want)
			}
			closed := false
			select {
			case <-closing:
				closed = true
			default:
			}
			if closed != tc.wantClosing {
				t.Fatalf("half-close started = %v", closed)
			}
			if tc.sendErr != nil {
				if sends != 1 || closes != 0 {
					t.Fatalf("continued after Send stopped: sends=%d closes=%d", sends, closes)
				}
				mustQueuePop(t, q, []byte{2, 0})
			} else if sends != 2 || closes != 1 {
				t.Fatalf("incomplete drain: sends=%d closes=%d", sends, closes)
			}
		})
	}
}

func TestGatewayRejectsAudioAfterEnd(t *testing.T) {
	halfClosed := make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &controlledWorkerStream{
			ctx:       ctx,
			send:      func(*asrv1.StreamingRecognizeRequest) error { return nil },
			closeSend: func() error { close(halfClosed); return nil },
			recv:      func() (*asrv1.StreamingRecognizeResponse, error) { <-ctx.Done(); return nil, ctx.Err() },
		}, nil
	}}
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	awaitGatewaySignal(t, h.ctx, halfClosed, "request half-closed")
	h.write(t, conn, websocket.MessageBinary, []byte{2, 0})
	h.expectClose(t, conn, websocket.StatusPolicyViolation)
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

func TestGatewayEmptyInputRemainsFailure(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{}))
	h := newGatewayHarness(t, worker, 1)
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectClose(t, conn, websocket.StatusInternalError)
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

func TestSessionWriteTimeoutCancelsWorker(t *testing.T) {
	audioSent, canceled := make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		go func() { <-ctx.Done(); close(canceled) }()
		return &controlledWorkerStream{
			ctx:  ctx,
			send: func(*asrv1.StreamingRecognizeRequest) error { close(audioSent); return nil },
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				select {
				case <-audioSent:
					return &asrv1.StreamingRecognizeResponse{Text: strings.Repeat("x", 8192)}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}, nil
	}}
	s, peer, _ := newDirectSession(t, context.Background(), worker)
	finished := runDirectStreamingTest(t, s, Config{AudioQueueMaxBytes: 32, AudioQueueMaxChunks: 4, ResultWriteTimeout: 50 * time.Millisecond})
	writeRawClientFrame(t, peer, 1, []byte(`{"type":"start","version":"v1"}`))
	writeRawClientFrame(t, peer, 2, []byte{0, 0})
	if _, err := io.ReadFull(peer, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, errResultWriteTimeout) {
			t.Fatalf("write stall lost timeout cause: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write timeout did not release Session")
	}
	awaitGatewaySignal(t, t.Context(), canceled, "write timeout canceled RPC")
}

// Worker 的等待时间不属于写入期限，前一个结果写完也不能取消下一个结果的上下文。
func TestGatewayWriteDeadlineAppliesPerResult(t *testing.T) {
	worker := startGatewayWorker(t, mockasr.New(mockasr.Config{
		PartialEvery: 100 * time.Millisecond, PartialTexts: []string{"first", "second"},
		ResponseDelay: 150 * time.Millisecond, FinalText: "final",
	}))
	h := newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: 1, ResultWriteTimeout: 100 * time.Millisecond})
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, make([]byte, 6400))
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectResult(t, conn, "first", false)
	h.expectResult(t, conn, "second", false)
	h.expectResult(t, conn, "final", true)
	h.expectClose(t, conn, websocket.StatusNormalClosure)
	h.waitHandlers(t, 1)
}

func stalledSendWorker(entered, canceled chan struct{}) *controlledWorkerClient {
	return &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		return &controlledWorkerStream{
			ctx: ctx,
			send: func(*asrv1.StreamingRecognizeRequest) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				return ctx.Err()
			},
			recv: func() (*asrv1.StreamingRecognizeResponse, error) { <-ctx.Done(); return nil, ctx.Err() },
		}, nil
	}}
}

// runDirectStreamingTest 设置独立的兜底截止时间；正常断言不依赖 Abort。
func runDirectStreamingTest(t *testing.T, s *session, cfg Config) <-chan error {
	t.Helper()
	_ = s.transport.SetDeadline(time.Now().Add(3 * time.Second))
	finished, exited := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(exited)
		finished <- s.run(cfg)
	}()
	t.Cleanup(func() {
		s.abort()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("direct Session leaked an execution loop")
		}
	})
	return finished
}
