package gateway

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func testProgress(t *testing.T, capacity int) *processingProgress {
	t.Helper()
	p, err := newProcessingProgress(capacity, 3*time.Second, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// 已发送但未确认的块仍占记录名额；确认释放后环形表可以重复使用。
func TestProcessingProgressCapacityAndWatermarks(t *testing.T) {
	p := testProgress(t, 2)
	now := p.epoch
	for seq := uint64(1); seq <= 2; seq++ {
		if err := p.admit(now); err != nil {
			t.Fatal(err)
		}
		if err := p.startSend(seq); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.admit(now); !errors.Is(err, errProgressCapacity) {
		t.Fatalf("unconfirmed capacity: %v", err)
	}
	if err := p.acknowledge(3, now); !errors.Is(err, errInvalidProgress) {
		t.Fatalf("future ack: %v", err)
	}
	if err := p.acknowledge(1, now); err != nil {
		t.Fatal(err)
	}
	if err := p.admit(now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := p.acknowledge(3, now); !errors.Is(err, errInvalidProgress) {
		t.Fatal("ack accepted before Send began")
	}
	if err := p.startSend(3); err != nil {
		t.Fatal(err)
	}
	if err := p.acknowledge(3, now); err != nil {
		t.Fatal(err)
	}
	if err := p.acknowledge(3, now); err != nil {
		t.Fatal("duplicate ack rejected", err)
	}
	if err := p.acknowledge(2, now); !errors.Is(err, errInvalidProgress) {
		t.Fatal("backward ack accepted")
	}
	if p.count != 0 || p.admitted != 3 || p.acknowledged != 3 {
		t.Fatalf("bad watermarks: %+v", p)
	}
	for _, stamp := range p.times {
		if stamp != 0 {
			t.Fatal("retained acknowledged timestamp")
		}
	}
	if deadline, err := p.status(now.Add(time.Hour)); err != nil || !deadline.IsZero() {
		t.Fatalf("processed silence started a timeout: %v %v", deadline, err)
	}
}

// 后到确认不能清除已经达到的期限，即使协调者尚未来得及消费计时器。
func TestProcessingProgressLateAcknowledgement(t *testing.T) {
	for _, offset := range []time.Duration{3 * time.Second, 4 * time.Second} {
		p := testProgress(t, 2)
		now := p.epoch
		_ = p.admit(now)
		_ = p.startSend(1)
		if _, err := p.status(now.Add(3*time.Second - time.Nanosecond)); err != nil {
			t.Fatal("expired early")
		}
		if err := p.acknowledge(1, now.Add(offset)); !errors.Is(err, errProcessingTimeout) {
			t.Fatalf("late ack masked deadline: %v", err)
		}
		if p.acknowledged != 0 {
			t.Fatal("late ack advanced watermark")
		}
		if err := p.complete(now.Add(offset)); !errors.Is(err, errProcessingTimeout) {
			t.Fatalf("late completion succeeded: %v", err)
		}
	}
}

func TestProcessingProgressEndBudgetAndMissingTail(t *testing.T) {
	p := testProgress(t, 2)
	now := p.epoch
	_ = p.admit(now)
	_ = p.startSend(1)
	p.end(now)
	if err := p.complete(now); !errors.Is(err, errUnconfirmedAudio) {
		t.Fatalf("missing tail accepted: %v", err)
	}
	if err := p.acknowledge(1, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	p.end(now.Add(4 * time.Second))
	deadline, err := p.status(now.Add(4 * time.Second))
	if err != nil || !deadline.Equal(now.Add(5*time.Second)) {
		t.Fatal("End was extended")
	}
	if err := p.complete(now.Add(5 * time.Second)); !errors.Is(err, errEndTimeout) {
		t.Fatalf("late EOF succeeded: %v", err)
	}
}

// 收到确认后旧音频期限必须撤销；新音频拥有自己的到达时间。
func TestProcessingProgressMovesDeadline(t *testing.T) {
	p := testProgress(t, 2)
	now := p.epoch
	_ = p.admit(now)
	_ = p.startSend(1)
	_ = p.admit(now.Add(time.Second))
	_ = p.startSend(2)
	if err := p.acknowledge(1, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	deadline, err := p.status(now.Add(3 * time.Second))
	if err != nil || !deadline.Equal(now.Add(4*time.Second)) {
		t.Fatalf("stale deadline: %v %v", deadline, err)
	}
}

// 无新输入/结果时，协调者必须仍能被音频或 End 期限唤醒。
func TestProcessingCoordinatorDeadlines(t *testing.T) {
	for _, end := range []bool{false, true} {
		p, err := newProcessingProgress(2, 30*time.Millisecond, 30*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		want := resultProcessingTimedOut
		if end {
			p.end(now)
			want = resultEndTimedOut
		} else {
			_ = p.admit(now)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		got := waitSessionResult(ctx, make(chan sessionResult), make(chan struct{}), p)
		cancel()
		if got.kind != want {
			t.Fatalf("got %+v want %v", got, want)
		}
	}
}

// 高频并发进度推进与协调者计时不得丢通知、串水位或留下未确认记录。
func TestProcessingProgressConcurrentAdvance(t *testing.T) {
	p := testProgress(t, 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	work, acked := make(chan uint64), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := range work {
			if err := p.acknowledge(seq, time.Now()); err != nil {
				t.Error(err)
			}
			acked <- struct{}{}
		}
	}()
	for seq := uint64(1); seq <= 1000; seq++ {
		if err := p.admit(time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := p.startSend(seq); err != nil {
			t.Fatal(err)
		}
		select {
		case work <- seq:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		<-acked
		if _, err := p.status(time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	close(work)
	wg.Wait()
	if err := p.complete(time.Now()); err != nil {
		t.Fatal(err)
	}
}

// 用实际 gRPC 和 WS 验证生产关闭码/原因，而不以实验适配器代替协议。
func TestGatewayProcessingProtocolAndTimeouts(t *testing.T) {
	cases := []struct {
		name   string
		cfg    Config
		code   websocket.StatusCode
		reason string
	}{
		{"stalled", Config{ProcessingTimeout: 40 * time.Millisecond}, websocket.StatusTryAgainLater, "processing_timeout"},
		{"end_hang", Config{EndTimeout: 40 * time.Millisecond}, websocket.StatusInternalError, "end_timeout"},
		{"future", Config{}, websocket.StatusInternalError, "worker failed"},
		{"backward", Config{}, websocket.StatusInternalError, "worker failed"},
		{"mixed", Config{}, websocket.StatusInternalError, "worker failed"},
		{"missing", Config{}, websocket.StatusInternalError, "worker failed"},
		{"duplicate", Config{}, websocket.StatusNormalClosure, "completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stopped := make(chan struct{})
			worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
				defer close(stopped)
				req, err := stream.Recv()
				if err != nil {
					return err
				}
				if req.AudioSeq != 1 {
					return errors.New("first audio sequence must be 1")
				}
				ack := func(seq uint64) error {
					return stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: seq}})
				}
				switch tc.name {
				case "future":
					return ack(2)
				case "mixed":
					return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "not allowed", Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: 1}})
				case "backward":
					if err := ack(1); err != nil {
						return err
					}
					return ack(0)
				case "stalled":
					<-stream.Context().Done()
					return stream.Context().Err()
				case "missing": // EOF after legal half-close, but no processed confirmation.
				default:
					if err := ack(1); err != nil {
						return err
					}
				}
				if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
					return err
				}
				if tc.name == "end_hang" {
					<-stream.Context().Done()
					return stream.Context().Err()
				}
				if tc.name == "duplicate" {
					if err := ack(1); err != nil {
						return err
					}
				}
				return nil
			}})
			cfg := tc.cfg
			cfg.MaxSessions = 1
			h := newGatewayHarnessWithConfig(t, worker, cfg)
			conn := h.mustDial(t)
			h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
			h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
			if tc.name == "end_hang" || tc.name == "missing" || tc.name == "duplicate" {
				h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
			}
			_, _, err := conn.Read(h.ctx)
			var closed websocket.CloseError
			if !errors.As(err, &closed) || closed.Code != tc.code || closed.Reason != tc.reason {
				t.Fatalf("close=%v want %d/%s", err, tc.code, tc.reason)
			}
			h.waitHandlers(t, 1)
			awaitGatewaySignal(t, h.ctx, stopped, "Worker exited")
			assertRegistrySessions(t, h.gateway.registry)
		})
	}
}

func TestGatewayUnprocessedCapacityIncludesSentAudio(t *testing.T) {
	received := make(chan struct{})
	worker := startGatewayWorker(t, gatewayWorkerServer{run: func(stream asrv1.ASRService_StreamingRecognizeServer) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		close(received)
		<-stream.Context().Done()
		return stream.Context().Err()
	}})
	h := newGatewayHarnessWithConfig(t, worker, Config{MaxSessions: 1, MaxUnprocessedChunks: 1})
	conn := h.mustDial(t)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`))
	h.write(t, conn, websocket.MessageBinary, []byte{1, 0})
	awaitGatewaySignal(t, h.ctx, received, "first block received, not processed")
	h.write(t, conn, websocket.MessageBinary, []byte{2, 0})
	_, _, err := conn.Read(h.ctx)
	var closed websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.StatusTryAgainLater || closed.Reason != "progress_capacity" {
		t.Fatalf("unexpected capacity close: %v", err)
	}
	h.waitHandlers(t, 1)
	assertRegistrySessions(t, h.gateway.registry)
}

// 确认发生在 Send 返回之前也必须合法，不能以 Send 返回水位校验。
func TestGatewayAcknowledgementBeforeSendReturns(t *testing.T) {
	entered, acknowledged, release, halfClosed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	worker := &controlledWorkerClient{open: func(ctx context.Context) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
		recvs := 0
		return &controlledWorkerStream{ctx: ctx,
			send: func(req *asrv1.StreamingRecognizeRequest) error {
				if req.AudioSeq != 1 {
					return errors.New("bad request sequence")
				}
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			closeSend: func() error { close(halfClosed); return nil },
			recv: func() (*asrv1.StreamingRecognizeResponse, error) {
				recvs++
				if recvs == 1 {
					select {
					case <-entered:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return &asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: 1}}, nil
				}
				// 再次 Recv 说明 download 已经成功处理上一次确认。
				close(acknowledged)
				select {
				case <-halfClosed:
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
	awaitGatewaySignal(t, h.ctx, acknowledged, "confirmation accepted before Send returned")
	close(release)
	h.write(t, conn, websocket.MessageText, []byte(`{"type":"end"}`))
	h.expectClose(t, conn, websocket.StatusNormalClosure)
	h.waitHandlers(t, 1)
}
