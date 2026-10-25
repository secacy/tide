package realasr

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// workerTestTrace 可在受控 RPC 和测试方之间安全读取调用证据。
type workerTestTrace struct {
	mu     sync.Mutex
	events []string
}

func (l *workerTestTrace) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *workerTestTrace) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// workerTestEngine 只由 RPC 所在 goroutine 操作。
// 测试方通过同步信号观察阻塞阶段，或等待 RPC 完成后读取普通字段。
type workerTestEngine struct {
	trace          *workerTestTrace
	hook           func(string) error
	updates        []string
	text           string
	finishText     *string
	stepsPerAccept int
	tailSteps      int
	pending        int
	decoded        int
	finished       bool
	closed         int
	accepted       [][]float32
}

func newWorkerTestEngine(updates ...string) *workerTestEngine {
	return &workerTestEngine{trace: &workerTestTrace{}, updates: updates, stepsPerAccept: 1, tailSteps: 1}
}

func (e *workerTestEngine) call(name string) error {
	e.trace.add(name)
	if e.hook != nil {
		return e.hook(name)
	}
	return nil
}

func (e *workerTestEngine) Accept(samples []float32) error {
	if err := e.call("accept"); err != nil {
		return err
	}
	e.accepted = append(e.accepted, append([]float32(nil), samples...))
	e.pending += e.stepsPerAccept
	return nil
}

func (e *workerTestEngine) Ready() (bool, error) {
	if err := e.call("ready"); err != nil {
		return false, err
	}
	return e.pending > 0, nil
}

func (e *workerTestEngine) Decode() error {
	if err := e.call("decode"); err != nil {
		return err
	}
	if e.pending <= 0 {
		return errors.New("test engine decoded without ready work")
	}
	e.pending--
	if e.decoded < len(e.updates) {
		e.text = e.updates[e.decoded]
	}
	e.decoded++
	return nil
}

func (e *workerTestEngine) Result() (string, error) {
	if err := e.call("result"); err != nil {
		return "", err
	}
	return e.text, nil
}

func (e *workerTestEngine) Finish() error {
	if err := e.call("finish"); err != nil {
		return err
	}
	e.finished = true
	e.pending += e.tailSteps
	if e.finishText != nil {
		e.text = *e.finishText
	}
	return nil
}

func (e *workerTestEngine) Close() {
	_ = e.call("close")
	e.closed++
	e.trace.add("closed")
}

// workerTestStream 只模拟本 Worker 使用的方法，其余 ServerStream 方法若被
// 新实现使用，应显式扩展测试。successful responses 与发送尝试分别保存。
type workerTestStream struct {
	grpc.ServerStream
	ctx       context.Context
	trace     *workerTestTrace
	requests  []*asrv1.StreamingRecognizeRequest
	reads     int
	recvErr   error
	onRecv    func(int)
	onSend    func(*asrv1.StreamingRecognizeResponse) error
	attempts  []*asrv1.StreamingRecognizeResponse
	responses []*asrv1.StreamingRecognizeResponse
}

func (s *workerTestStream) Context() context.Context { return s.ctx }
func (s *workerTestStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	if s.trace != nil {
		s.trace.add("recv")
	}
	index := s.reads
	s.reads++
	if s.onRecv != nil {
		s.onRecv(index)
	}
	if index < len(s.requests) {
		return s.requests[index], nil
	}
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	return nil, io.EOF
}
func (s *workerTestStream) Send(r *asrv1.StreamingRecognizeResponse) error {
	if s.trace != nil {
		s.trace.add("send")
	}
	s.attempts = append(s.attempts, proto.Clone(r).(*asrv1.StreamingRecognizeResponse))
	if s.onSend != nil {
		if err := s.onSend(r); err != nil {
			return err
		}
	}
	s.responses = append(s.responses, proto.Clone(r).(*asrv1.StreamingRecognizeResponse))
	return nil
}

func workerTestRequests(chunks ...[]byte) []*asrv1.StreamingRecognizeRequest {
	requests := make([]*asrv1.StreamingRecognizeRequest, len(chunks))
	for i, chunk := range chunks {
		requests[i] = &asrv1.StreamingRecognizeRequest{Data: chunk}
	}
	return requests
}

func newWorkerTestWorker(t *testing.T, cfg Config, factory EngineFactory) *Worker {
	t.Helper()
	w, err := New(cfg, factory)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func assertWorkerTestCleaned(t *testing.T, w *Worker, e *workerTestEngine) {
	t.Helper()
	if e.closed != 1 || len(w.active) != 0 {
		t.Fatalf("closed=%d active=%d, want exactly one Close and no active slot", e.closed, len(w.active))
	}
}

func TestWorkerConstruction(t *testing.T) {
	factoryCalls := 0
	factory := func(context.Context) (Engine, error) { factoryCalls++; return newWorkerTestEngine(), nil }
	for _, cfg := range []Config{
		{MaxAudioBytes: -2}, {MaxChunkBytes: -2}, {MaxTextBytes: -1},
		{MaxAudioBytes: 3}, {MaxChunkBytes: 3},
	} {
		if w, err := New(cfg, factory); w != nil || err == nil {
			t.Fatalf("New(%+v)=(%v,%v), want rejection", cfg, w, err)
		}
	}
	if w, err := New(Config{}, nil); w != nil || err == nil {
		t.Fatalf("nil factory accepted: %v, %v", w, err)
	}
	w := newWorkerTestWorker(t, Config{}, factory)
	if w.cfg != (Config{MaxAudioBytes: 960000, MaxChunkBytes: 3200, MaxTextBytes: 4096}) || cap(w.active) != 1 || len(w.active) != 0 {
		t.Fatalf("unexpected default configuration: %+v, slot %d/%d", w.cfg, len(w.active), cap(w.active))
	}
	cfg := Config{MaxAudioBytes: 8, MaxChunkBytes: 4, MaxTextBytes: 6}
	w = newWorkerTestWorker(t, cfg, factory)
	cfg.MaxAudioBytes = 20
	if w.cfg.MaxAudioBytes != 8 || factoryCalls != 0 {
		t.Fatalf("configuration not copied or model loaded during New: %+v, calls=%d", w.cfg, factoryCalls)
	}
}

func TestPCM16Samples(t *testing.T) {
	values := []int16{-32768, -1, 0, 1, 32767}
	data := make([]byte, 2*len(values))
	want := make([]float32, len(values))
	for i, value := range values {
		binary.LittleEndian.PutUint16(data[2*i:], uint16(value))
		want[i] = float32(value) / 32768
	}
	got := pcm16Samples(data)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("samples=%v, want %v", got, want)
	}
	clear(data)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("sample output aliases input bytes")
	}
}

func TestWorkerStreamsRevisionsAndDrainsTail(t *testing.T) {
	e := newWorkerTestEngine("患者发烧", "", "患者没有发烧", "患者没有发烧")
	e.tailSteps = 2 // 尾部需要多次 Decode，不能只排空一步。
	w := newWorkerTestWorker(t, Config{MaxAudioBytes: 6, MaxChunkBytes: 4}, func(context.Context) (Engine, error) { return e, nil })
	s := &workerTestStream{ctx: context.Background(), trace: e.trace, requests: workerTestRequests(nil, []byte{0, 0x80, 0xff, 0x7f}, []byte{1, 0})}
	s.onRecv = func(index int) {
		if index == 2 && len(s.responses) != 1 {
			t.Fatal("first partial was not delivered before next input block")
		}
	}
	if err := w.StreamingRecognize(s); err != nil {
		t.Fatal(err)
	}
	if len(s.responses) != 4 {
		t.Fatalf("responses=%v, want initial text, withdrawal, tail correction, final", s.responses)
	}
	for i, text := range []string{"患者发烧", "", "患者没有发烧", "患者没有发烧"} {
		assertSnapshotResponse(t, s.responses[i], text, i == 3)
	}
	if !reflect.DeepEqual(e.accepted, [][]float32{{-1, float32(32767) / 32768}, {float32(1) / 32768}}) || e.decoded != 4 || !e.finished {
		t.Fatalf("input/tail mismatch: accepted=%v decoded=%d finished=%v", e.accepted, e.decoded, e.finished)
	}
	events := e.trace.snapshot()
	finish := -1
	for i, event := range events {
		if event == "finish" {
			finish = i
		}
	}
	var tailDecodes int
	for _, event := range events[finish+1:] {
		if event == "decode" {
			tailDecodes++
		}
	}
	if finish < 0 || tailDecodes != 2 || !reflect.DeepEqual(events[len(events)-4:], []string{"result", "send", "close", "closed"}) {
		t.Fatalf("tail order invalid: %v", events)
	}
	assertWorkerTestCleaned(t, w, e)
}

func TestWorkerSilenceAndFinalSnapshotWithoutDecode(t *testing.T) {
	for _, finalText := range []string{"", "尾部文本"} {
		t.Run(finalText, func(t *testing.T) {
			e := newWorkerTestEngine()
			e.stepsPerAccept, e.tailSteps = 0, 0
			e.finishText = &finalText
			w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) { return e, nil })
			s := &workerTestStream{ctx: context.Background(), requests: workerTestRequests([]byte{0, 0})}
			if err := w.StreamingRecognize(s); err != nil {
				t.Fatal(err)
			}
			if len(s.responses) != 1 {
				t.Fatalf("expected exactly one final, got %v", s.responses)
			}
			assertSnapshotResponse(t, s.responses[0], finalText, true)
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerRejectsInvalidAudioBeforeAccept(t *testing.T) {
	for _, tc := range []struct {
		name     string
		requests []*asrv1.StreamingRecognizeRequest
		code     codes.Code
		accepted int
	}{
		{"empty_stream", nil, codes.InvalidArgument, 0},
		{"only_empty_chunks", workerTestRequests(nil, []byte{}), codes.InvalidArgument, 0},
		{"nil_request", []*asrv1.StreamingRecognizeRequest{nil}, codes.InvalidArgument, 0},
		{"odd_chunk", workerTestRequests([]byte{1}), codes.InvalidArgument, 0},
		{"oversized_chunk", workerTestRequests(make([]byte, 6)), codes.ResourceExhausted, 0},
		{"total_limit", workerTestRequests(make([]byte, 4), make([]byte, 4)), codes.ResourceExhausted, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newWorkerTestEngine("partial")
			w := newWorkerTestWorker(t, Config{MaxChunkBytes: 4, MaxAudioBytes: 6}, func(context.Context) (Engine, error) { return e, nil })
			s := &workerTestStream{ctx: context.Background(), requests: tc.requests}
			if err := w.StreamingRecognize(s); status.Code(err) != tc.code {
				t.Fatalf("error=%v, want %v", err, tc.code)
			}
			if len(e.accepted) != tc.accepted || e.finished {
				t.Fatalf("accepted=%d finished=%v", len(e.accepted), e.finished)
			}
			for _, r := range s.attempts {
				if r.IsFinal || r.Progress != nil {
					t.Fatalf("invalid input produced final/progress: %v", r)
				}
			}
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerFactoryFailureReturnsSlot(t *testing.T) {
	for _, nilEngine := range []bool{false, true} {
		calls := 0
		e := newWorkerTestEngine()
		w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) {
			calls++
			if calls == 1 {
				if nilEngine {
					return nil, nil
				}
				return nil, errors.New("private factory details")
			}
			return e, nil
		})
		err := w.StreamingRecognize(&workerTestStream{ctx: context.Background()})
		if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private") || len(w.active) != 0 {
			t.Fatalf("factory failure=%v active=%d", err, len(w.active))
		}
		if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background(), requests: workerTestRequests([]byte{0, 0})}); err != nil {
			t.Fatalf("slot was not reusable: %v", err)
		}
		assertWorkerTestCleaned(t, w, e)
	}
}

func TestWorkerModelFailuresDoNotFinalize(t *testing.T) {
	for _, stage := range []string{"accept", "ready", "decode", "result", "finish", "tail_ready", "tail_decode", "tail_result", "invalid_utf8", "oversized_text"} {
		t.Run(stage, func(t *testing.T) {
			e := newWorkerTestEngine("字")
			e.hook = func(name string) error {
				target := name
				if e.finished {
					target = "tail_" + name
				}
				if target == stage {
					return errors.New("private recognition details")
				}
				return nil
			}
			if stage == "invalid_utf8" {
				e.updates = []string{"\xff"}
			}
			if stage == "oversized_text" {
				e.updates = []string{"文字"}
			}
			w := newWorkerTestWorker(t, Config{MaxTextBytes: 3}, func(context.Context) (Engine, error) { return e, nil })
			s := &workerTestStream{ctx: context.Background(), requests: workerTestRequests([]byte{0, 0})}
			err := w.StreamingRecognize(s)
			if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private") {
				t.Fatalf("model failure=%v, want sanitized Internal", err)
			}
			for _, r := range s.attempts {
				if r.IsFinal {
					t.Fatalf("model failure emitted final: %v", r)
				}
			}
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerTransportErrorsArePreserved(t *testing.T) {
	for _, stage := range []string{"recv", "partial_send", "final_send"} {
		t.Run(stage, func(t *testing.T) {
			e := newWorkerTestEngine("partial")
			transportErr := status.Error(codes.Unavailable, "transport stopped")
			w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) { return e, nil })
			s := &workerTestStream{ctx: context.Background(), requests: workerTestRequests([]byte{0, 0})}
			if stage == "recv" {
				s.recvErr = transportErr
			} else {
				s.onSend = func(r *asrv1.StreamingRecognizeResponse) error {
					if r.IsFinal == (stage == "final_send") {
						return transportErr
					}
					return nil
				}
			}
			if err := w.StreamingRecognize(s); err != transportErr {
				t.Fatalf("error=%v, want original transport error %v", err, transportErr)
			}
			for _, r := range s.responses {
				if r.IsFinal {
					t.Fatal("transport failure produced a successful final")
				}
			}
			if stage != "final_send" && e.finished {
				t.Fatal("transport error triggered normal input finish")
			}
			wantAttempts := 1
			if stage == "final_send" {
				wantAttempts = 2
			}
			if len(s.attempts) != wantAttempts {
				t.Fatalf("attempts=%d, want %d without retries", len(s.attempts), wantAttempts)
			}
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerCancellationTakesPriorityAtCallBoundaries(t *testing.T) {
	for _, stage := range []string{"factory", "recv", "accept", "ready", "decode", "result", "finish", "send"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := newWorkerTestEngine("partial")
			e.hook = func(name string) error {
				if name == stage {
					cancel()
					return errors.New("model failed during cancellation")
				}
				return nil
			}
			w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) {
				if stage == "factory" {
					cancel() // 返回有效资源，Worker 仍必须 Close。
				}
				return e, nil
			})
			s := &workerTestStream{ctx: ctx, requests: workerTestRequests([]byte{0, 0})}
			if stage == "recv" {
				s.requests = nil
				s.onRecv = func(int) { cancel() } // 同时返回 EOF，不能误判为空输入。
			}
			if stage == "send" {
				s.onSend = func(*asrv1.StreamingRecognizeResponse) error { cancel(); return io.ErrClosedPipe }
			}
			if err := w.StreamingRecognize(s); status.Code(err) != codes.Canceled {
				t.Fatalf("error=%v, want Canceled", err)
			}
			for _, r := range s.attempts {
				if r.IsFinal {
					t.Fatal("cancellation triggered a final attempt")
				}
			}
			assertWorkerTestCleaned(t, w, e)
		})
	}
}

func TestWorkerAlreadyCanceledOrExpiredDoesNotCreateEngine(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		want := codes.Canceled
		if deadline {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = codes.DeadlineExceeded
		}
		cancel()
		calls := 0
		w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) { calls++; return nil, nil })
		if err := w.StreamingRecognize(&workerTestStream{ctx: ctx}); status.Code(err) != want || calls != 0 || len(w.active) != 0 {
			t.Fatalf("error=%v factoryCalls=%d active=%d", err, calls, len(w.active))
		}
	}
}

func TestWorkerCancellationWaitsForDecodeAndCloseBeforeReusingSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		factoryEntered, releaseFactory := make(chan struct{}), make(chan struct{})
		decodeEntered, releaseDecode := make(chan struct{}), make(chan struct{})
		closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
		var factoryOnce, decodeOnce, closeOnce sync.Once
		defer factoryOnce.Do(func() { close(releaseFactory) })
		defer decodeOnce.Do(func() { close(releaseDecode) })
		defer closeOnce.Do(func() { close(releaseClose) })
		e := newWorkerTestEngine("partial")
		e.hook = func(name string) error {
			switch name {
			case "decode":
				close(decodeEntered)
				<-releaseDecode
			case "close":
				close(closeEntered)
				<-releaseClose
			}
			return nil
		}
		next := newWorkerTestEngine()
		calls := 0
		w := newWorkerTestWorker(t, Config{}, func(context.Context) (Engine, error) {
			calls++
			if calls == 1 {
				close(factoryEntered)
				<-releaseFactory
				return e, nil
			}
			return next, nil
		})
		s := &workerTestStream{ctx: ctx, requests: workerTestRequests([]byte{0, 0})}
		probeBusy := func() {
			t.Helper()
			if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background()}); status.Code(err) != codes.ResourceExhausted || calls != 1 {
				t.Fatalf("busy probe=%v factoryCalls=%d", err, calls)
			}
		}
		done := make(chan error, 1)
		go func() { done <- w.StreamingRecognize(s) }()
		<-factoryEntered
		probeBusy() // 模型尚未创建完成，也已经占用唯一名额。
		factoryOnce.Do(func() { close(releaseFactory) })
		<-decodeEntered
		cancel()
		synctest.Wait()
		select {
		case <-closeEntered:
			t.Fatal("Close overlapped blocked Decode")
		default:
		}
		select {
		case err := <-done:
			t.Fatalf("RPC returned before Decode completed: %v", err)
		default:
		}
		probeBusy()
		decodeOnce.Do(func() { close(releaseDecode) })
		<-closeEntered
		probeBusy() // 已离开 Decode，但 Close 未返回，名额仍不能复用。
		closeOnce.Do(func() { close(releaseClose) })
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Fatalf("canceled RPC=%v", err)
		}
		if e.finished || len(s.attempts) != 0 {
			t.Fatal("canceled Decode continued into result/final processing")
		}
		assertWorkerTestCleaned(t, w, e)
		if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background(), requests: workerTestRequests([]byte{0, 0})}); err != nil {
			t.Fatalf("new RPC after actual cleanup failed: %v", err)
		}
		assertWorkerTestCleaned(t, w, next)
	})
}
