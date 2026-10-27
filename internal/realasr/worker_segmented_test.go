package realasr

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// segmentedTestEngine 只由 RPC 串行操作；空流时嵌入指针为 nil，误用即测试失败。
// trace 支持阻塞阶段的并发观察，其余字段只在 RPC 退出后读取。
type segmentedTestEngine struct {
	*workerTestEngine
	segments []*workerTestEngine
	trace    *workerTestTrace
	hook     func(string) error
	onOpen   func(*workerTestEngine) // 首段之后的测试定制，例如次段失败。
	silent   bool
	starts   int
	releases int
	closed   int
}

func newSegmentedTestEngine(silent bool) *segmentedTestEngine {
	e := &segmentedTestEngine{trace: &workerTestTrace{}, silent: silent}
	e.open()
	return e
}

func (e *segmentedTestEngine) open() {
	part := newWorkerTestEngine()
	part.trace = e.trace
	if !e.silent {
		part.updates = []string{fmt.Sprintf("partial-%d", len(e.segments)+1)}
		text := fmt.Sprintf("tail-%d", len(e.segments)+1)
		part.finishText = &text
	}
	e.segments = append(e.segments, part)
	e.workerTestEngine = part
	if e.onOpen != nil {
		e.onOpen(part)
	}
}

func (e *segmentedTestEngine) StartSegment() error {
	e.trace.add("start")
	e.starts++
	if e.hook != nil {
		if err := e.hook("start"); err != nil {
			return err
		}
	}
	if e.workerTestEngine != nil || e.closed != 0 {
		return errors.New("test engine invalid segment start")
	}
	e.open()
	return nil
}

func (e *segmentedTestEngine) ReleaseSegment() {
	if e.workerTestEngine == nil {
		return
	}
	e.trace.add("release")
	if e.hook != nil {
		_ = e.hook("release")
	}
	e.workerTestEngine = nil
	e.releases++
}

func (e *segmentedTestEngine) Close() {
	e.trace.add("close")
	if e.hook != nil {
		_ = e.hook("close")
	}
	e.ReleaseSegment()
	e.closed++
	e.trace.add("closed")
}

func segmentedTestWorker(t *testing.T, total, segment int, factory SegmentedEngineFactory) *Worker {
	t.Helper()
	w, err := NewSegmented(Config{MaxAudioBytes: total, MaxChunkBytes: 3200}, segment, factory)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestSegmentedWorkerConstruction(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		size int
	}{
		{Config{}, 0}, {Config{}, -2}, {Config{}, 3}, {Config{}, 960002},
		{Config{MaxAudioBytes: -2}, 2}, {Config{MaxAudioBytes: 3}, 2},
		{Config{MaxChunkBytes: -1}, 2}, {Config{MaxChunkBytes: 3}, 2},
		{Config{MaxTextBytes: -1}, 2},
	} {
		if w, err := NewSegmented(tc.cfg, tc.size, func(context.Context) (SegmentedEngine, error) {
			t.Fatal("constructor called factory")
			return nil, nil
		}); w != nil || err == nil {
			t.Fatalf("accepted config %+v / %d", tc.cfg, tc.size)
		}
	}
	if w, err := NewSegmented(Config{}, 2, nil); w != nil || err == nil {
		t.Fatal("accepted nil factory")
	}
	w := segmentedTestWorker(t, 0, 960000, func(context.Context) (SegmentedEngine, error) { return nil, nil })
	if w.cfg != (Config{MaxAudioBytes: 960000, MaxChunkBytes: 3200, MaxTextBytes: 4096}) || w.factory != nil || len(w.active) != 0 {
		t.Fatalf("unexpected normalized mode: %+v", w)
	}
}

func TestSegmentedWorkerSplitAndEOF(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chunks  [][]byte
		segment int
		lengths []int
		silent  bool
	}{
		{"boundary_eof", [][]byte{make([]byte, 8), nil}, 4, []int{4, 4}, false},
		{"short_tail", [][]byte{make([]byte, 10)}, 4, []int{4, 4, 2}, false},
		{"cross_requests", [][]byte{make([]byte, 2), make([]byte, 6), nil, make([]byte, 2)}, 4, []int{4, 4, 2}, false},
		{"silence", [][]byte{make([]byte, 10)}, 4, []int{4, 4, 2}, true},
		{"single_segment", [][]byte{make([]byte, 2)}, 4, []int{2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var source []byte
			for _, chunk := range tc.chunks {
				for i := 0; i < len(chunk); i += 2 {
					binary.LittleEndian.PutUint16(chunk[i:], uint16(len(source)+i+1))
				}
				source = append(source, chunk...)
			}
			e := newSegmentedTestEngine(tc.silent)
			calls := 0
			w := segmentedTestWorker(t, max(len(source), tc.segment), tc.segment, func(context.Context) (SegmentedEngine, error) { calls++; return e, nil })
			s := &workerTestStream{ctx: context.Background(), requests: workerTestRequests(tc.chunks...), trace: e.trace}
			s.onSend = func(r *asrv1.StreamingRecognizeResponse) error {
				if r.IsFinal {
					e.trace.add("final:" + r.SegmentId)
				}
				return nil
			}
			if err := w.StreamingRecognize(s); err != nil {
				t.Fatal(err)
			}
			var accepted []float32
			var lengths []int
			for _, part := range e.segments {
				count := 0
				for _, chunk := range part.accepted {
					accepted = append(accepted, chunk...)
					count += len(chunk) * 2
				}
				lengths = append(lengths, count)
				if !part.finished {
					t.Fatal("segment not normally finished")
				}
			}
			if !reflect.DeepEqual(accepted, pcm16Samples(source)) || !reflect.DeepEqual(lengths, tc.lengths) {
				t.Fatalf("input mismatch: lengths %v", lengths)
			}
			var finals []string
			finalized := map[string]bool{}
			for _, r := range s.responses {
				if r.Progress != nil || finalized[r.SegmentId] {
					t.Fatalf("invalid update after final: %v", r)
				}
				if r.IsFinal {
					finals = append(finals, r.SegmentId)
					finalized[r.SegmentId] = true
					want := ""
					if !tc.silent {
						want = "tail-" + r.SegmentId
					}
					if r.Text != want {
						t.Fatalf("final text %q, want %q", r.Text, want)
					}
				}
			}
			for i, id := range finals {
				if id != strconv.Itoa(i+1) {
					t.Fatal(finals)
				}
			}
			if len(finals) != len(tc.lengths) || calls != 1 || e.starts != len(tc.lengths)-1 || e.releases != len(tc.lengths) || e.closed != 1 || len(w.active) != 0 {
				t.Fatalf("lifecycle finals=%v calls=%d starts=%d releases=%d close=%d active=%d", finals, calls, e.starts, e.releases, e.closed, len(w.active))
			}
			// 满段必须在下一次 Recv 之前完成 final 与释放。
			trace := e.trace.snapshot()
			for i, event := range trace {
				if strings.HasPrefix(event, "final:") && (i+1 >= len(trace) || trace[i+1] != "release") {
					t.Fatalf("not immediately released after final: %v", trace)
				}
			}
		})
	}
}

func TestSegmentedWorkerInputAndFactoryFailures(t *testing.T) {
	for _, tc := range []struct {
		name             string
		requests         []*asrv1.StreamingRecognizeRequest
		total, chunk     int
		code             codes.Code
		accepted, finals int
	}{
		{"empty", nil, 8, 8, codes.InvalidArgument, 0, 0},
		{"nil_request", []*asrv1.StreamingRecognizeRequest{nil}, 8, 8, codes.InvalidArgument, 0, 0},
		{"odd", workerTestRequests(make([]byte, 3)), 8, 8, codes.InvalidArgument, 0, 0},
		{"chunk_limit", workerTestRequests(make([]byte, 6)), 8, 4, codes.ResourceExhausted, 0, 0},
		{"whole_request_total_limit", workerTestRequests(make([]byte, 2), make([]byte, 8)), 8, 8, codes.ResourceExhausted, 2, 0},
		{"no_start_after_total_limit", workerTestRequests(make([]byte, 4), make([]byte, 6)), 8, 8, codes.ResourceExhausted, 4, 1},
		{"extra_after_total", workerTestRequests(make([]byte, 8), make([]byte, 2)), 8, 8, codes.ResourceExhausted, 8, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newSegmentedTestEngine(true)
			w, err := NewSegmented(Config{MaxAudioBytes: tc.total, MaxChunkBytes: tc.chunk}, 4, func(context.Context) (SegmentedEngine, error) { return e, nil })
			if err != nil {
				t.Fatal(err)
			}
			s := &workerTestStream{ctx: context.Background(), requests: tc.requests}
			if err := w.StreamingRecognize(s); status.Code(err) != tc.code {
				t.Fatalf("error %v, want %v", err, tc.code)
			}
			accepted, finals := 0, 0
			for _, part := range e.segments {
				for _, chunk := range part.accepted {
					accepted += len(chunk) * 2
				}
			}
			for _, r := range s.responses {
				if r.IsFinal {
					finals++
				}
			}
			if accepted != tc.accepted || finals != tc.finals || e.closed != 1 || len(w.active) != 0 {
				t.Fatalf("accepted=%d finals=%d closed=%d", accepted, finals, e.closed)
			}
			if tc.name == "no_start_after_total_limit" && e.starts != 0 {
				t.Fatal("oversized next request created stream")
			}
		})
	}
	for _, nilEngine := range []bool{false, true} {
		calls := 0
		e := newSegmentedTestEngine(true)
		w := segmentedTestWorker(t, 8, 4, func(context.Context) (SegmentedEngine, error) {
			calls++
			if calls == 1 {
				if nilEngine {
					return nil, nil
				}
				return nil, errors.New("private factory details")
			}
			return e, nil
		})
		if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background()}); status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private") || len(w.active) != 0 {
			t.Fatalf("factory failure: %v", err)
		}
		if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background(), requests: workerTestRequests(make([]byte, 2))}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSegmentedWorkerErrorsAndCancellation(t *testing.T) {
	for _, stage := range []string{"factory", "recv", "accept", "ready", "decode", "result", "finish", "tail_ready", "tail_decode", "tail_result", "partial_send", "final_send", "start", "release", "close"} {
		for _, cancelled := range []bool{false, true} {
			if !cancelled && (stage == "factory" || stage == "release" || stage == "close") {
				continue
			}
			t.Run(fmt.Sprintf("%s/cancel=%v", stage, cancelled), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				e := newSegmentedTestEngine(false)
				failure := errors.New("private model details")
				fail := func(name string) error {
					if name == stage {
						if cancelled {
							cancel()
						}
						return failure
					}
					return nil
				}
				e.hook = fail
				e.workerTestEngine.hook = func(name string) error {
					if e.workerTestEngine.finished {
						name = "tail_" + name
					}
					return fail(name)
				}
				w := segmentedTestWorker(t, 12, 4, func(context.Context) (SegmentedEngine, error) {
					if stage == "factory" {
						cancel()
					}
					return e, nil
				})
				s := &workerTestStream{ctx: ctx, requests: workerTestRequests(make([]byte, 8))}
				if stage == "recv" {
					s.recvErr = failure
					s.requests = nil
					s.onRecv = func(int) {
						if cancelled {
							cancel()
						}
					}
				}
				s.onSend = func(r *asrv1.StreamingRecognizeResponse) error {
					if r.IsFinal {
						return fail("final_send")
					}
					return fail("partial_send")
				}
				err := w.StreamingRecognize(s)
				// Close 无 error 返回；其执行中观察到的取消不反写已选定的 RPC 结果。
				if stage == "close" {
					if err != nil {
						t.Fatal(err)
					}
				} else if cancelled {
					if status.Code(err) != codes.Canceled {
						t.Fatalf("error=%v", err)
					}
				} else if stage == "recv" || strings.HasSuffix(stage, "_send") {
					if err != failure {
						t.Fatalf("lost transport error: %v", err)
					}
				} else if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private") {
					t.Fatalf("model error=%v", err)
				}
				if e.closed != 1 || len(w.active) != 0 {
					t.Fatal("failed to clean model and slot")
				}
				if stage != "start" && stage != "release" && stage != "close" {
					for _, r := range s.responses {
						if r.IsFinal {
							t.Fatalf("unexpected final: %v", r)
						}
					}
				}
				if stage == "start" {
					if e.starts != 1 || len(e.segments) != 1 {
						t.Fatal("retried failed start")
					}
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := segmentedTestWorker(t, 8, 4, func(context.Context) (SegmentedEngine, error) {
		t.Fatal("canceled RPC called factory")
		return nil, nil
	})
	if err := w.StreamingRecognize(&workerTestStream{ctx: ctx}); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}

func TestSegmentedWorkerWaitsForReleaseAndCloseBeforeReturningSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		released, allowRelease := make(chan struct{}), make(chan struct{})
		closing, allowClose := make(chan struct{}), make(chan struct{})
		var releaseOnce, closeOnce sync.Once
		defer releaseOnce.Do(func() { close(allowRelease) })
		defer closeOnce.Do(func() { close(allowClose) })
		e := newSegmentedTestEngine(false)
		e.hook = func(stage string) error {
			switch stage {
			case "release":
				close(released)
				<-allowRelease
			case "close":
				close(closing)
				<-allowClose
			}
			return nil
		}
		calls := 0
		w := segmentedTestWorker(t, 8, 4, func(context.Context) (SegmentedEngine, error) {
			calls++
			if calls == 1 {
				return e, nil
			}
			return newSegmentedTestEngine(true), nil
		})
		done := make(chan error, 1)
		go func() {
			done <- w.StreamingRecognize(&workerTestStream{ctx: ctx, requests: workerTestRequests(make([]byte, 8))})
		}()
		<-released
		probe := func() {
			t.Helper()
			if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background()}); status.Code(err) != codes.ResourceExhausted || calls != 1 {
				t.Fatalf("early slot reuse: %v calls=%d", err, calls)
			}
		}
		probe()
		cancel()
		synctest.Wait()
		select {
		case <-closing:
			t.Fatal("Close overlapped ReleaseSegment")
		default:
		}
		releaseOnce.Do(func() { close(allowRelease) })
		<-closing
		probe()
		select {
		case err := <-done:
			t.Fatalf("returned before Close: %v", err)
		default:
		}
		closeOnce.Do(func() { close(allowClose) })
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
		if e.starts != 0 || e.closed != 1 {
			t.Fatal("continued after cancellation")
		}
		if err := w.StreamingRecognize(&workerTestStream{ctx: context.Background(), requests: workerTestRequests(make([]byte, 2))}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSegmentedWorkerTCPFinalBeforeHalfClose(t *testing.T) {
	e := newSegmentedTestEngine(false)
	w := segmentedTestWorker(t, 10, 4, func(context.Context) (SegmentedEngine, error) { return e, nil })
	client, exits := startWorkerTestServer(t, w)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.StreamingRecognize(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2"} {
		if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 4)}); err != nil {
			t.Fatal(err)
		}
		for {
			r, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if r.SegmentId != id {
				t.Fatalf("unexpected ID %v", r)
			}
			if r.IsFinal {
				break
			}
		}
	}
	if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: make([]byte, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	final := false
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if r.SegmentId != "3" {
			t.Fatal(r)
		}
		final = final || r.IsFinal
	}
	awaitWorkerTestExit(t, exits, codes.OK)
	if !final || e.closed != 1 || e.releases != 3 || len(w.active) != 0 {
		t.Fatal("incomplete segmented stream")
	}
}

func TestSegmentedWorkerFailureAfterOldFinal(t *testing.T) {
	for _, stage := range []string{"accept", "finish", "invalid_utf8", "oversized_text", "cancel_after_start"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := newSegmentedTestEngine(false)
			e.onOpen = func(part *workerTestEngine) {
				switch stage {
				case "cancel_after_start":
					cancel()
				case "invalid_utf8":
					part.updates = []string{"\xff"}
				case "oversized_text":
					part.updates = []string{strings.Repeat("x", 4097)}
				default:
					part.hook = func(name string) error {
						if name == stage {
							return errors.New("private second segment failure")
						}
						return nil
					}
				}
			}
			w := segmentedTestWorker(t, 8, 4, func(context.Context) (SegmentedEngine, error) { return e, nil })
			s := &workerTestStream{ctx: ctx, requests: workerTestRequests(make([]byte, 8))}
			err := w.StreamingRecognize(s)
			want := codes.Internal
			if stage == "cancel_after_start" {
				want = codes.Canceled
			}
			if status.Code(err) != want || strings.Contains(err.Error(), "private") {
				t.Fatalf("failure=%v", err)
			}
			var finals []string
			for _, r := range s.responses {
				if r.IsFinal {
					finals = append(finals, r.SegmentId)
				}
			}
			if !reflect.DeepEqual(finals, []string{"1"}) || e.closed != 1 || e.starts != 1 || e.releases != 2 || len(w.active) != 0 {
				t.Fatalf("lost old final or leaked stream: finals=%v starts=%d releases=%d close=%d", finals, e.starts, e.releases, e.closed)
			}
			if stage == "cancel_after_start" && len(e.segments[1].accepted) != 0 {
				t.Fatal("accepted input after canceled creation")
			}
		})
	}
}
