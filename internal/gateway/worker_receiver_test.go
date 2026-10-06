package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"testing/synctest"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/protobuf/proto"
)

func TestWorkerReceiverConstructionAndInvalidStartup(t *testing.T) {
	a, b := newWorkerReceiver(), newWorkerReceiver()
	if a.events == nil || a.done == nil || cap(a.events) != 0 || a.err != nil {
		t.Fatal("receiver must start with an independent unbuffered event channel and open completion")
	}
	if a.events == b.events || a.done == b.done {
		t.Fatal("receivers share channels")
	}
	select {
	case <-a.done:
		t.Fatal("construction started the receiver")
	default:
	}
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		stream workerStream
	}{
		{"nil context", nil, &uploadTestStream{}},
		{"nil stream", context.Background(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newWorkerReceiver()
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("invalid startup did not panic")
					}
				}()
				r.run(tc.ctx, tc.stream)
			}()
			select {
			case <-r.done:
				t.Fatal("invalid startup published completion")
			default:
			}
		})
	}
}

func TestDecodeWorkerResponse(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   *asrv1.StreamingRecognizeResponse
		want workerReceiveEvent
		bad  bool
	}{
		{"nil", nil, workerReceiveEvent{}, true},
		{"progress with segment", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}, SegmentId: "s"}, workerReceiveEvent{}, true},
		{"progress with text", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}, Text: "x"}, workerReceiveEvent{}, true},
		{"progress final", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}, IsFinal: true}, workerReceiveEvent{}, true},
		{"zero progress", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}}, workerReceiveEvent{kind: workerReceiveProgress}, false},
		{"positive progress", &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 17}}, workerReceiveEvent{kind: workerReceiveProgress, processedBytes: 17}, false},
		{"partial", &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "a"}, workerReceiveEvent{kind: workerReceiveResult, segmentID: "s", text: "a"}, false},
		{"final", &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: "abc", IsFinal: true}, workerReceiveEvent{kind: workerReceiveResult, segmentID: "s", text: "abc", isFinal: true}, false},
		{"empty result", &asrv1.StreamingRecognizeResponse{}, workerReceiveEvent{kind: workerReceiveResult}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before proto.Message
			if tc.in != nil {
				before = proto.Clone(tc.in)
			}
			got, err := decodeWorkerResponse(tc.in)
			if !reflect.DeepEqual(got, tc.want) || (errors.Is(err, errInvalidWorkerResponse) != tc.bad) {
				t.Fatalf("decode = (%+v, %v), want (%+v, bad=%v)", got, err, tc.want, tc.bad)
			}
			if tc.in != nil && !proto.Equal(tc.in, before) {
				t.Fatal("parser modified response")
			}
		})
	}
}

func TestWorkerReceiverOrderedEventsAndEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		responses := []*asrv1.StreamingRecognizeResponse{
			{SegmentId: "s", Text: "a"},
			{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 5}},
			{SegmentId: "s", Text: "ab", IsFinal: true},
			{SegmentId: "next", Text: "tail", IsFinal: true},
		}
		i := 0
		stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
			if i == len(responses) {
				return nil, fmt.Errorf("wrapped: %w", io.EOF)
			}
			resp := responses[i]
			i++
			return resp, nil
		}}
		r := newWorkerReceiver()
		go r.run(ctx, stream)
		want := []workerReceiveEvent{
			{kind: workerReceiveResult, segmentID: "s", text: "a"},
			{kind: workerReceiveProgress, processedBytes: 5},
			{kind: workerReceiveResult, segmentID: "s", text: "ab", isFinal: true},
			{kind: workerReceiveResult, segmentID: "next", text: "tail", isFinal: true},
		}
		for _, expected := range want {
			if got := <-r.events; got != expected {
				t.Fatalf("event = %+v, want %+v", got, expected)
			}
		}
		<-r.done
		if r.err != nil || i != len(responses) {
			t.Fatalf("completion = %v, reads=%d", r.err, i)
		}
	})
}

func TestWorkerReceiverBackpressureAndCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		reads := make(chan int, 3)
		count := 0
		stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
			count++
			reads <- count
			if count <= 2 {
				return &asrv1.StreamingRecognizeResponse{Text: fmt.Sprint(count)}, nil
			}
			return nil, io.EOF
		}}
		r := newWorkerReceiver()
		go r.run(ctx, stream)
		if n := <-reads; n != 1 {
			t.Fatalf("first read = %d", n)
		}
		synctest.Wait()
		if len(reads) != 0 || count != 1 {
			t.Fatal("receiver read ahead while first event had no consumer")
		}
		select {
		case <-r.done:
			t.Fatal("completion published before event handoff")
		default:
		}
		if got := <-r.events; got.text != "1" {
			t.Fatalf("first event = %+v", got)
		}
		if n := <-reads; n != 2 {
			t.Fatalf("second read = %d", n)
		}
		synctest.Wait()
		if count != 2 {
			t.Fatal("receiver read ahead while second event had no consumer")
		}
		if got := <-r.events; got.text != "2" {
			t.Fatalf("second event = %+v", got)
		}
		<-r.done
		if r.err != nil || count != 3 {
			t.Fatalf("completion = %v, reads=%d", r.err, count)
		}
	})
}

func TestWorkerReceiverReadTermination(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *asrv1.StreamingRecognizeResponse
		err  error
		want error
	}{
		{"read error", nil, errors.New("read failed"), nil},
		{"response and error", &asrv1.StreamingRecognizeResponse{Text: "ignored"}, errors.New("read failed"), nil},
		{"nil success response", nil, nil, errInvalidWorkerResponse},
		{"invalid progress", &asrv1.StreamingRecognizeResponse{Text: "bad", Progress: &asrv1.AudioProgress{}}, nil, errInvalidWorkerResponse},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				calls := 0
				stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
					calls++
					if calls > 1 {
						t.Fatal("receiver read after terminal error")
					}
					return tc.resp, tc.err
				}}
				r := newWorkerReceiver()
				go r.run(ctx, stream)
				<-r.done
				want := tc.want
				if want == nil {
					want = tc.err
				}
				if !errors.Is(r.err, want) || r.err == nil || calls != 1 {
					t.Fatalf("completion = %v, reads=%d, want %v", r.err, calls, want)
				}
				select {
				case event := <-r.events:
					t.Fatalf("terminal read delivered event %+v", event)
				default:
				}
				if context.Cause(ctx) != nil {
					t.Fatal("receiver canceled RPC on its own")
				}
			})
		})
	}
}

func TestWorkerReceiverCancellation(t *testing.T) {
	cause := errors.New("session stopped")
	for _, name := range []string{"before read", "during read nil", "during read EOF", "waiting to publish"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				reads := 0
				entered := make(chan struct{})
				release := make(chan struct{})
				stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
					reads++
					switch name {
					case "during read nil", "during read EOF":
						close(entered)
						<-release
						if name == "during read EOF" {
							return nil, io.EOF
						}
						return nil, nil
					default:
						return &asrv1.StreamingRecognizeResponse{Text: "unconsumed"}, nil
					}
				}}
				r := newWorkerReceiver()
				if name == "before read" {
					cancel(cause)
				}
				go r.run(ctx, stream)
				switch name {
				case "during read nil", "during read EOF":
					<-entered
					cancel(cause)
					synctest.Wait()
					select {
					case <-r.done:
						t.Fatal("done closed while Recv still blocked")
					default:
					}
					close(release)
				case "waiting to publish":
					synctest.Wait()
					if reads != 1 {
						t.Fatalf("reads before cancel = %d", reads)
					}
					cancel(cause)
				}
				<-r.done
				if !errors.Is(r.err, cause) {
					t.Fatalf("cause = %v, want %v", r.err, cause)
				}
				wantReads := 1
				if name == "before read" {
					wantReads = 0
				}
				if reads != wantReads {
					t.Fatalf("reads = %d, want %d", reads, wantReads)
				}
				select {
				case event := <-r.events:
					t.Fatalf("canceled response delivered: %+v", event)
				default:
				}
			})
		})
	}
}

func TestWorkerReceiverResultBufferFixture(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		responses := []*asrv1.StreamingRecognizeResponse{
			{SegmentId: "s", Text: "a"},
			{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 3}},
			{SegmentId: "s", Text: "ab", IsFinal: true},
			{SegmentId: "t", Text: "tail", IsFinal: true},
		}
		i := 0
		stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
			if i == len(responses) {
				return nil, io.EOF
			}
			resp := responses[i]
			i++
			return resp, nil
		}}
		b, err := newResultBuffer(100, 3)
		if err != nil {
			t.Fatal(err)
		}
		r := newWorkerReceiver()
		go r.run(ctx, stream)
		var progress []uint64
		for processed := 0; processed < len(responses); processed++ {
			event := <-r.events
			switch event.kind {
			case workerReceiveProgress:
				progress = append(progress, event.processedBytes)
			case workerReceiveResult:
				if _, err := b.append(event.segmentID, event.text, event.isFinal); err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unexpected kind %d", event.kind)
			}
		}
		<-r.done
		if r.err != nil || !reflect.DeepEqual(progress, []uint64{3}) || b.lastSeq != 3 {
			t.Fatalf("completion=%v progress=%v lastSeq=%d", r.err, progress, b.lastSeq)
		}
		for i, want := range []retainedResult{
			{seq: 1, segmentID: "s", text: "a"},
			{seq: 2, segmentID: "s", text: "ab", isFinal: true},
			{seq: 3, segmentID: "t", text: "tail", isFinal: true},
		} {
			got, ok, err := b.peekAfter(uint64(i))
			if err != nil || !ok || got != want {
				t.Fatalf("result after %d = (%+v, %v, %v), want %+v", i, got, ok, err, want)
			}
		}
		if ok, err := b.acknowledge(2); err != nil || !ok || b.count != 1 {
			t.Fatalf("ack = (%v,%v), count=%d", ok, err, b.count)
		}
	})
}

func TestWorkerReceiverBufferFullFixtureCancelsRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		reads := 0
		stream := &uploadTestStream{recv: func() (*asrv1.StreamingRecognizeResponse, error) {
			reads++
			if reads <= 2 {
				return &asrv1.StreamingRecognizeResponse{SegmentId: "s", Text: fmt.Sprint(reads)}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		b, err := newResultBuffer(10, 1)
		if err != nil {
			t.Fatal(err)
		}
		r := newWorkerReceiver()
		go r.run(ctx, stream)
		for i := 0; i < 2; i++ {
			event := <-r.events
			_, err := b.append(event.segmentID, event.text, event.isFinal)
			if i == 0 && err != nil {
				t.Fatal(err)
			}
			if i == 1 {
				if !errors.Is(err, errResultBufferFull) {
					t.Fatalf("second result = %v, want full", err)
				}
				cancel(err)
			}
		}
		<-r.done
		if !errors.Is(r.err, errResultBufferFull) || b.count != 1 || b.lastSeq != 1 || reads > 3 {
			t.Fatalf("completion=%v buffer=(%d,%d) reads=%d", r.err, b.count, b.lastSeq, reads)
		}
	})
}
