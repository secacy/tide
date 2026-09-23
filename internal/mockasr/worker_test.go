package mockasr

import (
	"context"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// processingStream 只模拟 Worker 使用的流接口，使处理时间测试不依赖网络和真实时钟。
// 未使用的 ServerStream 方法通过嵌入满足接口；若实现开始调用它们，测试应显式补充行为。
type processingStream struct {
	grpc.ServerStream
	ctx          context.Context
	requests     [][]byte
	reads        []time.Time                         // 包含最后一次返回 EOF 的 Recv 调用。
	responses    []*asrv1.StreamingRecognizeResponse // 所有响应，保留进度与文本的实际顺序。
	sentAt       []time.Time
	results      []*asrv1.StreamingRecognizeResponse // 仅文本结果，供原有文本行为断言使用。
	resultSentAt []time.Time
}

func (s *processingStream) Context() context.Context { return s.ctx }

// Recv 顺序交付预置音频，随后半关闭输入。
func (s *processingStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	index := len(s.reads)
	s.reads = append(s.reads, time.Now())
	if index == len(s.requests) {
		return nil, io.EOF
	}
	return &asrv1.StreamingRecognizeRequest{Data: s.requests[index]}, nil
}

// Send 记录全部响应，同时保留文本视图；进度测试检查完整记录，旧文本测试不将进度当作 partial。
func (s *processingStream) Send(response *asrv1.StreamingRecognizeResponse) error {
	at := time.Now()
	s.responses = append(s.responses, response)
	s.sentAt = append(s.sentAt, at)
	if response.GetProgress() == nil {
		s.results = append(s.results, response)
		s.resultSentAt = append(s.resultSentAt, at)
	}
	return nil
}

// TestProcessingDelayAppliesAfterPartialsExhausted 验证三块音频串行处理，
// 即使只配置一条 partial，后续两块仍需等待；final 不能早于全部处理完成。
func TestProcessingDelayAppliesAfterPartialsExhausted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 200 * time.Millisecond
		start := time.Now()
		stream := &processingStream{
			ctx: context.Background(),
			requests: [][]byte{make([]byte, audio.ChunkBytesDefault),
				make([]byte, audio.ChunkBytesDefault), make([]byte, audio.ChunkBytesDefault)},
		}
		worker := New(Config{ProcessingDelay: delay, PartialEvery: 100 * time.Millisecond,
			PartialTexts: []string{"partial"}, FinalText: "final"})
		if err := worker.StreamingRecognize(stream); err != nil {
			t.Fatal(err)
		}
		if len(stream.reads) != 4 || len(stream.results) != 2 {
			t.Fatalf("reads=%d responses=%d, want 4 and 2", len(stream.reads), len(stream.results))
		}
		for i, at := range stream.reads {
			if got := at.Sub(start); got != time.Duration(i)*delay {
				t.Fatalf("Recv %d at %v, want %v", i, got, time.Duration(i)*delay)
			}
		}
		if stream.resultSentAt[0].Sub(start) != delay || stream.resultSentAt[1].Sub(start) != 3*delay {
			t.Fatalf("unexpected partial/final timing: %v", stream.resultSentAt)
		}
		if stream.results[0].Text != "partial" || stream.results[0].IsFinal ||
			stream.results[1].Text != "final" || !stream.results[1].IsFinal {
			t.Fatalf("unexpected results: %v", stream.results)
		}
	})
}

// TestProcessingDelayCancellation 验证取消或期限到达时，正在处理的块不会产出结果，
// Worker 不再读取下一块，并立即结束模拟等待。synctest 使用虚拟时间。
func TestProcessingDelayCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
				}
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: [][]byte{make([]byte, audio.ChunkBytesDefault)}}
				worker := New(Config{ProcessingDelay: time.Hour, PartialEvery: 100 * time.Millisecond})
				done := make(chan error, 1)
				go func() { done <- worker.StreamingRecognize(stream) }()
				synctest.Wait() // 确认 Worker 已读到音频并阻塞在处理等待。
				if len(stream.reads) != 1 {
					t.Fatalf("reads=%d, want 1", len(stream.reads))
				}
				start := time.Now()
				want := codes.Canceled
				if deadline {
					want = codes.DeadlineExceeded
					time.Sleep(10 * time.Millisecond)
				} else {
					cancel()
				}
				synctest.Wait()
				select {
				case err := <-done:
					if status.Code(err) != want {
						t.Fatalf("got %v, want %v", err, want)
					}
				default:
					t.Fatal("Worker still waiting after cancellation")
				}
				if len(stream.responses) != 0 || len(stream.reads) != 1 || time.Since(start) > 10*time.Millisecond {
					t.Fatalf("continued work after cancellation: reads=%d responses=%d elapsed=%v", len(stream.reads), len(stream.responses), time.Since(start))
				}
			})
		})
	}
}

// TestProcessingDelayInputRules 验证空块和非法 PCM 不消耗处理等待，
// 非正配置不增加等待，避免将兼容行为与机器调度速度绑定。
func TestProcessingDelayInputRules(t *testing.T) {
	for _, tc := range []struct {
		name        string
		delay       time.Duration
		chunks      [][]byte
		wantCode    codes.Code
		wantElapsed time.Duration
		wantResults int
	}{
		{"zero", 0, [][]byte{make([]byte, audio.ChunkBytesDefault)}, codes.OK, 0, 2},
		{"negative", -time.Second, [][]byte{make([]byte, audio.ChunkBytesDefault)}, codes.OK, 0, 2},
		{"empty_then_valid", time.Second, [][]byte{nil, make([]byte, audio.ChunkBytesDefault)}, codes.OK, time.Second, 2},
		{"empty_only", time.Second, [][]byte{nil}, codes.InvalidArgument, 0, 0},
		{"invalid_pcm", time.Second, [][]byte{{1}}, codes.InvalidArgument, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stream := &processingStream{ctx: context.Background(), requests: tc.chunks}
				worker := New(Config{ProcessingDelay: tc.delay, PartialEvery: 100 * time.Millisecond})
				start := time.Now()
				err := worker.StreamingRecognize(stream)
				if status.Code(err) != tc.wantCode || time.Since(start) != tc.wantElapsed || len(stream.results) != tc.wantResults {
					t.Fatalf("code=%v elapsed=%v results=%d; want %v %v %d", status.Code(err), time.Since(start), len(stream.results), tc.wantCode, tc.wantElapsed, tc.wantResults)
				}
			})
		})
	}
}
