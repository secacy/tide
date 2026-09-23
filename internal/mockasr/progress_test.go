package mockasr

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// progressPoint 保留已发送累计值及其时刻，用于检查确认是否早于模拟处理完成。
type progressPoint struct {
	bytes uint64
	at    time.Time
}

// recordedProgress 从完整记录提取进度，并检查进度没有混入文本字段。
// 调用方必须等待流退出，或传入通过锁复制的快照。
func recordedProgress(t *testing.T, stream *processingStream) []progressPoint {
	t.Helper()
	var points []progressPoint
	for i, response := range stream.responses {
		if p := response.GetProgress(); p != nil {
			if response.GetSegmentId() != "" || response.GetText() != "" || response.GetIsFinal() {
				t.Fatalf("progress mixed with text: %v", response)
			}
			points = append(points, progressPoint{bytes: p.GetProcessedAudioBytes(), at: stream.sentAt[i]})
		}
	}
	return points
}

// TestProgressAfterProcessing 验证不同大小的静音 PCM 按真实字节累计，空块不计数，
// 仅配置一条 partial 后进度仍持续；ResponseDelay 不额外施加到进度消息上。
func TestProgressAfterProcessing(t *testing.T) {
	for _, responseDelay := range []time.Duration{0, 40 * time.Millisecond} {
		t.Run(responseDelay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const processingDelay = 20 * time.Millisecond
				worker := New(Config{ProcessingDelay: processingDelay, ResponseDelay: responseDelay,
					PartialEvery: 100 * time.Millisecond, PartialTexts: []string{"only partial"}, FinalText: "final"})
				// 同一个 Worker 连续处理两条流，累计值必须每次从零开始。
				for run := 0; run < 2; run++ {
					stream := &processingStream{ctx: context.Background(), requests: [][]byte{
						nil, make([]byte, 3200), nil, make([]byte, 1600), make([]byte, 6400),
					}}
					start := time.Now()
					if err := worker.StreamingRecognize(stream); err != nil {
						t.Fatal(err)
					}
					points := recordedProgress(t, stream)
					if len(points) != 3 || len(stream.results) != 2 || len(stream.responses) != 5 {
						t.Fatalf("run %d: progress=%v results=%v", run, points, stream.results)
					}
					for i, want := range []uint64{3200, 4800, 11200} {
						elapsed := time.Duration(i+1) * processingDelay
						if i > 0 {
							elapsed += responseDelay // 第一条 partial 的等待会推迟后续处理。
						}
						if points[i].bytes != want || points[i].at.Sub(start) != elapsed {
							t.Fatalf("progress %d: bytes=%d elapsed=%v, want %d at %v", i, points[i].bytes, points[i].at.Sub(start), want, elapsed)
						}
					}
					if stream.results[0].GetText() != "only partial" || stream.results[0].GetIsFinal() ||
						stream.results[1].GetText() != "final" || !stream.results[1].GetIsFinal() {
						t.Fatalf("unexpected texts: %v", stream.results)
					}
					if stream.responses[0].GetProgress() == nil || stream.responses[1].GetProgress() != nil ||
						stream.responses[2].GetProgress() == nil || stream.responses[3].GetProgress() == nil ||
						!stream.responses[4].GetIsFinal() {
						t.Fatalf("unexpected progress/text order: %v", stream.responses)
					}
					if got := time.Since(start); got != 3*processingDelay+2*responseDelay {
						t.Fatalf("total duration=%v: progress added extra response delay", got)
					}
				}
			})
		})
	}
}

// observedProgressStream 对运行中记录提供快照；synctest.Wait 不能代替
// 观测读取与未来计时器恢复后的写入之间所需的同步。
type observedProgressStream struct {
	*processingStream
	mu sync.Mutex
}

func (s *observedProgressStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processingStream.Recv()
}

func (s *observedProgressStream) Send(response *asrv1.StreamingRecognizeResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processingStream.Send(response)
}

func (s *observedProgressStream) snapshot() *processingStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &processingStream{
		reads: slices.Clone(s.reads), responses: slices.Clone(s.responses),
		sentAt: slices.Clone(s.sentAt), results: slices.Clone(s.results),
	}
}

// TestProgressPauseAndStall 验证暂停/停读期间进度不提前推进，恢复后继续累计，
// 取消时不能补发尚未处理的块。虚拟时钟避免依赖机器调度耗时。
func TestProgressPauseAndStall(t *testing.T) {
	for _, mode := range []string{"resume", "cancel_pause", "cancel_stall"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: [][]byte{make([]byte, 3200), make([]byte, 3200)}}
				observed := &observedProgressStream{processingStream: stream}
				cfg := Config{ProcessingDelay: 100 * time.Millisecond, PauseAfterChunks: 1, PauseDuration: time.Second}
				if mode == "cancel_stall" {
					cfg.StallAfterChunks = 1
				}
				done := make(chan error, 1)
				start := time.Now()
				go func() { done <- New(cfg).StreamingRecognize(observed) }()
				synctest.Wait()
				if len(observed.snapshot().responses) != 0 {
					t.Fatal("progress sent before first ProcessingDelay finished")
				}
				time.Sleep(100 * time.Millisecond)
				synctest.Wait()
				view := observed.snapshot()
				points := recordedProgress(t, view)
				if len(points) != 1 || points[0].bytes != 3200 || len(view.reads) != 1 {
					t.Fatalf("wrong progress at pause/stall boundary: %v reads=%d", points, len(view.reads))
				}
				if mode == "resume" {
					time.Sleep(time.Second)
					synctest.Wait()
					view = observed.snapshot()
					if len(recordedProgress(t, view)) != 1 || len(view.reads) != 2 {
						t.Fatal("second chunk acknowledged before its ProcessingDelay completed")
					}
					time.Sleep(100 * time.Millisecond)
				} else {
					// 跨过暂停时长：永久停读不能被暂停恢复逻辑解除。
					if mode == "cancel_stall" {
						time.Sleep(2 * time.Second)
					}
					cancel()
				}
				synctest.Wait()
				select {
				case err := <-done:
					if mode == "resume" {
						if err != nil {
							t.Fatal(err)
						}
					} else if status.Code(err) != codes.Canceled {
						t.Fatalf("cancel returned %v", err)
					}
				default:
					t.Fatal("Worker did not exit")
				}
				points = recordedProgress(t, stream)
				if mode == "resume" {
					if len(points) != 2 || points[1].bytes != 6400 || points[1].at.Sub(start) != 1200*time.Millisecond ||
						len(stream.results) != 1 || !stream.results[0].GetIsFinal() {
						t.Fatalf("wrong resumed progress/results: %v %v", points, stream.results)
					}
				} else if len(points) != 1 || len(stream.reads) != 1 || len(stream.results) != 0 {
					t.Fatalf("continued after pause/stall cancellation: progress=%v reads=%d results=%v", points, len(stream.reads), stream.results)
				}
			})
		})
	}
}

// TestProgressInvalidAndEmptyAudio 检查无效输入不能获得处理确认。
func TestProgressInvalidAndEmptyAudio(t *testing.T) {
	for _, tc := range []struct {
		name     string
		requests [][]byte
		want     int
	}{
		{"empty_only", [][]byte{nil, {}}, 0},
		{"invalid_first", [][]byte{{1}}, 0},
		{"invalid_after_valid", [][]byte{make([]byte, 3200), nil, {1}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &processingStream{ctx: context.Background(), requests: tc.requests}
			err := New(Config{}).StreamingRecognize(stream)
			if status.Code(err) != codes.InvalidArgument || len(recordedProgress(t, stream)) != tc.want || len(stream.results) != 0 {
				t.Fatalf("error=%v responses=%v, want InvalidArgument and %d progress only", err, stream.responses, tc.want)
			}
		})
	}
}

// failingProgressStream 仅在进度发送处注入错误，验证完整 Worker 方法保留错误链与状态。
type failingProgressStream struct {
	*processingStream
	failure  error
	attempts int
}

func (s *failingProgressStream) Send(response *asrv1.StreamingRecognizeResponse) error {
	if response.GetProgress() != nil {
		s.attempts++
		return s.failure
	}
	return s.processingStream.Send(response)
}

func TestProgressSendFailurePreservesCause(t *testing.T) {
	for _, cause := range []error{
		errors.New("injected progress transport error"),
		status.Error(codes.Canceled, "progress send canceled"),
		status.Error(codes.DeadlineExceeded, "progress send deadline"),
		status.Error(codes.Unavailable, "progress transport unavailable"),
	} {
		t.Run(status.Code(cause).String(), func(t *testing.T) {
			stream := &failingProgressStream{
				processingStream: &processingStream{ctx: context.Background(), requests: [][]byte{make([]byte, 3200), make([]byte, 3200)}},
				failure:          cause,
			}
			err := New(Config{PartialEvery: 100 * time.Millisecond}).StreamingRecognize(stream)
			if !errors.Is(err, cause) {
				t.Errorf("lost original send error: got %v, cause %v", err, cause)
			}
			if status.Code(err) != status.Code(cause) {
				t.Errorf("changed gRPC status: got %v, want %v", status.Code(err), status.Code(cause))
			}
			if stream.attempts != 1 || len(stream.reads) != 1 || len(stream.responses) != 0 {
				t.Fatalf("continued after progress send failed: attempts=%d reads=%d responses=%v", stream.attempts, len(stream.reads), stream.responses)
			}
		})
	}
}
