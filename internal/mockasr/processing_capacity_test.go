package mockasr

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWorkerProcessingConfig 检查公开配置语义和原有默认值，不绑定构造错误的类型。
func TestWorkerProcessingConfig(t *testing.T) {
	for _, limit := range []int{-1, 0, 1, 3} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			w, err := New(Config{ProcessingConcurrency: limit})
			if limit < 0 {
				if err == nil || w != nil {
					t.Fatalf("negative limit: worker=%v err=%v", w, err)
				}
				return
			}
			if err != nil || w == nil {
				t.Fatalf("construct: worker=%v err=%v", w, err)
			}
			if (w.slots == nil) != (limit == 0) {
				t.Fatalf("unexpected pool for limit %d", limit)
			}
			if w.cfg.PartialEvery != 500*time.Millisecond || len(w.cfg.PartialTexts) == 0 || w.cfg.FinalText == "" {
				t.Fatal("lost existing defaults")
			}
		})
	}
}

// TestWorkerSharedProcessingTiming 用虚拟时间验证同实例争用、关闭限制以及实例间隔离。
// 每条流连续发送两块；容量 1 时也必须逐块归还，不能等到整条流退出。
func TestWorkerSharedProcessingTiming(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit    int
		separate bool
		want     time.Duration
	}{
		{"disabled", 0, false, 20 * time.Millisecond},
		{"shared_one", 1, false, 40 * time.Millisecond},
		{"shared_two", 2, false, 20 * time.Millisecond},
		{"independent_workers", 1, true, 20 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := Config{ProcessingConcurrency: tc.limit, ProcessingDelay: 10 * time.Millisecond}
				workers := []*Worker{mustWorker(t, cfg), nil}
				workers[1] = workers[0]
				if tc.separate {
					workers[1] = mustWorker(t, cfg)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				streams := []*processingStream{
					{ctx: ctx, requests: [][]byte{make([]byte, 3200), make([]byte, 1600)}},
					{ctx: ctx, requests: [][]byte{make([]byte, 3200), make([]byte, 1600)}},
				}
				done := make(chan error, 2)
				start := time.Now()
				for i := range streams {
					go func() { done <- workers[i].StreamingRecognize(streams[i]) }()
				}
				for range streams {
					if err := <-done; err != nil {
						t.Errorf("stream: %v", err)
					}
				}
				if got := time.Since(start); got != tc.want {
					t.Errorf("elapsed=%v want=%v", got, tc.want)
				}
				for _, stream := range streams {
					points := recordedProgress(t, stream)
					if len(points) != 2 || points[0].bytes != 3200 || points[1].bytes != 4800 {
						t.Errorf("wrong per-stream progress: %v", points)
					}
					if len(stream.results) != 1 || !stream.results[0].GetIsFinal() {
						t.Errorf("missing final: %v", stream.results)
					}
				}
			})
		})
	}
}

// newCapacityStream 创建只有一块有效 PCM 的流；快照用于并发运行中的安全观测。
func newCapacityStream(ctx context.Context) *observedProgressStream {
	return &observedProgressStream{processingStream: &processingStream{ctx: ctx, requests: [][]byte{make([]byte, 3200)}}}
}

// TestWorkerProcessingCancellation 分别取消名额持有者和等待者，检查未完成的块无确认，
// 未取消的流仍能完成；同时验证 RPC 方法保留 Canceled/DeadlineExceeded 状态语义。
func TestWorkerProcessingCancellation(t *testing.T) {
	for _, holderCanceled := range []bool{false, true} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("holder=%v/deadline=%v", holderCanceled, deadline), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					if deadline {
						cancel()
						ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
					}
					defer cancel()
					holderCtx, waiterCtx := context.Background(), ctx
					if holderCanceled {
						holderCtx, waiterCtx = ctx, context.Background()
					}
					holder, waiter := newCapacityStream(holderCtx), newCapacityStream(waiterCtx)
					w := mustWorker(t, Config{ProcessingConcurrency: 1, ProcessingDelay: 100 * time.Millisecond})
					holderDone, waiterDone := make(chan error, 1), make(chan error, 1)
					go func() { holderDone <- w.StreamingRecognize(holder) }()
					synctest.Wait()
					go func() { waiterDone <- w.StreamingRecognize(waiter) }()
					synctest.Wait()
					if len(holder.snapshot().reads) != 1 || len(waiter.snapshot().reads) != 1 {
						t.Fatal("both streams must have received their first chunk")
					}
					if deadline {
						time.Sleep(10 * time.Millisecond)
					} else {
						cancel()
					}
					synctest.Wait()
					failed, failedDone, survivor, survivorDone := waiter, waiterDone, holder, holderDone
					if holderCanceled {
						failed, failedDone, survivor, survivorDone = holder, holderDone, waiter, waiterDone
					}
					want := codes.Canceled
					if deadline {
						want = codes.DeadlineExceeded
					}
					select {
					case err := <-failedDone:
						if status.Code(err) != want {
							t.Errorf("RPC code=%v (%v), want %v", status.Code(err), err, want)
						}
					default:
						t.Fatal("canceled stream did not exit")
					}
					if len(failed.snapshot().responses) != 0 {
						t.Error("unprocessed audio was acknowledged")
					}
					if err := <-survivorDone; err != nil {
						t.Errorf("surviving stream: %v", err)
					}
					points := recordedProgress(t, survivor.snapshot())
					if len(points) != 1 || points[0].bytes != 3200 {
						t.Errorf("survivor progress=%v", points)
					}
					// 两条流退出后再处理一条，确保池没有永久占位。
					if err := w.StreamingRecognize(newCapacityStream(context.Background())); err != nil {
						t.Errorf("reuse: %v", err)
					}
				})
			})
		}
	}
}

// TestProcessChunkAlreadyCanceled 覆盖关闭/启用限制与零耗时组合，辅助方法返回原始 context 错误。
func TestProcessChunkAlreadyCanceled(t *testing.T) {
	for _, limit := range []int{0, 1} {
		for _, delay := range []time.Duration{0, time.Second} {
			t.Run(fmt.Sprintf("limit=%d/delay=%v", limit, delay), func(t *testing.T) {
				w := mustWorker(t, Config{ProcessingConcurrency: limit, ProcessingDelay: delay})
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := w.processChunk(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v", err)
				}
				if w.slots != nil {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					release, err := w.slots.acquire(ctx)
					if err != nil {
						t.Fatal(err)
					}
					release()
				}
			})
		}
	}
}

// capacityBlockedSend 在指定发送点等待取消，模拟结果消费者不读取；不修改生产发送逻辑。
type capacityBlockedSend struct {
	*observedProgressStream
	kind    string
	entered chan struct{}
}

func (s *capacityBlockedSend) Send(r *asrv1.StreamingRecognizeResponse) error {
	match := s.kind == "progress" && r.GetProgress() != nil || s.kind == "partial" && r.GetProgress() == nil && !r.GetIsFinal() || s.kind == "final" && r.GetIsFinal()
	if match {
		close(s.entered)
		<-s.Context().Done()
		return s.Context().Err()
	}
	return s.observedProgressStream.Send(r)
}

// TestWorkerNonProcessingWaitsReleaseCapacity 检查发送、响应延迟及故障等待期间，
// 同一 Worker 的另一条流仍能处理音频并确认进度。该测试不声称真实网络吞吐能力。
func TestWorkerNonProcessingWaitsReleaseCapacity(t *testing.T) {
	for _, mode := range []string{"progress", "partial", "final", "response_delay", "pause", "stall"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cfg := Config{ProcessingConcurrency: 1, ProcessingDelay: 10 * time.Millisecond, PartialEvery: 100 * time.Millisecond}
				switch mode {
				case "response_delay":
					cfg.ResponseDelay = time.Hour
				case "pause":
					cfg.PauseAfterChunks = 1
					cfg.PauseDuration = time.Hour
				case "stall":
					cfg.StallAfterChunks = 1
				}
				w := mustWorker(t, cfg)
				blocked := &capacityBlockedSend{observedProgressStream: newCapacityStream(ctx), kind: mode, entered: make(chan struct{})}
				firstDone := make(chan error, 1)
				go func() { firstDone <- w.StreamingRecognize(blocked) }()
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				if mode == "progress" || mode == "partial" || mode == "final" {
					select {
					case <-blocked.entered:
					default:
						t.Fatal("did not reach target send")
					}
				} else {
					if len(recordedProgress(t, blocked.snapshot())) != 1 {
						t.Fatal("first chunk did not finish before wait")
					}
				}
				other := newCapacityStream(ctx)
				otherDone := make(chan error, 1)
				go func() { otherDone <- w.StreamingRecognize(other) }()
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				points := recordedProgress(t, other.snapshot())
				if len(points) != 1 || points[0].bytes != 3200 {
					t.Errorf("other stream blocked by %s: progress=%v", mode, points)
				}
				cancel()
				<-firstDone
				<-otherDone
			})
		})
	}
}
