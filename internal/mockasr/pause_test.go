package mockasr

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestPauseOnceAndResume 验证前两块及其 partial 完成后暂停，恢复后继续原流，
// 空块不会触发重复暂停，EOF 也必须等本次暂停结束后才能产生 final。
func TestPauseOnceAndResume(t *testing.T) {
	chunk := make([]byte, audio.ChunkBytesDefault)
	for _, tc := range []struct {
		name     string
		requests [][]byte
		valid    int
	}{
		{"more_audio", [][]byte{chunk, chunk, chunk, chunk}, 4},
		{"empty_after_pause", [][]byte{chunk, chunk, nil, chunk, chunk}, 4},
		{"end_at_boundary", [][]byte{chunk, chunk}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: tc.requests}
				worker := New(Config{PauseAfterChunks: 2, PauseDuration: time.Second,
					ProcessingDelay: 50 * time.Millisecond, ResponseDelay: 20 * time.Millisecond,
					PartialEvery: 100 * time.Millisecond, PartialTexts: []string{"p1", "p2", "p3", "p4"}, FinalText: "final"})
				start := time.Now()
				if err := worker.StreamingRecognize(stream); err != nil {
					t.Fatal(err)
				}
				if len(stream.reads) != len(tc.requests)+1 || len(stream.responses) != tc.valid+1 {
					t.Fatalf("reads=%d results=%d", len(stream.reads), len(stream.responses))
				}
				for i := 0; i < tc.valid; i++ {
					want := time.Duration(i+1) * 70 * time.Millisecond
					if i >= 2 {
						want += time.Second
					}
					if stream.sentAt[i].Sub(start) != want || stream.responses[i].IsFinal {
						t.Fatalf("partial %d at %v, want %v", i, stream.sentAt[i].Sub(start), want)
					}
				}
				// 第三次 Recv 在暂停结束后发生；提前消费下一条输入会破坏这个边界。
				if got := stream.reads[2].Sub(start); got != 1140*time.Millisecond {
					t.Fatalf("next Recv at %v, want 1.14s", got)
				}
				wantTotal := time.Duration(tc.valid)*70*time.Millisecond + time.Second + 20*time.Millisecond
				last := stream.responses[len(stream.responses)-1]
				if time.Since(start) != wantTotal || !last.IsFinal || last.Text != "final" {
					t.Fatalf("elapsed=%v final=%v, want %v and final", time.Since(start), last, wantTotal)
				}
			})
		})
	}
}

// TestPauseCancellation 验证在暂停期间取消或期限到达会退出，且错误遵循 gRPC 状态约定。
func TestPauseCancellation(t *testing.T) {
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
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				}
				defer cancel()
				chunk := make([]byte, audio.ChunkBytesDefault)
				stream := &processingStream{ctx: ctx, requests: [][]byte{chunk, chunk}}
				worker := New(Config{PauseAfterChunks: 1, PauseDuration: time.Hour, PartialEvery: 100 * time.Millisecond, PartialTexts: []string{"p1", "p2"}})
				done := make(chan error, 1)
				go func() { done <- worker.StreamingRecognize(stream) }()
				synctest.Wait()
				if len(stream.reads) != 1 || len(stream.responses) != 1 || stream.responses[0].IsFinal {
					t.Fatalf("not paused after first partial: reads=%d results=%v", len(stream.reads), stream.responses)
				}
				want := codes.Canceled
				if deadline {
					want = codes.DeadlineExceeded
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				synctest.Wait()
				select {
				case err := <-done:
					if status.Code(err) != want {
						t.Fatalf("error=%v code=%v, want %v", err, status.Code(err), want)
					}
				default:
					t.Fatal("Worker did not exit during pause")
				}
				if len(stream.reads) != 1 || len(stream.responses) != 1 {
					t.Fatal("Worker continued after cancellation")
				}
			})
		})
	}
}

// TestPauseDisabledOrNotReached 验证默认、非正配置和未达到边界的会话不会被延迟。
func TestPauseDisabledOrNotReached(t *testing.T) {
	for _, tc := range []struct {
		name     string
		after    int
		duration time.Duration
	}{
		{"default", 0, 0}, {"zero_count", 0, time.Second}, {"negative_count", -1, time.Second},
		{"zero_duration", 1, 0}, {"negative_duration", 1, -time.Second}, {"below_threshold", 2, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: [][]byte{make([]byte, audio.ChunkBytesDefault)}}
				start := time.Now()
				err := New(Config{PauseAfterChunks: tc.after, PauseDuration: tc.duration}).StreamingRecognize(stream)
				if err != nil || time.Since(start) != 0 || len(stream.responses) != 1 || !stream.responses[0].IsFinal {
					t.Fatalf("error=%v elapsed=%v results=%v", err, time.Since(start), stream.responses)
				}
			})
		})
	}
}

// TestPausePerStream 使用共享 Worker 验证各流独立暂停、取消与恢复。
func TestPausePerStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		worker := New(Config{PauseAfterChunks: 1, PauseDuration: time.Second, PartialEvery: 100 * time.Millisecond, PartialTexts: []string{"p1", "p2"}})
		var streams [2]*processingStream
		var cancels [2]context.CancelFunc
		var done [2]chan error
		for i := range streams {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancels[i] = cancel
			chunk := make([]byte, audio.ChunkBytesDefault)
			streams[i] = &processingStream{ctx: ctx, requests: [][]byte{chunk, chunk}}
			done[i] = make(chan error, 1)
			go func() { done[i] <- worker.StreamingRecognize(streams[i]) }()
		}
		synctest.Wait()
		cancels[0]()
		synctest.Wait()
		select {
		case err := <-done[0]:
			if status.Code(err) != codes.Canceled {
				t.Errorf("canceled stream returned %v", err)
			}
		default:
			t.Fatal("canceled stream did not exit")
		}
		select {
		case err := <-done[1]:
			t.Fatalf("second stream ended early: %v", err)
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-done[1]:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("second stream did not resume")
		}
		if len(streams[0].reads) != 1 || len(streams[1].reads) != 3 || len(streams[1].responses) != 3 || !streams[1].responses[2].IsFinal {
			t.Fatal("independent cancellation/resume produced wrong input or results")
		}
		// 完成通知之后才读取会恢复写入的测试流记录，避免测试观测自身的数据竞争。
		if elapsed := streams[1].sentAt[1].Sub(start); elapsed != time.Second {
			t.Fatalf("second stream resumed after %v, want its own one-second pause", elapsed)
		}
	})
}
