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

// TestStallAfterChunksBoundary 验证处理完 N 个有效块后不再调用 Recv，
// 包括下一次本应读到 EOF 的情况；空块不占用计数，取消或期限能结束停读。
func TestStallAfterChunksBoundary(t *testing.T) {
	chunk := make([]byte, audio.ChunkBytesDefault)
	for _, tc := range []struct {
		name     string
		requests [][]byte
		reads    int
		deadline bool
	}{
		{"unread_audio", [][]byte{chunk, chunk, chunk}, 2, false},
		{"unread_eof", [][]byte{chunk, chunk}, 2, false},
		{"empty_not_counted", [][]byte{nil, chunk, nil, chunk, chunk}, 4, false},
		{"deadline", [][]byte{chunk, chunk, chunk}, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				if tc.deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
				}
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: tc.requests}
				worker := New(Config{StallAfterChunks: 2, PartialEvery: 100 * time.Millisecond,
					PartialTexts: []string{"p1", "p2", "p3"}})
				done := make(chan error, 1)
				go func() { done <- worker.StreamingRecognize(stream) }()
				synctest.Wait() // 等其他 goroutine 停住；不会推进期限时钟。
				if len(stream.reads) != tc.reads {
					t.Fatalf("Recv calls=%d, want %d: Worker must stop before reading beyond two valid chunks", len(stream.reads), tc.reads)
				}
				if len(stream.responses) != 2 || stream.responses[0].Text != "p1" ||
					stream.responses[1].Text != "p2" || stream.responses[0].IsFinal || stream.responses[1].IsFinal {
					t.Fatalf("expected two partials and no final before stall, got %v", stream.responses)
				}
				select {
				case err := <-done:
					t.Fatalf("Worker exited before cancellation: %v", err)
				default:
				}
				want := codes.Canceled
				if tc.deadline {
					want = codes.DeadlineExceeded
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				synctest.Wait()
				select {
				case err := <-done:
					if status.Code(err) != want {
						t.Fatalf("exit=%v, want %v", err, want)
					}
				default:
					t.Fatal("Worker did not exit after cancellation")
				}
				if len(stream.reads) != tc.reads || len(stream.responses) != 2 {
					t.Fatal("Worker resumed reading or produced results after cancellation")
				}
			})
		})
	}
}

// TestStallAfterChunksDisabledOrNotReached 验证关闭注入或输入不足 N 块时仍正常完成，
// 非法 PCM 在计数前返回错误，不能被停读条件掩盖。
func TestStallAfterChunksDisabledOrNotReached(t *testing.T) {
	chunk := make([]byte, audio.ChunkBytesDefault)
	for _, tc := range []struct {
		name     string
		stall    int
		requests [][]byte
		wantCode codes.Code
	}{
		{"zero", 0, [][]byte{chunk}, codes.OK},
		{"negative", -1, [][]byte{chunk}, codes.OK},
		{"below_threshold", 2, [][]byte{chunk}, codes.OK},
		{"invalid_pcm", 2, [][]byte{{1}}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// 虚拟期限让意外停读表现为断言失败，不让测试永久等待。
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				stream := &processingStream{ctx: ctx, requests: tc.requests}
				err := New(Config{StallAfterChunks: tc.stall}).StreamingRecognize(stream)
				if status.Code(err) != tc.wantCode {
					t.Fatalf("exit=%v, want %v", err, tc.wantCode)
				}
				if tc.wantCode == codes.OK {
					if len(stream.responses) != 1 || !stream.responses[0].IsFinal {
						t.Fatalf("expected final result, got %v", stream.responses)
					}
				} else if len(stream.responses) != 0 {
					t.Fatalf("invalid audio produced results: %v", stream.responses)
				}
			})
		})
	}
}

// TestStallAfterChunksPerStream 验证共享 Worker 的两个流各自计数、独立取消。
func TestStallAfterChunksPerStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		worker := New(Config{StallAfterChunks: 2, PartialEvery: 100 * time.Millisecond,
			PartialTexts: []string{"p1", "p2", "p3"}})
		var streams [2]*processingStream
		var cancels [2]context.CancelFunc
		var done [2]chan error
		for i := range streams {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cancels[i] = cancel
			chunk := make([]byte, audio.ChunkBytesDefault)
			streams[i] = &processingStream{ctx: ctx, requests: [][]byte{chunk, chunk, chunk}}
			done[i] = make(chan error, 1)
			go func() { done[i] <- worker.StreamingRecognize(streams[i]) }()
		}
		synctest.Wait()
		for i, stream := range streams {
			if len(stream.reads) != 2 || len(stream.responses) != 2 {
				t.Fatalf("stream %d: reads=%d results=%d, want 2 and 2", i, len(stream.reads), len(stream.responses))
			}
		}
		for i := range streams {
			cancels[i]()
			synctest.Wait()
			select {
			case err := <-done[i]:
				if status.Code(err) != codes.Canceled {
					t.Fatalf("stream %d exit=%v", i, err)
				}
			default:
				t.Fatalf("stream %d did not exit", i)
			}
			if i == 0 {
				select {
				case err := <-done[1]:
					t.Fatalf("canceling first stream ended second: %v", err)
				default:
				}
			}
		}
	})
}
