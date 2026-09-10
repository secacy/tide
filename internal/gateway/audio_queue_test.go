package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestAudioQueueRejectsInvalidLimits(t *testing.T) {
	for _, limits := range [][2]int{{0, 1}, {1, 0}, {-1, 1}, {1, -1}} {
		if q, err := newAudioQueue(limits[0], limits[1]); q != nil || err == nil {
			t.Fatalf("newAudioQueue(%v) = (%v, %v), want invalid limits", limits, q, err)
		}
	}
}

// 两种上限独立生效；拒绝不能破坏 FIFO，出队后容量必须能够复用。
func TestAudioQueueCapacityAndWraparound(t *testing.T) {
	for _, tc := range []struct {
		name             string
		maxBytes, chunks int
	}{
		{"bytes", 6, 3},
		{"chunks", 100, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := mustAudioQueue(t, tc.maxBytes, tc.chunks)
			mustQueuePush(t, q, []byte("aa"))
			mustQueuePush(t, q, []byte("bbbb"))
			for range 3 {
				if err := q.tryPush([]byte("x")); !errors.Is(err, errAudioQueueFull) {
					t.Fatalf("push while full = %v", err)
				}
			}
			mustQueuePop(t, q, []byte("aa"))
			mustQueuePush(t, q, []byte("cc"))
			mustQueuePop(t, q, []byte("bbbb"))
			mustQueuePop(t, q, []byte("cc"))
			// 多次跨越环形数组末尾，检测旧槽位和字节计量是否污染后续入队。
			for range 10 {
				mustQueuePush(t, q, []byte("123456"))
				mustQueuePop(t, q, []byte("123456"))
			}
			assertAudioQueueReleased(t, q)
		})
	}
}

func TestAudioQueueRejectsEmptyAndOversizedChunks(t *testing.T) {
	q := mustAudioQueue(t, 4, 1)
	for _, data := range [][]byte{nil, {}} {
		if err := q.tryPush(data); !errors.Is(err, errEmptyAudioChunk) {
			t.Fatalf("empty push = %v", err)
		}
	}
	if err := q.tryPush(make([]byte, 5)); !errors.Is(err, errAudioQueueFull) {
		t.Fatalf("oversized push = %v", err)
	}
	mustQueuePush(t, q, []byte("full"))
	mustQueuePop(t, q, []byte("full"))
	assertAudioQueueReleased(t, q)
}

func TestAudioQueueOwnsAcceptedAudio(t *testing.T) {
	q := mustAudioQueue(t, 4, 2)
	source := make([]byte, 64*1024)
	copy(source, "ab")
	mustQueuePush(t, q, source[:2])
	copy(source, "cd")
	mustQueuePush(t, q, source[:2])
	clear(source)
	mustQueuePop(t, q, []byte("ab"))
	mustQueuePop(t, q, []byte("cd"))
	assertAudioQueueReleased(t, q)
}

func TestAudioQueueCloseDrainsBeforeEOF(t *testing.T) {
	q := mustAudioQueue(t, 4, 2)
	mustQueuePush(t, q, []byte("aa"))
	mustQueuePush(t, q, []byte("bb"))
	q.closeInput()
	q.closeInput()
	if err := q.tryPush([]byte("cc")); !errors.Is(err, errAudioQueueClosed) {
		t.Fatalf("push after close = %v", err)
	}
	mustQueuePop(t, q, []byte("aa"))
	mustQueuePop(t, q, []byte("bb"))
	for range 2 {
		data, err := q.pop(context.Background())
		if data != nil || !errors.Is(err, io.EOF) {
			t.Fatalf("pop drained queue = (%q, %v)", data, err)
		}
	}
	assertAudioQueueReleased(t, q)
}

// 即使已经有积压或正常 EOF，预先取消的消费者也不继续排空队列。
func TestAudioQueueCanceledPopPreservesBacklog(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		q := mustAudioQueue(t, 2, 1)
		mustQueuePush(t, q, []byte("ab"))
		q.closeInput()
		ctx, cancel := context.WithCancel(context.Background())
		want := context.Canceled
		if deadline {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		}
		cancel()
		if data, err := q.pop(ctx); data != nil || !errors.Is(err, want) {
			t.Fatalf("canceled pop = (%q, %v), want %v", data, err, want)
		}
		mustQueuePop(t, q, []byte("ab"))
	}
}

// 使用等待入口事件同步，避免 sleep 或仅依靠超时猜测消费者是否进入等待。
func TestAudioQueueWakesWaitingConsumer(t *testing.T) {
	for _, trigger := range []string{"push", "close", "cancel"} {
		t.Run(trigger, func(t *testing.T) {
			q := mustAudioQueue(t, 2, 1)
			// 留下一次过时通知，验证 pop 会重新检查队列并继续等待。
			mustQueuePush(t, q, []byte("ab"))
			mustQueuePop(t, q, []byte("ab"))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			observed := &audioQueueWaitContext{Context: ctx, waiting: make(chan struct{})}
			type result struct {
				data []byte
				err  error
			}
			done := make(chan result, 1)
			go func() {
				data, err := q.pop(observed)
				done <- result{data, err}
			}()
			select {
			case <-observed.waiting:
			case <-ctx.Done():
				t.Fatal("consumer did not reach wait")
			}
			var wantData []byte
			var wantErr error
			switch trigger {
			case "push":
				wantData = []byte("cd")
				mustQueuePush(t, q, wantData)
			case "close":
				q.closeInput()
				wantErr = io.EOF
			case "cancel":
				cancel()
				wantErr = context.Canceled
			}
			select {
			case got := <-done:
				if !bytes.Equal(got.data, wantData) || !errors.Is(got.err, wantErr) {
					t.Fatalf("pop = (%q, %v), want (%q, %v)", got.data, got.err, wantData, wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("consumer was not released")
			}
		})
	}
}

// 在单生产者/单消费者并发运行时反复复用小队列，验证顺序、通知及关闭排空。
func TestAudioQueueConcurrentTransfer(t *testing.T) {
	const chunks = 1000
	q := mustAudioQueue(t, 32, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		defer q.closeInput()
		buf := make([]byte, 8)
		for i := range chunks {
			binary.LittleEndian.PutUint64(buf, uint64(i))
			for {
				if err := ctx.Err(); err != nil {
					done <- err
					return
				}
				err := q.tryPush(buf)
				if err == nil {
					break
				}
				if !errors.Is(err, errAudioQueueFull) {
					done <- err
					return
				}
				// 仅测试驱动重试，生产 Reader 满时会直接报告会话失败。
				runtime.Gosched()
			}
		}
		done <- nil
	}()
	for i := range chunks {
		data, err := q.pop(ctx)
		if err != nil || len(data) != 8 || binary.LittleEndian.Uint64(data) != uint64(i) {
			t.Fatalf("chunk %d = (%v, %v)", i, data, err)
		}
	}
	if data, err := q.pop(ctx); data != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("final pop = (%v, %v), want EOF", data, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertAudioQueueReleased(t, q)
}

// 并发入队竞争同一容量，验证检查与记账不可分离；关闭输入也参与互斥。
func TestAudioQueueConcurrentAdmissionAndClose(t *testing.T) {
	for _, name := range []string{"capacity", "concurrent_close"} {
		t.Run(name, func(t *testing.T) {
			q := mustAudioQueue(t, 32, 16)
			start := make(chan struct{})
			results := make(chan error, 64)
			var wg sync.WaitGroup
			for i := range 64 {
				wg.Go(func() {
					<-start
					results <- q.tryPush([]byte{byte(i), 0})
				})
			}
			if name == "concurrent_close" {
				wg.Go(func() {
					<-start
					q.closeInput()
				})
			}
			close(start)
			wg.Wait()
			q.closeInput()
			close(results)
			accepted := 0
			for err := range results {
				switch {
				case err == nil:
					accepted++
				case errors.Is(err, errAudioQueueFull), errors.Is(err, errAudioQueueClosed):
				default:
					t.Fatal(err)
				}
			}
			if accepted > 16 {
				t.Fatalf("accepted %d chunks beyond limit", accepted)
			}
			if name == "capacity" && accepted != 16 {
				t.Fatalf("accepted %d chunks, want full capacity of 16", accepted)
			}
			seen := make(map[byte]bool)
			for range accepted {
				data, err := q.pop(context.Background())
				if err != nil || len(data) != 2 || seen[data[0]] {
					t.Fatalf("duplicate or invalid accepted chunk: %v, %v", data, err)
				}
				seen[data[0]] = true
			}
			if _, err := q.pop(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatalf("after accepted chunks: %v", err)
			}
			assertAudioQueueReleased(t, q)
		})
	}
}

func mustAudioQueue(t *testing.T, maxBytes, maxChunks int) *audioQueue {
	t.Helper()
	q, err := newAudioQueue(maxBytes, maxChunks)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func mustQueuePush(t *testing.T, q *audioQueue, data []byte) {
	t.Helper()
	if err := q.tryPush(data); err != nil {
		t.Fatalf("push %q: %v", data, err)
	}
}

func mustQueuePop(t *testing.T, q *audioQueue, want []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got, err := q.pop(ctx); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("pop = (%q, %v), want %q", got, err, want)
	}
}

// 此断言验证队列确实释放已消费音频的引用，避免仅计数归零而持续保留内存。
func assertAudioQueueReleased(t *testing.T, q *audioQueue) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count != 0 || q.bytes != 0 {
		t.Fatalf("drained queue has %d chunks, %d bytes", q.count, q.bytes)
	}
	for i, slot := range q.slots {
		if slot != nil {
			t.Fatalf("slot %d still retains audio", i)
		}
	}
}

// audioQueueWaitContext 观察 pop 进入 select 时的 Done 调用，不改变取消语义。
type audioQueueWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *audioQueueWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
