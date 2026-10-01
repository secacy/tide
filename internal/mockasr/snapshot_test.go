package mockasr

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// assertWorkerProcessingSnapshot 从公开方法检查状态与有效性，不直接读取内部池。
func assertWorkerProcessingSnapshot(t *testing.T, w *Worker, want ProcessingSnapshot, wantEnabled bool) {
	t.Helper()
	got, enabled := w.ProcessingSnapshot()
	if got != want || enabled != wantEnabled {
		t.Fatalf("snapshot=%+v enabled=%v; want=%+v enabled=%v", got, enabled, want, wantEnabled)
	}
}

// TestWorkerSnapshotDisabledDuringProcessing 关闭限制时仍可处理音频，false 表示没有名额池观测。
func TestWorkerSnapshotDisabledDuringProcessing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := mustWorker(t, Config{ProcessingConcurrency: 0, ProcessingDelay: time.Second})
		assertWorkerProcessingSnapshot(t, w, ProcessingSnapshot{}, false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 2)
		for range 2 {
			go func() { done <- w.processChunk(ctx) }()
		}
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("processing returned before delay or cancellation: %v", err)
		default:
		}
		assertWorkerProcessingSnapshot(t, w, ProcessingSnapshot{}, false)
		cancel()
		for range 2 {
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("processing did not observe cancellation: %v", err)
			}
		}
		assertWorkerProcessingSnapshot(t, w, ProcessingSnapshot{}, false)
	})
}

// TestWorkerSnapshotProcessingLifecycle 在实际逐块处理、争用、完成或取消时核对公开快照。
// 虚拟时间只用于确定状态转换，不把短处理时长作为性能测量。
func TestWorkerSnapshotProcessingLifecycle(t *testing.T) {
	for _, mode := range []string{"completed", "cancel_waiter", "cancel_holder"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := mustWorker(t, Config{ProcessingConcurrency: 1, ProcessingDelay: 10 * time.Millisecond})
				independent := mustWorker(t, Config{ProcessingConcurrency: 2})
				idle := ProcessingSnapshot{Limit: 1}
				busy := ProcessingSnapshot{Limit: 1, InUse: 1}
				queued := ProcessingSnapshot{Limit: 1, InUse: 1, Waiting: 1}
				assertWorkerProcessingSnapshot(t, w, idle, true)
				holderCtx, cancelHolder := context.WithCancel(context.Background())
				defer cancelHolder()
				waiterCtx, cancelWaiter := context.WithCancel(context.Background())
				defer cancelWaiter()
				holderDone, waiterDone := make(chan error, 1), make(chan error, 1)
				go func() { holderDone <- w.processChunk(holderCtx) }()
				synctest.Wait()
				assertWorkerProcessingSnapshot(t, w, busy, true)
				go func() { waiterDone <- w.processChunk(waiterCtx) }()
				synctest.Wait()
				assertWorkerProcessingSnapshot(t, w, queued, true)
				assertWorkerProcessingSnapshot(t, independent, ProcessingSnapshot{Limit: 2}, true)
				historical, _ := w.ProcessingSnapshot()
				modified := historical
				modified.Limit, modified.InUse, modified.Waiting = 99, 99, 99
				assertWorkerProcessingSnapshot(t, w, queued, true)
				if modified == historical {
					t.Fatal("test failed to modify copied snapshot")
				}

				firstDone, lastDone := holderDone, waiterDone
				var wantFirst error
				switch mode {
				case "completed":
					time.Sleep(10 * time.Millisecond)
				case "cancel_waiter":
					cancelWaiter()
					firstDone, lastDone = waiterDone, holderDone
					wantFirst = context.Canceled
				case "cancel_holder":
					cancelHolder()
					wantFirst = context.Canceled
				}
				synctest.Wait()
				if err := <-firstDone; !errors.Is(err, wantFirst) {
					t.Fatalf("first processing result=%v want=%v", err, wantFirst)
				}
				assertWorkerProcessingSnapshot(t, w, busy, true)
				if historical != queued {
					t.Fatalf("historical value changed: %+v", historical)
				}
				time.Sleep(10 * time.Millisecond)
				synctest.Wait()
				if err := <-lastDone; err != nil {
					t.Fatalf("surviving processing failed: %v", err)
				}
				assertWorkerProcessingSnapshot(t, w, idle, true)
				assertWorkerProcessingSnapshot(t, independent, ProcessingSnapshot{Limit: 2}, true)
			})
		})
	}
}
