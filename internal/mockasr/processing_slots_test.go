package mockasr

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// slotsResult 通过 channel 交接获取结果，避免测试与等待 goroutine 共享可变状态。
type slotsResult struct {
	release func()
	err     error
}

func mustProcessingSlots(t *testing.T, limit int) *processingSlots {
	t.Helper()
	s, err := newProcessingSlots(limit)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// takeProcessingSlot 使用兜底期限，防止名额泄漏把测试永久阻塞。
func takeProcessingSlot(t *testing.T, s *processingSlots) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := s.acquire(ctx)
	if err != nil || release == nil {
		t.Fatalf("acquire: release present=%v err=%v", release != nil, err)
	}
	return release
}

func assertSlotsWaiting(t *testing.T, done <-chan slotsResult) {
	t.Helper()
	select {
	case result := <-done:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("acquire returned while all slots held: %v", result.err)
	default:
	}
}

// assertAllSlotsReusable 验证测试结束时仍能同时取得全部名额，再全部归还。
func assertAllSlotsReusable(t *testing.T, s *processingSlots, limit int) {
	t.Helper()
	releases := make([]func(), 0, limit)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < limit; i++ {
		releases = append(releases, takeProcessingSlot(t, s))
	}
}

func TestProcessingSlotsInvalidCapacity(t *testing.T) {
	for _, limit := range []int{0, -1, -100} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			s, err := newProcessingSlots(limit)
			if s != nil || err == nil {
				t.Fatalf("invalid capacity accepted: slots=%v err=%v", s, err)
			}
		})
	}
}

// TestProcessingSlotsCapacityAndWakeup 使用虚拟调度确认满额等待，归还后才能继续。
func TestProcessingSlotsCapacityAndWakeup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustProcessingSlots(t, 2)
		first, second := takeProcessingSlot(t, s), takeProcessingSlot(t, s)
		defer first()
		defer second()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan slotsResult, 1)
		go func() { release, err := s.acquire(ctx); done <- slotsResult{release, err} }()
		synctest.Wait()
		assertSlotsWaiting(t, done)
		first()
		synctest.Wait()
		select {
		case result := <-done:
			if result.err != nil || result.release == nil {
				t.Fatalf("released slot did not wake waiter: %v", result.err)
			}
			result.release()
		default:
			t.Fatal("waiter did not resume after release")
		}
		second()
		assertAllSlotsReusable(t, s, 2)
	})
}

func TestProcessingSlotsAlreadyCanceled(t *testing.T) {
	s := mustProcessingSlots(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release, err := s.acquire(ctx)
	if release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled acquire: release=%v err=%v", release != nil, err)
	}
	assertAllSlotsReusable(t, s, 1)
}

// TestProcessingSlotsWaitingCancellation 验证等待时的取消和期限到期不影响其他持有者。
func TestProcessingSlotsWaitingCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline_%v", deadline), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := mustProcessingSlots(t, 1)
				held := takeProcessingSlot(t, s)
				defer held()
				ctx, cancel := context.WithCancel(context.Background())
				want := context.Canceled
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					want = context.DeadlineExceeded
				}
				defer cancel()
				done := make(chan slotsResult, 1)
				go func() { release, err := s.acquire(ctx); done <- slotsResult{release, err} }()
				synctest.Wait()
				assertSlotsWaiting(t, done)
				if deadline {
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				synctest.Wait()
				result := <-done
				if result.release != nil || !errors.Is(result.err, want) {
					t.Fatalf("canceled waiter: release=%v err=%v", result.release != nil, result.err)
				}
				// 失败的等待者不能擅自归还仍由 held 持有的名额。
				nextCtx, stop := context.WithCancel(context.Background())
				defer stop()
				go func() { release, err := s.acquire(nextCtx); done <- slotsResult{release, err} }()
				synctest.Wait()
				assertSlotsWaiting(t, done)
				held()
				synctest.Wait()
				next := <-done
				if next.err != nil || next.release == nil {
					t.Fatalf("remaining holder could not release: %v", next.err)
				}
				next.release()
				assertAllSlotsReusable(t, s, 1)
			})
		})
	}
}

// TestProcessingSlotsHolderCancellation 获取成功后的 context 取消不能提前腾出名额。
func TestProcessingSlotsHolderCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustProcessingSlots(t, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		held, err := s.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer held()
		cancel()
		nextCtx, stop := context.WithCancel(context.Background())
		defer stop()
		done := make(chan slotsResult, 1)
		go func() { release, err := s.acquire(nextCtx); done <- slotsResult{release, err} }()
		synctest.Wait()
		assertSlotsWaiting(t, done)
		held()
		synctest.Wait()
		result := <-done
		if result.err != nil || result.release == nil {
			t.Fatalf("explicit release failed: %v", result.err)
		}
		result.release()
		assertAllSlotsReusable(t, s, 1)
	})
}

// TestProcessingSlotsIdempotentRelease 尤其验证旧闭包不能归还后续持有者的名额。
func TestProcessingSlotsIdempotentRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustProcessingSlots(t, 1)
		old := takeProcessingSlot(t, s)
		old()
		held := takeProcessingSlot(t, s)
		defer held()
		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); old(); old() }()
		}
		wg.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan slotsResult, 1)
		go func() { release, err := s.acquire(ctx); done <- slotsResult{release, err} }()
		synctest.Wait()
		assertSlotsWaiting(t, done)
		held()
		synctest.Wait()
		result := <-done
		if result.err != nil || result.release == nil {
			t.Fatalf("current owner release failed: %v", result.err)
		}
		result.release()
		assertAllSlotsReusable(t, s, 1)
	})
}

// TestProcessingSlotsCancelReleaseRace 取消与归还同时发生，允许任一合法结果。
// 失败必须没有 release；成功必须由调用方归还，最后检查没有遗留占位。
func TestProcessingSlotsCancelReleaseRace(t *testing.T) {
	s := mustProcessingSlots(t, 1)
	for i := 0; i < 100; i++ {
		held := takeProcessingSlot(t, s)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan slotsResult, 1)
		go func() { release, err := s.acquire(ctx); done <- slotsResult{release, err} }()
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cancel() }()
		go func() { defer wg.Done(); <-start; held() }()
		close(start)
		wg.Wait()
		select {
		case result := <-done:
			if result.err == nil {
				if result.release == nil {
					t.Fatal("success without release")
				}
				result.release()
			} else if !errors.Is(result.err, context.Canceled) || result.release != nil {
				t.Fatalf("invalid race outcome: %+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter stuck after cancel and release")
		}
		assertAllSlotsReusable(t, s, 1)
	}
}

// TestProcessingSlotsConcurrentHolders 独立统计成功 acquire 至开始 release 之间的持有数。
// 主动让出 CPU 以增加交叠；不根据吞吐或耗时作性能结论。
func TestProcessingSlotsConcurrentHolders(t *testing.T) {
	const limit, callers, iterations = 3, 24, 25
	s := mustProcessingSlots(t, limit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var current, maximum, completed atomic.Int64
	failures := make(chan error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				release, err := s.acquire(ctx)
				if err != nil {
					failures <- err
					return
				}
				held := current.Add(1)
				for previous := maximum.Load(); held > previous; previous = maximum.Load() {
					if maximum.CompareAndSwap(previous, held) {
						break
					}
				}
				runtime.Gosched()
				current.Add(-1)
				release()
				completed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Errorf("concurrent acquisition failed: %v", err)
	}
	if maximum.Load() > limit || current.Load() != 0 || completed.Load() != callers*iterations {
		t.Fatalf("max=%d remaining=%d completed=%d", maximum.Load(), current.Load(), completed.Load())
	}
	assertAllSlotsReusable(t, s, limit)
}
