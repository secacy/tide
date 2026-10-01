package mockasr

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// assertProcessingSnapshot 核对三个字段，避免只检查占用而遗漏等待计数。
func assertProcessingSnapshot(t *testing.T, s *processingSlots, limit, inUse, waiting int) {
	t.Helper()
	want := processingSlotsSnapshot{Limit: limit, InUse: inUse, Waiting: waiting}
	if got := s.snapshot(); got != want {
		t.Fatalf("snapshot=%+v want=%+v", got, want)
	}
}

// TestProcessingSnapshotValues 验证容量内获取不登记等待，返回的是独立值副本。
func TestProcessingSnapshotValues(t *testing.T) {
	s := mustProcessingSlots(t, 2)
	assertProcessingSnapshot(t, s, 2, 0, 0)
	first := takeProcessingSlot(t, s)
	defer first()
	old := s.snapshot()
	assertProcessingSnapshot(t, s, 2, 1, 0)
	second := takeProcessingSlot(t, s)
	defer second()
	assertProcessingSnapshot(t, s, 2, 2, 0)
	if old != (processingSlotsSnapshot{Limit: 2, InUse: 1}) {
		t.Fatalf("old snapshot changed: %+v", old)
	}
	old.InUse = 99
	assertProcessingSnapshot(t, s, 2, 2, 0)
	first()
	assertProcessingSnapshot(t, s, 2, 1, 0)
	second()
	assertProcessingSnapshot(t, s, 2, 0, 0)
}

// TestProcessingSnapshotWaitingTransfers 使用虚拟调度确认等待登记、逐次交接和最终归零，不断言公平顺序。
func TestProcessingSnapshotWaitingTransfers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := mustProcessingSlots(t, 1)
		held := takeProcessingSlot(t, s)
		defer held()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		results := make(chan slotsResult, 3)
		for range 3 {
			go func() { release, err := s.acquire(ctx); results <- slotsResult{release, err} }()
		}
		synctest.Wait()
		assertProcessingSnapshot(t, s, 1, 1, 3)
		previous := held
		for remaining := 2; remaining >= 0; remaining-- {
			previous()
			synctest.Wait()
			assertProcessingSnapshot(t, s, 1, 1, remaining)
			select {
			case result := <-results:
				if result.err != nil || result.release == nil {
					t.Fatalf("handoff failed: %+v", result)
				}
				previous = result.release
			default:
				t.Fatal("waiter not woken")
			}
		}
		previous()
		assertProcessingSnapshot(t, s, 1, 0, 0)
	})
}

// TestProcessingSnapshotWaitingCancellation 验证单个等待者取消/到期只撤销自己的登记，其他请求仍能接棒。
func TestProcessingSnapshotWaitingCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline_%v", deadline), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := mustProcessingSlots(t, 1)
				held := takeProcessingSlot(t, s)
				defer held()
				ctx, cancel := context.WithCancel(context.Background())
				want := error(context.Canceled)
				if deadline {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					want = context.DeadlineExceeded
				}
				defer cancel()
				otherCtx, stopOther := context.WithCancel(context.Background())
				defer stopOther()
				canceled, other := make(chan slotsResult, 1), make(chan slotsResult, 1)
				go func() { release, err := s.acquire(ctx); canceled <- slotsResult{release, err} }()
				go func() { release, err := s.acquire(otherCtx); other <- slotsResult{release, err} }()
				synctest.Wait()
				assertProcessingSnapshot(t, s, 1, 1, 2)
				if deadline {
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				synctest.Wait()
				result := <-canceled
				if result.release != nil || !errors.Is(result.err, want) {
					t.Fatalf("cancellation=%+v want=%v", result, want)
				}
				assertProcessingSnapshot(t, s, 1, 1, 1)
				held()
				synctest.Wait()
				result = <-other
				if result.err != nil || result.release == nil {
					t.Fatalf("other waiter failed: %+v", result)
				}
				assertProcessingSnapshot(t, s, 1, 1, 0)
				result.release()
				assertProcessingSnapshot(t, s, 1, 0, 0)
			})
		})
	}
}

// TestProcessingSnapshotHolderAndRepeatedRelease 验证持有者取消不提前归还，旧闭包不会扣减新持有者。
func TestProcessingSnapshotHolderAndRepeatedRelease(t *testing.T) {
	s := mustProcessingSlots(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	old, err := s.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	assertProcessingSnapshot(t, s, 1, 1, 0)
	old()
	current := takeProcessingSlot(t, s)
	defer current()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() { defer wg.Done(); old(); old() }()
	}
	wg.Wait()
	assertProcessingSnapshot(t, s, 1, 1, 0)
	current()
	current()
	assertProcessingSnapshot(t, s, 1, 0, 0)
}

// slotsFirstCheckContext 在首次 Err 已读取后暂停，使测试能在第二次检查之前确定性取消。
// 只延迟首次检查的返回，不改变它读取到的结果；后续检查委托真实 context。
type slotsFirstCheckContext struct {
	context.Context
	once    sync.Once
	checked chan struct{}
	resume  chan struct{}
}

// Err 暂停首次已完成的读取，后续读取直接反映父 context 的取消状态。
func (c *slotsFirstCheckContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked); <-c.resume })
	return err
}

// TestProcessingSnapshotCancelBeforeLockRecheck 回归持锁复查取消后忘记解锁的问题，不让失败挂住整包测试。
func TestProcessingSnapshotCancelBeforeLockRecheck(t *testing.T) {
	s := mustProcessingSlots(t, 1)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &slotsFirstCheckContext{Context: parent, checked: make(chan struct{}), resume: make(chan struct{})}
	resultCh := make(chan slotsResult, 1)
	s.mu.Lock()
	go func() { release, err := s.acquire(ctx); resultCh <- slotsResult{release, err} }()
	<-ctx.checked
	cancel()
	close(ctx.resume)
	s.mu.Unlock()
	var result slotsResult
	select {
	case result = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("canceled acquire did not return")
	}
	if result.release != nil || !errors.Is(result.err, context.Canceled) {
		t.Fatalf("result=%+v", result)
	}
	if !s.mu.TryLock() {
		// acquire 已经返回，且没有其他使用者。只清理这个测试实例的遗留锁，以免后续断言挂住。
		s.mu.Unlock()
		t.Error("acquire returned context.Canceled without unlocking mu; snapshot and future acquisitions would block")
	} else {
		s.mu.Unlock()
	}
	assertProcessingSnapshot(t, s, 1, 0, 0)
	release := takeProcessingSlot(t, s)
	release()
	assertProcessingSnapshot(t, s, 1, 0, 0)
}

// slotsCancelOnCheckContext 在指定检查处取消真实 context，覆盖已经预占 token 的回滚路径。
// 一个实例仅供一次 acquire 使用，checks 不在多个 goroutine 之间共享访问。
type slotsCancelOnCheckContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
	at     int
}

// Err 在预定检查处触发取消，用于验证预占 token 后的回滚。
func (c *slotsCancelOnCheckContext) Err() error {
	c.checks++
	if c.checks == c.at {
		c.cancel()
	}
	return c.Context.Err()
}

// TestProcessingSnapshotTokenRollback 验证快慢两条路径在取得 token 后发现取消，都撤销物理与逻辑状态。
func TestProcessingSnapshotTokenRollback(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(fmt.Sprintf("slow_%v", slow), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := mustProcessingSlots(t, 1)
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &slotsCancelOnCheckContext{Context: parent, cancel: cancel, at: 3}
				var held func()
				if slow {
					held = takeProcessingSlot(t, s)
					defer held()
				}
				done := make(chan slotsResult, 1)
				go func() { release, err := s.acquire(ctx); done <- slotsResult{release, err} }()
				synctest.Wait()
				if slow {
					assertProcessingSnapshot(t, s, 1, 1, 1)
					held()
					synctest.Wait()
				}
				result := <-done
				if result.release != nil || !errors.Is(result.err, context.Canceled) {
					t.Fatalf("rollback=%+v", result)
				}
				assertProcessingSnapshot(t, s, 1, 0, 0)
				release := takeProcessingSlot(t, s)
				release()
				assertProcessingSnapshot(t, s, 1, 0, 0)
			})
		})
	}
}

// TestProcessingSnapshotConcurrentBounds 验证并发获取/归还/快照的边界；不把操作数解释为吞吐结果。
func TestProcessingSnapshotConcurrentBounds(t *testing.T) {
	const limit, callers, iterations = 3, 24, 25
	s := mustProcessingSlots(t, limit)
	start, stop := make(chan struct{}), make(chan struct{})
	observed := make(chan error, 1)
	go func() {
		<-start
		for {
			snapshot := s.snapshot()
			if snapshot.Limit != limit || snapshot.InUse < 0 || snapshot.InUse > limit || snapshot.Waiting < 0 || snapshot.InUse+snapshot.Waiting > callers {
				observed <- fmt.Errorf("invalid concurrent snapshot: %+v", snapshot)
				return
			}
			select {
			case <-stop:
				observed <- nil
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	var wg sync.WaitGroup
	errorsCh := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				release, err := s.acquire(context.Background())
				if err != nil {
					errorsCh <- err
					return
				}
				runtime.Gosched()
				release()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(stop)
	if err := <-observed; err != nil {
		t.Error(err)
	}
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	assertProcessingSnapshot(t, s, limit, 0, 0)
}
