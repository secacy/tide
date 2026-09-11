//go:build tide_load

package gateway

import (
	"context"
	"os"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// loadProcessingPool 是共享计算瓶颈的实验替身，不是生产 Worker 调度器。
// 每条 RPC 最多持有一个等待/处理中的块；槽位按块借用，结果发送前释放。
// channel 不承诺业务级公平性；模拟等待可取消，不创建每块 goroutine。
type loadProcessingPool struct {
	slots                                                   chan struct{}
	mu                                                      sync.Mutex
	waiting, active, peakWaiting, peakActive                int
	started, processed, canceledWaiting, canceledProcessing int
	wait, service                                           loadHistogram
}

func newLoadProcessingPool(slots int) *loadProcessingPool {
	if slots <= 0 {
		panic("processing slots must be positive")
	}
	return &loadProcessingPool{slots: make(chan struct{}, slots)}
}

// process 的等待统计包含取消的请求；service 只统计获得槽位后的持有时间。
// 所有路径释放槽位，已完成的计数不等价于结果已成功送达客户端。
func (p *loadProcessingPool) process(ctx context.Context, delay time.Duration) error {
	started := time.Now()
	p.mu.Lock()
	p.waiting++
	p.started++
	p.peakWaiting = max(p.peakWaiting, p.waiting)
	p.mu.Unlock()
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		p.mu.Lock()
		p.waiting--
		p.canceledWaiting++
		p.wait.add(time.Since(started))
		p.mu.Unlock()
		return ctx.Err()
	}
	held := time.Now()
	p.mu.Lock()
	p.waiting--
	p.active++
	p.peakActive = max(p.peakActive, p.active)
	p.wait.add(held.Sub(started))
	p.mu.Unlock()
	// 即使获取槽位与取消同时就绪，也必须在工作前重新检查取消。
	err := ctx.Err()
	if err == nil {
		err = loadWait(ctx, delay)
	}
	p.mu.Lock()
	p.active--
	p.service.add(time.Since(held))
	if err == nil {
		p.processed++
	} else {
		p.canceledProcessing++
	}
	// 持锁释放，使下一持有者更新 active 时不会把旧持有者重复计数。
	<-p.slots
	p.mu.Unlock()
	return err
}

// snapshot 的吞吐和占用率使用整个会话窗口；series 保留累积量供分窗分析。
// 占用率基于模拟处理时间，不表示 CPU/GPU 使用率。
func (p *loadProcessingPool) snapshot(full bool, elapsed time.Duration) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := map[string]any{"slots": cap(p.slots), "waiting": p.waiting, "active": p.active,
		"peak_waiting": p.peakWaiting, "peak_active": p.peakActive, "started": p.started,
		"processed": p.processed, "canceled_waiting": p.canceledWaiting, "canceled_processing": p.canceledProcessing,
		"held_ms": float64(p.service.total) / float64(time.Millisecond)}
	if full {
		r["slot_wait"] = p.wait.summary()
		r["slot_hold"] = p.service.summary()
		r["processed_per_second"] = float64(p.processed) / elapsed.Seconds()
		r["slot_occupancy_ratio"] = float64(p.service.total) / float64(elapsed) / float64(cap(p.slots))
	}
	return r
}

// drained 表示所有处理及等待均已结束，槽位全部归还；不是发出取消请求。
func (p *loadProcessingPool) assertDrained(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active != 0 || p.waiting != 0 || len(p.slots) != 0 ||
		p.started != p.processed+p.canceledWaiting+p.canceledProcessing {
		t.Errorf("pool not drained: active=%d waiting=%d slots=%d started=%d processed=%d canceled=%d/%d",
			p.active, p.waiting, len(p.slots), p.started, p.processed, p.canceledWaiting, p.canceledProcessing)
	}
}

// 验证等待取消、处理中取消及随后复用，避免实验因槽位泄漏制造假过载。
func TestLoadProcessingPoolCancellation(t *testing.T) {
	p := newLoadProcessingPool(1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.process(ctx, time.Minute) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		p.mu.Lock()
		active := p.active
		p.mu.Unlock()
		if active == 1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("holder did not start")
		case <-time.After(time.Millisecond):
		}
	}
	waitCtx, stopWait := context.WithCancel(t.Context())
	stopWait()
	if err := p.process(waitCtx, time.Minute); err != context.Canceled {
		t.Fatalf("waiting: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("processing: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("holder did not cancel")
	}
	if err := p.process(t.Context(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	p.assertDrained(t)
	if p.canceledWaiting != 1 || p.canceledProcessing != 1 || p.processed != 1 || p.peakActive != 1 {
		t.Fatalf("wrong pool accounting: %v", p.snapshot(true, time.Second))
	}
}

// TestExperimentSharedCapacity 测量固定共享能力下的并发曲线。
// 理想算术上限 4/10ms=400 块/秒，即 8 条 50 块/秒流；实际有调度开销。
// 所有连接一次接入，失败后不补建，因此必须结合退出时间解释幸存者延迟。
func TestExperimentSharedCapacity(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !loadOverlayEnabled {
		t.Fatal("run via scripts/run_streaming_load.py --experiment capacity")
	}
	duration := 20 * time.Second
	if value := os.Getenv("TIDE_LOAD_DURATION_MS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			t.Fatal("invalid duration override")
		}
		duration = time.Duration(n) * time.Millisecond
	}
	for _, n := range []int{4, 6, 8, 12, 16} {
		t.Run("shared_"+strconv.Itoa(n), func(t *testing.T) {
			runStreamingLoadWithPool(t, loadCase{"shared_" + strconv.Itoa(n), n, duration, 10 * time.Millisecond, false}, newLoadProcessingPool(4))
		})
	}
}

// 整个测试进程的实际 CPU 时间，包含 Gateway、驱动和模拟 Worker，不能归因于模型。
func loadProcessCPUSeconds(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}
