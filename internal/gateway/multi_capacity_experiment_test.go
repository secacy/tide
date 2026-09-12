//go:build tide_load

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// multiLatencyWindows 复用一个直方图，只保留每十秒的汇总；由 loadMetrics.mu 保护。
// 窗口按收到结果的时刻划分；没有结果的区间不伪造零延迟样本。
type multiLatencyWindows struct {
	epoch     time.Time
	index     int
	histogram loadHistogram
	rows      []map[string]any
}

func (w *multiLatencyWindows) add(latency time.Duration) {
	index := int(time.Since(w.epoch) / (10 * time.Second))
	if index != w.index {
		w.flush()
		w.index = index
	}
	w.histogram.add(latency)
}

func (w *multiLatencyWindows) flush() {
	if w.histogram.count == 0 {
		return
	}
	row := w.histogram.summary()
	row["start_ms"] = w.index * 10_000
	w.rows = append(w.rows, row)
	w.histogram = loadHistogram{}
}

// multiClientResult 保留 Worker 归属、失败尾部及完成时刻，不用幸存者替代初始并发。
type multiClientResult struct {
	ID         int    `json:"id"`
	Worker     int    `json:"worker"`
	Planned    int    `json:"planned"`
	Sent       int    `json:"sent"`
	Received   int    `json:"received"`
	Final      bool   `json:"final"`
	Code       int    `json:"code"`
	Reason     string `json:"reason"`
	Error      string `json:"error,omitempty"`
	FinishedMS int64  `json:"finished_ms"`
}

// TestExperimentMultiCapacity 提供三档容量及故障组；正式采样通过 case 分批选择。
// B 的故障组候选配额可显式传入，不能把预置值当成已经通过的容量。
func TestExperimentMultiCapacity(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !loadOverlayEnabled {
		t.Fatal("run via scripts/run_streaming_load.py --experiment multi")
	}
	duration := time.Minute
	if value := os.Getenv("TIDE_LOAD_DURATION_MS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 6000 {
			t.Fatal("multi duration must be >=6000ms")
		}
		duration = time.Duration(n) * time.Millisecond
	}
	candidate := 4
	if value := os.Getenv("TIDE_MULTI_CAPACITY_B"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 5 {
			t.Fatal("B capacity must be 1..5")
		}
		candidate = n
	}
	for _, b := range []int{3, 4, 5} {
		t.Run(fmt.Sprintf("normal_1_%d", b), func(t *testing.T) { runMultiCapacity(t, b, "normal", duration) })
	}
	for _, mode := range []string{"burst", "jitter", "slow"} {
		t.Run(fmt.Sprintf("%s_1_%d", mode, candidate), func(t *testing.T) { runMultiCapacity(t, candidate, mode, duration) })
	}
}

// runMultiCapacity 在总上限 64 下接满各 Worker，确保突发拒绝来自 Worker 配额。
// 每轮结束后重新接满并发送五块音频，验证全池复用；探针单独计数。
func runMultiCapacity(t *testing.T, b int, mode string, duration time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), duration+25*time.Second)
	defer cancel()
	capacities := []int{1, b}
	slots := []int{1, 3}
	observer := &loadMetrics{queues: make(map[*audioQueue]*loadQueueTrace)}
	loadObserver.Store(observer)
	t.Cleanup(func() { loadObserver.Store(nil) })
	workers := make([]*loadMetrics, 2)
	windows := make([]*multiLatencyWindows, 2)
	var configs []WorkerConfig
	for i := range workers {
		workers[i] = &loadMetrics{workerFinished: make(chan struct{}, 1), pool: newLoadProcessingPool(slots[i])}
		windows[i] = &multiLatencyWindows{}
		workers[i].onResult = windows[i].add
		configs = append(configs, WorkerConfig{ID: strconv.Itoa(i), Capacity: capacities[i], Client: startLoadWorker(t, loadCase{processing: 12 * time.Millisecond}, workers[i])})
	}
	pool, err := NewWorkerPool(configs, LeastReservedRatio)
	if err != nil {
		t.Fatal(err)
	}
	h := newGatewayHarnessWithPool(t, pool, Config{MaxSessions: 64})
	h.ctx = ctx
	t.Cleanup(h.gateway.Abort)
	// 依次升级、全部完成预留后再发 Start；快照差值此时可确定归属，没有并行释放。
	acquire := func() ([]*websocket.Conn, []int) {
		conns := make([]*websocket.Conn, 1+b)
		routes := make([]int, 1+b)
		for id := range conns {
			before := pool.Snapshot()
			conns[id] = h.mustDial(t)
			after := pool.Snapshot()
			found := -1
			for i := range after {
				if after[i].Reserved == before[i].Reserved+1 {
					if found != -1 {
						t.Fatal("ambiguous route")
					}
					found = i
				}
			}
			if found < 0 {
				t.Fatal("reservation not observed")
			}
			routes[id] = found
		}
		return conns, routes
	}
	conns, routes := acquire()
	runtime.GC()
	var idle runtime.MemStats
	runtime.ReadMemStats(&idle)
	idleG := runtime.NumGoroutine()
	cpuStart := loadProcessCPUSeconds(t)
	epoch := time.Now()
	for _, w := range windows {
		w.epoch = epoch
	}
	faultPeriod := min(10*time.Second, duration/6)
	if mode == "jitter" {
		workers[1].pool.pauseAt = epoch.Add(faultPeriod)
		workers[1].pool.pauseEvery = faultPeriod
		workers[1].pool.pauseFor = 500 * time.Millisecond
	}
	if mode == "slow" {
		workers[1].pool.slowAt = epoch.Add(duration / 3)
		workers[1].pool.slowDelay = 24 * time.Millisecond
	}
	chunks := int(duration / (20 * time.Millisecond))
	done := make(chan multiClientResult, len(conns))
	for id, conn := range conns {
		go func() {
			r := runLoadClient(ctx, conn, id, chunks, 20*time.Millisecond, epoch, workers[routes[id]])
			done <- multiClientResult{id, routes[id], chunks, r.sent, r.received, r.final, int(r.code), r.reason, r.err, r.finishedMS}
		}()
	}
	var clients []multiClientResult
	var series []map[string]any
	sample := func() {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		var states []map[string]any
		for _, m := range workers {
			m.mu.Lock()
			received := m.latency.count
			m.mu.Unlock()
			states = append(states, map[string]any{"client_sent": m.clientSent.Load(), "results": received, "active_rpc": m.workerActive.Load(), "processing": m.pool.snapshot(false, time.Since(epoch))})
		}
		observer.mu.Lock()
		queued := observer.queuedBytes
		observer.mu.Unlock()
		series = append(series, map[string]any{"at_ms": time.Since(epoch).Milliseconds(), "registered": len(h.gateway.registry.snapshot()), "reservations": pool.Snapshot(), "workers": states, "queue_bytes": queued, "heap_alloc": mem.HeapAlloc, "goroutines": runtime.NumGoroutine()})
	}
	sample()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	// 突发 goroutine 只负责接入；独立计数，拒绝不伪造音频延迟样本。
	type rejection struct {
		Status int     `json:"status"`
		MS     float64 `json:"ms"`
		Error  string  `json:"error,omitempty"`
	}
	rejected := make(chan rejection, 10)
	var rejections []rejection
	burstAt := duration / 12
	burstTimer := time.NewTimer(burstAt)
	defer burstTimer.Stop()
	var burst <-chan time.Time
	if mode == "burst" {
		burst = burstTimer.C
	}
	expectedRejections := 0
	if mode == "burst" {
		expectedRejections = 10
	}
	for len(clients) < len(conns) || len(rejections) < expectedRejections {
		select {
		case r := <-done:
			clients = append(clients, r)
		case <-ticker.C:
			sample()
		case <-burst:
			burst = nil
			if len(h.gateway.registry.snapshot()) != len(conns) {
				t.Fatal("initial sessions not active at burst")
			}
			for range 10 {
				go func() {
					start := time.Now()
					conn, response, err := h.dial()
					r := rejection{MS: float64(time.Since(start)) / float64(time.Millisecond)}
					if response != nil {
						r.Status = response.StatusCode
						_ = response.Body.Close()
					}
					if conn != nil {
						_ = conn.CloseNow()
					}
					if err == nil || r.Status != http.StatusServiceUnavailable {
						r.Error = fmt.Sprintf("unexpected admission: %v", err)
					}
					rejected <- r
				}()
			}
		case r := <-rejected:
			rejections = append(rejections, r)
		case <-ctx.Done():
			t.Fatal("multi workload did not finish:", ctx.Err())
		}
	}
	h.waitHandlers(t, len(conns)+expectedRejections)
	waitWorkers := func() {
		for _, m := range workers {
			for m.workerActive.Load() != 0 {
				select {
				case <-m.workerFinished:
				case <-ctx.Done():
					t.Fatal("Worker not drained")
				}
			}
			m.pool.assertDrained(t)
		}
	}
	waitWorkers()
	sample()
	mainElapsed := time.Since(epoch)
	cpu := loadProcessCPUSeconds(t) - cpuStart
	var workerResults []map[string]any
	for i, m := range workers {
		m.mu.Lock()
		windows[i].flush()
		workerResults = append(workerResults, map[string]any{"id": i, "capacity": capacities[i], "opened": m.workerOpened.Load(), "received": m.workerReceived.Load(), "latency": m.latency.summary(), "schedule_lateness": m.lateness.summary(), "windows": windows[i].rows, "processing": m.pool.snapshot(true, mainElapsed)})
		m.mu.Unlock()
		if m.workerOpened.Load() != int64(capacities[i]) {
			t.Error("unexpected RPC count")
		}
	}
	normal := 0
	for _, r := range clients {
		if r.Error != "" {
			t.Error(r.Error)
		}
		if r.Code == 1000 {
			normal++
			if !r.Final || r.Sent != chunks || r.Received != chunks {
				t.Error("incomplete normal close")
			}
		}
	}
	for _, r := range rejections {
		if r.Error != "" {
			t.Error(r.Error)
		}
	}
	checkDrained := func() {
		if len(h.gateway.registry.snapshot()) != 0 {
			t.Error("registry not drained")
		}
		for _, s := range pool.Snapshot() {
			if s.Reserved != 0 {
				t.Error("lease leak")
			}
		}
		observer.mu.Lock()
		defer observer.mu.Unlock()
		if observer.queuedBytes != 0 || len(observer.queues) != 0 {
			t.Error("queue leak")
		}
	}
	checkDrained()
	observer.mu.Lock()
	queue := map[string]any{"enqueued": observer.enqueued, "dequeued": observer.dequeued, "abandoned": observer.abandoned, "peak_bytes_session": observer.maxSessionBytes, "remaining_bytes": observer.queuedBytes, "remaining_queues": len(observer.queues)}
	observer.mu.Unlock()
	// 观测器保留按时间采样及窗口摘要；清理堆值包含这些已保存记录，不称作泄漏量。
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	afterG := runtime.NumGoroutine()
	probeConns, probeRoutes := acquire()
	probeEpoch := time.Now()
	probeDone := make(chan loadClientResult, len(probeConns))
	for id, conn := range probeConns {
		go func() {
			probeDone <- runLoadClient(ctx, conn, 100+id, 5, 20*time.Millisecond, probeEpoch, &loadMetrics{})
		}()
	}
	for range probeConns {
		select {
		case r := <-probeDone:
			if r.code != websocket.StatusNormalClosure || r.sent != 5 || r.received != 5 || !r.final || r.err != "" {
				t.Fatalf("reuse probe failed: %+v", r)
			}
		case <-ctx.Done():
			t.Fatal("probe timed out")
		}
	}
	h.waitHandlers(t, len(probeConns))
	waitWorkers()
	checkDrained()
	h.gateway.StopAccepting()
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{"experiment": "EXP-009", "case": fmt.Sprintf("%s_1_%d", mode, b), "mode": mode, "capacities": capacities, "slots": slots, "processing_ms": 12, "input_ms": duration.Milliseconds(), "max_sessions": 64, "planned_chunks_per_client": chunks, "normal_complete": normal, "clients": clients, "workers": workerResults, "series": series, "rejections": rejections, "burst_at_ms": burstAt.Milliseconds(), "fault_period_ms": faultPeriod.Milliseconds(), "pause_ms": 500, "slow_at_ms": (duration / 3).Milliseconds(), "slow_processing_ms": 24, "queue": queue, "main_elapsed_ms": mainElapsed.Milliseconds(), "cpu_seconds": cpu, "cpu_mean_cores": cpu / mainElapsed.Seconds(), "heap_idle": idle.HeapAlloc, "heap_after_gc": after.HeapAlloc, "goroutines_idle": idleG, "goroutines_after": afterG, "registered_after": len(h.gateway.registry.snapshot()), "reservations_after": pool.Snapshot(), "probe_sessions": len(probeConns), "probe_routes": probeRoutes, "probe_complete": true}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
}
