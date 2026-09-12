//go:build tide_load

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestExperimentWorkerSelection 分开记录逻辑事件重放与真实流式回显，不混用时间口径。
func TestExperimentWorkerSelection(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !loadOverlayEnabled {
		t.Fatal("run via scripts/run_streaming_load.py --experiment selection")
	}
	duration := 6 * time.Second
	if value := os.Getenv("TIDE_LOAD_DURATION_MS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 100 {
			t.Fatal("invalid duration")
		}
		duration = time.Duration(n) * time.Millisecond
	}
	for _, policy := range []WorkerSelectionPolicy{RoundRobin, LeastReservedRatio} {
		for _, heterogeneous := range []bool{false, true} {
			capacities, slots := []int{4, 4}, []int{2, 2}
			name := "equal"
			if heterogeneous {
				name = "heterogeneous"
				capacities = []int{2, 6}
				slots = []int{1, 3}
			}
			t.Run("logical_"+name+"_"+string(policy), func(t *testing.T) { runWorkerSelectionReplay(t, policy, capacities) })
			t.Run("stream_"+name+"_"+string(policy), func(t *testing.T) { runWorkerSelectionStream(t, policy, capacities, slots, duration) })
		}
	}
}

// selectionEvent 的 Tick 是虚拟序号；租期和到达序列对两种策略完全相同，与 Worker 无关。
type selectionEvent struct {
	Tick   int    `json:"tick"`
	Hold   int    `json:"hold_ticks"`
	Worker string `json:"worker"`
}

// runWorkerSelectionReplay 重放初始四会话与交错释放/突发接入；没有网络或真实计时。
func runWorkerSelectionReplay(t *testing.T, policy WorkerSelectionPolicy, capacities []int) {
	pool := testWorkerPool(t, policy, capacities...)
	type activeLease struct {
		until int
		lease *WorkerLease
	}
	var active []activeLease
	var events []selectionEvent
	var initial []WorkerSnapshot
	random := rand.New(rand.NewPCG(17, 29))
	admitted, rejected := 0, 0
	area := 0.0
	assignments := make([]int, len(capacities))
	for tick := 0; tick < 200; tick++ {
		kept := active[:0]
		for _, a := range active {
			if a.until <= tick {
				a.lease.Release()
			} else {
				kept = append(kept, a)
			}
		}
		active = kept
		arrivals := 0
		if tick == 0 {
			arrivals = 4
		} else if tick >= 20 && tick%3 == 0 {
			arrivals = 1
		}
		if tick == 60 || tick == 120 {
			arrivals += 10
		}
		for range arrivals {
			hold := 10 + random.IntN(40)
			lease, err := pool.TryAcquire()
			event := selectionEvent{Tick: tick, Hold: hold}
			if errors.Is(err, ErrNoWorkerCapacity) {
				rejected++
				event.Worker = "rejected"
			} else {
				if err != nil {
					t.Fatal(err)
				}
				admitted++
				event.Worker = lease.WorkerID()
				i, _ := strconv.Atoi(event.Worker)
				assignments[i]++
				active = append(active, activeLease{tick + hold, lease})
			}
			events = append(events, event)
		}
		snapshot := pool.Snapshot()
		if tick == 0 {
			initial = snapshot
		}
		low, high := 1.0, 0.0
		for _, s := range snapshot {
			if s.Reserved < 0 || s.Reserved > s.Capacity {
				t.Fatal(s)
			}
			ratio := float64(s.Reserved) / float64(s.Capacity)
			low = math.Min(low, ratio)
			high = math.Max(high, ratio)
		}
		area += high - low
	}
	for _, a := range active {
		a.lease.Release()
	}
	for _, s := range pool.Snapshot() {
		if s.Reserved != 0 {
			t.Fatal("replay leaked lease")
		}
	}
	emitSelectionResult(t, map[string]any{"kind": "logical", "policy": policy, "capacities": capacities, "seed": []int{17, 29}, "ticks": 200, "admitted": admitted, "rejected": rejected, "assignments": assignments, "initial": initial, "mean_reserved_ratio_gap": area / 200, "events": events, "after": pool.Snapshot()})
}

// runWorkerSelectionStream 四会话输入同样的 20 ms 音频块，各 Worker 有独立共享处理槽位。
// 异构组故意在小 Worker 两会话时形成处理压力，用来观察分配差异；配额不是生产建议。
func runWorkerSelectionStream(t *testing.T, policy WorkerSelectionPolicy, capacities, slots []int, duration time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), duration+15*time.Second)
	defer cancel()
	observer := &loadMetrics{queues: make(map[*audioQueue]*loadQueueTrace)}
	loadObserver.Store(observer)
	t.Cleanup(func() { loadObserver.Store(nil) })
	var workers []*loadMetrics
	var configs []WorkerConfig
	for i, cap := range capacities {
		m := &loadMetrics{workerFinished: make(chan struct{}, 1), pool: newLoadProcessingPool(slots[i])}
		workers = append(workers, m)
		configs = append(configs, WorkerConfig{ID: strconv.Itoa(i), Capacity: cap, Client: startLoadWorker(t, loadCase{processing: 12 * time.Millisecond}, m)})
	}
	pool, err := NewWorkerPool(configs, policy)
	if err != nil {
		t.Fatal(err)
	}
	h := newGatewayHarnessWithPool(t, pool, Config{MaxSessions: 8})
	h.ctx = ctx
	t.Cleanup(h.gateway.Abort)
	const clients = 4
	conns := make([]*websocket.Conn, clients)
	for i := range conns {
		conns[i] = h.mustDial(t)
	}
	initial := pool.Snapshot()
	epoch := time.Now()
	chunks := int(duration / (20 * time.Millisecond))
	done := make(chan loadClientResult, clients)
	for id, conn := range conns {
		go func() { done <- runLoadClient(ctx, conn, id, chunks, 20*time.Millisecond, epoch, observer) }()
	}
	normal, sent, received := 0, 0, 0
	closeCodes := map[string]int{}
	var details []map[string]any
	for range clients {
		select {
		case r := <-done:
			if r.err != "" {
				t.Error(r.err)
			}
			closeCodes[strconv.Itoa(int(r.code))]++
			sent += r.sent
			received += r.received
			if r.code == websocket.StatusNormalClosure {
				if r.sent != chunks || r.received != chunks || !r.final {
					t.Error("normal close without full results")
				}
				normal++
			}
			details = append(details, map[string]any{"id": r.id, "sent": r.sent, "received": r.received, "final": r.final, "close_code": r.code, "reason": r.reason, "error": r.err, "finished_ms": r.finishedMS})
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	h.waitHandlers(t, clients)
	h.gateway.StopAccepting()
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	var workerResults []map[string]any
	var opened int64
	for _, m := range workers {
		for m.workerActive.Load() != 0 {
			select {
			case <-m.workerFinished:
			case <-ctx.Done():
				t.Fatal("Worker not drained")
			}
		}
		m.pool.assertDrained(t)
		opened += m.workerOpened.Load()
		workerResults = append(workerResults, map[string]any{"opened": m.workerOpened.Load(), "received": m.workerReceived.Load(), "active_after": m.workerActive.Load(), "processing": m.pool.snapshot(true, time.Since(epoch))})
	}
	if opened != clients {
		t.Fatalf("opened %d RPCs, want %d", opened, clients)
	}
	for _, s := range pool.Snapshot() {
		if s.Reserved != 0 {
			t.Fatal("stream leaked lease")
		}
	}
	observer.mu.Lock()
	metrics := map[string]any{"latency": observer.latency.summary(), "schedule_lateness": observer.lateness.summary(), "queue_wait": observer.wait.summary(), "queue_peak_bytes_session": observer.maxSessionBytes, "queue_bytes_after": observer.queuedBytes, "queues_after": len(observer.queues)}
	if observer.queuedBytes != 0 || len(observer.queues) != 0 {
		t.Error("queue observer leaked")
	}
	observer.mu.Unlock()
	emitSelectionResult(t, map[string]any{"kind": "stream", "policy": policy, "capacities": capacities, "slots": slots, "processing_ms": 12, "input_ms": duration.Milliseconds(), "clients": clients, "planned_chunks": clients * chunks, "normal_complete": normal, "sent": sent, "received": received, "close_codes": closeCodes, "initial": initial, "after": pool.Snapshot(), "registered_after": len(h.gateway.registry.snapshot()), "workers": workerResults, "metrics": metrics, "clients_detail": details})
}

// emitSelectionResult 沿用 Go JSON 日志封装，长记录由通用解析器重新拼接。
func emitSelectionResult(t *testing.T, result map[string]any) {
	result["experiment"] = "EXP-008"
	result["case"] = t.Name()[len("TestExperimentWorkerSelection/"):]
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", data)
}

// BenchmarkWorkerSelection 测量预留+释放开销；不执行 WS/RPC，不代表会话吞吐容量。
// parallel 使用 4*GOMAXPROCS 个调用者；配额足够，不混入满额拒绝的低成本路径。
func BenchmarkWorkerSelection(b *testing.B) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		b.Skip("explicit experiment opt-in required")
	}
	for _, policy := range []WorkerSelectionPolicy{RoundRobin, LeastReservedRatio} {
		for _, count := range []int{2, 8, 32} {
			for _, parallel := range []bool{false, true} {
				mode := "serial"
				if parallel {
					mode = "parallel"
				}
				b.Run(fmt.Sprintf("%s/workers%d/%s", policy, count, mode), func(b *testing.B) {
					configs := make([]WorkerConfig, count)
					for i := range configs {
						configs[i] = WorkerConfig{ID: strconv.Itoa(i), Client: &unusedGatewayWorker{}, Capacity: 1_000_000}
					}
					pool, err := NewWorkerPool(configs, policy)
					if err != nil {
						b.Fatal(err)
					}
					operation := func() {
						lease, err := pool.TryAcquire()
						if err != nil {
							b.Error(err)
							return
						}
						lease.Release()
					}
					b.ReportAllocs()
					b.SetParallelism(4)
					b.ResetTimer()
					if parallel {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								operation()
							}
						})
					} else {
						for range b.N {
							operation()
						}
					}
					b.StopTimer()
					for _, s := range pool.Snapshot() {
						if s.Reserved != 0 {
							b.Fatal("benchmark leaked lease")
						}
					}
				})
			}
		}
	}
}
