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

// admissionClient 保留接入失败与已接纳后失败的区别，不能只统计成功流延迟。
type admissionClient struct {
	ID         int     `json:"id"`
	Cohort     string  `json:"cohort"`
	HTTP       int     `json:"http_status"`
	DialMS     float64 `json:"dial_ms"`
	StartedMS  int64   `json:"started_ms"`
	FinishedMS int64   `json:"finished_ms"`
	Planned    int     `json:"planned"`
	Sent       int     `json:"sent"`
	Received   int     `json:"received"`
	Code       int     `json:"close_code"`
	Reason     string  `json:"reason"`
	Final      bool    `json:"final"`
	Error      string  `json:"error,omitempty"`
}

// TestExperimentAdmissionProtection 先运行六条流，再突发十次接入。
// 对照只改变 MaxSessions；pause 组在独立时间窗模拟一次共享能力暂停。
func TestExperimentAdmissionProtection(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !loadOverlayEnabled {
		t.Fatal("run via scripts/run_streaming_load.py --experiment admission")
	}
	duration := time.Minute
	if value := os.Getenv("TIDE_LOAD_DURATION_MS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 6000 {
			t.Fatal("admission duration override must be at least 6000 ms")
		}
		duration = time.Duration(n) * time.Millisecond
	}
	for _, cfg := range []struct {
		name  string
		limit int
		pause bool
	}{
		{"limit16", 16, false}, {"limit6", 6, false}, {"limit16_pause", 16, true}, {"limit6_pause", 6, true},
	} {
		t.Run(cfg.name, func(t *testing.T) { runAdmissionExperiment(t, cfg.name, cfg.limit, cfg.pause, duration) })
	}
}

// runAdmissionExperiment 使用原生产接入流程，所有客户端及 Worker handler 结束后才报告。
// input 为旧会话计划输入时长，新会话在 input/12 接入，输入剩余时长。
func runAdmissionExperiment(t *testing.T, name string, limit int, pause bool, input time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), input+20*time.Second)
	defer cancel()
	m := &loadMetrics{queues: make(map[*audioQueue]*loadQueueTrace), workerFinished: make(chan struct{}, 1), pool: newLoadProcessingPool(4)}
	loadObserver.Store(m)
	t.Cleanup(func() { loadObserver.Store(nil) })
	worker := startLoadWorker(t, loadCase{processing: 10 * time.Millisecond}, m)
	h := newGatewayHarness(t, worker, limit)
	h.ctx = ctx
	// 失败驱动也先解除 I/O，再由已有 harness 清理，避免悬挂污染下一子测试。
	t.Cleanup(h.gateway.Abort)
	groups := map[string]*loadMetrics{"existing": {}, "incoming": {}}
	epoch := time.Now()
	burstAt := input / 12
	if pause {
		m.pool.pauseAt = epoch.Add(input / 6)
		m.pool.pauseFor = 500 * time.Millisecond
	}
	done := make(chan admissionClient, 16)
	launch := func(first, n int, cohort string, offset time.Duration) {
		for id := first; id < first+n; id++ {
			go func() {
				r := admissionClient{ID: id, Cohort: cohort, StartedMS: time.Since(epoch).Milliseconds()}
				started := time.Now()
				conn, response, err := h.dial()
				r.DialMS = float64(time.Since(started)) / float64(time.Millisecond)
				if response != nil {
					r.HTTP = response.StatusCode
				}
				if err != nil {
					if response != nil {
						_ = response.Body.Close()
					}
					if r.HTTP != http.StatusServiceUnavailable {
						r.Error = fmt.Sprintf("dial: %v", err)
					}
				} else {
					r.Planned = int((input - offset) / (20 * time.Millisecond))
					v := runLoadClient(ctx, conn, id, r.Planned, 20*time.Millisecond, time.Now(), groups[cohort])
					_ = conn.CloseNow()
					r.Sent, r.Received, r.Code, r.Reason, r.Final, r.Error = v.sent, v.received, int(v.code), v.reason, v.final, v.err
				}
				r.FinishedMS = time.Since(epoch).Milliseconds()
				done <- r
			}()
		}
	}
	launch(0, 6, "existing", 0)
	burst := time.NewTimer(burstAt)
	defer burst.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	series := make([]map[string]any, 0, int(input/(200*time.Millisecond))+32)
	sample := func() {
		m.mu.Lock()
		queued := m.queuedBytes
		m.mu.Unlock()
		series = append(series, map[string]any{"at_ms": time.Since(epoch).Milliseconds(), "sessions": len(h.gateway.registry.snapshot()), "queue_bytes": queued, "pool": m.pool.snapshot(false, time.Since(epoch))})
	}
	sample()
	clients := make([]admissionClient, 0, 16)
	burstLaunched := false
	for len(clients) < 16 {
		select {
		case <-burst.C:
			if len(clients) != 0 || m.workerOpened.Load() != 6 || len(h.gateway.registry.snapshot()) != 6 {
				t.Fatal("initial six sessions not established before burst")
			}
			burstLaunched = true
			launch(6, 10, "incoming", burstAt)
		case r := <-done:
			clients = append(clients, r)
		case <-ticker.C:
			sample()
		case <-ctx.Done():
			t.Fatal("admission workload did not finish:", ctx.Err())
		}
	}
	if !burstLaunched {
		t.Fatal("burst was not launched")
	}
	h.waitHandlers(t, 16)
	waitWorker := func() {
		for m.workerActive.Load() != 0 {
			select {
			case <-m.workerFinished:
			case <-ctx.Done():
				t.Fatal("Worker handlers did not exit")
			}
		}
	}
	waitWorker()
	sample()
	elapsed := time.Since(epoch)
	m.pool.assertDrained(t)
	if len(h.gateway.registry.snapshot()) != 0 {
		t.Fatal("registered sessions remain")
	}
	opened := m.workerOpened.Load()
	workerReceived := m.workerReceived.Load()
	admitted, rejected := 0, 0
	for _, r := range clients {
		if r.Error != "" {
			t.Errorf("client %d: %s", r.ID, r.Error)
		}
		if r.HTTP == 101 {
			admitted++
			if r.Code == 1000 && (!r.Final || r.Sent != r.Planned || r.Received != r.Planned) {
				t.Errorf("incomplete normal client: %+v", r)
			}
		} else if r.HTTP == 503 {
			rejected++
		} else {
			t.Errorf("unexpected HTTP status: %+v", r)
		}
		if limit == 6 && r.Cohort == "existing" && r.Code != 1000 {
			t.Errorf("protected initial session failed: %+v", r)
		}
		if limit == 6 && r.Cohort == "incoming" && r.HTTP != 503 {
			t.Errorf("excess request was not refused: %+v", r)
		}
	}
	if opened != int64(admitted) {
		t.Errorf("RPC opens=%d admitted=%d", opened, admitted)
	}
	if limit == 16 && admitted != 16 {
		t.Errorf("baseline did not admit all requests: %d", admitted)
	}
	if pause && m.pool.pauseHits == 0 {
		t.Error("shared processing pause never occurred")
	}
	cohorts := make(map[string]any)
	for name, g := range groups {
		g.mu.Lock()
		cohorts[name] = map[string]any{"result_latency": g.latency.summary(), "schedule_lateness": g.lateness.summary()}
		g.mu.Unlock()
	}
	m.mu.Lock()
	queueMetrics := map[string]any{"enqueued": m.enqueued, "dequeued": m.dequeued, "abandoned": m.abandoned, "peak_bytes_session": m.maxSessionBytes, "wait": m.wait.summary(), "remaining_queues": len(m.queues), "remaining_bytes": m.queuedBytes}
	if len(m.queues) != 0 || m.queuedBytes != 0 || m.enqueued != m.dequeued+m.abandoned {
		t.Error("queue accounting did not drain")
	}
	m.mu.Unlock()
	poolMetrics := m.pool.snapshot(true, elapsed)
	// 全部旧连接注销后，用同一个仍在接入的 Gateway 验证名额可复用。
	// 探针不计入主负载的时长、队列、吞吐或延迟，只报告独立结果。
	probeConn := h.mustDial(t)
	probe := runLoadClient(ctx, probeConn, 100, 5, 20*time.Millisecond, time.Now(), &loadMetrics{})
	_ = probeConn.CloseNow()
	h.waitHandlers(t, 1)
	waitWorker()
	h.gateway.StopAccepting()
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	m.pool.assertDrained(t)
	if probe.code != websocket.StatusNormalClosure || probe.received != 5 || !probe.final || probe.err != "" || m.workerOpened.Load() != opened+1 {
		t.Fatalf("slot reuse failed: %+v", probe)
	}
	result := map[string]any{"experiment": "EXP-007", "case": name, "max_sessions": limit, "input_ms": input.Milliseconds(), "burst_at_ms": burstAt.Milliseconds(), "pause_at_ms": input.Milliseconds() / 6, "pause_ms": m.pool.pauseFor.Milliseconds(), "elapsed_ms": elapsed.Milliseconds(), "admitted": admitted, "rejected": rejected, "rpc_opened": opened, "worker_received": workerReceived, "clients": clients, "cohorts": cohorts, "pool": poolMetrics, "queue": queueMetrics, "series": series, "reuse_probe_complete": true, "registered_after_cleanup": len(h.gateway.registry.snapshot()), "go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "num_cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
}
