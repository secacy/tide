package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
)

// backendModelEvent 的时间由驱动观察完整日志行时记录，不使用 Worker 墙钟。
type backendModelEvent struct {
	ObservedNS int64    `json:"observed_ns"`
	Kind       string   `json:"kind"`
	ModelID    int      `json:"model_id"`
	LoadMS     *float64 `json:"load_ms,omitempty"`
	CloseMS    *float64 `json:"close_ms,omitempty"`
}

// backendRuntimeSample 代表驱动的一次观测；Worker RSS 与驱动 Go 堆分开。
// ElapsedNS/WallNS 同时记录，以暴露休眠或采样停顿；各字段并非原子快照。
type backendRuntimeSample struct {
	ElapsedNS        int64   `json:"elapsed_ns"`
	WallNS           int64   `json:"wall_ns"`
	DurationNS       int64   `json:"duration_ns"`
	Phase            string  `json:"phase"`
	Round            int     `json:"round"`
	ActiveSessions   int     `json:"active_sessions"`
	DriverGoroutines int     `json:"driver_goroutines"`
	DriverHeapBytes  uint64  `json:"driver_heap_bytes"`
	WorkerRSSBytes   *uint64 `json:"worker_rss_bytes"`
	WorkerRSSError   string  `json:"worker_rss_error,omitempty"`
	ModelsCreated    int     `json:"models_created"`
	ModelsClosed     int     `json:"models_closed"`
}

// backendMonitor 串行拥有日志读取游标、事件与文件；sampler 和清理观察共享它。
// events 只保留预登记上限内的生命周期事件，不保留任何转录全文。
type backendMonitor struct {
	mu        sync.Mutex
	start     time.Time
	g         *gateway.Gateway
	pid       int
	phase     string
	round     int
	log       *os.File
	reader    *bufio.Reader
	pending   string
	events    []backendModelEvent
	created   int
	maxEvents int // 默认 22；其他显式实验在启动采集前按预登记规模设置。
	closed    int
	eventFile *os.File
	samples   *os.File
	err       error // 首次采集错误；不以空数据或零代替。
	cancel    context.CancelFunc
	done      chan struct{}
}

func newBackendMonitor(out string, pid int, g *gateway.Gateway) (*backendMonitor, error) {
	log, err := os.Open(filepath.Join(out, "server.log"))
	if err != nil {
		return nil, err
	}
	events, err := os.OpenFile(filepath.Join(out, "model-events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Close()
		return nil, err
	}
	samples, err := os.OpenFile(filepath.Join(out, "samples.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Close()
		events.Close()
		return nil, err
	}
	return &backendMonitor{start: time.Now(), g: g, pid: pid, phase: "startup", maxEvents: 22, log: log, reader: bufio.NewReader(log), eventFile: events, samples: samples}, nil
}

// refreshLocked 只处理完整 JSON 行；EOF 前的半行保留至下一次读取。
// 64KiB 行长和固定事件数上限使意外日志不能导致工具无限积累。
func (m *backendMonitor) refreshLocked() error {
	if m.err != nil {
		return m.err
	}
	for {
		part, err := m.reader.ReadString('\n')
		m.pending += part
		if len(m.pending) > 65536 {
			m.err = errors.New("Worker log line exceeds 64KiB")
			return m.err
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			m.err = err
			return err
		}
		var row struct {
			Msg   string   `json:"msg"`
			ID    int      `json:"model_id"`
			Load  *float64 `json:"load_ms"`
			Close *float64 `json:"close_ms"`
		}
		if err := json.Unmarshal([]byte(m.pending), &row); err != nil {
			m.pending = "" // 原生日志可为非 JSON；原文仍保留在 server.log。
			continue
		}
		m.pending = ""
		if row.Msg == "model_creation_failed" {
			m.err = errors.New("Worker model creation failed")
			return m.err
		}
		if row.Msg != "model_created" && row.Msg != "engine_closed" {
			continue
		}
		if len(m.events) >= m.maxEvents {
			m.err = errors.New("too many model events")
			return m.err
		}
		if row.Msg == "model_created" {
			if row.ID != m.created+1 || m.created != m.closed || row.Load == nil {
				m.err = errors.New("unexpected model creation order")
				return m.err
			}
			m.created++
		} else {
			if row.ID != m.closed+1 || row.ID != m.created || row.Close == nil {
				m.err = errors.New("unexpected model close order")
				return m.err
			}
			m.closed++
		}
		event := backendModelEvent{time.Since(m.start).Nanoseconds(), row.Msg, row.ID, row.Load, row.Close}
		if err := json.NewEncoder(m.eventFile).Encode(event); err != nil {
			m.err = err
			return err
		}
		m.events = append(m.events, event)
	}
}

// readWorkerRSS 每次只查询已知子进程 PID；独立超时，不让 ps 阻塞采集退出。
func readWorkerRSS(ctx context.Context, pid int) (*uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	data, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return nil, err
	}
	kib, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || kib == 0 || kib > ^uint64(0)/1024 {
		return nil, fmt.Errorf("invalid ps RSS %q", data)
	}
	value := kib * 1024 // macOS/Linux ps 的 RSS 单位为 KiB。
	return &value, nil
}

func (m *backendMonitor) sample(ctx context.Context) {
	start := time.Now()
	// 先固定阶段和时间标签；RSS 查询期间阶段仍可能由主驱动推进。
	m.mu.Lock()
	if err := m.refreshLocked(); err != nil {
		m.mu.Unlock()
		return
	}
	s := backendRuntimeSample{ElapsedNS: start.Sub(m.start).Nanoseconds(), WallNS: start.UnixNano(), Phase: m.phase, Round: m.round, ModelsCreated: m.created, ModelsClosed: m.closed}
	m.mu.Unlock()
	s.ActiveSessions = m.g.Snapshot().ActiveSessions
	s.DriverGoroutines = runtime.NumGoroutine()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	s.DriverHeapBytes = mem.HeapAlloc
	rss, err := readWorkerRSS(ctx, m.pid)
	if ctx.Err() != nil {
		return
	} // 正常停止采集时不制造一条被主动取消的 RSS 失败。
	s.WorkerRSSBytes = rss
	if err != nil {
		s.WorkerRSSError = err.Error()
	}
	s.DurationNS = time.Since(start).Nanoseconds()
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := json.NewEncoder(m.samples).Encode(s); err != nil && m.err == nil {
		m.err = err
	}
}

// startSampling/stop 配对；stop 等待任务退出后才关闭所有输出。
func (m *backendMonitor) startSampling() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel, m.done = cancel, make(chan struct{})
	go func() {
		defer close(m.done)
		m.sample(ctx)
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				m.sample(ctx)
			}
		}
	}()
}

func (m *backendMonitor) stop() error {
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.refreshLocked()
	if m.pending != "" {
		err = errors.Join(err, errors.New("incomplete Worker log at monitor stop"))
	}
	return errors.Join(err, m.log.Close(), m.eventFile.Close(), m.samples.Close())
}

func (m *backendMonitor) setPhase(phase string, round int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.phase, m.round = phase, round
}

// waitCleanup 自然观察，不发出取消。idle 是 Run 返回后首次观察到 Gateway=0 的时刻。
// 模型释放可能先于客户端返回，独立事件保留其更早的观察值。
func (m *backendMonitor) waitCleanup(expected int, budget time.Duration, idle **int64) error {
	deadline := time.NewTimer(budget)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.Lock()
		err := m.refreshLocked()
		closed := m.closed
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if m.g.Snapshot().ActiveSessions == 0 {
			if *idle == nil {
				at := time.Since(m.start).Nanoseconds()
				*idle = &at
			}
			if closed == expected {
				return nil
			}
		}
		select {
		case <-deadline.C:
			return errors.New("natural Gateway/native cleanup did not complete")
		case <-tick.C:
		}
	}
}

func (m *backendMonitor) modelEvents(id int) []backendModelEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []backendModelEvent
	for _, e := range m.events {
		if e.ModelID == id {
			events = append(events, e)
		}
	}
	return events
}
