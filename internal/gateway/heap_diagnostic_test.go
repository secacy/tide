package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// heapMemory 只保存数值；HeapInuse/HeapSys 不是对象净大小或 RSS。
type heapMemory struct {
	HeapAlloc    uint64 `json:"heap_alloc"`
	HeapObjects  uint64 `json:"heap_objects"`
	HeapInuse    uint64 `json:"heap_inuse"`
	HeapIdle     uint64 `json:"heap_idle"`
	HeapReleased uint64 `json:"heap_released"`
	HeapSys      uint64 `json:"heap_sys"`
	StackInuse   uint64 `json:"stack_inuse"`
	BuckHashSys  uint64 `json:"buck_hash_sys"`
	TotalAlloc   uint64 `json:"total_alloc"`
	NumGC        uint32 `json:"num_gc"`
}

// readHeapMemory 返回当前 MemStats 的数值副本；调用方不能据此取得对象引用。
func readHeapMemory() heapMemory {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return heapMemory{m.HeapAlloc, m.HeapObjects, m.HeapInuse, m.HeapIdle, m.HeapReleased, m.HeapSys, m.StackInuse, m.BuckHashSys, m.TotalAlloc, m.NumGC}
}

// heapResources 是阶段判据，不持有被测实例。观察前后不保证原子一致。
type heapResources struct {
	Sessions     int `json:"sessions"`
	Registry     int `json:"registry"`
	WorkerActive int `json:"worker_active"`
	Handshakes   int `json:"handshakes"`
	FaultTimers  int `json:"fault_timers"`
}

// active 核对原会话/注册项/RPC 仍各为一，不要求恢复期间没有入口或计时任务。
func (s heapResources) active() bool {
	return s.Sessions == 1 && s.Registry == 1 && s.WorkerActive == 1
}

// heapCheckpoint 保存一个采集干预；全部时间相对诊断进程内的 Origin。
// PlannedNS 只用于 P1/P2；FixtureNS 对齐原事件时间轴，P0/P4 为 null。
type heapCheckpoint struct {
	Version         int           `json:"version"`
	Name            string        `json:"name"`
	PlannedNS       *int64        `json:"planned_ns"`
	StartNS         int64         `json:"start_ns"`
	EndNS           int64         `json:"end_ns"`
	FixtureNS       *int64        `json:"fixture_ns"`
	RateBefore      int           `json:"rate_before"`
	RateAfter       int           `json:"rate_after"`
	GCRequested     int           `json:"gc_requested"`
	Before          heapMemory    `json:"before"`
	AfterGC         heapMemory    `json:"after_gc"`
	AfterWrite      heapMemory    `json:"after_write"`
	ResourcesBefore heapResources `json:"resources_before"`
	ResourcesAfter  heapResources `json:"resources_after"`
	File            string        `json:"file"`
	Bytes           int64         `json:"bytes"`
	SHA256          string        `json:"sha256"`
	WriteError      string        `json:"write_error"`
	CloseError      string        `json:"close_error"`
	Valid           bool          `json:"valid"`
	Reason          string        `json:"reason"`
}

// heapProfileSink 检测短写，增量计算实际接纳字节的摘要，不保留 profile 内容。
type heapProfileSink struct {
	writer io.Writer
	digest hash.Hash
	bytes  int64
}

// Write 只对底层实际接纳的 p 字节计数和取摘要；短写必须返回错误。
func (s *heapProfileSink) Write(p []byte) (int, error) {
	n, err := s.writer.Write(p)
	if n < 0 || n > len(p) {
		return 0, io.ErrShortWrite
	}
	s.digest.Write(p[:n])
	s.bytes += int64(n)
	if n < len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// captureHeapCheckpointTo 接管 writer 的写入与唯一 Close，profiler 在正式路径是 WriteHeapProfile。
// 不可取消的 GC/文件写入由外部拥有进程的总期限约束。
func captureHeapCheckpointTo(writer io.WriteCloser, profiler func(io.Writer) error, origin time.Time, name string, planned *int64) heapCheckpoint {
	c := heapCheckpoint{Version: 1, Name: name, PlannedNS: planned, StartNS: time.Since(origin).Nanoseconds(), RateBefore: runtime.MemProfileRate, GCRequested: 2}
	c.Before = readHeapMemory()
	runtime.GC()
	runtime.GC()
	c.AfterGC = readHeapMemory()
	sink := &heapProfileSink{writer: writer, digest: sha256.New()}
	if err := profiler(sink); err != nil {
		c.WriteError = err.Error()
	}
	if err := writer.Close(); err != nil {
		c.CloseError = err.Error()
	}
	c.Bytes = sink.bytes
	c.SHA256 = hex.EncodeToString(sink.digest.Sum(nil))
	c.AfterWrite = readHeapMemory()
	c.RateAfter = runtime.MemProfileRate
	c.EndNS = time.Since(origin).Nanoseconds()
	c.Valid = c.WriteError == "" && c.CloseError == "" && c.Bytes > 0 && c.RateBefore == 1 && c.RateAfter == 1 && c.AfterGC.NumGC >= c.Before.NumGC+2
	if !c.Valid {
		c.Reason = "profile_write_close_rate_or_gc_invalid"
	}
	return c
}

// writeHeapJSON 独占创建并核对写入/关闭；失败不覆盖已有元数据。
func writeHeapJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, werr := f.Write(append(data, '\n'))
	cerr := f.Close()
	if werr == nil && n != len(data)+1 {
		werr = io.ErrShortWrite
	}
	return errors.Join(werr, cerr)
}

// heapCheckpointRunner 拥有一个串行采集任务；stop 必须等待正在执行的 action 返回。
// Cancel 不会中止已经开始的 WriteHeapProfile，也不代表任务已退出。
type heapCheckpointRunner struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// newHeapCheckpointRunner 的 action 独占采集顺序；ctx 只取消尚未开始的采集。
func newHeapCheckpointRunner(action func(context.Context)) *heapCheckpointRunner {
	ctx, cancel := context.WithCancel(context.Background())
	r := &heapCheckpointRunner{cancel: cancel, done: make(chan struct{})}
	go func() { defer close(r.done); action(ctx) }()
	return r
}

// stop 可重复调用；等待 done 关闭后才返回，nil 表示从未启动。
func (r *heapCheckpointRunner) stop() {
	if r == nil {
		return
	}
	r.once.Do(r.cancel)
	<-r.done
}

// heapCaseObserver 固定最多五条元数据；state 闭包只在夹具作用域存活，detach 后清空。
// 活动快照由唯一任务写入；其余字段在该任务 join 后由测试主任务使用。
type heapCaseObserver struct {
	Origin          time.Time
	Dir             string
	Early, Late     time.Duration
	Rows            []heapCheckpoint
	TCP             baselineTCPCleanup
	OwnedJoined     bool
	Detached        bool
	FirstNS         *int64
	FixtureOriginNS int64
	ReturnedNS      int64
	returnedNS      atomic.Int64
	runner          *heapCheckpointRunner
	state           func() heapResources
	fixtureOrigin   time.Time
	metadataError   error
}

// capture 文件错误仍记录元数据；采集完成后核对阶段，避免文件存在即成功。
func (h *heapCaseObserver) capture(name string, planned *int64, phaseOK bool) {
	file := name + ".pb.gz"
	f, err := os.OpenFile(filepath.Join(h.Dir, file), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	c := heapCheckpoint{Version: 1, Name: name, PlannedNS: planned, File: file, StartNS: time.Since(h.Origin).Nanoseconds()}
	var before heapResources
	if h.state != nil {
		before = h.state()
	}
	if err != nil {
		c.WriteError = err.Error()
		c.Reason = "profile_open_failed"
		c.EndNS = time.Since(h.Origin).Nanoseconds()
	} else {
		c = captureHeapCheckpointTo(f, pprof.WriteHeapProfile, h.Origin, name, planned)
		c.File = file
	}
	c.ResourcesBefore = before
	if h.state != nil {
		c.ResourcesAfter = h.state()
	}
	if !h.fixtureOrigin.IsZero() && name != "after_fixture" {
		v := h.Origin.Add(time.Duration(c.StartNS)).Sub(h.fixtureOrigin).Nanoseconds()
		c.FixtureNS = &v
	}
	if !phaseOK {
		c.Valid = false
		c.Reason = "resource_phase_invalid"
	}
	if planned != nil {
		returned := h.returnedNS.Load()
		if c.StartNS < *planned || c.StartNS-*planned > int64(time.Second) || !before.active() || !c.ResourcesAfter.active() || (returned >= 0 && h.Origin.Add(time.Duration(c.EndNS)).Sub(h.fixtureOrigin).Nanoseconds() >= returned) {
			c.Valid = false
			c.Reason = "active_checkpoint_late_inactive_or_crossed_return"
		}
	}
	h.Rows = append(h.Rows, c)
	if err := writeHeapJSON(filepath.Join(h.Dir, name+".json"), c); err != nil {
		h.metadataError = errors.Join(h.metadataError, err)
	}
}

// start 等首个 Read 的单调时间值发布；Read 不等待诊断器，不改变源时间轴。
func (h *heapCaseObserver) start(origin time.Time, first *atomic.Int64, state func() heapResources) {
	h.fixtureOrigin = origin
	h.FixtureOriginNS = origin.Sub(h.Origin).Nanoseconds()
	h.state = state
	h.returnedNS.Store(-1)
	h.runner = newHeapCheckpointRunner(func(ctx context.Context) {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for first.Load() < 0 {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
		value := first.Load()
		h.FirstNS = &value
		for i, delay := range []time.Duration{h.Early, h.Late} {
			target := origin.Add(time.Duration(value) + delay)
			timer := time.NewTimer(time.Until(target))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if ctx.Err() != nil {
				return
			}
			planned := target.Sub(h.Origin).Nanoseconds()
			name := "early_active"
			if i == 1 {
				name = "late_active"
			}
			h.capture(name, &planned, true)
		}
	})
}

// stopActive 先请求停止，再等待实际退出；不清空 P3 仍需读取的 state。
func (h *heapCaseObserver) stopActive() { h.runner.stop() }

// idle 的 natural 必须来自无兜底的空闲观察；为假时仍保留文件并标记阶段无效。
func (h *heapCaseObserver) idle(natural bool) { h.capture("session_idle", nil, natural) }

// detach 丢掉捕获 Gateway 的闭包和任务对象，保证 P4 外层不持有业务实例。
func (h *heapCaseObserver) detach() {
	h.stopActive()
	h.runner = nil
	h.state = nil
	h.Detached = true
	h.ReturnedNS = h.returnedNS.Load()
}

// runHeapDiagnosticCase 的 P4 严格在非并行子测试返回（含所有 Cleanup）后采集。
func runHeapDiagnosticCase(t *testing.T, cfg extendedConfig, dir string, probe bool) {
	t.Helper()
	if runtime.MemProfileRate != 1 {
		t.Fatal("heap diagnostics require startup GODEBUG=memprofilerate=1")
	}
	if err := pprof.WriteHeapProfile(io.Discard); err != nil {
		t.Fatal(err)
	}
	early, late := 90*time.Second, 570*time.Second
	if probe {
		early, late = 2*time.Second, 10*time.Second
	}
	h := &heapCaseObserver{Origin: time.Now(), Dir: dir, Early: early, Late: late, Rows: make([]heapCheckpoint, 0, 5)}
	h.capture("before_fixture", nil, true)
	var summary extendedSummary
	childOK := t.Run("fixture", func(t *testing.T) {
		defer h.detach()
		summary = runExtendedProbeObserved(t, cfg, dir, h)
		if !summary.Result.Completed || summary.LogError || summary.Forced || !summary.NaturalIdle {
			t.Error("heap diagnostic business or natural cleanup failed")
		}
	})
	h.capture("after_fixture", nil, childOK && h.Detached && h.OwnedJoined && h.TCP.ClientClosed && h.TCP.ServeExited)
	report := struct {
		Version         int                `json:"version"`
		Experiment      string             `json:"experiment"`
		Case            string             `json:"case"`
		OriginUTC       string             `json:"origin_utc"`
		FixtureOriginNS int64              `json:"fixture_origin_ns"`
		FirstNS         *int64             `json:"first_ns"`
		ReturnedNS      int64              `json:"returned_ns"`
		TCP             baselineTCPCleanup `json:"tcp_cleanup"`
		OwnedJoined     bool               `json:"owned_joined"`
		Detached        bool               `json:"detached"`
		ChildOK         bool               `json:"child_ok"`
		Checkpoints     []heapCheckpoint   `json:"checkpoints"`
	}{1, "EXP-006-06", cfg.Case, h.Origin.UTC().Format(time.RFC3339Nano), h.FixtureOriginNS, h.FirstNS, h.ReturnedNS, h.TCP, h.OwnedJoined, h.Detached, childOK, h.Rows}
	if err := writeHeapJSON(filepath.Join(dir, "heap-report.json"), report); err != nil {
		t.Error(err)
	}
	if h.metadataError != nil {
		t.Error(h.metadataError)
	}
	if len(h.Rows) != 5 {
		t.Errorf("only %d of 5 checkpoints captured", len(h.Rows))
	}
	for _, c := range h.Rows {
		if !c.Valid {
			t.Errorf("checkpoint %s: %s", c.Name, c.Reason)
		}
	}
}

// TestHeapDiagnosticCollectedCase 默认跳过；一次只接纳预登记的 A/B，formal 显式开启。
func TestHeapDiagnosticCollectedCase(t *testing.T) {
	mode := os.Getenv("TIDE_HEAP_MODE")
	if mode == "" {
		t.Skip("explicit heap diagnostic only")
	}
	name := os.Getenv("TIDE_HEAP_CASE")
	if name != "A_normal" && name != "B_budget_8" {
		t.Fatal("heap diagnosis accepts only A/B")
	}
	cfg, err := extendedCollectedConfig(mode, name, os.Getenv("TIDE_RUN_HEAP_FORMAL"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("TIDE_HEAP_OUTPUT")
	if dir == "" {
		t.Fatal("missing heap output")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Logf("go=%s mode=%s case=%s rate=%d", runtime.Version(), mode, name, runtime.MemProfileRate)
	runHeapDiagnosticCase(t, cfg, dir, mode == "probe")
}
