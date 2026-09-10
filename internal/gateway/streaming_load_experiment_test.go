//go:build tide_load

package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// 此文件与脚本生成的 Go overlay 配合，仅在实验构建中观测队列。
// hooks 在原有队列锁内执行，不更改队列状态；全局指标锁不反向获取队列锁。
var loadOverlayEnabled bool
var loadObserver atomic.Pointer[loadMetrics]

type loadQueueTrace struct {
	times []time.Time // 与队列槽位对应，记录成功入队时间。
	bytes int
	count int
}

// loadHistogram 固定内存，避免采样数组随会话时长增长污染堆趋势。
// 百分位按 1 ms 向上取整；超过 60 秒的样本计入末桶并单独报告。
type loadHistogram struct {
	bins     [60_001]uint64
	count    uint64
	overflow uint64
	total    time.Duration
	maximum  time.Duration
}

func (h *loadHistogram) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	index := int((d + time.Millisecond - 1) / time.Millisecond)
	if index > 60_000 {
		index = 60_000
		h.overflow++
	}
	h.bins[index]++
	h.count++
	h.total += d
	if d > h.maximum {
		h.maximum = d
	}
}

func (h *loadHistogram) summary() map[string]any {
	percentile := func(p float64) int {
		if h.count == 0 {
			return 0
		}
		target, accumulated := uint64(math.Ceil(float64(h.count)*p)), uint64(0)
		for i, n := range h.bins {
			accumulated += n
			if accumulated >= target {
				return i
			}
		}
		return 60_000
	}
	mean := 0.0
	if h.count > 0 {
		mean = float64(h.total) / float64(time.Millisecond) / float64(h.count)
	}
	return map[string]any{"count": h.count, "mean_ms": mean, "p50_ms": percentile(.5),
		"p95_ms": percentile(.95), "p99_ms": percentile(.99), "max_ms": float64(h.maximum) / float64(time.Millisecond), "over_60s": h.overflow}
}

type loadMetrics struct {
	mu                                               sync.Mutex
	queues                                           map[*audioQueue]*loadQueueTrace
	wait, send, latency, lateness                    loadHistogram
	queuedBytes, queuedChunks, peakBytes, peakChunks int
	maxSessionBytes, maxSessionChunks                int
	enqueued, dequeued, abandoned                    int
	workerReceived                                   atomic.Int64
	workerActive                                     atomic.Int64
	workerPeak                                       atomic.Int64
	workerFinished                                   chan struct{}
}

func loadQueuePush(q *audioQueue) {
	m := loadObserver.Load()
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.queues[q]
	if state == nil {
		state = &loadQueueTrace{times: make([]time.Time, len(q.slots))}
		m.queues[q] = state
	}
	state.times[(q.head+q.count-1)%len(q.slots)] = time.Now()
	m.queuedBytes += q.bytes - state.bytes
	m.queuedChunks += q.count - state.count
	state.bytes, state.count = q.bytes, q.count
	m.peakBytes = max(m.peakBytes, m.queuedBytes)
	m.peakChunks = max(m.peakChunks, m.queuedChunks)
	m.maxSessionBytes = max(m.maxSessionBytes, q.bytes)
	m.maxSessionChunks = max(m.maxSessionChunks, q.count)
	m.enqueued++
}

// 在 pop 移除槽位之前调用，因此这里读取的是仍受队列锁保护的原状态。
func loadQueuePop(q *audioQueue) {
	m := loadObserver.Load()
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.queues[q]
	if state == nil {
		panic("load observer missed enqueue")
	}
	m.wait.add(time.Since(state.times[q.head]))
	state.times[q.head] = time.Time{}
	size := len(q.slots[q.head])
	state.bytes -= size
	state.count--
	m.queuedBytes -= size
	m.queuedChunks--
	m.dequeued++
}

// run 返回时移除引用，否则观测器本身会保留异常会话的积压音频。
func loadQueueRelease(q *audioQueue) {
	m := loadObserver.Load()
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.queues[q]; state != nil {
		m.queuedBytes -= state.bytes
		m.queuedChunks -= state.count
		m.abandoned += state.count
		delete(m.queues, q)
	}
}

type loadCase struct {
	name                 string
	concurrency          int
	duration, processing time.Duration
	jitter               bool
}

// TestExperimentStreamingLoad 使用真实 gRPC 和内存 WebSocket 传输。
// Worker 每块模拟处理后返回可关联的结果；没有真实推理或共享计算池限制。
func TestExperimentStreamingLoad(t *testing.T) {
	if os.Getenv("TIDE_RUN_EXPERIMENTS") != "1" {
		t.Skip("explicit experiment opt-in required")
	}
	if !loadOverlayEnabled {
		t.Fatal("run via scripts/run_streaming_load.py to enable queue observation")
	}
	cases := []loadCase{
		{"normal_1", 1, 8 * time.Second, 5 * time.Millisecond, false},
		{"normal_8", 8, 8 * time.Second, 5 * time.Millisecond, false},
		{"normal_32", 32, 8 * time.Second, 5 * time.Millisecond, false},
		{"jitter_8", 8, 12 * time.Second, 5 * time.Millisecond, true},
		{"slow_8", 8, 20 * time.Second, 50 * time.Millisecond, false},
		{"sustained_8", 8, 60 * time.Second, 5 * time.Millisecond, false},
	}
	for _, cfg := range cases {
		t.Run(cfg.name, func(t *testing.T) {
			if value := os.Getenv("TIDE_LOAD_DURATION_MS"); value != "" {
				n, err := strconv.Atoi(value)
				if err != nil || n <= 0 {
					t.Fatal("invalid duration override")
				}
				cfg.duration = time.Duration(n) * time.Millisecond
			}
			runStreamingLoad(t, cfg)
		})
	}
}

type loadClientResult struct {
	code           websocket.StatusCode
	sent, received int
	final          bool
	err            string
}

func runStreamingLoad(t *testing.T, cfg loadCase) {
	ctx, cancel := context.WithTimeout(t.Context(), cfg.duration*3+10*time.Second)
	defer cancel()
	m := &loadMetrics{queues: make(map[*audioQueue]*loadQueueTrace), workerFinished: make(chan struct{}, 1)}
	loadObserver.Store(m)
	t.Cleanup(func() { loadObserver.Store(nil) })
	worker := startLoadWorker(t, cfg, m)
	h := newGatewayHarness(t, worker, cfg.concurrency)
	// 原 harness 的五秒只用于测试操作，不是 Gateway 会话父 Context。
	// 在启动客户端前改用本负载的独立预算。
	h.ctx = ctx
	connections := make([]*websocket.Conn, cfg.concurrency)
	for i := range connections {
		connections[i] = h.mustDial(t)
	}
	const interval = 20 * time.Millisecond
	chunks := max(1, int(cfg.duration/interval))
	runtime.GC()
	var idle runtime.MemStats
	runtime.ReadMemStats(&idle)
	idleGoroutines := runtime.NumGoroutine()
	epoch := time.Now()
	completed := make(chan loadClientResult, cfg.concurrency)
	for id, conn := range connections {
		go func() { completed <- runLoadClient(ctx, conn, id, chunks, interval, epoch, m) }()
	}
	type sample struct {
		AtMS       int64  `json:"at_ms"`
		Sessions   int    `json:"sessions"`
		QueueBytes int    `json:"queue_bytes"`
		Heap       uint64 `json:"heap_alloc"`
		Goroutines int    `json:"goroutines"`
	}
	series := make([]sample, 0, int(cfg.duration/(200*time.Millisecond))+32)
	peakHeap, peakGoroutines := idle.HeapAlloc, idleGoroutines
	sampleNow := func() {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		m.mu.Lock()
		queued := m.queuedBytes
		m.mu.Unlock()
		n := runtime.NumGoroutine()
		peakHeap, peakGoroutines = max(peakHeap, mem.HeapAlloc), max(peakGoroutines, n)
		series = append(series, sample{time.Since(epoch).Milliseconds(), len(h.gateway.registry.snapshot()), queued, mem.HeapAlloc, n})
	}
	sampleNow()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	results := make([]loadClientResult, 0, cfg.concurrency)
	for len(results) < cfg.concurrency {
		select {
		case result := <-completed:
			results = append(results, result)
		case <-ticker.C:
			sampleNow()
		case <-ctx.Done():
			t.Fatalf("load did not terminate: %v", ctx.Err())
		}
	}
	h.waitHandlers(t, cfg.concurrency)
	h.gateway.StopAccepting()
	if err := h.gateway.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	for m.workerActive.Load() != 0 {
		select {
		case <-m.workerFinished:
		case <-ctx.Done():
			t.Fatal("Worker handlers did not finish")
		}
	}
	sampleNow()
	elapsed := time.Since(epoch)
	closeCodes := make(map[string]int)
	sent, received, succeeded := 0, 0, 0
	var failures []string
	for _, result := range results {
		closeCodes[strconv.Itoa(int(result.code))]++
		sent += result.sent
		received += result.received
		if result.code == websocket.StatusNormalClosure && result.final && result.received == chunks && result.sent == chunks {
			succeeded++
		}
		if result.err != "" {
			failures = append(failures, result.err)
		}
	}
	m.mu.Lock()
	if len(m.queues) != 0 || m.queuedBytes != 0 || m.queuedChunks != 0 {
		t.Error("queue observer still retains session data after cleanup")
	}
	metrics := map[string]any{
		"queue_wait": m.wait.summary(), "grpc_send_wait": m.send.summary(), "chunk_result_latency": m.latency.summary(), "client_schedule_lateness": m.lateness.summary(),
		"queue_peak_bytes_all": m.peakBytes, "queue_peak_chunks_all": m.peakChunks,
		"queue_peak_bytes_session": m.maxSessionBytes, "queue_peak_chunks_session": m.maxSessionChunks,
		"audio_enqueued": m.enqueued, "audio_dequeued": m.dequeued, "queued_chunks_abandoned_on_failure": m.abandoned,
		"observed_queues_remaining": len(m.queues), "queue_bytes_remaining": m.queuedBytes,
	}
	m.mu.Unlock()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	result := map[string]any{
		"experiment": "EXP-003", "case": cfg.name, "concurrency": cfg.concurrency, "input_ms": cfg.duration.Milliseconds(),
		"chunk_bytes": 640, "chunk_interval_ms": 20, "planned_chunks_per_session": chunks, "worker_processing_ms": cfg.processing.Milliseconds(), "jitter": cfg.jitter,
		"elapsed_ms": elapsed.Milliseconds(), "normal_complete": succeeded, "close_codes": closeCodes, "client_errors": failures,
		"chunks_sent": sent, "results_received": received, "worker_received": m.workerReceived.Load(), "worker_peak_streams": m.workerPeak.Load(),
		"registered_after_cleanup": len(h.gateway.registry.snapshot()),
		"heap_idle":                idle.HeapAlloc, "heap_peak_sampled": peakHeap, "heap_after_sessions_gc": after.HeapAlloc,
		"goroutines_idle": idleGoroutines, "goroutines_peak_sampled": peakGoroutines, "goroutines_after_sessions": runtime.NumGoroutine(),
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "num_cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0),
		"metrics": metrics, "series": series,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EXPERIMENT_RESULT %s", encoded)
	if len(h.gateway.registry.snapshot()) != 0 {
		t.Error("sessions remain registered")
	}
	// 故障状态作为测量结果保留；仅驱动错误、错误关联或清理失败使实验失败。
	if len(failures) > 0 {
		t.Errorf("client driver errors: %v", failures)
	}
}

func runLoadClient(ctx context.Context, conn *websocket.Conn, id, chunks int, interval time.Duration, epoch time.Time, m *loadMetrics) loadClientResult {
	sendCtx, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	type sendResult struct {
		sent int
		err  error
	}
	sender := make(chan sendResult, 1)
	go func() {
		result := sendResult{}
		defer func() { sender <- result }()
		if result.err = conn.Write(sendCtx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); result.err != nil {
			return
		}
		data := make([]byte, 640)
		binary.LittleEndian.PutUint64(data[16:], uint64(id))
		for i := 0; i < chunks; i++ {
			due := epoch.Add(time.Duration(i) * interval)
			if err := loadWait(sendCtx, time.Until(due)); err != nil {
				result.err = err
				return
			}
			created := time.Now()
			m.mu.Lock()
			m.lateness.add(created.Sub(due))
			m.mu.Unlock()
			binary.LittleEndian.PutUint64(data, uint64(i))
			binary.LittleEndian.PutUint64(data[8:], uint64(created.Sub(epoch)))
			if result.err = conn.Write(sendCtx, websocket.MessageBinary, data); result.err != nil {
				return
			}
			result.sent++
		}
		result.err = conn.Write(sendCtx, websocket.MessageText, []byte(`{"type":"end"}`))
	}()
	result := loadClientResult{}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			result.code = websocket.CloseStatus(err)
			break
		}
		var response wsprotocol.ResultMessage
		if err = json.Unmarshal(data, &response); err != nil {
			result.err = err.Error()
			break
		}
		if response.IsFinal {
			result.final = true
			continue
		}
		seq, err := strconv.Atoi(response.SegmentID)
		if err != nil || seq != result.received {
			result.err = "result order or session correlation violated"
			break
		}
		clientID, timestamp, ok := strings.Cut(response.Text, ":")
		if !ok || clientID != strconv.Itoa(id) {
			result.err = "result belongs to another client"
			break
		}
		stamp, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			result.err = "invalid echoed timestamp"
			break
		}
		m.mu.Lock()
		m.latency.add(time.Since(epoch) - time.Duration(stamp))
		m.mu.Unlock()
		result.received++
	}
	cancelSend()
	if result.err != "" {
		_ = conn.CloseNow()
	}
	sent := <-sender
	result.sent = sent.sent
	if result.code != websocket.StatusNormalClosure && result.code != websocket.StatusTryAgainLater {
		result.err = fmt.Sprintf("unexpected close %d: send=%v", result.code, sent.err)
	}
	return result
}

// loadWorker 为每个 RPC 独立模拟耗时，不模拟共享模型的 CPU/GPU 容量。
type loadWorker struct {
	asrv1.UnimplementedASRServiceServer
	cfg     loadCase
	metrics *loadMetrics
}

func (w *loadWorker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	n := w.metrics.workerActive.Add(1)
	defer func() {
		w.metrics.workerActive.Add(-1)
		select {
		case w.metrics.workerFinished <- struct{}{}:
		default:
		}
	}()
	for old := w.metrics.workerPeak.Load(); n > old; old = w.metrics.workerPeak.Load() {
		if w.metrics.workerPeak.CompareAndSwap(old, n) {
			break
		}
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.Send(&asrv1.StreamingRecognizeResponse{Text: "final", IsFinal: true})
		}
		if err != nil {
			return err
		}
		if len(req.Data) != 640 {
			return fmt.Errorf("unexpected audio length %d", len(req.Data))
		}
		w.metrics.workerReceived.Add(1)
		seq := binary.LittleEndian.Uint64(req.Data)
		delay := w.cfg.processing
		if w.cfg.jitter && seq%100 == 50 {
			delay = 500 * time.Millisecond
		}
		if err = loadWait(stream.Context(), delay); err != nil {
			return err
		}
		text := fmt.Sprintf("%d:%d", binary.LittleEndian.Uint64(req.Data[16:]), binary.LittleEndian.Uint64(req.Data[8:]))
		if err = stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: strconv.FormatUint(seq, 10), Text: text}); err != nil {
			return err
		}
	}
}

func loadWait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 固定流/连接接收窗口，避免动态调窗掩盖不同轮次的输入条件。
func startLoadWorker(t *testing.T, cfg loadCase, m *loadMetrics) asrv1.ASRServiceClient {
	t.Helper()
	listener := bufconn.Listen(64 * 1024)
	server := grpc.NewServer(grpc.InitialWindowSize(64*1024), grpc.InitialConnWindowSize(1024*1024))
	asrv1.RegisterASRServiceServer(server, &loadWorker{cfg: cfg, metrics: m})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///load-worker", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(64*1024), grpc.WithInitialConnWindowSize(1024*1024),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &loadWorkerClient{ASRServiceClient: asrv1.NewASRServiceClient(conn), metrics: m}
}

type loadWorkerClient struct {
	asrv1.ASRServiceClient
	metrics *loadMetrics
}

func (c *loadWorkerClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	stream, err := c.ASRServiceClient.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &loadWorkerStream{BidiStreamingClient: stream, metrics: c.metrics}, nil
}

type loadWorkerStream struct {
	grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	metrics *loadMetrics
}

func (s *loadWorkerStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	started := time.Now()
	err := s.BidiStreamingClient.Send(req)
	s.metrics.mu.Lock()
	s.metrics.send.add(time.Since(started))
	s.metrics.mu.Unlock()
	return err
}
