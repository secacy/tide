package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// groupResumeConfig 描述一次共享入口观察；probe 缩短输入和保护预算，正式条件另行固定。
// BarrierChunk 在正常组也使用；Fault 决定屏障之后放行还是统一断线。
type groupResumeConfig struct {
	Case           string                  `json:"case"`
	Clients        int                     `json:"clients"`
	MaxHandshakes  int                     `json:"max_handshakes"`
	Chunks         int                     `json:"chunks"`
	ChunkBytes     int                     `json:"chunk_bytes"`
	BarrierChunk   int                     `json:"barrier_chunk"`
	Fault          bool                    `json:"fault"`
	Busy           time.Duration           `json:"busy_ns"`
	Processing     time.Duration           `json:"processing_ns"`
	ServerWindow   time.Duration           `json:"server_window_ns"`
	RunTimeout     time.Duration           `json:"run_timeout_ns"`
	CleanupTimeout time.Duration           `json:"cleanup_timeout_ns"`
	SamplePeriod   time.Duration           `json:"sample_period_ns"`
	SyncLimit      time.Duration           `json:"sync_limit_ns"`
	Resume         wsclient.V2ResumeConfig `json:"resume"`
}

// validate 在创建服务前限制输入规模、事件数量和观察期限，拒绝隐式无限预算。
func (c groupResumeConfig) validate() error {
	if c.Clients < 1 || c.Clients > 32 || c.MaxHandshakes < 1 || c.MaxHandshakes > 32 ||
		c.Chunks < 2 || c.Chunks > 256 || c.ChunkBytes < 8 || c.ChunkBytes > 3200 || c.ChunkBytes%2 != 0 ||
		c.BarrierChunk < 2 || c.BarrierChunk > c.Chunks || c.Busy < 0 || (c.Busy > 0 && !c.Fault) ||
		c.Processing < 0 || c.ServerWindow <= 0 || c.RunTimeout <= 0 || c.RunTimeout > time.Minute ||
		c.CleanupTimeout <= 0 || c.CleanupTimeout > time.Minute || c.SamplePeriod < time.Millisecond || c.SyncLimit <= 0 ||
		c.Resume.MaxAttempts < 1 || c.Resume.MaxAttempts > 16 || c.Resume.Window <= 0 ||
		c.Resume.InitialBackoff <= 0 || c.Resume.MaxBackoff < c.Resume.InitialBackoff {
		return errors.New("invalid group resume experiment configuration")
	}
	return nil
}

// groupResumeAttempt 不存身份或握手原文；ID 是输入实验编号，Connection 是该编号的代理请求序号。
// ArrivedAt 是前端 HTTP 到达，不等同于实际 Dial 启动或 Gateway 接纳。
type groupResumeAttempt struct {
	ID          int                    `json:"id"`
	Connection  int                    `json:"connection"`
	ArrivedAt   time.Time              `json:"arrived_at"`
	Kind        string                 `json:"kind"`
	BackendAt   *time.Time             `json:"backend_at"`
	ExitedAt    *time.Time             `json:"exited_at"`
	HTTPStatus  int                    `json:"http_status"`
	Code        wsprotocol.V2ErrorCode `json:"code"`
	CodeWritten bool                   `json:"code_written"`
}

type groupResumeSession struct {
	ID            int                    `json:"id"`
	Started       bool                   `json:"started"`
	LastEntryCode wsprotocol.V2ErrorCode `json:"last_entry_code"` // 从 Run 错误中提取，不等同于代理 Write 成功。
	Observation   resumeExperimentReport `json:"observation"`
}

// MarshalJSON 复用已有记录器，但不输出旧单会话实验的配置、派生指标和局部资源字段。
// groupResumeReport.Config/GatewayConfig 与 observation.client_config 是本轮实际条件。
func (s groupResumeSession) MarshalJSON() ([]byte, error) {
	r := s.Observation
	observation := struct {
		InputSHA256   string                  `json:"input_sha256"`
		StartedAt     time.Time               `json:"started_at"`
		ReturnedAt    *time.Time              `json:"returned_at"`
		ClientConfig  resumeClientSettings    `json:"client_config"`
		RunResult     wsclient.V2RunResult    `json:"run_result"`
		RunErrorClass string                  `json:"run_error_class"`
		OuterDeadline bool                    `json:"outer_deadline"`
		SourceReads   []resumeSourceRead      `json:"source_reads"`
		SourceCloses  int                     `json:"source_closes"`
		Frames        []resumeFrame           `json:"frames"`
		Readies       []resumeReady           `json:"readies"`
		Worker        resumeWorkerObservation `json:"worker"`
		Applications  []resumeApplication     `json:"applications"`
		ToolErrors    []string                `json:"tool_errors"`
	}{r.InputSHA256, r.StartedAt, r.ReturnedAt, r.ClientConfig, r.RunResult, r.RunErrorClass, r.OuterDeadline, r.SourceReads, r.SourceCloses, r.Frames, r.Readies, r.Worker, r.Applications, r.ToolErrors}
	return json.Marshal(struct {
		ID            int                    `json:"id"`
		Started       bool                   `json:"started"`
		LastEntryCode wsprotocol.V2ErrorCode `json:"last_entry_code"`
		Observation   any                    `json:"observation"`
	}{s.ID, s.Started, s.LastEntryCode, observation})
}

// groupResumeEnvironment 保存实际执行条件，Race 来自构建约束，不能靠调用方声明覆盖。
type groupResumeEnvironment struct {
	GoVersion  string `json:"go_version"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	CPUs       int    `json:"cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	Race       bool   `json:"race"`
	Transport  string `json:"transport"`
	Topology   string `json:"topology"`
}

// groupResumeSample 各计数与运行时值依次读取，均落在 begin_at/at 之间。
// Heap 是整个 Go 进程的观测值，不解释为单场内存或严格上界。
type groupResumeSample struct {
	resumeResourceSample
	Goroutines int    `json:"goroutines"`
	HeapAlloc  uint64 `json:"heap_alloc_bytes"`
	HeapInuse  uint64 `json:"heap_inuse_bytes"`
}

// groupResumeReport 在业务失败时仍保留全体计划会话；资源快照沿用单会话观察的非原子口径。
// version=2 固定群体实验输出；缺失时间为 null，未采到的时延不冒充零。
type groupResumeReport struct {
	Version         int                    `json:"version"`
	Config          groupResumeConfig      `json:"config"`
	Environment     groupResumeEnvironment `json:"environment"`
	GatewayConfig   Config                 `json:"gateway_config"`
	StartedAt       time.Time              `json:"started_at"`
	FaultAt         *time.Time             `json:"fault_at"`
	OccupiedAt      *time.Time             `json:"occupied_at"`
	ReleasedAt      *time.Time             `json:"released_at"`
	ReturnedAt      *time.Time             `json:"returned_at"`
	NaturalIdleAt   *time.Time             `json:"natural_idle_at"`
	BarrierAt       []*time.Time           `json:"barrier_at"`
	CloseStartedAt  []*time.Time           `json:"close_started_at"`
	CloseFinishedAt []*time.Time           `json:"close_finished_at"`
	Attempts        []groupResumeAttempt   `json:"attempts"`
	Samples         []groupResumeSample    `json:"samples"`
	Sessions        []groupResumeSession   `json:"sessions"`
	Picks           int                    `json:"picks"`
	WorkerCalls     int                    `json:"worker_calls"`
	ForcedCleanup   bool                   `json:"forced_cleanup"`
	ToolErrors      []string               `json:"tool_errors"`
	EvidenceErrors  []string               `json:"evidence_errors"`
}

// groupPattern 的首八字节编码实验编号，其余内容按编号变化；不同输入不能互换。
func groupPattern(id, size int) []byte {
	data := resumePattern(size)
	for i := range data {
		data[i] ^= byte(id * 37)
	}
	binary.LittleEndian.PutUint64(data, uint64(id))
	return data
}

// groupWorker 由首块音频识别 RPC 归属；不依赖并发建流顺序。
// 各流复用位置 Worker，recorders 是初始化后不变的独立记录器列表。
type groupWorker struct {
	asrv1.UnimplementedASRServiceServer
	recorders     []*resumeRecorder
	cfg           groupResumeConfig
	active, calls atomic.Int32
	toolError     func(string)
}

// StreamingRecognize 首块查验编号后把整条原 RPC 交给对应记录器，不重建识别流。
func (w *groupWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) error {
	w.calls.Add(1)
	w.active.Add(1)
	defer w.active.Add(-1)
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(first.Data) < 8 {
		w.toolError("Worker missing input identity")
		return status.Error(codes.InvalidArgument, "invalid experiment input")
	}
	id := binary.LittleEndian.Uint64(first.Data)
	if id < 1 || id > uint64(len(w.recorders)) {
		w.toolError("Worker unknown input identity")
		return status.Error(codes.InvalidArgument, "invalid experiment identity")
	}
	wrapped := &groupWorkerStream{BidiStreamingServer: stream, first: first, id: int(id), expected: groupPattern(int(id), w.cfg.Chunks*w.cfg.ChunkBytes), cfg: w.cfg, toolError: w.toolError}
	worker := &resumePositionWorker{recorder: w.recorders[id-1], delay: w.cfg.Processing}
	return worker.StreamingRecognize(wrapped)
}

// groupWorkerStream 给位置结果加实验编号，并在记录前限制块数/字节及验证整场输入。
type groupWorkerStream struct {
	grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]
	first              *asrv1.StreamingRecognizeRequest
	id, offset, chunks int
	expected           []byte
	cfg                groupResumeConfig
	toolError          func(string)
}

// Recv 首次返回已读取的首块，随后读取真实流；记录前校验范围和内容。
func (s *groupWorkerStream) Recv() (*asrv1.StreamingRecognizeRequest, error) {
	req, err := s.first, error(nil)
	if req != nil {
		s.first = nil
	} else {
		req, err = s.BidiStreamingServer.Recv()
	}
	if err != nil {
		return req, err
	}
	end := s.offset + len(req.Data)
	if len(req.Data) != s.cfg.ChunkBytes || s.chunks >= s.cfg.Chunks || end > len(s.expected) || !bytes.Equal(req.Data, s.expected[s.offset:end]) {
		s.toolError("Worker input mismatch or recording limit exceeded")
		return nil, status.Error(codes.InvalidArgument, "invalid experiment audio")
	}
	s.offset, s.chunks = end, s.chunks+1
	return req, nil
}

// Send 保留进度消息，给文本加编号使客户端能发现结果串场。
func (s *groupWorkerStream) Send(msg *asrv1.StreamingRecognizeResponse) error {
	if msg.Progress != nil {
		return s.BidiStreamingServer.Send(msg)
	}
	copy := &asrv1.StreamingRecognizeResponse{SegmentId: msg.SegmentId, Text: fmt.Sprintf("%d:%s", s.id, msg.Text), IsFinal: msg.IsFinal}
	return s.BidiStreamingServer.Send(copy)
}

// groupResumeHarness 拥有共享服务和每场初始 socket 对，初始化后以指针使用。
// mu 只保护报告与连接登记，不跨网络 I/O 或等待；生命期独立于客户端业务 context。
type groupResumeHarness struct {
	mu                         sync.Mutex
	sampleMu                   sync.Mutex // 串行化定期与屏障采样，保持报告时间顺序；不参与业务 I/O。
	report                     groupResumeReport
	recorders                  []*resumeRecorder
	cfg                        groupResumeConfig
	life                       context.Context
	stop                       context.CancelFunc
	gateway                    *Gateway
	worker                     *groupWorker
	selector                   *v2EntrySelector
	backendURL, proxyURL       string
	server, proxyServer        *httptest.Server
	initial                    [][2]*websocket.Conn
	connections                []int
	initialReady               []chan struct{}
	barrier                    chan int
	barrierRelease             chan struct{}
	holderDone                 []chan struct{}
	proxyActive, gatewayActive atomic.Int32
	maxSamples                 int
}

// toolError 保存固定提示并限制数量；不接收包含凭据的原始网络错误。
func (h *groupResumeHarness) toolError(message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.report.ToolErrors) < 32 {
		h.report.ToolErrors = append(h.report.ToolErrors, message)
	}
}

// newGroupResumeHarness 创建一次独立批次，共享同一 gate、tracker、registry 和 TCP ClientConn。
func newGroupResumeHarness(t *testing.T, cfg groupResumeConfig) *groupResumeHarness {
	t.Helper()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	life, stop := context.WithCancel(context.Background())
	h := &groupResumeHarness{cfg: cfg, life: life, stop: stop, barrier: make(chan int, cfg.Clients), barrierRelease: make(chan struct{}),
		initial: make([][2]*websocket.Conn, cfg.Clients), connections: make([]int, cfg.Clients), initialReady: make([]chan struct{}, cfg.Clients), holderDone: make([]chan struct{}, cfg.MaxHandshakes),
		maxSamples: int((cfg.RunTimeout+cfg.CleanupTimeout+5*time.Second)/cfg.SamplePeriod) + 16}
	h.report = groupResumeReport{Version: 2, Config: cfg, Environment: groupResumeEnvironment{runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0), groupResumeRaceEnabled, "loopback_tcp", "same_process"}, StartedAt: time.Now(), BarrierAt: make([]*time.Time, cfg.Clients), CloseStartedAt: make([]*time.Time, cfg.Clients), CloseFinishedAt: make([]*time.Time, cfg.Clients)}
	for i := 0; i < cfg.Clients; i++ {
		h.initialReady[i] = make(chan struct{})
		c := resumeExperimentConfig{Case: cfg.Case, Chunks: cfg.Chunks, ChunkBytes: cfg.ChunkBytes, Resume: true}
		h.recorders = append(h.recorders, &resumeRecorder{report: resumeExperimentReport{Version: 1, Config: c, InputSHA256: resumeDigest(groupPattern(i+1, cfg.Chunks*cfg.ChunkBytes))}})
	}
	for i := range h.holderDone {
		h.holderDone[i] = make(chan struct{})
	}
	h.worker = &groupWorker{recorders: h.recorders, cfg: cfg, toolError: h.toolError}
	h.selector = &v2EntrySelector{client: newBaselineTCPWorkerClient(t, h.worker)}
	gcfg := resumeGatewayConfig(resumeExperimentConfig{ServerWindow: cfg.ServerWindow})
	gcfg.MaxSessions, gcfg.V2.MaxHandshakes = cfg.Clients, cfg.MaxHandshakes
	var err error
	h.gateway, err = New(life, h.selector, gcfg)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	h.report.GatewayConfig = h.gateway.cfg
	// New 将 v2 条件独立保存；报告补回实际归一化的副本，不能把 nil 解释为未启用。
	v2Config := *h.gateway.v2
	h.report.GatewayConfig.V2 = &v2Config
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.gatewayActive.Add(1)
		defer h.gatewayActive.Add(-1)
		if holder := r.URL.Query().Get("holder"); holder != "" {
			index, err := strconv.Atoi(holder)
			if err != nil || index < 0 || index >= len(h.holderDone) {
				http.Error(w, "invalid holder", http.StatusBadRequest)
				return
			}
			defer close(h.holderDone[index]) // ServeV2HTTP's deferred gate release runs before this notification.
		}
		h.gateway.ServeV2HTTP(w, r)
	}))
	h.backendURL = "ws" + strings.TrimPrefix(h.server.URL, "http") + "/v2/asr"
	h.proxyServer = httptest.NewServer(http.HandlerFunc(h.serve))
	h.proxyURL = "ws" + strings.TrimPrefix(h.proxyServer.URL, "http")
	t.Cleanup(func() {
		h.gateway.StopAccepting()
		h.stop()
		h.proxyServer.Close()
		h.server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.gateway.Wait(ctx); err != nil {
			t.Errorf("group fixture cleanup: %v", err)
		}
	})
	return h
}

// serve 记录一次有界尝试并转发原握手，不保存其凭据。
// 后端 Dial 期间持续读取前端，客户端断开可取消尚未完成的接入。
func (h *groupResumeHarness) serve(w http.ResponseWriter, req *http.Request) {
	id, err := strconv.Atoi(req.URL.Query().Get("id"))
	if err != nil || id < 1 || id > h.cfg.Clients {
		http.Error(w, "invalid experiment identity", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.connections[id-1]++
	number := h.connections[id-1]
	if number > h.cfg.Resume.MaxAttempts+1 {
		h.mu.Unlock()
		h.toolError("proxy attempt recording limit exceeded")
		http.Error(w, "attempt limit", http.StatusServiceUnavailable)
		return
	}
	index := len(h.report.Attempts)
	h.report.Attempts = append(h.report.Attempts, groupResumeAttempt{ID: id, Connection: number, ArrivedAt: time.Now()})
	h.mu.Unlock()
	h.proxyActive.Add(1)
	defer h.proxyActive.Add(-1)
	defer func() { h.mu.Lock(); h.report.Attempts[index].ExitedAt = resumeAt(time.Now()); h.mu.Unlock() }()
	front, err := websocket.Accept(w, req, nil)
	if err != nil {
		return
	}
	defer front.CloseNow()
	front.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(h.life)
	defer cancel()
	typ, data, err := front.Read(ctx)
	if err != nil {
		return
	}
	hs, err := wsprotocol.DecodeV2Handshake(data)
	if err != nil || typ != websocket.MessageText {
		h.toolError("proxy invalid handshake")
		return
	}
	kind := "start"
	if hs.Kind == wsprotocol.V2HandshakeResume {
		kind = "resume"
	}
	h.mu.Lock()
	h.report.Attempts[index].Kind = kind
	h.mu.Unlock()
	input, consumed, readerDone := make(chan resumeProxyInput, 1), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			typ, data, err := front.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			select {
			case input <- resumeProxyInput{typ, data}:
			case <-ctx.Done():
				return
			}
			select {
			case <-consumed:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); _ = front.CloseNow(); <-readerDone }()
	h.mu.Lock()
	h.report.Attempts[index].BackendAt = resumeAt(time.Now())
	h.mu.Unlock()
	back, response, err := websocket.Dial(ctx, h.backendURL, nil)
	if response != nil {
		h.mu.Lock()
		h.report.Attempts[index].HTTPStatus = response.StatusCode // 成功升级与拒绝均保留实际 HTTP 状态。
		h.mu.Unlock()
	}
	if err != nil {
		if response != nil {
			defer response.Body.Close()
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
			var reject wsprotocol.V2ErrorMessage
			if readErr == nil && len(body) <= 4096 && json.Unmarshal(body, &reject) == nil && reject.Type == wsprotocol.MessageTypeError && reject.Code != "" {
				writeErr := front.Write(ctx, websocket.MessageText, body)
				h.mu.Lock()
				row := &h.report.Attempts[index]
				row.HTTPStatus, row.Code, row.CodeWritten = response.StatusCode, reject.Code, writeErr == nil
				h.mu.Unlock()
			} else {
				h.toolError("backend HTTP rejection missing valid error code")
			}
		}
		return
	}
	defer back.CloseNow()
	back.SetReadLimit(65536)
	if err := back.Write(ctx, typ, data); err != nil {
		return
	}
	if number == 1 {
		h.mu.Lock()
		h.initial[id-1] = [2]*websocket.Conn{front, back}
		h.mu.Unlock()
	}
	recorder := h.recorders[id-1]
	group, streamCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		chunks := 0
		for {
			var event resumeProxyInput
			select {
			case event = <-input:
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
			rowIndex := -1
			if event.kind == websocket.MessageBinary {
				a, err := wsprotocol.DecodeV2Audio(event.data)
				if err != nil {
					h.toolError("proxy invalid audio frame")
					return err
				}
				chunks++
				if chunks > h.cfg.Chunks || a.Offset > uint64(h.cfg.Chunks*h.cfg.ChunkBytes) || len(a.Payload) != h.cfg.ChunkBytes || a.Offset+uint64(len(a.Payload)) > uint64(h.cfg.Chunks*h.cfg.ChunkBytes) {
					h.toolError("proxy audio bounds exceeded")
					return errors.New("proxy audio bounds exceeded")
				}
				row := resumeFrame{resumeRange: resumeRange{a.Offset, a.Offset + uint64(len(a.Payload))}, Connection: number, ReadAt: time.Now(), SHA256: resumeDigest(a.Payload), Dropped: h.cfg.Fault && number == 1 && chunks == h.cfg.BarrierChunk}
				recorder.change(func(r *resumeExperimentReport) { rowIndex = len(r.Frames); r.Frames = append(r.Frames, row) })
				if number == 1 && chunks == h.cfg.BarrierChunk {
					h.mu.Lock()
					h.report.BarrierAt[id-1] = resumeAt(time.Now())
					h.mu.Unlock()
					select {
					case h.barrier <- id:
					case <-streamCtx.Done():
						return streamCtx.Err()
					}
					select {
					case <-h.barrierRelease:
					case <-streamCtx.Done():
						return streamCtx.Err()
					}
					if h.cfg.Fault {
						return errClientTestCut
					}
				}
				recorder.change(func(r *resumeExperimentReport) { r.Frames[rowIndex].ForwardStartedAt = resumeAt(time.Now()) })
			}
			err := back.Write(streamCtx, event.kind, event.data)
			if rowIndex >= 0 {
				recorder.change(func(r *resumeExperimentReport) {
					r.Frames[rowIndex].ForwardReturnedAt = resumeAt(time.Now())
					r.Frames[rowIndex].Forwarded = err == nil
				})
			}
			if err != nil {
				return err
			}
			select {
			case consumed <- struct{}{}:
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
		}
	})
	group.Go(func() error {
		for {
			typ, data, err := back.Read(streamCtx)
			if err != nil {
				return err
			}
			var tag struct {
				Type wsprotocol.MessageType `json:"type"`
			}
			if err := json.Unmarshal(data, &tag); err != nil {
				h.toolError("proxy invalid server JSON")
				return err
			}
			if err := front.Write(streamCtx, typ, data); err != nil {
				return err
			}
			if tag.Type == wsprotocol.MessageTypeReady {
				var ready wsprotocol.ReadyMessage
				if err := json.Unmarshal(data, &ready); err != nil {
					return err
				}
				recorder.change(func(r *resumeExperimentReport) {
					if len(r.Readies) < h.cfg.Resume.MaxAttempts+1 {
						r.Readies = append(r.Readies, resumeReady{number, ready.Generation, ready.NextOffset, ready.AckedResultSeq, time.Now()})
					}
				})
				if number == 1 {
					select {
					case <-h.initialReady[id-1]:
					default:
						close(h.initialReady[id-1])
					}
				}
			}
			if tag.Type == wsprotocol.MessageTypeError {
				var reject wsprotocol.V2ErrorMessage
				if err := json.Unmarshal(data, &reject); err != nil {
					return err
				}
				h.mu.Lock()
				h.report.Attempts[index].Code, h.report.Attempts[index].CodeWritten = reject.Code, true
				h.mu.Unlock()
			}
		}
	})
	_ = group.Wait()
}

// sample 依次读取组件计数，不宣称跨组件原子性；达到容量上限标记工具失败。
func (h *groupResumeHarness) sample() resumeResourceSample {
	h.sampleMu.Lock()
	defer h.sampleMu.Unlock()
	s := resumeResourceSample{BeginAt: time.Now(), Sessions: h.gateway.Snapshot().ActiveSessions, Handshakes: entryHandshakeCount(h.gateway.gate), WorkerActive: int(h.worker.active.Load()), GatewayHandlers: int(h.gatewayActive.Load()), ProxyHandlers: int(h.proxyActive.Load())}
	h.gateway.registry.mu.Lock()
	s.Registry = len(h.gateway.registry.entries)
	h.gateway.registry.mu.Unlock()
	s.At = time.Now()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	row := groupResumeSample{resumeResourceSample: s, Goroutines: runtime.NumGoroutine(), HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse}
	row.At = time.Now()
	h.mu.Lock()
	if len(h.report.Samples) < h.maxSamples {
		h.report.Samples = append(h.report.Samples, row)
	} else if len(h.report.ToolErrors) < 32 {
		h.report.ToolErrors = append(h.report.ToolErrors, "sample recording limit exceeded")
	}
	h.mu.Unlock()
	return row.resumeResourceSample
}

// clientConfig 在已有位置结果回调前校验编号，单场应用记录有固定上限。
func (h *groupResumeHarness) clientConfig(id int) wsclient.V2Config {
	recorder := h.recorders[id-1]
	c := resumeExperimentConfig{Chunks: h.cfg.Chunks, ChunkBytes: h.cfg.ChunkBytes, Resume: true, ClientWindow: h.cfg.Resume.Window}
	cfg := resumeClientConfig(c, fmt.Sprintf("%s?id=%d", h.proxyURL, id), recorder)
	resume := h.cfg.Resume
	cfg.Resume = &resume
	apply := cfg.OnResult
	cfg.OnResult = func(ctx context.Context, msg wsprotocol.SequencedResultMessage, at time.Time) error {
		prefix := strconv.Itoa(id) + ":"
		if !strings.HasPrefix(msg.Text, prefix) || msg.Seq > uint64(h.cfg.Chunks+1) {
			h.toolError("result identity mismatch or recording bounds exceeded")
			return errors.New("invalid group result")
		}
		recorder.mu.Lock()
		count := len(recorder.report.Applications)
		recorder.mu.Unlock()
		if count >= h.cfg.Chunks+1 {
			h.toolError("application recording bounds exceeded")
			return errors.New("application recording limit")
		}
		msg.Text = strings.TrimPrefix(msg.Text, prefix)
		return apply(ctx, msg, at)
	}
	return cfg
}

// inject 等待全体音频位置屏障，占用真实握手名额，再统一切断原连接。
// 计时器只由此任务拥有；ReleasedAt 必须在占位 handler 实际归还名额之后记录。
func (h *groupResumeHarness) inject(ctx context.Context) {
	seen := make(map[int]bool)
	for len(seen) < h.cfg.Clients {
		select {
		case id := <-h.barrier:
			seen[id] = true
		case <-ctx.Done():
			h.toolError("group barrier incomplete")
			return
		}
	}
	before := h.sample()
	if before.Sessions != h.cfg.Clients || before.Registry != h.cfg.Clients || before.WorkerActive != h.cfg.Clients {
		h.toolError("group barrier reached without all original sessions active")
		return
	}
	var holders []*websocket.Conn
	defer func() {
		for _, conn := range holders {
			_ = conn.CloseNow()
		}
	}()
	if h.cfg.Busy > 0 {
		for entryHandshakeCount(h.gateway.gate) != 0 {
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				h.toolError("initial gate did not drain")
				return
			}
		}
		for i := 0; i < h.cfg.MaxHandshakes; i++ {
			conn, _, err := websocket.Dial(ctx, fmt.Sprintf("%s?holder=%d", h.backendURL, i), nil)
			if err != nil {
				h.toolError("holder failed to acquire real slot")
				return
			}
			holders = append(holders, conn)
		}
		if entryHandshakeCount(h.gateway.gate) != h.cfg.MaxHandshakes || h.gateway.Snapshot().ActiveSessions != h.cfg.Clients || int(h.worker.calls.Load()) != h.cfg.Clients {
			h.toolError("holder occupancy or original sessions mismatch")
			return
		}
		h.mu.Lock()
		h.report.OccupiedAt = resumeAt(time.Now())
		h.mu.Unlock()
	}
	if !h.cfg.Fault {
		close(h.barrierRelease)
		return
	}
	h.mu.Lock()
	h.report.FaultAt = resumeAt(time.Now())
	pairs := append([][2]*websocket.Conn(nil), h.initial...)
	h.mu.Unlock()
	var closes sync.WaitGroup
	for i, pair := range pairs {
		closes.Add(1)
		go func() {
			defer closes.Done()
			h.mu.Lock()
			h.report.CloseStartedAt[i] = resumeAt(time.Now())
			h.mu.Unlock()
			for _, conn := range pair {
				if conn != nil {
					_ = conn.CloseNow()
				} else {
					h.toolError("initial connection missing at cut")
				}
			}
			h.mu.Lock()
			h.report.CloseFinishedAt[i] = resumeAt(time.Now())
			h.mu.Unlock()
		}()
	}
	closes.Wait()
	close(h.barrierRelease)
	if len(holders) == 0 {
		return
	}
	h.mu.Lock()
	releaseAt := h.report.FaultAt.Add(h.cfg.Busy)
	h.mu.Unlock()
	if err := waitGroupRelease(ctx, func() time.Duration {
		now := time.Now()
		// JSON 不保留单调时钟，独立分析用墙钟；两种口径均不得提前释放。
		return max(releaseAt.Sub(now), releaseAt.Round(0).Sub(now.Round(0)))
	}); err != nil {
		h.toolError("holder release canceled")
		return
	}
	for _, conn := range holders {
		_ = conn.CloseNow()
	}
	for _, done := range h.holderDone {
		select {
		case <-done:
		case <-ctx.Done():
			h.toolError("holder slot release not observed")
			return
		}
	}
	h.mu.Lock()
	h.report.ReleasedAt = resumeAt(time.Now())
	h.mu.Unlock()
}

// waitGroupRelease 在定时器唤醒后重新检查剩余时间，防止墙钟调整造成导出时间提前。
// remaining 仅由本任务调用，返回所有时间约束中最大的剩余值；非正表示可以释放。
// context 仍限制等待，墙钟反复后退不会产生超出业务观察窗口的无限占位。
func waitGroupRelease(ctx context.Context, remaining func() time.Duration) error {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		delay := remaining()
		if delay <= 0 {
			return nil
		}
		timer.Reset(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// groupResumeSource 通知实际首次读取，不阻塞 Read 或移动固定采集时间轴。
// 串行启动按事件交接，避免资源采样间隔人为拖慢每场初始化。
type groupResumeSource struct {
	*resumeSource
	firstRead chan struct{}
	once      sync.Once
}

// Read 沿用有限源记录，读取完成后通知一次；Close 由嵌入源提供。
func (s *groupResumeSource) Read(data []byte) (int, error) {
	n, err := s.resumeSource.Read(data)
	s.once.Do(func() { close(s.firstRead) })
	return n, err
}

// runGroupResumeProbe 等待客户端、注入及远端任务收尾后冻结观察，业务失败仍返回报告。
// 自然观察失败后才停服兜底，强制清理后的零计数不能补写 NaturalIdleAt。
func runGroupResumeProbe(t *testing.T, cfg groupResumeConfig) groupResumeReport {
	t.Helper()
	return runGroupResumeWithSeeds(t, cfg, nil)
}

// runGroupResumeWithSeeds 允许新实验为每场指定固定种子；nil 保留原确定性夹具。
// 种子只在启动前写入独立配置，不修改生产 Client/Run 的随机状态。
func runGroupResumeWithSeeds(t *testing.T, cfg groupResumeConfig, seeds map[int]uint64) groupResumeReport {
	t.Helper()
	if seeds != nil {
		if !cfg.Resume.Jitter || cfg.Resume.JitterSeed != 0 || len(seeds) != cfg.Clients {
			t.Fatal("per-session seed plan requires jitter and all planned slots")
		}
		for id := 1; id <= cfg.Clients; id++ {
			if seeds[id] == 0 {
				t.Fatal("per-session experiment seeds must be nonzero")
			}
		}
	}
	h := newGroupResumeHarness(t, cfg)
	h.report.StartedAt = time.Now() // 第一场业务启动的观察原点，不包含夹具构造。
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancel()
	startupCtx, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStartup()
	var runs sync.WaitGroup
	started := make([]bool, cfg.Clients)
	entryCodes := make([]wsprotocol.V2ErrorCode, cfg.Clients)
	ticker := time.NewTicker(cfg.SamplePeriod)
	defer ticker.Stop()
	for i, recorder := range h.recorders {
		clientCfg := h.clientConfig(i + 1)
		if seeds != nil {
			clientCfg.Resume.JitterSeed = seeds[i+1]
		}
		client, err := wsclient.NewV2(clientCfg)
		if err != nil {
			h.toolError("client configuration failed")
			break
		}
		source := &groupResumeSource{resumeSource: &resumeSource{recorder: recorder, input: groupPattern(i+1, cfg.Chunks*cfg.ChunkBytes)}, firstRead: make(chan struct{})}
		recorder.change(func(r *resumeExperimentReport) {
			r.StartedAt = time.Now()
			r.ClientConfig = resumeClientSettings{clientCfg.ChunkBytes, clientCfg.Realtime, clientCfg.ReadLimit, clientCfg.MaxReplayBytes, clientCfg.MaxReplayChunks, clientCfg.HandshakeTimeout, clientCfg.WriteTimeout, clientCfg.ApplyTimeout, clientCfg.CompletionTimeout, clientCfg.Resume}
		})
		started[i] = true
		runs.Add(1)
		sessionDone := make(chan struct{})
		go func() {
			defer runs.Done()
			defer close(sessionDone)
			result, err := client.Run(ctx, source)
			var entry *wsclient.V2EntryError
			if errors.As(err, &entry) {
				entryCodes[i] = entry.Code
			}
			recorder.change(func(r *resumeExperimentReport) {
				r.RunResult = result
				r.RunErrorClass = resumeErrorClass(err)
				r.ReturnedAt = resumeAt(time.Now())
				r.OuterDeadline = errors.Is(ctx.Err(), context.DeadlineExceeded)
			})
		}()
		read, ready := source.firstRead, h.initialReady[i]
		failed := false
		for (read != nil || ready != nil) && !failed {
			select {
			case <-read:
				read = nil
			case <-ready:
				ready = nil
			case <-ticker.C:
				h.sample()
			case <-sessionDone:
				failed = true
			case <-startupCtx.Done():
				failed = true
			}
		}
		if failed {
			h.toolError("initial client failed before capture")
			break
		}
	}
	injected := make(chan struct{})
	go func() { defer close(injected); h.inject(ctx) }()
	returned := make(chan struct{})
	go func() { defer close(returned); runs.Wait() }()
	for running := true; running; {
		select {
		case <-returned:
			running = false
		case <-ticker.C:
			h.sample()
		}
	}
	h.mu.Lock()
	h.report.ReturnedAt = resumeAt(time.Now())
	h.mu.Unlock()
	cleanup := time.NewTimer(cfg.CleanupTimeout)
	defer cleanup.Stop()
	natural := false
	for !natural {
		s := h.sample()
		injectionDone := false
		select {
		case <-injected:
			injectionDone = true
		default:
		}
		if injectionDone && s.idle() && s.tasksExited() {
			h.mu.Lock()
			h.report.NaturalIdleAt = resumeAt(s.At)
			h.mu.Unlock()
			natural = true
			break
		}
		select {
		case <-ticker.C:
		case <-cleanup.C:
			h.mu.Lock()
			h.report.ForcedCleanup = true
			h.mu.Unlock()
			goto observed
		}
	}
observed:
	if !natural {
		cancel()
		h.gateway.StopAccepting()
		h.stop()
	}
	// The injector shares the business context; closing all clients does not leave a blocked barrier behind.
	cancel()
	<-injected
	if !natural {
		forcedCtx, forcedCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := h.gateway.Wait(forcedCtx); err != nil {
			h.toolError("forced Gateway cleanup did not finish")
		}
		for {
			s := h.sample()
			if s.idle() && s.tasksExited() {
				break
			}
			select {
			case <-time.After(time.Millisecond):
			case <-forcedCtx.Done():
				h.toolError("forced task cleanup did not finish")
				goto frozen
			}
		}
	frozen:
		forcedCancel()
	}
	h.mu.Lock()
	report := h.report
	report.Attempts = slices.Clone(h.report.Attempts)
	report.Samples = slices.Clone(h.report.Samples)
	report.BarrierAt = slices.Clone(h.report.BarrierAt)
	report.CloseStartedAt = slices.Clone(h.report.CloseStartedAt)
	report.CloseFinishedAt = slices.Clone(h.report.CloseFinishedAt)
	report.ToolErrors = slices.Clone(h.report.ToolErrors)
	report.Picks, report.WorkerCalls = int(h.selector.picks.Load()), int(h.worker.calls.Load())
	h.mu.Unlock()
	for i, recorder := range h.recorders {
		report.Sessions = append(report.Sessions, groupResumeSession{ID: i + 1, Started: started[i], LastEntryCode: entryCodes[i], Observation: recorder.snapshot()})
	}
	validateGroupResumeReport(&report)
	return report
}

// validateGroupResumeReport 校验原始事实；场景完成率或重试性能预期由调用方另行判断。
func validateGroupResumeReport(r *groupResumeReport) {
	r.EvidenceErrors = nil
	problem := func(message string) { r.EvidenceErrors = append(r.EvidenceErrors, message) }
	if err := r.Config.validate(); err != nil {
		problem("invalid report configuration")
		return
	}
	if len(r.BarrierAt) != r.Config.Clients || len(r.CloseStartedAt) != r.Config.Clients || len(r.CloseFinishedAt) != r.Config.Clients {
		problem("observation slot count mismatch")
		return
	}
	if len(r.Sessions) != r.Config.Clients {
		problem("planned session denominator mismatch")
	}
	if r.Picks != r.Config.Clients || r.WorkerCalls != r.Config.Clients {
		problem("original RPC assignment mismatch")
	}
	if r.ForcedCleanup || r.NaturalIdleAt == nil {
		problem("natural cleanup not observed")
	}
	var firstReadMin, firstReadMax, closeMin, closeMax time.Time
	seenIDs := make(map[int]bool)
	for _, session := range r.Sessions {
		if session.ID < 1 || session.ID > r.Config.Clients || seenIDs[session.ID] {
			problem("invalid or duplicate session identity")
			continue
		}
		seenIDs[session.ID] = true
		s := session.Observation
		if len(s.ToolErrors) != 0 {
			problem("session observer reported tool errors")
		}
		if !session.Started || s.ReturnedAt == nil || s.SourceCloses != 1 || s.Worker.Calls != 1 || s.Worker.ExitedAt == nil {
			problem(fmt.Sprintf("session %d lifecycle incomplete", session.ID))
		}
		input := groupPattern(session.ID, r.Config.Chunks*r.Config.ChunkBytes)
		if s.InputSHA256 != resumeDigest(input) {
			problem("input digest mismatch")
		}
		var captured uint64
		for _, read := range s.SourceReads {
			if read.End < read.Begin || read.End > uint64(len(input)) || read.Begin != captured || read.FinishedAt.Before(read.StartedAt) {
				problem("invalid source prefix or timestamp")
			}
			captured = read.End
		}
		if len(s.Worker.Audio) > len(input) || !bytes.Equal(s.Worker.Audio, input[:min(len(input), len(s.Worker.Audio))]) {
			problem("Worker input is not exact session prefix")
		}
		if uint64(len(s.Worker.Audio)) > captured || len(s.Worker.Chunks)*r.Config.ChunkBytes != len(s.Worker.Audio) {
			problem("Worker bytes exceed captured input or chunk records")
		}
		dropped := 0
		for _, frame := range s.Frames {
			if frame.End <= frame.Begin || frame.End > captured || frame.End > uint64(len(input)) {
				problem("frame outside captured input")
				continue
			}
			if frame.SHA256 != resumeDigest(input[frame.Begin:frame.End]) {
				problem("frame digest mismatch")
			}
			if frame.Dropped {
				dropped++
				if frame.Connection != 1 || frame.Begin != uint64((r.Config.BarrierChunk-1)*r.Config.ChunkBytes) || frame.Forwarded || frame.ForwardStartedAt != nil {
					problem("invalid dropped fault frame")
				}
			}
			if frame.Forwarded && (frame.ForwardStartedAt == nil || frame.ForwardReturnedAt == nil) {
				problem("frame forwarding timestamps missing")
			}
		}
		if (r.Config.Fault && dropped != 1) || (!r.Config.Fault && dropped != 0) {
			problem("fault frame count mismatch")
		}
		if len(s.SourceReads) == 0 {
			problem("source never read")
		} else {
			at := s.SourceReads[0].StartedAt
			if firstReadMin.IsZero() || at.Before(firstReadMin) {
				firstReadMin = at
			}
			if at.After(firstReadMax) {
				firstReadMax = at
			}
		}
		for i, a := range s.Applications {
			if a.Seq != uint64(i+1) || a.AudioEnd != uint64(min(i+1, r.Config.Chunks)*r.Config.ChunkBytes) || a.Final != (i == r.Config.Chunks) {
				problem("result order or position mismatch")
			}
			if a.AppliedAt.Before(a.ReceivedAt) {
				problem("result application preceded receive")
			}
			if !a.Final && (i >= len(s.Worker.Chunks) || s.Worker.Chunks[i].ProcessedAt == nil || a.ReceivedAt.Before(*s.Worker.Chunks[i].ProcessedAt)) {
				problem("result lacks matching Worker processing")
			}
		}
		if s.RunResult.AppliedSeq != uint64(len(s.Applications)) {
			problem("applied result count mismatch")
		}
		if s.RunResult.Completed && (!bytes.Equal(s.Worker.Audio, input) || len(s.Applications) != r.Config.Chunks+1 || s.RunResult.FinalOffset != uint64(len(input))) {
			problem("completed without complete audio and results")
		}
		starts, resumes := 0, 0
		for _, a := range r.Attempts {
			if a.ID == session.ID {
				if a.Kind == "start" {
					starts++
				}
				if a.Kind == "resume" {
					resumes++
				}
				if a.ExitedAt == nil {
					problem("proxy attempt did not exit")
				}
			}
		}
		if starts != 1 || resumes > s.RunResult.ResumeAttempts {
			problem("start fallback or impossible attempt observation count")
		}
	}
	if firstReadMax.Sub(firstReadMin) > r.Config.SyncLimit {
		problem("capture startup spread exceeded sync limit")
	}
	for i, at := range r.BarrierAt {
		if at == nil {
			problem(fmt.Sprintf("session %d missing barrier", i+1))
		}
	}
	if r.Config.Fault {
		if r.FaultAt == nil {
			problem("fault not injected")
		}
		for i, at := range r.CloseStartedAt {
			end := r.CloseFinishedAt[i]
			if at == nil || end == nil {
				problem("connection cut not observed")
				continue
			}
			if closeMin.IsZero() || at.Before(closeMin) {
				closeMin = *at
			}
			if end.After(closeMax) {
				closeMax = *end
			}
		}
		if closeMax.Sub(closeMin) > r.Config.SyncLimit {
			problem("connection cut spread exceeded sync limit")
		}
	} else if r.FaultAt != nil {
		problem("normal batch was cut")
	}
	if r.Config.Busy > 0 {
		if r.OccupiedAt == nil || r.ReleasedAt == nil {
			problem("holder lifecycle missing")
		}
		if r.ReleasedAt != nil && r.FaultAt != nil && (r.ReleasedAt.Before(r.FaultAt.Add(r.Config.Busy)) ||
			r.ReleasedAt.Round(0).Before(r.FaultAt.Round(0).Add(r.Config.Busy))) {
			problem("holder released before planned interval")
		}
		rejections := 0
		for _, a := range r.Attempts {
			if a.Code == wsprotocol.V2ErrorHandshakeLimit && a.HTTPStatus == http.StatusServiceUnavailable && a.CodeWritten {
				rejections++
			}
		}
		if rejections == 0 {
			problem("real handshake rejection not observed")
		}
	}
	for _, s := range r.Samples {
		if s.Sessions < 0 || s.Handshakes < 0 || s.Registry < 0 || s.WorkerActive < 0 || s.Sessions > r.Config.Clients || s.Handshakes > r.Config.MaxHandshakes || s.Registry > r.Config.Clients || s.WorkerActive > r.Config.Clients {
			problem("observed admission bound exceeded")
		}
	}
}
