package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
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
)

const extendedChunkBytes = 3200

// extendedConfig 固定单场条件；FaultChunks 为零基块号，仅配置六个或零个故障。
// 新夹具独立于旧全量 resumeRecorder，不改变旧实验的结构或条件。
type extendedConfig struct {
	Case           string        `json:"case"`
	Chunks         int           `json:"chunks"`
	FaultChunks    []int         `json:"fault_chunks"`
	Attempts       int           `json:"attempts"`
	Outage         time.Duration `json:"outage_ns"`
	Processing     time.Duration `json:"processing_ns"`
	RunTimeout     time.Duration `json:"run_timeout_ns"`
	CleanupTimeout time.Duration `json:"cleanup_timeout_ns"`
	SamplePeriod   time.Duration `json:"sample_period_ns"`
}

// extendedCases 返回独立的条件副本；正式入口后续由独立采集器显式执行。
func extendedCases(probe bool) []extendedConfig {
	chunks, faults, timeout := 6000, []int{300, 1200, 2100, 3000, 3900, 4800}, 630*time.Second
	if probe {
		chunks, faults, timeout = 120, []int{10, 25, 40, 55, 70, 85}, 42*time.Second
	}
	cases := make([]extendedConfig, 3)
	for i, name := range []string{"A_normal", "B_budget_8", "C_budget_5"} {
		cases[i] = extendedConfig{name, chunks, nil, 8, 500 * time.Millisecond, 10 * time.Millisecond, timeout, 15 * time.Second, 100 * time.Millisecond}
		if i > 0 {
			cases[i].FaultChunks = append([]int(nil), faults...)
		}
		if i == 2 {
			cases[i].Attempts = 5
		}
	}
	return cases
}

// validate 拒绝无序/重复/越界故障及不符合音频格式的条件，运行前调用。
func (c extendedConfig) validate() error {
	if c.Case == "" || c.Chunks < 1 || c.Chunks > 6000 || c.Attempts < 1 || c.Outage <= 0 || c.Processing <= 0 || c.RunTimeout <= 0 || c.CleanupTimeout <= 0 || c.SamplePeriod <= 0 || (len(c.FaultChunks) != 0 && len(c.FaultChunks) != 6) {
		return errors.New("invalid extended configuration")
	}
	last := -1
	for _, n := range c.FaultChunks {
		if n <= last || n < 1 || n >= c.Chunks {
			return errors.New("invalid extended fault position")
		}
		last = n
	}
	return nil
}

// extendedPattern 实现预登记字节算法；内存只占一块，与整场长度无关。
func extendedPattern(index uint64) []byte {
	var suffix [8]byte
	binary.BigEndian.PutUint64(suffix[:], index)
	input := append([]byte("tide-exp-006-05:"), suffix[:]...)
	sum := sha256.Sum256(input)
	return bytes.Repeat(sum[:], 100)
}

func extendedSHA(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

// extendedSource 的唯一读取者是客户端采集任务；mu 同步 Close 和最终计数。
// 不保存整场输入，仅生成下一块并保留增量摘要。
type extendedSource struct {
	mu           sync.Mutex
	log          *extendedLog
	chunks, next int
	closed       bool
	closes       int
	digest       hash.Hash
	firstReadNS  atomic.Int64 // 初始 -1；首个非空读取的单调时间轴原点。
}

func (s *extendedSource) Read(data []byte) (int, error) {
	began := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	if s.next == s.chunks {
		s.log.emit("source", "eof", began, map[string]any{"offset": s.next * extendedChunkBytes})
		return 0, io.EOF
	}
	if len(data) < extendedChunkBytes {
		s.log.fail("short source read buffer")
		return 0, errors.New("short source buffer")
	}
	block := extendedPattern(uint64(s.next))
	copy(data, block)
	first := s.firstReadNS.Load()
	if first < 0 {
		first = began.Sub(s.log.origin).Nanoseconds()
		s.firstReadNS.Store(first)
	}
	begin := s.next * extendedChunkBytes
	s.digest.Write(block)
	s.next++
	s.log.emit("source", "read", began, map[string]any{"begin": begin, "end": begin + extendedChunkBytes, "sha256": extendedSHA(block), "finished_ns": time.Since(s.log.origin).Nanoseconds(), "planned_ns": first + int64(s.next-1)*int64(100*time.Millisecond)})
	return extendedChunkBytes, nil
}

func (s *extendedSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.closes++
	s.log.emit("source", "close", time.Now(), map[string]any{"count": s.closes, "captured": s.next * extendedChunkBytes, "sha256": hex.EncodeToString(s.digest.Sum(nil))})
	return nil
}

// extendedWorker 使用真实 TCP gRPC，在线检查连续块；只保存计数与增量摘要。
type extendedWorker struct {
	asrv1.UnimplementedASRServiceServer
	log                 *extendedLog
	cfg                 extendedConfig
	active, calls       atomic.Int32
	mu                  sync.Mutex
	received, processed uint64
	ended               bool
	digest              hash.Hash
	done                chan struct{} // 唯一 RPC handler 发布退出事件后关闭。
}

func (w *extendedWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (runErr error) {
	number := w.calls.Add(1)
	w.active.Add(1)
	w.log.emit("worker", "start", time.Now(), map[string]any{"rpc": number})
	defer func() {
		w.log.emit("worker", "exit", time.Now(), map[string]any{"rpc": number, "error_class": resumeErrorClass(runErr)})
		w.active.Add(-1)
		if number == 1 {
			close(w.done)
		}
	}()
	if number != 1 {
		w.log.fail("replacement Worker RPC")
		return errors.New("unexpected RPC")
	}
	var index uint64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			w.mu.Lock()
			w.ended = true
			w.mu.Unlock()
			w.log.emit("worker", "end", time.Now(), map[string]any{"offset": index * extendedChunkBytes})
			err = stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "extended-tail", Text: "extended-tail", IsFinal: true})
			w.log.emit("worker", "tail", time.Now(), map[string]any{"sent": err == nil})
			return err
		}
		if err != nil {
			return err
		}
		if index >= uint64(w.cfg.Chunks) || !bytes.Equal(req.Data, extendedPattern(index)) {
			w.log.fail("Worker input mismatch")
			return errors.New("invalid Worker input")
		}
		begin := index * extendedChunkBytes
		next := begin + extendedChunkBytes
		w.mu.Lock()
		w.received = next
		w.digest.Write(req.Data)
		w.mu.Unlock()
		w.log.emit("worker", "received", time.Now(), map[string]any{"begin": begin, "end": next, "sha256": extendedSHA(req.Data)})
		timer := time.NewTimer(w.cfg.Processing)
		select {
		case <-stream.Context().Done():
			timer.Stop()
			return stream.Context().Err()
		case <-timer.C:
		}
		w.mu.Lock()
		w.processed = next
		w.mu.Unlock()
		w.log.emit("worker", "processed", time.Now(), map[string]any{"offset": next})
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: next}}); err != nil {
			return err
		}
		w.log.emit("worker", "progress_sent", time.Now(), map[string]any{"offset": next})
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "extended-position", Text: strconv.FormatUint(next, 10)}); err != nil {
			return err
		}
		w.log.emit("worker", "result_sent", time.Now(), map[string]any{"offset": next})
		index++
	}
}

// extendedProxy 保留最多六轮故障状态；mu 保护轮次/开放屏障和内存身份核对。
// tasks/ timers 的真实退出在关闭监听器后等待；active 用于采样，不代替 join。
type extendedProxy struct {
	log                       *extendedLog
	cfg                       extendedConfig
	url                       string
	life                      context.Context
	mu                        sync.Mutex
	round                     int
	opened                    <-chan struct{}
	id, token                 string // 只在内存核对，禁止写日志。
	count, active, timerCount atomic.Int32
	tasks, timers             sync.WaitGroup
}

// trigger 仅首次看到下一计划 offset 时切断；重放同一块不会推进或重置故障。
func (p *extendedProxy) trigger(offset uint64, connection int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.round >= len(p.cfg.FaultChunks) || offset != uint64(p.cfg.FaultChunks[p.round]*extendedChunkBytes) {
		return false
	}
	p.round++
	round := p.round
	opened := make(chan struct{})
	p.opened = opened
	now := time.Now()
	p.log.emit("proxy", "fault", now, map[string]any{"round": round, "offset": offset, "connection": connection})
	p.timerCount.Add(1)
	p.timers.Add(1)
	go func() {
		defer p.timers.Done()
		defer p.timerCount.Add(-1)
		timer := time.NewTimer(p.cfg.Outage)
		defer timer.Stop()
		select {
		case <-timer.C:
			p.log.emit("proxy", "opened", time.Now(), map[string]any{"round": round, "duration_ns": time.Since(now).Nanoseconds()})
			close(opened)
		case <-p.life.Done():
			p.log.emit("proxy", "opening_cancelled", time.Now(), map[string]any{"round": round})
		}
	}()
	return true
}

// serve 监测前端断开，即使恢复握手仍在等待开放也能退出。
func (p *extendedProxy) serve(w http.ResponseWriter, r *http.Request) {
	p.tasks.Add(1)
	defer p.tasks.Done()
	p.active.Add(1)
	number := 0
	defer func() {
		p.active.Add(-1)
		p.log.emit("proxy", "handler_exit", time.Now(), map[string]any{"connection": number})
	}()
	front, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer front.CloseNow()
	front.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(p.life)
	defer cancel()
	number = int(p.count.Add(1))
	typ, data, err := front.Read(ctx)
	if err != nil {
		return
	}
	request, err := wsprotocol.DecodeV2Handshake(data)
	if err != nil || typ != websocket.MessageText {
		p.log.fail("invalid proxy handshake")
		return
	}
	name := "start"
	if request.Kind == wsprotocol.V2HandshakeResume {
		name = "resume"
	}
	p.log.emit("proxy", "handshake", time.Now(), map[string]any{"connection": number, "kind": name, "applied_seq": request.AppliedSeq})
	p.mu.Lock()
	gate := p.opened
	valid := number == 1 && request.Kind == wsprotocol.V2HandshakeStart || number > 1 && request.Kind == wsprotocol.V2HandshakeResume && request.SessionID == p.id && request.ResumeToken == p.token
	p.mu.Unlock()
	if !valid {
		p.log.fail("proxy identity changed or fallback start")
		return
	}
	inputs := make(chan resumeProxyInput, 1)
	consumed := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			typ, data, err := front.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			select {
			case inputs <- resumeProxyInput{typ, data}:
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
	if name == "resume" && gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return
		}
	}
	back, _, err := websocket.Dial(ctx, p.url, nil)
	if err != nil {
		return
	}
	defer back.CloseNow()
	back.SetReadLimit(65536)
	if err := back.Write(ctx, typ, data); err != nil {
		return
	}
	p.log.emit("proxy", "handshake_forwarded", time.Now(), map[string]any{"connection": number})
	group, streamCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		for {
			var input resumeProxyInput
			select {
			case input = <-inputs:
			case <-streamCtx.Done():
				return streamCtx.Err()
			}
			var payload any
			kind := "control"
			if input.kind == websocket.MessageBinary {
				audio, err := wsprotocol.DecodeV2Audio(input.data)
				if err != nil {
					p.log.fail("invalid proxy audio")
					return err
				}
				payload = map[string]any{"connection": number, "begin": audio.Offset, "end": audio.Offset + uint64(len(audio.Payload)), "sha256": extendedSHA(audio.Payload)}
				kind = "audio"
				p.log.emit("proxy", "audio_read", time.Now(), payload)
				if p.trigger(audio.Offset, number) {
					_ = front.CloseNow()
					_ = back.CloseNow()
					return errClientTestCut
				}
			} else {
				control, err := wsprotocol.DecodeV2Control(input.data)
				if err != nil {
					p.log.fail("invalid proxy control")
					return err
				}
				payload = map[string]any{"connection": number, "kind": control.Kind, "offset": control.Offset, "seq": control.Seq}
			}
			p.log.emit("proxy", kind+"_write_started", time.Now(), payload)
			err := back.Write(streamCtx, input.kind, input.data)
			p.log.emit("proxy", kind+"_write_returned", time.Now(), map[string]any{"connection": number, "success": err == nil})
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
				p.log.fail("invalid server message")
				return err
			}
			var payload any
			kind := "server_message"
			switch tag.Type {
			case wsprotocol.MessageTypeReady:
				var ready wsprotocol.ReadyMessage
				if err := json.Unmarshal(data, &ready); err != nil {
					return err
				}
				p.mu.Lock()
				if number == 1 {
					p.id, p.token = ready.SessionID, ready.ResumeToken
				}
				valid := ready.SessionID == p.id && ready.ResumeToken == p.token
				p.mu.Unlock()
				if !valid {
					p.log.fail("ready identity changed")
					return errors.New("identity mismatch")
				}
				kind = "ready"
				payload = map[string]any{"connection": number, "generation": ready.Generation, "next_offset": ready.NextOffset, "acked_seq": ready.AckedResultSeq, "input_ended": ready.InputEnded}
			case wsprotocol.MessageTypeAudioAck:
				var ack wsprotocol.AudioAckMessage
				if err := json.Unmarshal(data, &ack); err != nil {
					return err
				}
				kind = "audio_ack"
				payload = map[string]any{"connection": number, "generation": ack.Generation, "next_offset": ack.NextOffset, "input_ended": ack.InputEnded}
			case wsprotocol.MessageTypeError:
				var rejection wsprotocol.V2ErrorMessage
				if err := json.Unmarshal(data, &rejection); err != nil {
					return err
				}
				kind = "entry_error"
				payload = map[string]any{"connection": number, "code": rejection.Code}
			default:
				payload = map[string]any{"connection": number, "type": tag.Type}
			}
			if err := front.Write(streamCtx, typ, data); err != nil {
				return err
			}
			p.log.emit("proxy", kind, time.Now(), payload)
		}
	})
	_ = group.Wait()
}

// extendedSummary 是固定大小终态，原始历史只存在事件文件中。
type extendedSummary struct {
	Config        extendedConfig       `json:"config"`
	GatewayConfig Config               `json:"gateway_config"`
	ClientConfig  resumeClientSettings `json:"client_config"`
	LogLimits     extendedLogLimits    `json:"log_limits"`
	Result        wsclient.V2RunResult `json:"result"`
	ErrorClass    string               `json:"error_class"`
	Captured      uint64               `json:"captured_bytes"`
	Received      uint64               `json:"received_bytes"`
	Processed     uint64               `json:"processed_bytes"`
	SourceSHA     string               `json:"source_sha256"`
	WorkerSHA     string               `json:"worker_sha256"`
	SourceCloses  int                  `json:"source_closes"`
	WorkerCalls   int                  `json:"worker_calls"`
	Picks         int                  `json:"worker_picks"`
	Connections   int                  `json:"connections"`
	Faults        int                  `json:"faults"`
	NaturalIdle   bool                 `json:"natural_idle"`
	Forced        bool                 `json:"forced_cleanup"`
	LogError      bool                 `json:"log_error"`
	QueuePeak     int64                `json:"observed_queue_peak"`
	Events        uint64               `json:"events"`
	Written       uint64               `json:"buffer_accepted_bytes"`
	ReturnedNS    int64                `json:"returned_ns"`
	IdleNS        int64                `json:"idle_ns"`
}

// runExtendedProbe 构建独立服务生命期，返回前 join 生产者并关闭记录器。
// dir 必须为调用方已创建的空目录；事件文件独占创建，终态在测试退出前保存。
func runExtendedProbe(t *testing.T, cfg extendedConfig, dir string) extendedSummary {
	t.Helper()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	life, stop := context.WithCancel(context.Background())
	defer stop()
	runCtx, cancelRun := context.WithTimeout(life, cfg.RunTimeout)
	defer cancelRun()
	file, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	log, err := newExtendedLog(file, time.Now(), stop, extendedLogLimits{1024, 8192, 65536})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	defer log.finish()
	worker := &extendedWorker{log: log, cfg: cfg, digest: sha256.New(), done: make(chan struct{})}
	pool := &v2EntrySelector{client: newBaselineTCPWorkerClient(t, worker)}
	gateway, err := New(life, pool, resumeGatewayConfig(resumeExperimentConfig{ServerWindow: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	actualGatewayConfig := gateway.cfg
	actualV2 := *gateway.v2
	actualGatewayConfig.V2 = &actualV2
	var gatewayHandlers atomic.Int32
	var gatewayTasks sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayTasks.Add(1)
		defer gatewayTasks.Done()
		gatewayHandlers.Add(1)
		defer gatewayHandlers.Add(-1)
		gateway.ServeV2HTTP(w, r)
	}))
	proxy := &extendedProxy{log: log, cfg: cfg, url: "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/asr", life: life}
	proxyServer := httptest.NewServer(http.HandlerFunc(proxy.serve))
	defer func() {
		gateway.StopAccepting()
		stop()
		proxyServer.Close()
		server.Close()
		proxy.tasks.Wait()
		proxy.timers.Wait()
		gatewayTasks.Wait()
	}()
	source := &extendedSource{log: log, chunks: cfg.Chunks, digest: sha256.New()}
	source.firstReadNS.Store(-1)
	clientCfg := resumeClientConfig(resumeExperimentConfig{ChunkBytes: extendedChunkBytes}, "ws"+strings.TrimPrefix(proxyServer.URL, "http"), nil)
	clientCfg.Resume = &wsclient.V2ResumeConfig{MaxAttempts: cfg.Attempts, Window: 5 * time.Second, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	var applied uint64
	clientCfg.OnResult = func(ctx context.Context, msg wsprotocol.SequencedResultMessage, received time.Time) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := uint64(cfg.Chunks * extendedChunkBytes)
		if msg.Seq != applied+1 {
			log.fail("application sequence gap or duplicate")
			return errors.New("invalid result sequence")
		}
		if msg.IsFinal {
			if msg.SegmentID != "extended-tail" || msg.Text != "extended-tail" || msg.Seq != uint64(cfg.Chunks+1) {
				log.fail("invalid tail")
				return errors.New("invalid tail")
			}
		} else {
			value, err := strconv.ParseUint(msg.Text, 10, 64)
			if err != nil || msg.SegmentID != "extended-position" || value != msg.Seq*extendedChunkBytes {
				log.fail("invalid position result")
				return errors.New("invalid position result")
			}
			end = value
		}
		log.emit("run", "application", received, map[string]any{"seq": msg.Seq, "end": end, "final": msg.IsFinal, "applied_ns": time.Since(log.origin).Nanoseconds()})
		applied = msg.Seq
		return nil
	}
	client, err := wsclient.NewV2(clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	clientSettings := resumeClientSettings{clientCfg.ChunkBytes, clientCfg.Realtime, clientCfg.ReadLimit, clientCfg.MaxReplayBytes, clientCfg.MaxReplayChunks, clientCfg.HandshakeTimeout, clientCfg.WriteTimeout, clientCfg.ApplyTimeout, clientCfg.CompletionTimeout, clientCfg.Resume}
	log.emit("run", "configuration", time.Now(), map[string]any{"experiment": "EXP-006-05", "config": cfg, "client": clientSettings, "gateway": actualGatewayConfig, "log_limits": log.limits, "origin_utc": log.origin.UTC().Format(time.RFC3339Nano)})
	registryCount := func() int {
		gateway.registry.mu.Lock()
		defer gateway.registry.mu.Unlock()
		return len(gateway.registry.entries)
	}
	idle := func() bool {
		return gateway.Snapshot().ActiveSessions == 0 && entryHandshakeCount(gateway.gate) == 0 && registryCount() == 0 && worker.active.Load() == 0 && gatewayHandlers.Load() == 0 && proxy.active.Load() == 0 && proxy.timerCount.Load() == 0
	}
	sampleCtx, stopSample := context.WithCancel(life)
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		for {
			began := time.Now()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			sample := map[string]any{"sessions": gateway.Snapshot().ActiveSessions, "handshakes": entryHandshakeCount(gateway.gate), "registry": registryCount(), "worker_active": worker.active.Load(), "gateway_handlers": gatewayHandlers.Load(), "proxy_handlers": proxy.active.Load(), "fault_timers": proxy.timerCount.Load(), "goroutines": runtime.NumGoroutine(), "heap_alloc": mem.HeapAlloc, "heap_inuse": mem.HeapInuse, "total_alloc": mem.TotalAlloc, "num_gc": mem.NumGC, "queue_depth": len(log.queue), "queue_peak": log.peak.Load(), "written": log.written.Load()}
			sample["finished_ns"] = time.Since(log.origin).Nanoseconds()
			log.emit("sample", "resources", began, sample)
			timer := time.NewTimer(cfg.SamplePeriod)
			select {
			case <-timer.C:
			case <-sampleCtx.Done():
				timer.Stop()
				return
			}
		}
	}()
	result, runErr := client.Run(runCtx, source)
	summary := extendedSummary{Config: cfg, GatewayConfig: actualGatewayConfig, ClientConfig: clientSettings, LogLimits: log.limits, Result: result, ErrorClass: resumeErrorClass(runErr), ReturnedNS: time.Since(log.origin).Nanoseconds()}
	log.emit("run", "returned", time.Now(), map[string]any{"result": result, "error_class": summary.ErrorClass, "outer_expired": runCtx.Err() != nil})
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
	for !idle() {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-cleanupCtx.Done():
			summary.Forced = true
			goto observeDone
		}
	}
	if life.Err() != nil {
		summary.Forced = true
		goto observeDone
	}
	summary.NaturalIdle = true
	summary.IdleNS = time.Since(log.origin).Nanoseconds()
	log.emit("run", "natural_idle", time.Now(), map[string]any{"sessions": gateway.Snapshot().ActiveSessions, "handshakes": entryHandshakeCount(gateway.gate), "registry": registryCount(), "worker_active": worker.active.Load(), "gateway_handlers": gatewayHandlers.Load(), "proxy_handlers": proxy.active.Load(), "fault_timers": proxy.timerCount.Load(), "stopping": gateway.Snapshot().Stopping, "service_cancelled": life.Err() != nil})
observeDone:
	cancelCleanup()
	if summary.Forced {
		gateway.StopAccepting()
		stop()
	}
	// 关闭监听器防止新的 Add；实际 join 后才关闭记录通道。
	proxyServer.Close()
	server.Close()
	proxy.tasks.Wait()
	proxy.timers.Wait()
	gatewayTasks.Wait()
	gateway.StopAccepting()
	waitCtx, endWait := context.WithTimeout(context.Background(), 5*time.Second)
	if err := gateway.Wait(waitCtx); err != nil {
		log.fail("Gateway join failed")
	}
	endWait()
	if worker.calls.Load() != 0 {
		<-worker.done
	}
	stopSample()
	<-sampleDone
	source.mu.Lock()
	summary.Captured = uint64(source.next * extendedChunkBytes)
	summary.SourceCloses = source.closes
	summary.SourceSHA = hex.EncodeToString(source.digest.Sum(nil))
	source.mu.Unlock()
	worker.mu.Lock()
	summary.Received, summary.Processed = worker.received, worker.processed
	summary.WorkerSHA = hex.EncodeToString(worker.digest.Sum(nil))
	workerEnded := worker.ended
	worker.mu.Unlock()
	summary.WorkerCalls = int(worker.calls.Load())
	summary.Picks = int(pool.picks.Load())
	summary.Connections = int(proxy.count.Load())
	proxy.mu.Lock()
	summary.Faults = proxy.round
	proxy.mu.Unlock()
	log.emit("run", "joined", time.Now(), map[string]any{"captured": summary.Captured, "received": summary.Received, "processed": summary.Processed, "source_sha256": summary.SourceSHA, "worker_sha256": summary.WorkerSHA, "source_closes": summary.SourceCloses, "worker_calls": summary.WorkerCalls, "picks": summary.Picks, "connections": summary.Connections, "faults": summary.Faults, "worker_ended": workerEnded})
	summary.LogError = log.finish() != nil
	summary.QueuePeak = log.peak.Load()
	summary.Events = log.events.Load()
	summary.Written = log.written.Load()
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	return summary
}
