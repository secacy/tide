package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

// resumeRecorder 仅记录事实，锁不跨网络、等待和外部回调；每场独立创建。
// 报告在 Run 返回、自然观察及必要兜底完成后由主线程冻结。
type resumeRecorder struct {
	mu     sync.Mutex
	report resumeExperimentReport
}

func (p *resumeRecorder) change(f func(*resumeExperimentReport)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(&p.report)
}
func (p *resumeRecorder) toolError(message string) {
	p.change(func(r *resumeExperimentReport) { r.ToolErrors = append(r.ToolErrors, message) })
}

// snapshot 深拷贝所有可变事实；即使清理失败仍不得把活跃任务的切片交给分析者。
func (p *resumeRecorder) snapshot() resumeExperimentReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.report
	r.SourceReads = append([]resumeSourceRead(nil), r.SourceReads...)
	r.Frames = append([]resumeFrame(nil), r.Frames...)
	r.Handshakes = append([]resumeHandshake(nil), r.Handshakes...)
	r.Readies = append([]resumeReady(nil), r.Readies...)
	r.Applications = append([]resumeApplication(nil), r.Applications...)
	r.Samples = append([]resumeResourceSample(nil), r.Samples...)
	r.ToolErrors = append([]string{}, r.ToolErrors...)
	r.Worker.Audio = append([]byte(nil), r.Worker.Audio...)
	r.Worker.Chunks = append([]resumeWorkerChunk(nil), r.Worker.Chunks...)
	return r
}

// resumeSource 是确定性有限输入，唯一采集任务读取，Close 与 Read 可并发。
// 内存读取无需等待，Close 拒绝后续 Read；零字节 EOF 也保留时间，便于核对封口。
type resumeSource struct {
	recorder *resumeRecorder
	input    []byte
	offset   int
	closed   bool
}

func (s *resumeSource) Read(data []byte) (int, error) {
	began := time.Now()
	s.recorder.mu.Lock()
	defer s.recorder.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	begin := s.offset
	n := copy(data, s.input[begin:])
	s.offset += n
	s.recorder.report.SourceReads = append(s.recorder.report.SourceReads, resumeSourceRead{resumeRange{uint64(begin), uint64(s.offset)}, began, time.Now(), n == 0})
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}
func (s *resumeSource) Close() error {
	s.recorder.change(func(r *resumeExperimentReport) { s.closed = true; r.SourceCloses++ })
	return nil
}

// resumePositionWorker 以真实 TCP gRPC 工作；累计位置标记只供实验分析。
// Active 与 ExitedAt 区分运行和真实 RPC 返回，不把取消通知当作已释放。
type resumePositionWorker struct {
	asrv1.UnimplementedASRServiceServer
	recorder *resumeRecorder
	delay    time.Duration
	active   atomic.Int32
}

func (w *resumePositionWorker) StreamingRecognize(s grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (runErr error) {
	w.active.Add(1)
	w.recorder.change(func(r *resumeExperimentReport) { r.Worker.Calls++; r.Worker.StartedAt = resumeAt(time.Now()) })
	defer func() {
		w.recorder.change(func(r *resumeExperimentReport) {
			r.Worker.ExitedAt = resumeAt(time.Now())
			r.Worker.ExitClass = resumeErrorClass(runErr)
		})
		w.active.Add(-1)
	}()
	var offset uint64
	for {
		req, err := s.Recv()
		if errors.Is(err, io.EOF) {
			err = s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "resume-tail", Text: "resume-tail", IsFinal: true})
			if err == nil {
				w.recorder.change(func(r *resumeExperimentReport) { r.Worker.TailSentAt = resumeAt(time.Now()) })
			}
			return err
		}
		if err != nil {
			return err
		}
		if len(req.Data) == 0 {
			w.recorder.toolError("Worker received empty audio")
			return status.Error(codes.InvalidArgument, "empty experiment audio")
		}
		next := offset + uint64(len(req.Data))
		index := 0
		w.recorder.change(func(r *resumeExperimentReport) {
			index = len(r.Worker.Chunks)
			r.Worker.Chunks = append(r.Worker.Chunks, resumeWorkerChunk{resumeRange: resumeRange{offset, next}, ReceivedAt: time.Now()})
			r.Worker.Audio = append(r.Worker.Audio, req.Data...)
		})
		timer := time.NewTimer(w.delay)
		select {
		case <-s.Context().Done():
			timer.Stop()
			return status.FromContextError(s.Context().Err()).Err()
		case <-timer.C:
		}
		w.recorder.change(func(r *resumeExperimentReport) { r.Worker.Chunks[index].ProcessedAt = resumeAt(time.Now()) })
		if err := s.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: next}}); err != nil {
			return err
		}
		w.recorder.change(func(r *resumeExperimentReport) { r.Worker.Chunks[index].ProgressSentAt = resumeAt(time.Now()) })
		if err := s.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "resume-position", Text: strconv.FormatUint(next, 10)}); err != nil {
			return err
		}
		w.recorder.change(func(r *resumeExperimentReport) { r.Worker.Chunks[index].ResultSentAt = resumeAt(time.Now()) })
		offset = next
	}
}

// resumeExperimentProxy 独占故障计时器和两侧 socket，沿用已有故障代理的双向转发方式。
// 连接 ctx 管理一次 handler；life 管理实验，不随客户端业务期限取消。
type resumeExperimentProxy struct {
	recorder    *resumeRecorder
	config      resumeExperimentConfig
	backendURL  string
	life        context.Context
	opened      chan struct{}
	once        sync.Once
	connections atomic.Int32
	active      atomic.Int32
	timers      atomic.Int32
}

func (p *resumeExperimentProxy) cut() {
	p.once.Do(func() {
		p.recorder.change(func(r *resumeExperimentReport) { r.CutAt = resumeAt(time.Now()) })
		p.timers.Add(1)
		go func() {
			defer p.timers.Add(-1)
			timer := time.NewTimer(p.config.Outage)
			defer timer.Stop()
			select {
			case <-timer.C:
				p.recorder.change(func(r *resumeExperimentReport) { r.OpenedAt = resumeAt(time.Now()) })
				close(p.opened)
			case <-p.life.Done():
			}
		}()
	})
}

type resumeProxyInput struct {
	kind websocket.MessageType
	data []byte
}

// serve 每侧仅一个 reader/writer。前端读取允许监测等待 resume 时的客户端断开。
// 每帧转发回执之后才读取下一帧，避免 EOF 越过已交付的 completed_ack。
func (p *resumeExperimentProxy) serve(rw http.ResponseWriter, req *http.Request) {
	p.active.Add(1)
	defer p.active.Add(-1)
	front, err := websocket.Accept(rw, req, nil)
	if err != nil {
		return
	}
	defer front.CloseNow()
	front.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(p.life)
	defer cancel()
	number := int(p.connections.Add(1))
	kind, data, err := front.Read(ctx)
	if err != nil {
		return
	}
	handshake, err := wsprotocol.DecodeV2Handshake(data)
	if err != nil || kind != websocket.MessageText {
		p.recorder.toolError("proxy invalid handshake")
		return
	}
	name := "start"
	if handshake.Kind == wsprotocol.V2HandshakeResume {
		name = "resume"
	}
	index := 0
	p.recorder.change(func(r *resumeExperimentReport) {
		index = len(r.Handshakes)
		r.Handshakes = append(r.Handshakes, resumeHandshake{Connection: number, Kind: name, ReadAt: time.Now()})
	})
	input := make(chan resumeProxyInput, 1)
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
	if handshake.Kind == wsprotocol.V2HandshakeResume {
		select {
		case <-p.opened:
		case <-ctx.Done():
			return
		}
	}
	back, _, err := websocket.Dial(ctx, p.backendURL, nil)
	if err != nil {
		return
	}
	defer back.CloseNow()
	back.SetReadLimit(65536)
	if err := back.Write(ctx, kind, data); err != nil {
		return
	}
	p.recorder.change(func(r *resumeExperimentReport) { r.Handshakes[index].ForwardedAt = resumeAt(time.Now()) })
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
				parsed, err := wsprotocol.DecodeV2Audio(event.data)
				if err != nil {
					p.recorder.toolError("proxy invalid audio frame")
					return err
				}
				chunks++
				row := resumeFrame{resumeRange: resumeRange{parsed.Offset, parsed.Offset + uint64(len(parsed.Payload))}, Connection: number, ReadAt: time.Now(), SHA256: resumeDigest(parsed.Payload)}
				drop := number == 1 && p.config.CutChunk > 0 && chunks == p.config.CutChunk
				row.Dropped = drop
				p.recorder.change(func(r *resumeExperimentReport) { rowIndex = len(r.Frames); r.Frames = append(r.Frames, row) })
				if drop {
					p.cut()
					_ = front.CloseNow()
					_ = back.CloseNow()
					return errClientTestCut
				}
				p.recorder.change(func(r *resumeExperimentReport) { r.Frames[rowIndex].ForwardStartedAt = resumeAt(time.Now()) })
			}
			err := back.Write(streamCtx, event.kind, event.data)
			if rowIndex >= 0 {
				p.recorder.change(func(r *resumeExperimentReport) {
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
				p.recorder.toolError("proxy invalid server JSON")
				return err
			}
			var ready *wsprotocol.ReadyMessage
			if tag.Type == wsprotocol.MessageTypeReady {
				var value wsprotocol.ReadyMessage
				if err := json.Unmarshal(data, &value); err != nil {
					return err
				}
				ready = &value
			}
			if err := front.Write(streamCtx, typ, data); err != nil {
				return err
			}
			if ready != nil {
				p.recorder.change(func(r *resumeExperimentReport) {
					r.Readies = append(r.Readies, resumeReady{number, ready.Generation, ready.NextOffset, ready.AckedResultSeq, time.Now()})
				})
			}
		}
	})
	_ = group.Wait()
}

// resumeErrorClass 输出固定类别，不将 URL、原始消息或可能包含凭据的错误写入报告。
func resumeErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, wsclient.ErrV2RecoveryExhausted):
		return "recovery_exhausted"
	case errors.Is(err, context.DeadlineExceeded), status.Code(err) == codes.DeadlineExceeded:
		return "deadline"
	case errors.Is(err, context.Canceled), status.Code(err) == codes.Canceled:
		return "canceled"
	case errors.Is(err, wsclient.ErrV2Incomplete):
		return "incomplete"
	}
	var entry *wsclient.V2EntryError
	if errors.As(err, &entry) {
		return "entry_rejected"
	}
	return "transport_or_terminal"
}

func resumeGatewayConfig(c resumeExperimentConfig) Config {
	return Config{MaxSessions: 1, MaxMessageBytes: 1048576, WorkerSendTimeout: 2 * time.Second, ResultWriteTimeout: 2 * time.Second,
		V2: &V2Config{MaxHandshakes: 4, EntryTimeout: 10 * time.Second, InputProgressTimeout: 30 * time.Second, ResumeWindow: c.ServerWindow, ResultRetentionTimeout: c.ServerWindow, WorkerStatusTimeout: 2 * time.Second, MaxAudioBytes: 1048576, MaxAudioChunks: 256, MaxResultBytes: 1048576, MaxResults: 256}}
}
func resumeClientConfig(c resumeExperimentConfig, url string, recorder *resumeRecorder) wsclient.V2Config {
	cfg := wsclient.V2Config{URL: url, ChunkBytes: c.ChunkBytes, Realtime: true, ReadLimit: 65536, MaxReplayBytes: 320000, MaxReplayChunks: 128, HandshakeTimeout: 10 * time.Second, WriteTimeout: 5 * time.Second, ApplyTimeout: 5 * time.Second, CompletionTimeout: 30 * time.Second}
	if c.Resume {
		cfg.Resume = &wsclient.V2ResumeConfig{MaxAttempts: 5, Window: c.ClientWindow, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}
	}
	cfg.OnResult = func(ctx context.Context, msg wsprotocol.SequencedResultMessage, received time.Time) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := uint64(c.Chunks * c.ChunkBytes)
		if msg.IsFinal {
			if msg.SegmentID != "resume-tail" || msg.Text != "resume-tail" {
				recorder.toolError("invalid Mock tail result")
				return fmt.Errorf("invalid Mock tail result")
			}
		} else {
			value, err := strconv.ParseUint(msg.Text, 10, 64)
			if err != nil || msg.SegmentID != "resume-position" {
				recorder.toolError("invalid Mock position result")
				return fmt.Errorf("invalid Mock position result")
			}
			end = value
		}
		recorder.change(func(r *resumeExperimentReport) {
			r.Applications = append(r.Applications, resumeApplication{msg.Seq, end, msg.IsFinal, received, time.Now()})
		})
		return nil
	}
	return cfg
}

// runResumeExperiment 使用独立生命期和清理窗口，不借用快速夹具的 5s 总期限。
// 返回前等待自然退出；观察失败后才停止服务，报告保留 forced_cleanup 和既有事实。
func runResumeExperiment(t *testing.T, cfg resumeExperimentConfig) resumeExperimentReport {
	t.Helper()
	if err := validateResumeConfig(cfg); err != nil {
		t.Fatal(err)
	}
	recorder := &resumeRecorder{report: resumeExperimentReport{Version: 1, Config: cfg, ToolErrors: []string{}}}
	life, stop := context.WithCancel(context.Background())
	worker := &resumePositionWorker{recorder: recorder, delay: cfg.Processing}
	selector := &v2EntrySelector{client: newBaselineTCPWorkerClient(t, worker)}
	gateway, err := New(life, selector, resumeGatewayConfig(cfg))
	if err != nil {
		stop()
		t.Fatal(err)
	}
	var gatewayHandlers atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gatewayHandlers.Add(1)
		defer gatewayHandlers.Add(-1)
		gateway.ServeV2HTTP(w, r)
	}))
	proxy := &resumeExperimentProxy{recorder: recorder, config: cfg, backendURL: "ws" + strings.TrimPrefix(server.URL, "http") + "/v2/asr", life: life, opened: make(chan struct{})}
	proxyServer := httptest.NewServer(http.HandlerFunc(proxy.serve))
	// Stop 是正常测量之后的环境销毁；只有自然观察失败才算兜底。
	defer func() {
		gateway.StopAccepting()
		stop()
		proxyServer.Close()
		server.Close()
		waitCtx, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
		if err := gateway.Wait(waitCtx); err != nil {
			t.Errorf("experiment teardown: %v", err)
		}
	}()
	clientCfg := resumeClientConfig(cfg, "ws"+strings.TrimPrefix(proxyServer.URL, "http"), recorder)
	client, err := wsclient.NewV2(clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	source := &resumeSource{recorder: recorder, input: resumePattern(cfg.Chunks * cfg.ChunkBytes)}
	recorder.change(func(r *resumeExperimentReport) {
		r.GatewayConfig = gateway.cfg
		v2 := *gateway.v2
		r.GatewayConfig.V2 = &v2
		r.ClientConfig = resumeClientSettings{clientCfg.ChunkBytes, clientCfg.Realtime, clientCfg.ReadLimit, clientCfg.MaxReplayBytes, clientCfg.MaxReplayChunks, clientCfg.HandshakeTimeout, clientCfg.WriteTimeout, clientCfg.ApplyTimeout, clientCfg.CompletionTimeout, clientCfg.Resume}
		r.InputSHA256 = resumeDigest(source.input)
	})
	began := time.Now()
	recorder.change(func(r *resumeExperimentReport) { r.StartedAt = began })
	runCtx, cancelRun := context.WithTimeout(context.Background(), cfg.RunTimeout)
	defer cancelRun()
	type outcome struct {
		result wsclient.V2RunResult
		err    error
		at     time.Time
		outer  bool
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := client.Run(runCtx, source)
		done <- outcome{result, err, time.Now(), errors.Is(runCtx.Err(), context.DeadlineExceeded)}
	}()
	sample := func() resumeResourceSample {
		s := resumeResourceSample{BeginAt: time.Now(), Sessions: gateway.Snapshot().ActiveSessions, Handshakes: entryHandshakeCount(gateway.gate), WorkerActive: int(worker.active.Load()), GatewayHandlers: int(gatewayHandlers.Load()), ProxyHandlers: int(proxy.active.Load()), FaultTimers: int(proxy.timers.Load())}
		gateway.registry.mu.Lock()
		s.Registry = len(gateway.registry.entries)
		gateway.registry.mu.Unlock()
		s.At = time.Now()
		return s
	}
	ticker := time.NewTicker(cfg.SamplePeriod)
	defer ticker.Stop()
	var cleanupTimer *time.Timer
	var cleanupC <-chan time.Time
	complete := false
	for !complete {
		select {
		case out := <-done:
			done = nil
			recorder.change(func(r *resumeExperimentReport) {
				r.RunResult = out.result
				r.ReturnedAt = resumeAt(out.at)
				r.RunErrorClass = resumeErrorClass(out.err)
				r.OuterDeadline = out.outer
			})
			cleanupTimer = time.NewTimer(cfg.CleanupTimeout)
			cleanupC = cleanupTimer.C
		case <-ticker.C:
		case <-cleanupC:
			recorder.change(func(r *resumeExperimentReport) { r.ForcedCleanup = true })
			complete = true
			continue // 到期之后的全零不能改写自然观察结果。
		}
		row := sample()
		recorder.change(func(r *resumeExperimentReport) { r.Samples = append(r.Samples, row) })
		if done == nil && row.idle() && row.tasksExited() {
			recorder.change(func(r *resumeExperimentReport) { r.NaturalTasksExited = true })
			complete = true
		}
	}
	if cleanupTimer != nil {
		cleanupTimer.Stop()
	}
	// 只有失败的自然观察才提前 stop；正常场合等事实冻结后再销毁环境。
	recorder.mu.Lock()
	forced := recorder.report.ForcedCleanup
	recorder.mu.Unlock()
	if forced {
		gateway.StopAccepting()
		stop()
		waitCtx, end := context.WithTimeout(context.Background(), 5*time.Second)
		if err := gateway.Wait(waitCtx); err != nil {
			recorder.toolError("forced Gateway cleanup failed")
		}
		end()
		proxyServer.Close()
		server.Close()
		// 确保计时器与 Worker 的最后记录不会在报告冻结后写入。
		deadline := time.Now().Add(5 * time.Second)
		for (proxy.timers.Load() != 0 || worker.active.Load() != 0) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if proxy.timers.Load() != 0 || worker.active.Load() != 0 {
			recorder.toolError("forced task cleanup failed")
		}
	}
	recorder.change(func(r *resumeExperimentReport) { r.WorkerPicks = int(selector.picks.Load()) })
	report := recorder.snapshot()
	analyzeResumeReport(&report)
	return report
}
