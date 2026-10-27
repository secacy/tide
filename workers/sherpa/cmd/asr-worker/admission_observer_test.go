package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// admissionAssignment 由路由观察器串行记录，不进入产品协议；trace 不写 JSON。
type admissionAssignment struct {
	Worker          string           `json:"worker"`
	SelectedNS      int64            `json:"selected_ns"`
	RPCStartNS      int64            `json:"rpc_start_ns"`
	RPCEndNS        int64            `json:"rpc_end_ns"`
	TerminalMessage string           `json:"terminal_message"`
	Trace           *gatewayRPCTrace `json:"-"`
}

// admissionRouter 只标记驱动当前新建尝试。驱动等到该尝试 RPC 已打开才启动下一场，
// 因而不会根据 goroutine 调度顺序猜测映射。已运行流永久保存自己的标签。
type admissionRouter struct {
	mu         sync.Mutex
	start      time.Time
	pending    string
	pool       *workerpool.CapacityRoundRobin
	roundRobin *workerpool.RoundRobin
	calls      map[string]*admissionAssignment
	events     *os.File
	err        error
}

func (r *admissionRouter) eventLocked(value any) {
	if r.err == nil {
		r.err = json.NewEncoder(r.events).Encode(value)
	}
}
func (r *admissionRouter) setPending(name string) { r.mu.Lock(); defer r.mu.Unlock(); r.pending = name }
func (r *admissionRouter) TryAcquire() (*workerpool.WorkerLease, error) {
	var lease *workerpool.WorkerLease
	var err error
	if r.pool != nil {
		lease, err = r.pool.TryAcquire()
	} else {
		lease = workerpool.NewUnreservedLease(r.roundRobin.Pick())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	name := r.pending
	if _, exists := r.calls[name]; exists {
		if lease != nil {
			lease.Release()
		}
		return nil, errors.New("duplicate allocation for one attempt")
	}
	if err != nil {
		r.eventLocked(map[string]any{"type": "allocation_failed", "case": name, "at_ns": time.Since(r.start).Nanoseconds(), "error": err.Error()})
		return nil, err
	}
	a := &admissionAssignment{Worker: lease.Worker().ID, SelectedNS: time.Since(r.start).Nanoseconds()}
	r.calls[name] = a
	r.eventLocked(map[string]any{"type": "selected", "case": name, "worker": a.Worker, "at_ns": a.SelectedNS})
	return lease, nil
}
func (r *admissionRouter) assignment(name string) (admissionAssignment, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.calls[name]
	if !ok {
		return admissionAssignment{}, false
	}
	return *a, true
}

// admissionClient 记录真实 TCP RPC；不改变 Send/Recv 负载，不增加重试。
type admissionClient struct {
	client asrv1.ASRServiceClient
	router *admissionRouter
	worker string
}

func (c *admissionClient) StreamingRecognize(ctx context.Context, opts ...grpc.CallOption) (asrv1.ASRService_StreamingRecognizeClient, error) {
	r := c.router
	r.mu.Lock()
	name := r.pending
	a := r.calls[name]
	if a == nil || a.Worker != c.worker || a.Trace != nil {
		r.mu.Unlock()
		return nil, errors.New("RPC assignment mismatch")
	}
	a.RPCStartNS = time.Since(r.start).Nanoseconds()
	r.mu.Unlock()
	stream, err := c.client.StreamingRecognize(ctx, opts...)
	if err != nil {
		return nil, err
	}
	trace := &gatewayRPCTrace{audioHash: sha256.New()}
	r.mu.Lock()
	a.Trace = trace
	r.eventLocked(map[string]any{"type": "rpc_open", "case": name, "worker": c.worker, "at_ns": a.RPCStartNS})
	r.mu.Unlock()
	return &admissionStream{gatewayObservedStream: &gatewayObservedStream{ASRService_StreamingRecognizeClient: stream, trace: trace}, router: r, name: name}, nil
}

type admissionStream struct {
	*gatewayObservedStream
	router *admissionRouter
	name   string
}

func (s *admissionStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	response, err := s.gatewayObservedStream.Recv()
	if err != nil {
		s.router.mu.Lock()
		defer s.router.mu.Unlock()
		a := s.router.calls[s.name]
		a.RPCEndNS = time.Since(s.router.start).Nanoseconds()
		a.TerminalMessage = status.Convert(err).Message()
		code := status.Code(err).String()
		if err == io.EOF {
			code = "EOF"
		}
		s.router.eventLocked(map[string]any{"type": "rpc_end", "case": s.name, "worker": a.Worker, "at_ns": a.RPCEndNS, "code": code, "message": a.TerminalMessage})
	}
	return response, err
}

// admissionSampler 连续采样两个已知 PID；g 在批次间切换，锁内只复制引用。
// 不轮询 TryAcquire 探测容量，避免测量改变轮询游标；池复用只在批末检查。
type admissionSampler struct {
	mu     sync.Mutex
	start  time.Time
	arm    string
	g      *gateway.Gateway
	models [2]*backendMonitor
	pids   [2]int
	file   *os.File
	err    error
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *admissionSampler) setArm(arm string, g *gateway.Gateway) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arm, s.g = arm, g
}
func (s *admissionSampler) sample(ctx context.Context) {
	at := time.Now()
	s.mu.Lock()
	arm, g := s.arm, s.g
	s.mu.Unlock()
	active := 0
	if g != nil {
		active = g.Snapshot().ActiveSessions
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	row := map[string]any{"elapsed_ns": at.Sub(s.start).Nanoseconds(), "wall_ns": at.UnixNano(), "arm": arm, "active_sessions": active, "driver_goroutines": runtime.NumGoroutine(), "driver_heap_bytes": mem.HeapAlloc}
	workers := make(map[string]any)
	for i, m := range s.models {
		m.mu.Lock()
		err := m.refreshLocked()
		created, closed := m.created, m.closed
		m.mu.Unlock()
		rss, rssErr := readWorkerRSS(ctx, s.pids[i])
		if ctx.Err() != nil {
			return
		}
		worker := map[string]any{"pid": s.pids[i], "rss_bytes": rss, "created": created, "closed": closed}
		if err != nil {
			worker["log_error"] = err.Error()
		}
		if rssErr != nil {
			worker["rss_error"] = rssErr.Error()
		}
		workers[string(rune('A'+i))] = worker
	}
	row["workers"] = workers
	row["duration_ns"] = time.Since(at).Nanoseconds()
	if err := json.NewEncoder(s.file).Encode(row); err != nil && s.err == nil {
		s.err = err
	}
}
func (s *admissionSampler) startSampling(out string) error {
	file, err := os.OpenFile(filepath.Join(out, "samples.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	s.file = file
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		s.sample(ctx)
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				s.sample(ctx)
			}
		}
	}()
	return nil
}
func (s *admissionSampler) stop() error {
	s.cancel()
	<-s.done
	return errors.Join(s.err, s.file.Close())
}

// modelCounts 通过同一读取器刷新原生日志；事件时刻均来自本次驱动。
func modelCounts(m *backendMonitor) (int, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.refreshLocked()
	return m.created, m.closed, err
}
