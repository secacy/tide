package gateway

import (
	"errors"
	"fmt"
	"math/bits"
	"sync"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// WorkerSelectionPolicy 是显式选择策略；零值无效，多 Worker 不隐式推荐算法。
type WorkerSelectionPolicy string

const (
	RoundRobin         WorkerSelectionPolicy = "round_robin"          // 从上次选择之后轮询，跳过满员及停止接入的 Worker。
	LeastReservedRatio WorkerSelectionPolicy = "least_reserved_ratio" // 选择 reserved/capacity 最小者，并轮换处理相同比例。
)

var (
	ErrNoWorkerCapacity = errors.New("no worker capacity available") // 没有允许接入且有空位的 Worker。
	ErrUnknownWorker    = errors.New("unknown worker")               // Worker ID 不在固定列表中。
)

// WorkerConfig 描述一个后端及其本地会话配额。Client 的连接生命周期由调用方管理。
type WorkerConfig struct {
	ID       string                 // 固定且唯一的 Worker 标识，用于日志和显式停止接入。
	Client   asrv1.ASRServiceClient // 会话固定使用的共享客户端，不表示后端已健康。
	Capacity int                    // 正整数会话配额，不是音频块处理槽位数。
}

// workerState 的配置创建后不变；reserved/accepting 受 Pool.mu 保护。
type workerState struct {
	cfg       WorkerConfig
	reserved  int
	accepting bool
}

// WorkerPool 管理单 Gateway 的本地预留账本。必须经 NewWorkerPool 创建，使用后不能复制。
// 锁内只有选择和计数，不执行网络操作；成员列表及容量在生命周期内固定。
type WorkerPool struct {
	mu      sync.Mutex
	workers []workerState
	byID    map[string]int
	policy  WorkerSelectionPolicy
	next    int // 轮询起点，同时用来打破最小比例相同的平局。
}

// NewWorkerPool 复制配置并校验列表、唯一 ID、客户端、配额和显式策略。
// 它不连接后端、不探测健康，也不取得客户端连接的所有权。
func NewWorkerPool(configs []WorkerConfig, policy WorkerSelectionPolicy) (*WorkerPool, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("worker list is empty")
	}
	if policy != RoundRobin && policy != LeastReservedRatio {
		return nil, fmt.Errorf("invalid worker selection policy %q", policy)
	}
	p := &WorkerPool{workers: make([]workerState, len(configs)), byID: make(map[string]int, len(configs)), policy: policy}
	for i, cfg := range configs {
		if cfg.ID == "" || cfg.Client == nil || cfg.Capacity <= 0 {
			return nil, fmt.Errorf("invalid worker config at index %d", i)
		}
		if _, ok := p.byID[cfg.ID]; ok {
			return nil, fmt.Errorf("duplicate worker ID %q", cfg.ID)
		}
		p.byID[cfg.ID] = i
		p.workers[i] = workerState{cfg: cfg, accepting: true}
	}
	return p, nil
}

// WorkerLease 表示一次预留，没有超时或续租。只能使用构造者返回的指针，使用后不能复制。
// 接入流程持有 Lease；必须在相关会话资源清理后调用 Release。
type WorkerLease struct {
	pool  *WorkerPool
	index int
	once  sync.Once // 防止重复或并发释放减去另一次预留。
}

// WorkerID 返回本次预留固定的 Worker ID。
func (l *WorkerLease) WorkerID() string { return l.pool.workers[l.index].cfg.ID }

// Client 返回本次会话固定使用的客户端；Release 后不得用于创建新会话。
func (l *WorkerLease) Client() asrv1.ASRServiceClient { return l.pool.workers[l.index].cfg.Client }

// Release 归还一次预留，nil、重复及并发调用均安全；不取消 RPC 或关闭连接。
func (l *WorkerLease) Release() {
	if l == nil || l.pool == nil {
		return
	}
	l.once.Do(func() {
		l.pool.mu.Lock()
		defer l.pool.mu.Unlock()
		l.pool.workers[l.index].reserved--
	})
}

// TryAcquire 原子选择并预留。没有空位立即返回错误，不等待容量，但可能短暂等待互斥锁。
func (p *WorkerPool) TryAcquire() (*WorkerLease, error) {
	if p == nil {
		return nil, ErrNoWorkerCapacity
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	chosen := -1
	for offset := 0; offset < len(p.workers); offset++ {
		i := (p.next + offset) % len(p.workers)
		w := &p.workers[i]
		if !w.accepting || w.reserved >= w.cfg.Capacity {
			continue
		}
		if chosen < 0 || lessReservedRatio(w, &p.workers[chosen]) {
			chosen = i
		}
		if p.policy == RoundRobin {
			break
		}
	}
	if chosen < 0 {
		return nil, ErrNoWorkerCapacity
	}
	p.workers[chosen].reserved++
	p.next = (chosen + 1) % len(p.workers)
	return &WorkerLease{pool: p, index: chosen}, nil
}

// lessReservedRatio 精确比较两个占用比例，用双字乘积避免大配额交叉相乘溢出。
func lessReservedRatio(a, b *workerState) bool {
	ah, al := bits.Mul64(uint64(a.reserved), uint64(b.cfg.Capacity))
	bh, bl := bits.Mul64(uint64(b.reserved), uint64(a.cfg.Capacity))
	return ah < bh || (ah == bh && al < bl)
}

// StopAccepting 永久停止该 Worker 的新预留，重复调用安全，已有 Lease 继续有效。
// 它不执行健康检测、取消或等待；与 TryAcquire 共用锁确定先后边界。
func (p *WorkerPool) StopAccepting(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	i, ok := p.byID[id]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownWorker, id)
	}
	p.workers[i].accepting = false
	return nil
}

// WorkerSnapshot 是一次复制的状态观察，不能用“先观察再修改”替代 TryAcquire。
type WorkerSnapshot struct {
	ID        string `json:"id"`
	Capacity  int    `json:"capacity"`
	Reserved  int    `json:"reserved"`
	Accepting bool   `json:"accepting"` // 调度许可，不是后端健康状态。
}

// Snapshot 在锁内复制状态，调用方修改返回切片不会更改 Pool。
func (p *WorkerPool) Snapshot() []WorkerSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]WorkerSnapshot, len(p.workers))
	for i, w := range p.workers {
		out[i] = WorkerSnapshot{w.cfg.ID, w.cfg.Capacity, w.reserved, w.accepting}
	}
	return out
}
