package workerpool

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ErrNoWorkerCapacity 表示本地配置条目全部被占用；不表示远端实时容量。
var ErrNoWorkerCapacity = errors.New("no local worker capacity")

// WorkerCapacity 配置一个独立后端的本地占用上限。
type WorkerCapacity struct {
	Worker Worker // ID 唯一、Client 非 nil；连接仍由外部拥有。
	Limit  int    // 正数；真实单名额 Worker 本轮设为 1。
}

// capacityEntry 由池独占；使用后配置不可变。
type capacityEntry struct {
	worker Worker // 构造时复制的后端信息。
	limit  int    // 该条目的本地上限。
	inUse  int    // 已取得且未释放的本地名额，始终位于 [0, limit]。
}

// CapacityRoundRobin 支持并发调用；经构造创建，以指针使用，不得复制。
type CapacityRoundRobin struct {
	mu      sync.Mutex      // 覆盖检查、选择、占位和归还；锁内不调用外部代码。
	entries []capacityEntry // 构造时复制，不暴露可变状态。
	next    int             // 下次扫描起点；成功选择后移至选中项之后。
}

// WorkerLease 表示本地占用，不是服务端租约；必须以指针交接，不得复制。
// 由分配器创建；调用者最终负责 Release，释放后不得继续使用该次分配运行 RPC。
type WorkerLease struct {
	worker  Worker    // 该次分配的只读身份与客户端引用。
	release func()    // 私有归还动作，仅更新本地计数，不执行网络操作。
	once    sync.Once // 防止多条清理路径或并发调用重复扣减。
}

// NewCapacityRoundRobin 创建轮询池。
// 复制配置，计数从零开始；不连接后端、不启动任务。
func NewCapacityRoundRobin(workers []WorkerCapacity) (*CapacityRoundRobin, error) {
	if len(workers) == 0 {
		return nil, errors.New("worker list is empty")
	}
	p := &CapacityRoundRobin{
		entries: make([]capacityEntry, len(workers)),
	}
	seen := make(map[string]struct{}, len(workers))
	for i, cfg := range workers {
		w := cfg.Worker
		if strings.TrimSpace(w.ID) == "" {
			return nil, fmt.Errorf("worker %d: empty ID", i)
		}
		if _, exists := seen[w.ID]; exists {
			return nil, fmt.Errorf("worker %d: duplicate ID %q", i, w.ID)
		}
		if w.Client == nil {
			return nil, fmt.Errorf("worker %d ID %q: nil client", i, w.ID)
		}
		if cfg.Limit <= 0 {
			return nil, fmt.Errorf("worker %d ID %q: invalid limit %d", i, w.ID, cfg.Limit)
		}
		seen[w.ID] = struct{}{}

		p.entries[i] = capacityEntry{
			worker: w,
			limit:  cfg.Limit,
			inUse:  0,
		}
	}
	return p, nil
}

// TryAcquire 从 next 开始至多扫描一轮，选择首个 inUse < limit 的条目。
// 检查与加一原子完成；成功返回独占 Lease，失败返回 nil, ErrNoWorkerCapacity。
// 失败不移动 next；只描述本地容量，不等待或查询 Worker。
func (p *CapacityRoundRobin) TryAcquire() (*WorkerLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := len(p.entries)

	for step := range n {
		idx := (p.next + step) % n
		entry := &p.entries[idx]

		if entry.inUse >= entry.limit {
			continue
		}

		// 选择和占位必须处于同一临界区。
		entry.inUse++

		// 下一次从当前 Worker 的后一个位置开始。
		p.next = (idx + 1) % n

		lease := &WorkerLease{
			worker: entry.worker,
			release: func() {
				p.mu.Lock()
				defer p.mu.Unlock()

				p.entries[idx].inUse--
			},
		}

		return lease, nil
	}

	// 全满时不修改 next 或任何占用计数。
	return nil, ErrNoWorkerCapacity
}

// Worker 返回配置值的副本；修改返回值不影响池或占用计数。
func (l *WorkerLease) Worker() Worker {
	return l.worker
}

// Release 恰好归还一次本地名额；允许重复/并发调用。
// 调用方保证已完成其负责的本地清理；不关闭 ClientConn，不确认远程释放。
func (l *WorkerLease) Release() {
	l.once.Do(l.release)
}
