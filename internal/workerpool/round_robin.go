package workerpool

import (
	"errors"
	"slices"
	"strings"
	"sync"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Worker 描述可被选中的计算后端。
// Client 由外部创建并复用，ID 用于记录会话归属。
type Worker struct {
	ID     string                 // 列表内唯一、非空白的稳定标识。
	Client asrv1.ASRServiceClient // 发起 RPC 的客户端，连接生命周期由外部管理。
}

// RoundRobin 在固定 Worker 列表上循环选择，支持并发调用。
// 必须通过 NewRoundRobin 创建，使用后不得复制。
type RoundRobin struct {
	mu      sync.Mutex // 保护 next；持锁期间不执行网络操作。
	workers []Worker   // 构造时复制，之后不再修改。
	next    int        // 下次选择的下标，始终小于 len(workers)。
}

// NewRoundRobin 校验并复制 Worker 列表，保留传入顺序。
// 空列表、空白 ID、重复 ID 或 nil Client 返回错误。
// 本函数不建立连接，也不检查 Worker 是否在线。
func NewRoundRobin(workers []Worker) (*RoundRobin, error) {
	if len(workers) == 0 {
		return nil, errors.New("at least one worker is required")
	}
	m := map[string]struct{}{}
	for _, w := range workers {
		if strings.TrimSpace(w.ID) == "" {
			return nil, errors.New("worker ID is required")
		}
		if _, ok := m[w.ID]; ok {
			return nil, errors.New("worker ID is duplicated")
		}
		if w.Client == nil {
			return nil, errors.New("worker client is required")
		}
		m[w.ID] = struct{}{}
	}
	return &RoundRobin{
		workers: slices.Clone(workers),
		next:    0,
	}, nil
}

// Pick 返回本次选中的 Worker，并推进下次选择位置。
// 每次调用算一次选择，后续建流失败不回退。
// 本方法不判断 Worker 健康状态或剩余处理能力。
func (r *RoundRobin) Pick() Worker {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.workers[r.next]
	r.next = (r.next + 1) % len(r.workers)
	return w
}
