package workerpool

import (
	"errors"
	"strings"
	"sync"
)

// MaxTotalWeight 是固定权重总和的配置上限。
// 用于约束权重及选择分数的算术范围，不表示会话容量。
const MaxTotalWeight int64 = 1_000_000

// WeightedWorker 将后端身份与固定分配权重关联。
// Weight 表示选择比例，不表示并发上限或处理名额。
type WeightedWorker struct {
	Worker Worker // 后端身份与客户端；连接生命周期由外部管理。
	Weight int64  // 必须为正数，构造后不再改变。
}

// weightedEntry 保存选择器私有的配置和运行状态。
type weightedEntry struct {
	worker  Worker // 构造时复制；Client 引用仍与外部共享。
	weight  int64  // 固定权重，每次选择时加入 current。
	current int64  // 动态选择分数，初始为零，允许为负。
}

// WeightedRoundRobin 在固定后端列表上执行平滑加权轮询。
// 支持并发调用；必须通过构造函数创建，使用后不得复制。
type WeightedRoundRobin struct {
	mu          sync.Mutex      // 保护一次完整选择中的所有分数更新。
	entries     []weightedEntry // 保留配置顺序，构造后不增删。
	totalWeight int64           // 固定权重之和，用于选中后的扣分。
}

// NewWeightedRoundRobin 校验配置并复制后端列表。
// 拒绝空列表、空白或重复 ID、nil Client、非正权重及总权重超限。
// 保留 ID 原值和配置顺序，不建立连接或探测后端健康。
func NewWeightedRoundRobin(workers []WeightedWorker) (*WeightedRoundRobin, error) {
	if len(workers) == 0 {
		return nil, errors.New("at least one worker is required")
	}

	m := map[string]struct{}{}
	entries := make([]weightedEntry, 0, len(workers))
	var totalWeight int64

	for _, ww := range workers {
		if strings.TrimSpace(ww.Worker.ID) == "" {
			return nil, errors.New("worker ID is required")
		}
		if _, ok := m[ww.Worker.ID]; ok {
			return nil, errors.New("worker ID is duplicated")
		}
		if ww.Worker.Client == nil {
			return nil, errors.New("worker client is required")
		}
		if ww.Weight <= 0 {
			return nil, errors.New("worker weight must be positive")
		}
		if ww.Weight > MaxTotalWeight-totalWeight {
			return nil, errors.New("total worker weight exceeds limit")
		}

		m[ww.Worker.ID] = struct{}{}
		totalWeight += ww.Weight

		entries = append(entries, weightedEntry{
			worker:  ww.Worker,
			weight:  ww.Weight,
			current: 0,
		})
	}

	return &WeightedRoundRobin{
		entries:     entries,
		totalWeight: totalWeight,
	}, nil
}

// Pick 按平滑加权轮询选择一个 Worker。
// 同分时优先配置顺序靠前的实例。
// 每次调用消耗一次选择，后续建流失败不回退。
// 不执行网络操作，不判断健康状态或剩余处理能力。
func (r *WeightedRoundRobin) Pick() Worker {
	r.mu.Lock()
	defer r.mu.Unlock()

	best := 0

	for i := range r.entries {
		r.entries[i].current += r.entries[i].weight

		if r.entries[i].current > r.entries[best].current {
			best = i
		}
	}

	r.entries[best].current -= r.totalWeight
	return r.entries[best].worker
}
