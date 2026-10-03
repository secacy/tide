package main

import (
	"fmt"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
)

// newWorkerSelector 根据固定策略创建共享选择器。
// workers 的顺序决定后端顺序；加权策略按相同位置对应 weights。
// 普通轮询要求 weights 为 nil；加权策略要求数量与 workers 一致。
// 后端身份、客户端及权重数值由对应选择器构造器校验。
// 本函数不创建连接、不执行 RPC，也不调用 Pick。
// 失败返回 nil 接口和错误。
func newWorkerSelector(
	workers []workerpool.Worker,
	strategy string,
	weights []int64,
) (gateway.WorkerSelector, error) {
	switch strategy {
	case workerStrategyRoundRobin:
		// 注意这里必须判断 nil，而不是 len(weights)。
		// 非 nil 空切片同样表示调用方传入了权重配置。
		if weights != nil {
			return nil, fmt.Errorf("worker weights are not allowed for strategy %q", strategy)
		}

		selector, err := workerpool.NewRoundRobin(workers)
		if err != nil {
			return nil, err
		}
		return selector, nil

	case workerStrategyWeightedRoundRobin:
		if len(weights) != len(workers) {
			return nil, fmt.Errorf("worker weights count %d does not match workers count %d", len(weights), len(workers))
		}

		weightedWorkers := make([]workerpool.WeightedWorker, len(workers))
		for i, worker := range workers {
			weightedWorkers[i] = workerpool.WeightedWorker{
				Worker: worker,
				Weight: weights[i],
			}
		}

		selector, err := workerpool.NewWeightedRoundRobin(weightedWorkers)
		if err != nil {
			return nil, err
		}
		return selector, nil

	default:
		return nil, fmt.Errorf("unknown worker selection strategy %q", strategy)
	}
}
