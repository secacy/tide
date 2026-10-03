package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/workerpool"
)

// selectorFactoryWorkers 的客户端只提供身份，若工厂调用 RPC 则会失败。
func selectorFactoryWorkers() []workerpool.Worker {
	return []workerpool.Worker{{ID: "B", Client: &configOnlyWorker{}}, {ID: "A", Client: &configOnlyWorker{}}}
}

// TestNewWorkerSelectorValues 验证配置顺序、权重映射及构造未消耗第一次选择。
func TestNewWorkerSelectorValues(t *testing.T) {
	for _, tc := range []struct {
		name, strategy, sequence string
		weights                  []int64
		single                   bool
	}{
		{"round_robin", workerStrategyRoundRobin, "BABABA", nil, false},
		{"weighted_two_to_one", workerStrategyWeightedRoundRobin, "BABBAB", []int64{2, 1}, false},
		{"weighted_one_to_two", workerStrategyWeightedRoundRobin, "ABAABA", []int64{1, 2}, false},
		{"weighted_single_at_limit", workerStrategyWeightedRoundRobin, "BBBBBB", []int64{workerpool.MaxTotalWeight}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := selectorFactoryWorkers()
			if tc.single {
				workers = workers[:1]
			}
			selector, err := newWorkerSelector(workers, tc.strategy, tc.weights)
			if err != nil || selector == nil {
				t.Fatalf("selector=%v err=%v", selector, err)
			}
			for i, id := range tc.sequence {
				want := workers[0]
				if id == 'A' {
					want = workers[1]
				}
				if got := selector.Pick(); got != want {
					t.Fatalf("selection %d: got=%+v want=%+v", i, got, want)
				}
			}
		})
	}
}

// TestNewWorkerSelectorRejected 尤其检查构造器错误不能变成含 nil 指针的非 nil 接口。
func TestNewWorkerSelectorRejected(t *testing.T) {
	for _, name := range []string{"unknown", "empty_strategy", "round_with_weights", "round_with_empty_weights", "round_empty_workers", "round_nil_client", "round_duplicate", "weighted_missing_weights", "weighted_short", "weighted_long", "weighted_empty_workers", "weighted_nil_client", "weighted_duplicate", "weighted_blank_id", "weighted_zero", "weighted_negative", "weighted_over_limit"} {
		t.Run(name, func(t *testing.T) {
			workers := selectorFactoryWorkers()
			strategy := workerStrategyWeightedRoundRobin
			weights := []int64{2, 1}
			switch name {
			case "unknown":
				strategy = "unknown"
			case "empty_strategy":
				strategy = ""
			case "round_with_weights":
				strategy = workerStrategyRoundRobin
			case "round_with_empty_weights":
				strategy = workerStrategyRoundRobin
				weights = []int64{}
			case "round_empty_workers":
				strategy = workerStrategyRoundRobin
				workers = nil
				weights = nil
			case "round_nil_client":
				strategy = workerStrategyRoundRobin
				weights = nil
				workers[1].Client = nil
			case "round_duplicate":
				strategy = workerStrategyRoundRobin
				weights = nil
				workers[1].ID = workers[0].ID
			case "weighted_missing_weights":
				weights = nil
			case "weighted_short":
				weights = []int64{2}
			case "weighted_long":
				weights = []int64{2, 1, 1}
			case "weighted_empty_workers":
				workers = nil
				weights = nil
			case "weighted_nil_client":
				workers[1].Client = nil
			case "weighted_duplicate":
				workers[1].ID = workers[0].ID
			case "weighted_blank_id":
				workers[1].ID = " \t"
			case "weighted_zero":
				weights[1] = 0
			case "weighted_negative":
				weights[1] = -1
			case "weighted_over_limit":
				weights = []int64{workerpool.MaxTotalWeight, 1}
			}
			selector, err := newWorkerSelector(workers, strategy, weights)
			if err == nil || selector != nil {
				t.Fatalf("failed factory leaked selector: selector=%v err=%v", selector, err)
			}
		})
	}
}

// TestNewWorkerSelectorIsolation 工厂传递的输入不得与选择器私有配置共用可变状态。
func TestNewWorkerSelectorIsolation(t *testing.T) {
	workers := selectorFactoryWorkers()
	original := append([]workerpool.Worker(nil), workers...)
	weights := []int64{2, 1}
	selector, err := newWorkerSelector(workers, workerStrategyWeightedRoundRobin, weights)
	if err != nil {
		t.Fatal(err)
	}
	workers[0] = workerpool.Worker{}
	workers[1].ID = "mutated"
	weights[0] = 1
	weights[1] = 2
	for _, i := range []int{0, 1, 0, 0, 1, 0} {
		if got := selector.Pick(); got != original[i] {
			t.Fatalf("input mutation changed selection: %+v", got)
		}
	}
}

// TestRunSelectorFailure 验证 run 实际使用工厂，并在失败时返回组装错误。
// Context 只作测试兜底；正确实现会在启动 HTTP 之前返回。
func TestRunSelectorFailure(t *testing.T) {
	for _, strategy := range []string{"unknown", "weighted_round_robin"} {
		t.Run(strategy, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cfg := gatewayConfig{WorkerAddrs: []string{"127.0.0.1:50051"}, WorkerStrategy: strategy}
			err := run(ctx, cfg)
			if err == nil || !strings.Contains(err.Error(), "create worker selector") {
				t.Fatalf("run did not return factory failure: %v", err)
			}
		})
	}
}
