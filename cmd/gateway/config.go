package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/secacy/tide-artisan/internal/gateway"
)

const (
	workerStrategyRoundRobin         = "round_robin"
	workerStrategyWeightedRoundRobin = "weighted_round_robin"
)

// gatewayConfig 描述 Gateway 进程的启动配置。
type gatewayConfig struct {
	WorkerAddrs []string // 有序后端地址；顺序决定选择顺序，加权策略按相同位置对应权重。

	WorkerStrategy string  // 已校验的选择策略，默认为普通轮询。
	WorkerWeights  []int64 // 加权策略的正权重列表；普通轮询时为 nil。

	Gateway gateway.Config // 会话处理与保护配置。
}

// parseGatewayConfig 解析启动参数，不创建连接或启动服务。
// 地址列表在此校验；音频预算等业务配置继续由 gateway.New 校验。
func parseGatewayConfig(args []string) (gatewayConfig, error) {
	cfg := gatewayConfig{
		WorkerAddrs:    []string{"localhost:50051"},
		WorkerStrategy: workerStrategyRoundRobin,
		Gateway:        gateway.Config{MaxMessageBytes: 1024 * 1024},
	}
	workerAddrs := strings.Join(cfg.WorkerAddrs, ",")
	var workerWeights string
	var enableV2 bool
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.BoolVar(&enableV2, "enable-v2", false, "enable the experimental resumable /v2/asr endpoint")
	fs.Int64Var(
		&cfg.Gateway.MaxPendingAudioBytes,
		"max-pending-audio-bytes",
		0,
		"maximum pending raw audio bytes; 0 disables the limit, positive values enable it, negative values are invalid",
	)
	fs.StringVar(
		&workerAddrs,
		"workers",
		workerAddrs,
		"comma-separated list of gRPC worker addresses; order determines worker selection order and corresponding weight positions",
	)
	fs.StringVar(
		&cfg.WorkerStrategy,
		"worker-strategy",
		workerStrategyRoundRobin,
		"worker selection strategy: round_robin or weighted_round_robin",
	)
	fs.StringVar(
		&workerWeights,
		"worker-weights",
		"",
		"comma-separated positive worker weights; required for weighted_round_robin",
	)

	if err := fs.Parse(args); err != nil {
		return gatewayConfig{}, err
	}

	if fs.NArg() != 0 {
		return gatewayConfig{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	addrs, err := parseWorkerAddresses(workerAddrs)
	if err != nil {
		return gatewayConfig{}, err
	}

	weightsProvided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "worker-weights" {
			weightsProvided = true
		}
	})

	switch cfg.WorkerStrategy {
	case workerStrategyRoundRobin:
		if weightsProvided {
			return gatewayConfig{}, fmt.Errorf("worker weights must not be provided for strategy %q", workerStrategyRoundRobin)
		}

	case workerStrategyWeightedRoundRobin:
		if !weightsProvided || workerWeights == "" {
			return gatewayConfig{}, fmt.Errorf("worker weights are required for strategy %q", workerStrategyWeightedRoundRobin)
		}

		weights, err := parseWorkerWeights(workerWeights, len(addrs))
		if err != nil {
			return gatewayConfig{}, err
		}
		cfg.WorkerWeights = weights

	default:
		return gatewayConfig{}, fmt.Errorf("unknown worker strategy %q", cfg.WorkerStrategy)
	}

	cfg.WorkerAddrs = addrs
	if enableV2 {
		cfg.Gateway.V2 = &gateway.V2Config{}
	}
	return cfg, nil
}
