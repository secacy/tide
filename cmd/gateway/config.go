package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/secacy/tide-artisan/internal/gateway"
)

// gatewayConfig 描述 Gateway 进程的启动配置。
// 后端地址用于组装依赖，Gateway 描述会话处理与保护规则。
type gatewayConfig struct {
	WorkerAddrs []string       // 有序后端地址列表，顺序决定轮询起点和顺序。
	Gateway     gateway.Config // 传给 gateway.New 的业务配置。
}

// parseGatewayConfig 解析启动参数，不创建连接或启动服务。
// 地址列表在此校验；音频预算等业务配置继续由 gateway.New 校验。
func parseGatewayConfig(args []string) (gatewayConfig, error) {
	cfg := gatewayConfig{
		WorkerAddrs: []string{"localhost:50051"},
		Gateway:     gateway.Config{MaxMessageBytes: 1024 * 1024},
	}
	workerAddrs := strings.Join(cfg.WorkerAddrs, ",")
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
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
		"comma-separated list of gRPC worker addresses; order matters for round-robin selection",
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
	cfg.WorkerAddrs = addrs
	return cfg, nil
}
