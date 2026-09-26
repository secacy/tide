package main

import (
	"flag"
	"fmt"

	"github.com/secacy/tide-artisan/internal/gateway"
)

// parseGatewayConfig 将启动参数转换成 Gateway 配置。
// args 不包含程序名；本函数不启动服务或建立连接。
// 配置的语义校验交给 gateway.New，例如拒绝负数预算。
func parseGatewayConfig(args []string) (gateway.Config, error) {
	cfg := gateway.Config{MaxMessageBytes: 1024 * 1024}
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.Int64Var(
		&cfg.MaxPendingAudioBytes,
		"max-pending-audio-bytes",
		0,
		"maximum pending raw audio bytes; 0 disables the limit, positive values enable it, negative values are invalid",
	)
	if err := fs.Parse(args); err != nil {
		return gateway.Config{}, err
	}
	if fs.NArg() != 0 {
		return gateway.Config{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	return cfg, nil
}
