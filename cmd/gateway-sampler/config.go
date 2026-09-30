package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// samplerConfig 描述一次采样命令的配置。
type samplerConfig struct {
	// 地址、查询间隔与单次查询期限，复用已有核心配置。
	Sampling loadgen.GatewaySamplingConfig

	// 整体采样预算，必须为正。
	// 从运行层建立 context 起计算，不保证文件收尾也在此期限内完成。
	Duration time.Duration

	// 运行时独占创建的新目录；解析阶段保留用户输入。
	OutputDir string
}

// parseSamplerConfig 解析不含程序名的命令参数并校验。
// 不访问网络，不检查或创建目录，不注册信号，不退出进程。
// 请求帮助时返回 flag.ErrHelp；任何错误均返回零配置。
func parseSamplerConfig(args []string) (samplerConfig, error) {
	cfg := samplerConfig{
		Sampling: loadgen.GatewaySamplingConfig{
			Endpoint:       "http://localhost:8080/debug/gateway",
			Interval:       100 * time.Millisecond,
			RequestTimeout: time.Second,
		},
		Duration: 30 * time.Second,
	}

	fs := flag.NewFlagSet("gateway-sampler", flag.ContinueOnError)

	fs.StringVar(&cfg.Sampling.Endpoint, "url", cfg.Sampling.Endpoint, "Gateway snapshot endpoint URL")
	fs.DurationVar(&cfg.Sampling.Interval, "interval", cfg.Sampling.Interval, "wait after each delivered sample before the next query")
	fs.DurationVar(&cfg.Sampling.RequestTimeout, "request-timeout", cfg.Sampling.RequestTimeout, "timeout for each Gateway snapshot request")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "sampling time budget; file finalization may take longer")
	fs.StringVar(&cfg.OutputDir, "output-dir", "", "new output directory for samples and manifest")

	if err := fs.Parse(args); err != nil {
		return samplerConfig{}, err
	}

	if fs.NArg() != 0 {
		return samplerConfig{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	if err := cfg.Sampling.Validate(); err != nil {
		return samplerConfig{}, fmt.Errorf("invalid sampling config: %w", err)
	}

	if cfg.Duration <= 0 {
		return samplerConfig{}, fmt.Errorf("duration must be greater than zero: %s", cfg.Duration)
	}

	if strings.TrimSpace(cfg.OutputDir) == "" {
		return samplerConfig{}, fmt.Errorf("output directory is required")
	}

	if cfg.OutputDir == "-" {
		return samplerConfig{}, fmt.Errorf(`output directory "-" is not supported`)
	}

	return cfg, nil
}
