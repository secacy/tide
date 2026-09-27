package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/secacy/tide-artisan/internal/mockasr"
)

// workerConfig 描述 Worker 进程的启动配置。
// 监听地址由启动入口使用，Mock 描述语音处理与响应行为。
type workerConfig struct {
	ListenAddr string         // TCP 监听地址，例如 ":50051"。
	Mock       mockasr.Config // 传给 mockasr.New 的行为配置。
}

// parseWorkerConfig 将命令行参数转换成 Worker 启动配置。
// args 不包含程序名；本函数不创建 Worker、不监听端口、不退出进程。
// 请求帮助时返回 flag.ErrHelp，由 main 决定正常退出。
func parseWorkerConfig(args []string) (workerConfig, error) {
	cfg := workerConfig{
		ListenAddr: ":50051",
		Mock: mockasr.Config{
			PartialEvery:  500 * time.Millisecond,
			ResponseDelay: 50 * time.Millisecond,
			PartialTexts: []string{
				"今", "今天", "今天天气", "今天天气不错",
			},
			FinalText: "今天天气不错",
		},
	}

	fs := flag.NewFlagSet("asr-worker", flag.ContinueOnError)

	fs.StringVar(
		&cfg.ListenAddr,
		"listen",
		cfg.ListenAddr,
		"TCP listen address",
	)
	fs.IntVar(
		&cfg.Mock.ProcessingConcurrency,
		"processing-concurrency",
		cfg.Mock.ProcessingConcurrency,
		"maximum number of audio chunks processed concurrently; 0 disables the shared limit",
	)
	fs.DurationVar(
		&cfg.Mock.ProcessingDelay,
		"processing-delay",
		cfg.Mock.ProcessingDelay,
		"processing delay for each valid audio chunk",
	)
	fs.DurationVar(
		&cfg.Mock.ResponseDelay,
		"response-delay",
		cfg.Mock.ResponseDelay,
		"delay before sending ASR responses",
	)

	if err := fs.Parse(args); err != nil {
		return workerConfig{}, err
	}

	if fs.NArg() != 0 {
		return workerConfig{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	if strings.TrimSpace(cfg.ListenAddr) == "" {
		return workerConfig{}, fmt.Errorf("missing listen address")
	}

	if cfg.Mock.ProcessingDelay < 0 {
		return workerConfig{}, fmt.Errorf("processing-delay must be >= 0")
	}

	if cfg.Mock.ResponseDelay < 0 {
		return workerConfig{}, fmt.Errorf("response-delay must be >= 0")
	}

	return cfg, nil
}
