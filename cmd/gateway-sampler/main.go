package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// main 解析参数、注册停止信号、输出摘要并决定进程退出状态。
// 调用 os.Exit 前显式释放 signal 资源。
func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := parseSamplerConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		logger.Error("invalid gateway sampler arguments", "error", err)
		os.Exit(1)
	}

	logger.Info(
		"starting gateway sampling",
		"url", cfg.Sampling.Endpoint,
		"interval", cfg.Sampling.Interval,
		"request_timeout", cfg.Sampling.RequestTimeout,
		"duration", cfg.Duration,
		"output_dir", cfg.OutputDir,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	result, err := run(ctx, cfg)

	// os.Exit 不执行 defer，因此在决定退出状态前显式释放 signal 资源。
	stop()

	logger.Info(
		"gateway sampling summary",
		"output_dir", cfg.OutputDir,
		"samples_written", result.Recording.SamplesWritten,
		"successful_samples", result.Recording.SuccessfulSamples,
		"failed_samples", result.Recording.FailedSamples,
		"samples_path", result.Recording.OutputPath,
		"manifest_path", result.ManifestPath,
		"manifest_saved", result.ManifestSaved,
	)

	if err != nil {
		logger.Error("gateway sampling failed", "error", err)
		os.Exit(1)
	}

	logger.Info("gateway sampling completed")
}
