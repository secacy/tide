package main

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// loadConfig 描述一次负载命令的输入与输出位置。
type loadConfig struct {
	Batch      loadgen.BatchConfig // 有限批次的负载条件。
	OutputPath string              // 报告路径；解析阶段不创建文件。
}

// parseLoadConfig 解析不含程序名的参数并校验。
// 不运行负载或访问文件；请求帮助时返回 flag.ErrHelp。
// 解析或校验失败时返回零配置和错误。
func parseLoadConfig(args []string) (loadConfig, error) {
	cfg := loadConfig{
		Batch: loadgen.BatchConfig{
			Sessions: 1,
			Session: loadgen.SessionConfig{
				URL:        "ws://localhost:8080/v1/asr",
				AudioBytes: 1920000,
				ChunkBytes: 3200,
				Realtime:   true,
				Timeout:    90 * time.Second,
			},
		},
	}

	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	fs.StringVar(&cfg.Batch.Session.URL, "url", cfg.Batch.Session.URL, "Gateway WebSocket URL")
	fs.IntVar(&cfg.Batch.Sessions, "sessions", cfg.Batch.Sessions, "number of sessions to run")
	fs.Int64Var(&cfg.Batch.Session.AudioBytes, "audio-bytes", cfg.Batch.Session.AudioBytes, "audio bytes per session")
	fs.IntVar(&cfg.Batch.Session.ChunkBytes, "chunk-bytes", cfg.Batch.Session.ChunkBytes, "maximum audio bytes per chunk")
	fs.BoolVar(&cfg.Batch.Session.Realtime, "realtime", cfg.Batch.Session.Realtime, "pace audio in real time")
	fs.DurationVar(&cfg.Batch.Session.Timeout, "session-timeout", cfg.Batch.Session.Timeout, "timeout for each session")
	fs.StringVar(&cfg.Batch.Session.ExpectedFinalText, "expected-final-text", "", "expected final Mock result text")
	fs.StringVar(&cfg.OutputPath, "output", "", "JSON report output path")

	if err := fs.Parse(args); err != nil {
		return loadConfig{}, err
	}

	if fs.NArg() != 0 {
		return loadConfig{}, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	if err := cfg.Batch.Validate(); err != nil {
		return loadConfig{}, fmt.Errorf("invalid load config: %w", err)
	}

	parsedURL, err := url.Parse(cfg.Batch.Session.URL)
	if err != nil {
		return loadConfig{}, fmt.Errorf("invalid url %q: %w", cfg.Batch.Session.URL, err)
	}

	if parsedURL.Scheme != "ws" && parsedURL.Scheme != "wss" {
		return loadConfig{}, fmt.Errorf("invalid url scheme %q: want ws or wss", parsedURL.Scheme)
	}

	if parsedURL.Hostname() == "" {
		return loadConfig{}, fmt.Errorf("invalid url %q: hostname is empty", cfg.Batch.Session.URL)
	}

	if strings.TrimSpace(cfg.OutputPath) == "" {
		return loadConfig{}, fmt.Errorf("output path must not be empty")
	}

	if cfg.OutputPath == "-" {
		return loadConfig{}, fmt.Errorf("output path %q is not supported", cfg.OutputPath)
	}

	return cfg, nil
}
