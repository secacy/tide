package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// WorkerSamplingConfig 指定串行采样条件，两个时长都必须为正。
type WorkerSamplingConfig struct {
	Endpoint       string        // 完整的 http/https 查询 URL，必须包含主机名。
	Interval       time.Duration // 上条样本交付完成后的等待时长，不是固定起点周期
	RequestTimeout time.Duration // 单次查询期限，包含响应体读取
}

// Validate 只检查配置，不创建请求、连接或后台任务。
func (cfg WorkerSamplingConfig) Validate() error {
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid worker sampling endpoint %q: %w", cfg.Endpoint, err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid worker sampling endpoint scheme %q: want http or https", parsed.Scheme)
	}

	if parsed.Hostname() == "" {
		return fmt.Errorf("invalid worker sampling endpoint %q: hostname is empty", cfg.Endpoint)
	}

	if cfg.Interval <= 0 {
		return fmt.Errorf("worker sampling interval must be greater than zero: %s", cfg.Interval)
	}

	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("worker sampling request timeout must be greater than zero: %s", cfg.RequestTimeout)
	}

	return nil
}

// WorkerSample 保存一次已开始的查询尝试，包含失败尝试。
type WorkerSample struct {
	Index int // 本次运行内从 0 递增的尝试序号。

	StartedAt  time.Time // 查询前记录，保留单调时钟信息。
	FinishedAt time.Time // 查询返回后立即记录。

	// FinishedAt.Sub(StartedAt)，不包含交付和后续等待。
	Duration time.Duration

	State *WorkerState // 成功时非 nil，包括限制未启用；失败时 nil。
	Err   error        // 查询原始错误；成功时 nil。
}

// RunWorkerSampling 在当前 goroutine 中串行查询并交付样本。
// ctx 控制整体运行；client 由调用方创建并复用。
// emit 必须非 nil 且及时返回，每次查询结果同步交付一次。
// 查询失败继续采样；父 context 结束或 emit 失败时退出。
// 不保存样本历史，不关闭 client，不创建后台 goroutine。
func RunWorkerSampling(
	ctx context.Context,
	client *http.Client,
	cfg WorkerSamplingConfig,
	emit func(WorkerSample) error,
) error {
	if ctx == nil {
		return errors.New("run worker sampling: context is nil")
	}
	if client == nil {
		return errors.New("run worker sampling: client is nil")
	}
	if emit == nil {
		return errors.New("run worker sampling: emit is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("run worker sampling: invalid config: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("run worker sampling: context already done: %w", err)
	}

	err := runSamplingLoop(ctx, cfg.Interval, cfg.RequestTimeout,
		func(requestCtx context.Context) (WorkerState, error) {
			return FetchWorkerSnapshot(requestCtx, client, cfg.Endpoint)
		},
		func(sample samplingAttempt[WorkerState]) error {
			return emit(WorkerSample{
				Index: sample.Index, StartedAt: sample.StartedAt, FinishedAt: sample.FinishedAt,
				Duration: sample.Duration, State: sample.State, Err: sample.Err,
			})
		},
	)
	if err != nil {
		return fmt.Errorf("run worker sampling: %w", err)
	}
	return nil
}
