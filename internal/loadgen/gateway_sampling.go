package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// GatewaySamplingConfig 指定串行采样条件，两个时长都必须为正。
type GatewaySamplingConfig struct {
	Endpoint       string        // 完整的 http/https 查询 URL，必须包含主机名。
	Interval       time.Duration // 上条样本交付完成后的等待时长，不是固定起点周期
	RequestTimeout time.Duration // 单次查询期限，包含响应体读取
}

// Validate 只检查配置，不创建请求、连接或后台任务。
func (cfg GatewaySamplingConfig) Validate() error {
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid gateway sampling endpoint %q: %w", cfg.Endpoint, err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid gateway sampling endpoint scheme %q: want http or https", parsed.Scheme)
	}

	if parsed.Hostname() == "" {
		return fmt.Errorf("invalid gateway sampling endpoint %q: hostname is empty", cfg.Endpoint)
	}

	if cfg.Interval <= 0 {
		return fmt.Errorf("gateway sampling interval must be greater than zero: %s", cfg.Interval)
	}

	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("gateway sampling request timeout must be greater than zero: %s", cfg.RequestTimeout)
	}

	return nil
}

// GatewaySample 保存一次已开始的查询尝试，包含失败尝试。
type GatewaySample struct {
	Index int // 本次运行内从 0 递增的尝试序号。

	StartedAt  time.Time // 查询前记录，保留单调时钟信息。
	FinishedAt time.Time // 查询返回后立即记录。

	// FinishedAt.Sub(StartedAt)，不包含交付和后续等待。
	Duration time.Duration

	State *GatewayState // 成功时非 nil；失败时 nil。
	Err   error         // 查询原始错误；成功时 nil。
}

// RunGatewaySampling 在当前 goroutine 中串行查询并交付样本。
// ctx 控制整体运行；client 由调用方创建并复用。
// emit 必须非 nil 且及时返回，每次查询结果同步交付一次。
// 查询失败继续采样；父 context 结束或 emit 失败时退出。
// 不保存样本历史，不关闭 client，不创建后台 goroutine。
func RunGatewaySampling(
	ctx context.Context,
	client *http.Client,
	cfg GatewaySamplingConfig,
	emit func(GatewaySample) error,
) error {
	if ctx == nil {
		return errors.New("run gateway sampling: context is nil")
	}
	if client == nil {
		return errors.New("run gateway sampling: client is nil")
	}
	if emit == nil {
		return errors.New("run gateway sampling: emit is nil")
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("run gateway sampling: invalid config: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("run gateway sampling: context already done: %w", err)
	}

	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("run gateway sampling: context ended: %w", err)
		}

		startedAt := time.Now()

		requestCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
		state, fetchErr := FetchGatewaySnapshot(requestCtx, client, cfg.Endpoint)
		finishedAt := time.Now()
		cancel()

		sample := GatewaySample{
			Index:      index,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Duration:   finishedAt.Sub(startedAt),
			Err:        fetchErr,
		}

		if fetchErr == nil {
			stateCopy := state
			sample.State = &stateCopy
		}

		if err := emit(sample); err != nil {
			return fmt.Errorf("run gateway sampling: emit sample %d: %w", index, err)
		}

		if err := ctx.Err(); err != nil {
			return fmt.Errorf("run gateway sampling: context ended after sample %d: %w", index, err)
		}

		timer := time.NewTimer(cfg.Interval)

		select {
		case <-timer.C:
			// 下一轮继续。

		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("run gateway sampling: wait after sample %d: %w", index, ctx.Err())
		}
	}
}
