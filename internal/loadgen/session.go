package loadgen

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// validate 检查会话负载参数，不建立连接。
// RunSession 和 RunBatch 共用，避免无效配置启动一批失败任务。
func (cfg SessionConfig) validate() error {
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be a positive number")
	}
	if cfg.ChunkBytes <= 0 {
		return fmt.Errorf("chunk bytes must be a positive number")
	}
	if cfg.ChunkBytes%audio.BytesDepth != 0 {
		return fmt.Errorf("chunk bytes %d must align to %d-byte PCM samples", cfg.ChunkBytes, audio.BytesDepth)
	}
	if cfg.AudioBytes <= 0 {
		return fmt.Errorf("audio bytes must be a positive number")
	}
	if cfg.AudioBytes%int64(audio.BytesDepth) != 0 {
		return fmt.Errorf("audio bytes %d must align to %d-byte PCM samples", cfg.AudioBytes, audio.BytesDepth)
	}
	if cfg.ExpectedFinalText == "" {
		return fmt.Errorf("expected final text must be a non-empty string")
	}
	if cfg.URL == "" {
		return fmt.Errorf("URL must be a non-empty string")
	}
	return nil
}

// SessionConfig 定义一次 Mock 负载会话的输入和验证条件。
type SessionConfig struct {
	URL               string        // Gateway WebSocket 地址。
	AudioBytes        int64         // 计划音频总字节数，正值且采样对齐。
	ChunkBytes        int           // 每块字节上限，正值且采样对齐。
	Realtime          bool          // 是否按现有 Pacer 节奏发送。
	Timeout           time.Duration // 整场期限，包含拨号、发送和等待关闭。
	ExpectedFinalText string        // 本次 Mock 的预期尾部文本，必须非空。
}

// SessionOutcome 表示一次已开始尝试的最终分类。
type SessionOutcome string

const (
	SessionCompleted SessionOutcome = "completed" // 满足当前 Mock 的完整完成条件。
	SessionFailed    SessionOutcome = "failed"    // 运行或完整性检查失败。
	SessionCanceled  SessionOutcome = "canceled"  // 运行失败且观察到取消。
	SessionTimedOut  SessionOutcome = "timed_out" // 运行失败且观察到期限到达。
)

// SessionReport 保存一次尝试的配置、时间和完整观测。
// 它不代表服务端资源已经回收，也不评价识别质量。
type SessionReport struct {
	Config      SessionConfig      // 本次采用的负载参数。
	StartedAt   time.Time          // 整场尝试开始时间，包含拨号。
	FinishedAt  time.Time          // Run 返回后立即记录的时间。
	Outcome     SessionOutcome     // 最终分类。
	Observation SessionObservation // Run 返回后的记录器快照。

	// 仅完整完成时填写；nil 表示没有可用样本，不是零延迟。
	TailLatency *time.Duration
}

// RunSession 执行一场有限输入的 Mock 会话，结束后形成报告。
// 前置配置或构造失败返回零报告和错误。
// 已开始的尝试即使失败，也返回包含已有观测的报告和非 nil 错误。
func RunSession(ctx context.Context, cfg SessionConfig) (SessionReport, error) {
	// 1. 检查参数、准备组件
	if err := cfg.validate(); err != nil {
		return SessionReport{}, err
	}
	source, err := NewSilenceSource(cfg.AudioBytes)
	if err != nil {
		return SessionReport{}, fmt.Errorf("unable to create Silence source: %w", err)
	}
	var recorder SessionRecorder
	var lastResult wsprotocol.ResultMessage

	client, err := wsclient.New(wsclient.Config{
		URL:        cfg.URL,
		ChunkBytes: cfg.ChunkBytes,
		Realtime:   cfg.Realtime,

		OnWrite: recorder.ObserveWrite,
		OnResult: func(result wsprotocol.ResultMessage, receivedAt time.Time) {
			recorder.ObserveResult(result, receivedAt)
			lastResult = result
		},
	})
	if err != nil {
		return SessionReport{}, fmt.Errorf("unable to create client: %w", err)
	}

	// 2. 运行并等待收尾
	report := SessionReport{
		Config:    cfg,
		StartedAt: time.Now(),
	}

	sessionCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	runErr := client.Run(sessionCtx, source)

	report.FinishedAt = time.Now()
	ctxErr := sessionCtx.Err()
	report.Observation = recorder.Snapshot()

	// 3. 处理运行错误
	if runErr != nil {
		combinedErr := errors.Join(runErr, ctxErr)

		switch {
		case errors.Is(runErr, context.DeadlineExceeded),
			errors.Is(ctxErr, context.DeadlineExceeded):
			report.Outcome = SessionTimedOut

		case errors.Is(runErr, context.Canceled),
			errors.Is(ctxErr, context.Canceled):
			report.Outcome = SessionCanceled

		default:
			report.Outcome = SessionFailed
		}

		return report, fmt.Errorf("run session: %w", combinedErr)
	}

	// Run 正常结束后，不再使用随后可能发生的 context 取消改写结果。
	// 接下来只判断本场 Mock 会话是否“完整完成”。

	observation := report.Observation

	if err := validateSessionCompletion(cfg, observation, lastResult); err != nil {
		report.Outcome = SessionFailed
		return report, fmt.Errorf("session incomplete: %w", err)
	}

	tail := observation.LastResultAt.Sub(
		observation.EndWrite.StartedAt,
	)

	report.Outcome = SessionCompleted
	report.TailLatency = &tail

	return report, nil
}

// validateSessionCompletion 核对正常返回的会话是否满足当前 Mock 的完整完成条件。
// 只检查已收集的事实；状态分类和尾部等待计算由 RunSession 负责。
func validateSessionCompletion(cfg SessionConfig, observation SessionObservation, lastResult wsprotocol.ResultMessage) error {
	if observation.StartWrite.Kind != wsclient.WriteStart {
		return errors.New("start write was not observed")
	}
	if observation.StartWrite.Err != nil {
		return fmt.Errorf("start write failed: %w", observation.StartWrite.Err)
	}

	if observation.WriteFailures != 0 {
		return fmt.Errorf("observed %d write failures", observation.WriteFailures)
	}

	if observation.AudioBytesWritten != cfg.AudioBytes {
		return fmt.Errorf("wrote %d audio bytes, want %d", observation.AudioBytesWritten, cfg.AudioBytes)
	}

	if observation.EndWrite.Kind != wsclient.WriteEnd {
		return errors.New("end write was not observed")
	}
	if observation.EndWrite.Err != nil {
		return fmt.Errorf("end write failed: %w", observation.EndWrite.Err)
	}

	if observation.ResultCount == 0 {
		return errors.New("no result received")
	}
	if !lastResult.IsFinal {
		return errors.New("last result is not final")
	}
	if lastResult.Text != cfg.ExpectedFinalText {
		return fmt.Errorf("final text mismatch: got %q, want %q", lastResult.Text, cfg.ExpectedFinalText)
	}

	if observation.LastResultAt.IsZero() {
		return errors.New("last result time is zero")
	}
	if observation.EndWrite.StartedAt.IsZero() {
		return errors.New("end write start time is zero")
	}
	if observation.LastResultAt.Before(observation.EndWrite.StartedAt) {
		return errors.New(
			"last result arrived before end write started",
		)
	}

	return nil
}
