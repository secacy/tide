package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// errSamplingDurationReached 标记本命令设置的采样预算到期。
// 用于区别父 context 的提前取消或到期。
var errSamplingDurationReached = errors.New("sampling duration reached")

// run 执行一次有限时长采样，创建并回收自己的 HTTP 连接池。
// ctx 控制外部停止；cfg 描述采样条件、运行预算与输出目录。
// 返回原始文件编排结果；error 表示命令是否完成约定的运行。
// 不修改报告中的原始错误，不注册信号，不退出进程。
func run(
	ctx context.Context,
	cfg samplerConfig,
) (loadgen.WorkerRecordingResult, error) {
	if ctx == nil {
		return loadgen.WorkerRecordingResult{}, errors.New("run worker sampler: context is nil")
	}
	if cfg.Duration <= 0 {
		return loadgen.WorkerRecordingResult{}, fmt.Errorf("run worker sampler: duration must be greater than zero: %s", cfg.Duration)
	}
	if err := ctx.Err(); err != nil {
		return loadgen.WorkerRecordingResult{}, fmt.Errorf("run worker sampler: context already done: %w", err)
	}

	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return loadgen.WorkerRecordingResult{}, fmt.Errorf("run worker sampler: http.DefaultTransport has type %T, want *http.Transport", http.DefaultTransport)
	}

	transport := base.Clone()
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
	}

	runCtx, cancel := context.WithTimeoutCause(
		ctx,
		cfg.Duration,
		errSamplingDurationReached,
	)
	defer cancel()

	result, recordingErr := loadgen.RunWorkerRecording(
		runCtx,
		client,
		cfg.Sampling,
		cfg.OutputDir,
	)

	stopCause := context.Cause(runCtx)
	commandErr := samplerRunError(result, recordingErr, stopCause)

	return result, commandErr
}

// samplerRunError 判断一次采样是否满足命令的完成条件。
// result 是文件编排结果，recordingErr 是核心返回的原始错误。
// stopCause 是运行 context 的结束原因，须在主动 cancel 前读取。
// 不执行 I/O，不修改输入；满足完成条件时返回 nil。
func samplerRunError(
	result loadgen.WorkerRecordingResult,
	recordingErr error,
	stopCause error,
) error {
	if result.Recording.StartedAt.IsZero() {
		if recordingErr != nil {
			return errors.Join(recordingErr, errors.New("worker sampling recording did not start"))
		}
		if result.ManifestErr != nil {
			return errors.Join(result.ManifestErr, errors.New("worker sampling recording did not start"))
		}
		return errors.New("worker sampling recording did not start")
	}

	var saveErr error

	if result.Recording.OutputErr != nil {
		saveErr = errors.Join(saveErr, fmt.Errorf("worker sample output failed: %w", result.Recording.OutputErr))
	}

	if result.Recording.CloseErr != nil {
		saveErr = errors.Join(saveErr, fmt.Errorf("worker sample file close failed: %w", result.Recording.CloseErr))
	}

	if result.ManifestErr != nil {
		saveErr = errors.Join(saveErr, fmt.Errorf("worker manifest save failed: %w", result.ManifestErr))
	}

	if !result.ManifestSaved {
		saveErr = errors.Join(saveErr, errors.New("worker manifest was not saved"))
	}

	if saveErr != nil {
		return errors.Join(recordingErr, saveErr)
	}

	if !errors.Is(stopCause, errSamplingDurationReached) {
		if stopCause != nil {
			return errors.Join(recordingErr, fmt.Errorf("worker sampling stopped before planned duration: %w", stopCause))
		}
		return errors.Join(recordingErr, errors.New("worker sampling returned before planned duration"))
	}

	if !errors.Is(recordingErr, context.DeadlineExceeded) {
		return errors.Join(recordingErr, errors.New("worker recording did not stop with context deadline exceeded"))
	}

	if !errors.Is(result.Recording.SamplingErr, context.DeadlineExceeded) {
		return errors.Join(
			recordingErr,
			result.Recording.SamplingErr,
			errors.New("worker sampling loop did not stop with context deadline exceeded"),
		)
	}

	if result.Recording.SuccessfulSamples <= 0 {
		return errors.Join(recordingErr, errors.New("sampling produced no valid worker snapshots"))
	}

	return nil
}
