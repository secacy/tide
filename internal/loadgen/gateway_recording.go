package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// GatewayRecordingReport 保存一次文件记录的配置、时间和退出事实。
// StartedAt 为零表示文件记录尚未启动；本步不直接序列化。
type GatewayRecordingReport struct {
	Config     GatewaySamplingConfig // 实际采样配置。
	OutputPath string                // 实际输出路径。

	StartedAt  time.Time // 文件创建成功后、开始采样前。
	FinishedAt time.Time // 文件关闭尝试完成后。

	SamplesWritten    int64 // 行输出函数返回 nil 的记录数。
	SuccessfulSamples int64 // 已写出记录中，查询成功的数量。
	FailedSamples     int64 // 已写出记录中，查询失败的数量。

	SamplingErr error // RunGatewaySampling 返回的原错误。
	OutputErr   error // emit 中行输出函数返回的原错误。
	CloseErr    error // 关闭样本文件时返回的原错误。
}

// RunGatewaySamplingToFile 独占创建样本文件，并在返回前关闭。
// 前置校验失败返回零报告，不创建文件、不发起查询。
// 文件创建后即使取消或失败，也保留文件并返回已有运行事实。
// 返回错误合并采样与关闭错误；client 仍由调用方拥有。
func RunGatewaySamplingToFile(
	ctx context.Context,
	client *http.Client,
	cfg GatewaySamplingConfig,
	outputPath string,
) (report GatewayRecordingReport, err error) {
	if ctx == nil {
		return GatewayRecordingReport{}, errors.New("run gateway sampling to file: context is nil")
	}
	if client == nil {
		return GatewayRecordingReport{}, errors.New("run gateway sampling to file: client is nil")
	}
	if err := cfg.Validate(); err != nil {
		return GatewayRecordingReport{}, fmt.Errorf("run gateway sampling to file: invalid config: %w", err)
	}
	if strings.TrimSpace(outputPath) == "" {
		return GatewayRecordingReport{}, errors.New("run gateway sampling to file: output path is empty")
	}
	if outputPath == "-" {
		return GatewayRecordingReport{}, errors.New(`run gateway sampling to file: output path "-" is not supported`)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return GatewayRecordingReport{}, fmt.Errorf("run gateway sampling to file: context already done: %w", ctxErr)
	}

	file, openErr := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if openErr != nil {
		return GatewayRecordingReport{}, fmt.Errorf("create gateway samples %q: %w", outputPath, openErr)
	}

	report = GatewayRecordingReport{
		Config:     cfg,
		OutputPath: outputPath,
		StartedAt:  time.Now(),
	}

	defer func() {
		report.CloseErr = file.Close()
		report.FinishedAt = time.Now()

		if report.CloseErr != nil {
			err = errors.Join(err, fmt.Errorf("close gateway samples %q: %w", outputPath, report.CloseErr))
		}
	}()

	emit := func(sample GatewaySample) error {
		if writeErr := WriteGatewaySampleJSON(file, sample); writeErr != nil {
			report.OutputErr = writeErr
			return writeErr
		}

		report.SamplesWritten++

		if sample.Err == nil {
			report.SuccessfulSamples++
		} else {
			report.FailedSamples++
		}

		return nil
	}

	report.SamplingErr = RunGatewaySampling(ctx, client, cfg, emit)

	return report, report.SamplingErr
}
