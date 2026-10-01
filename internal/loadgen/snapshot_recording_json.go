package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// samplingConfigJSON 保存采样配置。
type samplingConfigJSON struct {
	Endpoint         string `json:"endpoint"`           // 状态查询 URL。
	IntervalNS       int64  `json:"interval_ns"`        // 样本交付后的等待间隔，纳秒。
	RequestTimeoutNS int64  `json:"request_timeout_ns"` // 单次查询期限，纳秒。
}

// recordingJSON 是采样运行清单 v1。
type recordingJSON struct {
	SourceKind    string             `json:"source_kind,omitempty"` // 新的 Worker 格式标记；历史 Gateway v1 省略。
	SchemaVersion int                `json:"schema_version"`        // 固定为 1。
	Config        samplingConfigJSON `json:"config"`                // 实际采样配置。
	OutputPath    string             `json:"output_path"`           // JSONL 样本文件路径。

	StartedAt  time.Time `json:"started_at"`  // 文件记录开始时间，UTC。
	FinishedAt time.Time `json:"finished_at"` // 文件关闭尝试完成时间，UTC。

	SamplesWritten    int64 `json:"samples_written"`    // 成功写出的样本行数。
	SuccessfulSamples int64 `json:"successful_samples"` // 查询成功且已写出的样本数。
	FailedSamples     int64 `json:"failed_samples"`     // 查询失败但已写出的样本数。

	StopReason string `json:"stop_reason"` // 采样循环的结构化退出原因。

	SamplingError *string `json:"sampling_error"` // 采样循环原始错误；无错误为 null。
	OutputError   *string `json:"output_error"`   // 样本写入错误；无错误为 null。
	CloseError    *string `json:"close_error"`    // 样本文件关闭错误；无错误为 null。
}

// recordingStopReason 分类采样循环的退出原因。
// 文件关闭错误单独保存，不参与此分类。
func recordingStopReason[C recordingConfigValidator](report recordingReport[C]) string {
	switch {
	case report.OutputErr != nil:
		return "output_error"
	case errors.Is(report.SamplingErr, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(report.SamplingErr, context.Canceled):
		return "canceled"
	case report.SamplingErr != nil:
		return "sampling_error"
	default:
		return "returned"
	}
}

// writeRecordingJSON 将已收尾的记录报告导出为清单 v1。
// 报告中的运行、输出和关闭错误是待保存事实，不阻止有效清单输出。
// 返回错误仅表示校验、编码或清单写入失败。
// 不修改 report，不访问 OutputPath，不创建、关闭或刷新 writer。
func writeRecordingJSON[C recordingConfigValidator](w io.Writer, report recordingReport[C], config samplingConfigJSON, sourceKind string) error {
	if w == nil {
		return errors.New("write snapshot recording json: writer is nil")
	}

	if err := report.Config.Validate(); err != nil {
		return fmt.Errorf("write snapshot recording json: invalid config: %w", err)
	}

	if strings.TrimSpace(report.OutputPath) == "" {
		return errors.New("write snapshot recording json: output path is empty")
	}

	if report.OutputPath == "-" {
		return errors.New(`write snapshot recording json: output path "-" is not supported`)
	}

	if report.StartedAt.IsZero() {
		return errors.New("write snapshot recording json: started_at is zero")
	}

	if report.FinishedAt.IsZero() {
		return errors.New("write snapshot recording json: finished_at is zero")
	}

	if report.SamplesWritten < 0 {
		return fmt.Errorf("write snapshot recording json: samples_written is negative: %d", report.SamplesWritten)
	}

	if report.SuccessfulSamples < 0 {
		return fmt.Errorf("write snapshot recording json: successful_samples is negative: %d", report.SuccessfulSamples)
	}

	if report.FailedSamples < 0 {
		return fmt.Errorf("write snapshot recording json: failed_samples is negative: %d", report.FailedSamples)
	}

	if report.SuccessfulSamples > report.SamplesWritten {
		return fmt.Errorf(
			"write snapshot recording json: successful_samples %d exceeds samples_written %d",
			report.SuccessfulSamples,
			report.SamplesWritten,
		)
	}

	if report.FailedSamples != report.SamplesWritten-report.SuccessfulSamples {
		return fmt.Errorf(
			"write snapshot recording json: failed_samples is %d, want %d",
			report.FailedSamples,
			report.SamplesWritten-report.SuccessfulSamples,
		)
	}

	if report.OutputErr != nil && report.SamplingErr == nil {
		return errors.New("write snapshot recording json: output error exists but sampling error is nil")
	}

	output := recordingJSON{
		SchemaVersion:     1,
		Config:            config,
		SourceKind:        sourceKind,
		OutputPath:        report.OutputPath,
		StartedAt:         report.StartedAt.UTC(),
		FinishedAt:        report.FinishedAt.UTC(),
		SamplesWritten:    report.SamplesWritten,
		SuccessfulSamples: report.SuccessfulSamples,
		FailedSamples:     report.FailedSamples,
		StopReason:        recordingStopReason(report),
		SamplingError:     errorText(report.SamplingErr),
		OutputError:       errorText(report.OutputErr),
		CloseError:        errorText(report.CloseErr),
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("write snapshot recording json: encode: %w", err)
	}

	data = append(data, '\n')

	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("write snapshot recording json: write: %w", err)
	}

	if n != len(data) {
		return fmt.Errorf("write snapshot recording json: wrote %d of %d bytes: %w", n, len(data), io.ErrShortWrite)
	}

	return nil
}
