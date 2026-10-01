package loadgen

import (
	"context"
	"io"
	"net/http"
	"time"
)

// WorkerRecordingReport 保存一次文件记录的配置、时间和退出事实。
// StartedAt 为零表示文件记录尚未启动；序列化由单独的清单写入函数负责。
type WorkerRecordingReport struct {
	Config     WorkerSamplingConfig // 实际采样配置。
	OutputPath string               // 实际输出路径。

	StartedAt  time.Time // 文件创建成功后、开始采样前。
	FinishedAt time.Time // 文件关闭尝试完成后。

	SamplesWritten    int64 // 行输出函数返回 nil 的记录数。
	SuccessfulSamples int64 // 已写出记录中，查询成功的数量。
	FailedSamples     int64 // 已写出记录中，查询失败的数量。

	SamplingErr error // RunWorkerSampling 返回的原错误。
	OutputErr   error // emit 中行输出函数返回的原错误。
	CloseErr    error // 关闭样本文件时返回的原错误。
}

// WorkerRecordingResult 分别保存样本记录与清单保存的结果。
type WorkerRecordingResult struct {
	// 样本文件层的原始报告，保留实际绝对路径。
	Recording WorkerRecordingReport

	// 清单绝对路径，目录创建成功后填写。
	ManifestPath string

	// 清单创建、编码、写入或关闭错误；多个错误合并保留。
	ManifestErr error

	// 仅清单写入和关闭都成功时为 true。
	// 不表示查询全部成功、实验成功或数据已持久化到磁盘。
	ManifestSaved bool
}

// RunWorkerSamplingToFile 独占创建并关闭样本文件，失败时保留已写出的事实。
// client 由调用方拥有；报告中的成功数包含查询成功但限制未启用的响应，不表示已经取得有效名额计数。
func RunWorkerSamplingToFile(ctx context.Context, client *http.Client, cfg WorkerSamplingConfig, outputPath string) (WorkerRecordingReport, error) {
	report, err := runSamplingToFile(ctx, client, cfg, outputPath, RunWorkerSampling, WriteWorkerSampleJSON, func(s WorkerSample) bool { return s.Err == nil })
	return WorkerRecordingReport(report), err
}

// RunWorkerRecording 在新的 outputDir 中保存 samples.jsonl 和 manifest.json。
// 前置失败不创建目录；启动后取消仍保存清单，不覆盖旧目录、不关闭 client。
func RunWorkerRecording(ctx context.Context, client *http.Client, cfg WorkerSamplingConfig, outputDir string) (WorkerRecordingResult, error) {
	result, err := runRecording(ctx, client, cfg, outputDir, RunWorkerSampling, WriteWorkerSampleJSON, func(s WorkerSample) bool { return s.Err == nil },
		func(w io.Writer, r recordingReport[WorkerSamplingConfig]) error {
			return WriteWorkerRecordingJSON(w, WorkerRecordingReport(r))
		})
	return WorkerRecordingResult{Recording: WorkerRecordingReport(result.Recording), ManifestPath: result.ManifestPath, ManifestErr: result.ManifestErr, ManifestSaved: result.ManifestSaved}, err
}
