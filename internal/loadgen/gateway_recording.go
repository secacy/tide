package loadgen

import (
	"context"
	"io"
	"net/http"
	"time"
)

// GatewayRecordingReport 保存一次文件记录的配置、时间和退出事实。
// StartedAt 为零表示文件记录尚未启动；序列化由单独的清单写入函数负责。
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

// GatewayRecordingResult 分别保存样本记录与清单保存的结果。
type GatewayRecordingResult struct {
	// 样本文件层的原始报告，保留实际绝对路径。
	Recording GatewayRecordingReport

	// 清单绝对路径，目录创建成功后填写。
	ManifestPath string

	// 清单创建、编码、写入或关闭错误；多个错误合并保留。
	ManifestErr error

	// 仅清单写入和关闭都成功时为 true。
	// 不表示查询全部成功、实验成功或数据已持久化到磁盘。
	ManifestSaved bool
}

// RunGatewaySamplingToFile 独占创建并关闭样本文件，失败时保留已写出的事实。
// client 由调用方拥有；查询失败样本也保留，采样和文件关闭错误分别记录。
func RunGatewaySamplingToFile(ctx context.Context, client *http.Client, cfg GatewaySamplingConfig, outputPath string) (GatewayRecordingReport, error) {
	report, err := runSamplingToFile(ctx, client, cfg, outputPath, RunGatewaySampling, WriteGatewaySampleJSON, func(s GatewaySample) bool { return s.Err == nil })
	return GatewayRecordingReport(report), err
}

// RunGatewayRecording 在新的 outputDir 中保存 samples.jsonl 和 manifest.json。
// 前置失败不创建目录；启动后取消仍保存清单，不覆盖旧目录、不关闭 client。
func RunGatewayRecording(ctx context.Context, client *http.Client, cfg GatewaySamplingConfig, outputDir string) (GatewayRecordingResult, error) {
	result, err := runRecording(ctx, client, cfg, outputDir, RunGatewaySampling, WriteGatewaySampleJSON, func(s GatewaySample) bool { return s.Err == nil },
		func(w io.Writer, r recordingReport[GatewaySamplingConfig]) error {
			return WriteGatewayRecordingJSON(w, GatewayRecordingReport(r))
		})
	return GatewayRecordingResult{Recording: GatewayRecordingReport(result.Recording), ManifestPath: result.ManifestPath, ManifestErr: result.ManifestErr, ManifestSaved: result.ManifestSaved}, err
}
