package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	gatewaySamplesFileName  = "samples.jsonl"
	gatewayManifestFileName = "manifest.json"
)

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

// RunGatewayRecording 在新的 outputDir 中记录样本并保存清单。
// 前置检查失败不创建目录、不查询；目录创建后保留所有产物。
// 返回前关闭清单文件，错误合并样本记录与清单保存两个层次。
// 不吞掉父取消，不修改或关闭 client，不启动后台 goroutine。
func RunGatewayRecording(
	ctx context.Context,
	client *http.Client,
	cfg GatewaySamplingConfig,
	outputDir string,
) (result GatewayRecordingResult, err error) {
	if ctx == nil {
		return GatewayRecordingResult{}, errors.New("run gateway recording: context is nil")
	}
	if client == nil {
		return GatewayRecordingResult{}, errors.New("run gateway recording: client is nil")
	}
	if err := cfg.Validate(); err != nil {
		return GatewayRecordingResult{}, fmt.Errorf("run gateway recording: invalid config: %w", err)
	}
	if strings.TrimSpace(outputDir) == "" {
		return GatewayRecordingResult{}, errors.New("run gateway recording: output directory is empty")
	}
	if outputDir == "-" {
		return GatewayRecordingResult{}, errors.New(`run gateway recording: output directory "-" is not supported`)
	}

	absDir, err := filepath.Abs(outputDir)
	if err != nil {
		return GatewayRecordingResult{}, fmt.Errorf("run gateway recording: resolve output directory %q: %w", outputDir, err)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return GatewayRecordingResult{}, fmt.Errorf("run gateway recording: context already done: %w", ctxErr)
	}

	if err := os.Mkdir(absDir, 0700); err != nil {
		return GatewayRecordingResult{}, fmt.Errorf("create gateway recording directory %q: %w", absDir, err)
	}

	samplesPath := filepath.Join(absDir, gatewaySamplesFileName)
	manifestPath := filepath.Join(absDir, gatewayManifestFileName)

	result.ManifestPath = manifestPath

	manifestFile, openErr := os.OpenFile(
		manifestPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if openErr != nil {
		wrapped := fmt.Errorf("create gateway recording manifest %q: %w", manifestPath, openErr)
		result.ManifestErr = wrapped
		return result, wrapped
	}

	manifestWritten := false

	defer func() {
		closeErr := manifestFile.Close()
		if closeErr != nil {
			wrapped := fmt.Errorf("close gateway recording manifest %q: %w", manifestPath, closeErr)
			result.ManifestErr = errors.Join(result.ManifestErr, wrapped)
			err = errors.Join(err, wrapped)
		}

		result.ManifestSaved = manifestWritten && closeErr == nil
	}()

	recording, recordingErr := RunGatewaySamplingToFile(
		ctx,
		client,
		cfg,
		samplesPath,
	)
	result.Recording = recording

	if recording.StartedAt.IsZero() {
		if recordingErr != nil {
			return result, recordingErr
		}

		return result, errors.New("run gateway recording: sampling returned an unstarted report without error")
	}

	manifestReport := recording
	manifestReport.OutputPath = gatewaySamplesFileName

	if manifestErr := WriteGatewayRecordingJSON(manifestFile, manifestReport); manifestErr != nil {
		wrapped := fmt.Errorf("write gateway recording manifest %q: %w", manifestPath, manifestErr)
		result.ManifestErr = wrapped
		return result, errors.Join(recordingErr, wrapped)
	}

	manifestWritten = true

	return result, errors.Join(recordingErr, result.ManifestErr)
}
