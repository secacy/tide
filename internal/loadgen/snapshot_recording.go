package loadgen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// recordingConfigValidator 是文件层唯一需要的配置行为；协议查询由调用方提供。
type recordingConfigValidator interface{ Validate() error }

const (
	snapshotSamplesFileName  = "samples.jsonl"
	snapshotManifestFileName = "manifest.json"
)

// recordingReport 保存一次文件记录的配置、时间和退出事实。
// StartedAt 为零表示文件记录尚未启动；序列化由单独的清单写入函数负责。
type recordingReport[C recordingConfigValidator] struct {
	Config     C      // 实际采样配置。
	OutputPath string // 实际输出路径。

	StartedAt  time.Time // 文件创建成功后、开始采样前。
	FinishedAt time.Time // 文件关闭尝试完成后。

	SamplesWritten    int64 // 行输出函数返回 nil 的记录数。
	SuccessfulSamples int64 // 已写出记录中，查询成功的数量。
	FailedSamples     int64 // 已写出记录中，查询失败的数量。

	SamplingErr error // 采样循环 返回的原错误。
	OutputErr   error // emit 中行输出函数返回的原错误。
	CloseErr    error // 关闭样本文件时返回的原错误。
}

// recordingResult 分开保留样本记录与清单保存结果，不以文件存在代表成功。
type recordingResult[C recordingConfigValidator] struct {
	Recording     recordingReport[C] // 样本关闭后的原始报告。
	ManifestPath  string             // 清单实际绝对路径。
	ManifestErr   error              // 清单创建、编码、写入或关闭错误。
	ManifestSaved bool               // 写入和关闭均成功，不表示 fsync 或实验成功。
}

// runSamplingToFile 独占创建文件，执行注入的串行采样和行写入，最后关闭文件。
// 回调由包内类型适配层提供且非 nil；失败产物保留，不关闭 client。
func runSamplingToFile[C recordingConfigValidator, T any](ctx context.Context, client *http.Client, cfg C, outputPath string,
	sample func(context.Context, *http.Client, C, func(T) error) error,
	writeSample func(io.Writer, T) error, successful func(T) bool,
) (report recordingReport[C], err error) {
	if ctx == nil {
		return recordingReport[C]{}, errors.New("run snapshot sampling to file: context is nil")
	}
	if client == nil {
		return recordingReport[C]{}, errors.New("run snapshot sampling to file: client is nil")
	}
	if err := cfg.Validate(); err != nil {
		return recordingReport[C]{}, fmt.Errorf("run snapshot sampling to file: invalid config: %w", err)
	}
	if strings.TrimSpace(outputPath) == "" {
		return recordingReport[C]{}, errors.New("run snapshot sampling to file: output path is empty")
	}
	if outputPath == "-" {
		return recordingReport[C]{}, errors.New(`run snapshot sampling to file: output path "-" is not supported`)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return recordingReport[C]{}, fmt.Errorf("run snapshot sampling to file: context already done: %w", ctxErr)
	}

	file, openErr := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if openErr != nil {
		return recordingReport[C]{}, fmt.Errorf("create snapshot samples %q: %w", outputPath, openErr)
	}

	report = recordingReport[C]{
		Config:     cfg,
		OutputPath: outputPath,
		StartedAt:  time.Now(),
	}

	defer func() {
		report.CloseErr = file.Close()
		report.FinishedAt = time.Now()

		if report.CloseErr != nil {
			err = errors.Join(err, fmt.Errorf("close snapshot samples %q: %w", outputPath, report.CloseErr))
		}
	}()

	emit := func(sample T) error {
		if writeErr := writeSample(file, sample); writeErr != nil {
			report.OutputErr = writeErr
			return writeErr
		}

		report.SamplesWritten++

		if successful(sample) {
			report.SuccessfulSamples++
		} else {
			report.FailedSamples++
		}

		return nil
	}

	report.SamplingErr = sample(ctx, client, cfg, emit)

	return report, report.SamplingErr
}

// runRecording 独占创建新目录，提前预留清单，再记录样本并保存最终事实。
// 所有回调由包内适配层提供；返回前完成关闭，保留失败产物和全部错误。
func runRecording[C recordingConfigValidator, T any](ctx context.Context, client *http.Client, cfg C, outputDir string,
	sample func(context.Context, *http.Client, C, func(T) error) error,
	writeSample func(io.Writer, T) error, successful func(T) bool,
	writeManifest func(io.Writer, recordingReport[C]) error,
) (result recordingResult[C], err error) {
	if ctx == nil {
		return recordingResult[C]{}, errors.New("run snapshot recording: context is nil")
	}
	if client == nil {
		return recordingResult[C]{}, errors.New("run snapshot recording: client is nil")
	}
	if err := cfg.Validate(); err != nil {
		return recordingResult[C]{}, fmt.Errorf("run snapshot recording: invalid config: %w", err)
	}
	if strings.TrimSpace(outputDir) == "" {
		return recordingResult[C]{}, errors.New("run snapshot recording: output directory is empty")
	}
	if outputDir == "-" {
		return recordingResult[C]{}, errors.New(`run snapshot recording: output directory "-" is not supported`)
	}

	absDir, err := filepath.Abs(outputDir)
	if err != nil {
		return recordingResult[C]{}, fmt.Errorf("run snapshot recording: resolve output directory %q: %w", outputDir, err)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return recordingResult[C]{}, fmt.Errorf("run snapshot recording: context already done: %w", ctxErr)
	}

	if err := os.Mkdir(absDir, 0700); err != nil {
		return recordingResult[C]{}, fmt.Errorf("create snapshot recording directory %q: %w", absDir, err)
	}

	samplesPath := filepath.Join(absDir, snapshotSamplesFileName)
	manifestPath := filepath.Join(absDir, snapshotManifestFileName)

	result.ManifestPath = manifestPath

	manifestFile, openErr := os.OpenFile(
		manifestPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0600,
	)
	if openErr != nil {
		wrapped := fmt.Errorf("create snapshot recording manifest %q: %w", manifestPath, openErr)
		result.ManifestErr = wrapped
		return result, wrapped
	}

	manifestWritten := false

	defer func() {
		closeErr := manifestFile.Close()
		if closeErr != nil {
			wrapped := fmt.Errorf("close snapshot recording manifest %q: %w", manifestPath, closeErr)
			result.ManifestErr = errors.Join(result.ManifestErr, wrapped)
			err = errors.Join(err, wrapped)
		}

		result.ManifestSaved = manifestWritten && closeErr == nil
	}()

	recording, recordingErr := runSamplingToFile(ctx, client, cfg, samplesPath, sample, writeSample, successful)
	result.Recording = recording

	if recording.StartedAt.IsZero() {
		if recordingErr != nil {
			return result, recordingErr
		}

		return result, errors.New("run snapshot recording: sampling returned an unstarted report without error")
	}

	manifestReport := recording
	manifestReport.OutputPath = snapshotSamplesFileName

	if manifestErr := writeManifest(manifestFile, manifestReport); manifestErr != nil {
		wrapped := fmt.Errorf("write snapshot recording manifest %q: %w", manifestPath, manifestErr)
		result.ManifestErr = wrapped
		return result, errors.Join(recordingErr, wrapped)
	}

	manifestWritten = true

	return result, errors.Join(recordingErr, result.ManifestErr)
}
