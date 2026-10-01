package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// workerSampleRow 只解码跨进程接线所需字段；完整协议由 loadgen 测试覆盖。
type workerSampleRow struct {
	SourceKind string `json:"source_kind"`
	Index      int    `json:"index"`
	State      *struct {
		Enabled    bool `json:"processing_limit_enabled"`
		Processing *struct {
			Limit   int `json:"limit"`
			InUse   int `json:"in_use"`
			Waiting int `json:"waiting"`
		} `json:"processing"`
	} `json:"state"`
	Error *string `json:"error"`
}

// verifyWorkerSamplerPipeline 启动真实 Worker 和采样命令，再通过 gRPC 发送音频。
// 必须先看到空闲样本才开始 RPC，最终文件应依次包含空闲、占用、再次空闲。
// 这是接线正确性检查，短窗口和模拟延迟不作为性能或稳定容量证据。
func verifyWorkerSamplerPipeline(t *testing.T, workerBinary string) {
	t.Helper()
	samplerBinary := filepath.Join(t.TempDir(), "worker-sampler")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer buildCancel()
	if out, err := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", samplerBinary, "../worker-sampler").CombinedOutput(); err != nil {
		t.Fatalf("build sampler: %v\n%s", err, out)
	}
	worker := startWorkerProcess(t, workerBinary, "-debug-listen=127.0.0.1:0", "-processing-concurrency=1", "-processing-delay=500ms", "-response-delay=0s")
	outputDir := filepath.Join(t.TempDir(), "run")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, samplerBinary, "-url=http://"+worker.debugAddress+"/debug/worker", "-interval=5ms", "-request-timeout=200ms", "-duration=3s", "-output-dir="+outputDir)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var runErr error // 仅在 done 关闭后读取，日志同样如此。
	go func() { runErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("sampler process not reaped")
		}
	})

	samplePath := filepath.Join(outputDir, "samples.jsonl")
	readyDeadline := time.NewTimer(2 * time.Second)
	defer readyDeadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
ready:
	for {
		data, err := os.ReadFile(samplePath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		// 写入中的最后一行可能尚不完整，只读取已经带换行的第一行。
		if end := bytes.IndexByte(data, '\n'); end >= 0 {
			var row workerSampleRow
			if err := json.Unmarshal(data[:end], &row); err != nil {
				t.Fatal(err)
			}
			if row.Error != nil || row.State == nil || !row.State.Enabled || row.State.Processing == nil || row.State.Processing.InUse != 0 {
				t.Fatalf("first observation is not idle: %s", data[:end])
			}
			break ready
		}
		select {
		case <-done:
			t.Fatalf("sampler exited before first sample: %v\n%s", runErr, logs.String())
		case <-readyDeadline.C:
			t.Fatal("sampler did not become ready")
		case <-ticker.C:
		}
	}
	verifyWorkerRPC(t, worker.address)
	<-done // CommandContext 限制子进程总等待时间。
	if runErr != nil {
		t.Fatalf("sampler exit: %v\n%s", runErr, logs.String())
	}
	data, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatal("incomplete samples file")
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	var successes, failures int
	var sawBusy, sawIdleAfterBusy bool
	for i, line := range lines {
		var row workerSampleRow
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if row.SourceKind != "worker" || row.Index != i || (row.State == nil) == (row.Error == nil) {
			t.Fatalf("invalid row %d: %s", i, line)
		}
		if row.Error != nil {
			failures++ // 预算到期时的在途查询可以失败；不能伪装成零占用。
			continue
		}
		successes++
		p := row.State.Processing
		if !row.State.Enabled || p == nil || p.Limit != 1 || p.Waiting != 0 || p.InUse < 0 || p.InUse > 1 {
			t.Fatalf("invalid processing state: %s", line)
		}
		if p.InUse == 1 {
			sawBusy = true
		} else if sawBusy {
			sawIdleAfterBusy = true
		}
	}
	if !sawBusy || !sawIdleAfterBusy {
		t.Fatalf("missing transitions: busy=%v idle_after_busy=%v", sawBusy, sawIdleAfterBusy)
	}
	data, err = os.ReadFile(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		SourceKind        string  `json:"source_kind"`
		OutputPath        string  `json:"output_path"`
		SamplesWritten    int     `json:"samples_written"`
		SuccessfulSamples int     `json:"successful_samples"`
		FailedSamples     int     `json:"failed_samples"`
		StopReason        string  `json:"stop_reason"`
		OutputError       *string `json:"output_error"`
		CloseError        *string `json:"close_error"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SourceKind != "worker" || manifest.OutputPath != "samples.jsonl" || manifest.SamplesWritten != len(lines) || manifest.SuccessfulSamples != successes || manifest.FailedSamples != failures || manifest.StopReason != "deadline_exceeded" || manifest.OutputError != nil || manifest.CloseError != nil {
		t.Fatalf("manifest does not match recording: %s", data)
	}
}
