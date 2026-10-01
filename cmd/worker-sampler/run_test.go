package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

const samplerSnapshotJSON = `{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":8,"in_use":0,"waiting":0}}`

// TestSamplerRunError 验证命令完成条件与错误链；构造报告不等于真实磁盘故障注入。
func TestSamplerRunError(t *testing.T) {
	outputErr, closeErr, manifestErr := errors.New("disk write"), errors.New("sample close"), errors.New("manifest close")
	otherErr := errors.New("unexpected failure")
	for _, mode := range []string{
		"complete", "mixed", "unstarted", "unstarted_without_error", "manifest_create_failed",
		"output_failed", "sample_close_failed", "manifest_failed", "manifest_not_saved", "combined_save_failures",
		"parent_canceled", "parent_deadline", "custom_parent_cause", "no_stop_cause",
		"nil_recording_error", "other_recording_error", "canceled_recording_error",
		"nil_sampling_error", "other_sampling_error", "canceled_sampling_error", "all_queries_failed", "zero_rows",
	} {
		t.Run(mode, func(t *testing.T) {
			result := loadgen.WorkerRecordingResult{
				Recording: loadgen.WorkerRecordingReport{
					StartedAt: time.Now(), FinishedAt: time.Now(), OutputPath: "/unused/samples.jsonl",
					SamplesWritten: 1, SuccessfulSamples: 1, SamplingErr: fmt.Errorf("sampling ended: %w", context.DeadlineExceeded),
				},
				ManifestPath: "/unused/manifest.json", ManifestSaved: true,
			}
			recordingErr := error(fmt.Errorf("recording ended: %w", context.DeadlineExceeded))
			cause := error(errSamplingDurationReached)
			wantOK := mode == "complete" || mode == "mixed"
			var retained []error
			switch mode {
			case "mixed":
				result.Recording.SamplesWritten, result.Recording.FailedSamples = 3, 2
			case "unstarted":
				result = loadgen.WorkerRecordingResult{}
				recordingErr = otherErr
			case "unstarted_without_error":
				result = loadgen.WorkerRecordingResult{}
				recordingErr = nil
			case "manifest_create_failed":
				result = loadgen.WorkerRecordingResult{ManifestPath: "/unused/manifest.json", ManifestErr: manifestErr}
				recordingErr = fmt.Errorf("create manifest: %w", manifestErr)
				retained = append(retained, manifestErr)
			case "output_failed":
				result.Recording.OutputErr = outputErr
				result.Recording.SamplingErr = outputErr
				retained = append(retained, outputErr)
			case "sample_close_failed":
				result.Recording.CloseErr = closeErr
				retained = append(retained, closeErr)
			case "manifest_failed":
				result.ManifestErr, result.ManifestSaved = manifestErr, false
				retained = append(retained, manifestErr)
			case "manifest_not_saved":
				result.ManifestSaved = false
			case "combined_save_failures":
				result.Recording.OutputErr, result.Recording.CloseErr = outputErr, closeErr
				result.Recording.SamplingErr = outputErr
				result.ManifestErr, result.ManifestSaved = manifestErr, false
				retained = append(retained, outputErr, closeErr, manifestErr)
			case "parent_canceled", "parent_deadline", "custom_parent_cause":
				cause = context.Canceled
				if mode == "parent_deadline" {
					cause = context.DeadlineExceeded
				}
				if mode == "custom_parent_cause" {
					cause = otherErr
				}
				retained = append(retained, cause)
			case "no_stop_cause":
				cause = nil
			case "nil_recording_error":
				recordingErr = nil
			case "other_recording_error":
				recordingErr = otherErr
			case "canceled_recording_error":
				recordingErr = context.Canceled
			case "nil_sampling_error":
				result.Recording.SamplingErr = nil
			case "other_sampling_error":
				result.Recording.SamplingErr = otherErr
				retained = append(retained, otherErr)
			case "canceled_sampling_error":
				result.Recording.SamplingErr = context.Canceled
				retained = append(retained, context.Canceled)
			case "all_queries_failed":
				result.Recording.SuccessfulSamples, result.Recording.FailedSamples = 0, 1
			case "zero_rows":
				result.Recording.SamplesWritten, result.Recording.SuccessfulSamples = 0, 0
			}
			before := result
			got := samplerRunError(result, recordingErr, cause)
			if (got == nil) != wantOK {
				t.Fatalf("error=%v want success=%v", got, wantOK)
			}
			if !reflect.DeepEqual(result, before) {
				t.Fatal("changed raw result")
			}
			if !wantOK {
				if recordingErr != nil {
					retained = append(retained, recordingErr)
				}
				for _, target := range retained {
					if !errors.Is(got, target) {
						t.Fatalf("lost %v: %v", target, got)
					}
				}
			}
		})
	}
}

// samplerTestConfig 固定短观察窗口用于正确性测试，不作为负载或容量条件。
func samplerTestConfig(t *testing.T, endpoint string) samplerConfig {
	t.Helper()
	return samplerConfig{
		Sampling: loadgen.WorkerSamplingConfig{Endpoint: endpoint, Interval: 10 * time.Millisecond, RequestTimeout: 60 * time.Millisecond},
		Duration: 300 * time.Millisecond, OutputDir: filepath.Join(t.TempDir(), "run"),
	}
}

// samplerManifest 只读取命令验收所需字段；完整格式由 loadgen 序列化测试覆盖。
type samplerManifest struct {
	SourceKind        string  `json:"source_kind"`
	DisabledSamples   int64   `json:"-"` // 测试从样本计算，不是清单字段。
	SchemaVersion     int     `json:"schema_version"`
	OutputPath        string  `json:"output_path"`
	SamplesWritten    int64   `json:"samples_written"`
	SuccessfulSamples int64   `json:"successful_samples"`
	FailedSamples     int64   `json:"failed_samples"`
	StopReason        string  `json:"stop_reason"`
	SamplingError     *string `json:"sampling_error"`
	OutputError       *string `json:"output_error"`
	CloseError        *string `json:"close_error"`
}

// readSamplerArtifacts 核对落盘样本的序号、状态/错误互斥与清单计数，保留失败行。
func readSamplerArtifacts(t *testing.T, dir string) samplerManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc samplerManifest
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("manifest: %v\n%s", err, data)
	}
	if doc.SourceKind != "worker" || doc.SchemaVersion != 1 || doc.OutputPath != "samples.jsonl" || doc.OutputError != nil || doc.CloseError != nil || doc.SamplingError == nil {
		t.Fatalf("unexpected manifest: %+v", doc)
	}
	data, err = os.ReadFile(filepath.Join(dir, doc.OutputPath))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("missing complete sample: %q", data)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	var successes, failures int64
	for index, line := range lines {
		var row struct {
			SourceKind string `json:"source_kind"`
			Index      int    `json:"index"`
			State      *struct {
				Enabled    *bool `json:"processing_limit_enabled"`
				Processing *struct {
					Limit   *int `json:"limit"`
					InUse   *int `json:"in_use"`
					Waiting *int `json:"waiting"`
				} `json:"processing"`
			} `json:"state"`
			Error *string `json:"error"`
		}
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatal(err)
		}
		if row.SourceKind != "worker" || row.Index != index || (row.State == nil) == (row.Error == nil) {
			t.Fatalf("invalid row %d: %s", index, line)
		}
		if row.Error == nil {
			if row.State.Enabled == nil {
				t.Fatal("missing enabled flag")
			}
			p := row.State.Processing
			if !*row.State.Enabled {
				if p != nil {
					t.Fatal("disabled sample has counts")
				}
				doc.DisabledSamples++
			} else if p == nil || p.Limit == nil || p.InUse == nil || p.Waiting == nil || *p.Limit <= 0 || *p.InUse < 0 || *p.InUse > *p.Limit || *p.Waiting < 0 {
				t.Fatal("invalid processing counts")
			}
			successes++
		} else {
			failures++
		}
	}
	if doc.SamplesWritten != int64(len(lines)) || doc.SuccessfulSamples != successes || doc.FailedSamples != failures {
		t.Fatalf("manifest=%+v lines=%d success=%d failure=%d", doc, len(lines), successes, failures)
	}
	return doc
}

// TestSamplerRunHTTP 检查计划到期、查询失败和父停止的真实 HTTP、文件及命令错误。
func TestSamplerRunHTTP(t *testing.T) {
	for _, mode := range []string{"complete", "disabled", "mixed", "all_failed", "request_timeout_then_recovery", "inflight_at_duration", "parent_cancel", "parent_deadline", "custom_parent_cause"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			customCause := errors.New("operator stopped observation")
			var stopCause context.CancelCauseFunc
			if mode == "custom_parent_cause" {
				ctx, stopCause = context.WithCancelCause(ctx)
				defer stopCause(nil)
			}
			if mode == "parent_deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 300*time.Millisecond)
				defer stop()
			}
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if n == 2 && (mode == "parent_cancel" || mode == "custom_parent_cause") {
					if mode == "parent_cancel" {
						cancel()
					} else {
						stopCause(customCause)
					}
					<-r.Context().Done()
					return
				}
				if (n == 1 && mode == "request_timeout_then_recovery") || (n >= 2 && (mode == "parent_deadline" || mode == "inflight_at_duration")) {
					<-r.Context().Done()
					return
				}
				if mode == "all_failed" || (mode == "mixed" && n%2 == 0) {
					http.Error(w, "unavailable", 503)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if mode == "disabled" {
					_, _ = io.WriteString(w, `{"schema_version":1,"processing_limit_enabled":false,"processing":null}`)
					return
				}
				_, _ = io.WriteString(w, samplerSnapshotJSON)
			}))
			defer server.Close()
			cfg := samplerTestConfig(t, server.URL)
			if strings.HasPrefix(mode, "parent_") || mode == "custom_parent_cause" {
				cfg.Duration = 3 * time.Second
			}
			if mode == "parent_deadline" || mode == "inflight_at_duration" {
				cfg.Sampling.RequestTimeout = time.Second
			}
			result, err := run(ctx, cfg)
			wantOK := mode == "complete" || mode == "disabled" || mode == "mixed" || mode == "request_timeout_then_recovery" || mode == "inflight_at_duration"
			if (err == nil) != wantOK {
				t.Fatalf("result=%+v err=%v want success=%v", result, err, wantOK)
			}
			if !result.ManifestSaved || result.ManifestErr != nil {
				t.Fatalf("manifest not saved: %+v", result)
			}
			wantRaw := error(context.DeadlineExceeded)
			wantReason := "deadline_exceeded"
			if mode == "parent_cancel" || mode == "custom_parent_cause" {
				wantRaw, wantReason = context.Canceled, "canceled"
			}
			if !errors.Is(result.Recording.SamplingErr, wantRaw) {
				t.Fatalf("raw stop lost: %+v", result)
			}
			if !wantOK && !errors.Is(err, wantRaw) {
				t.Fatalf("command lost raw stop: %v", err)
			}
			if mode == "custom_parent_cause" && !errors.Is(err, customCause) {
				t.Fatalf("lost custom cause: %v", err)
			}
			doc := readSamplerArtifacts(t, cfg.OutputDir)
			if doc.StopReason != wantReason || doc.SamplesWritten != result.Recording.SamplesWritten || doc.SuccessfulSamples != result.Recording.SuccessfulSamples || doc.FailedSamples != result.Recording.FailedSamples {
				t.Fatalf("doc=%+v result=%+v", doc, result)
			}
			if mode == "all_failed" {
				if doc.SuccessfulSamples != 0 || !strings.Contains(err.Error(), "no valid") {
					t.Fatalf("all failures accepted: %+v %v", doc, err)
				}
			} else if doc.SuccessfulSamples < 1 {
				t.Fatalf("missing valid observation: %+v", doc)
			}
			if mode == "disabled" && doc.DisabledSamples != doc.SuccessfulSamples {
				t.Fatalf("unmeasured success lost: %+v", doc)
			}
			if mode != "complete" && mode != "disabled" && doc.FailedSamples < 1 {
				t.Fatalf("failure disappeared: %+v", doc)
			}
			if doc.SamplesWritten < calls.Load() {
				t.Fatalf("requests=%d rows=%d", calls.Load(), doc.SamplesWritten)
			}
		})
	}
}

// TestSamplerRunPreflight 验证运行层拒绝前置错误，且不创建新证据或发请求。
func TestSamplerRunPreflight(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); http.Error(w, "unexpected", 503) }))
	defer server.Close()
	for _, mode := range []string{"nil_context", "zero_duration", "negative_duration", "pre_canceled", "pre_deadline", "invalid_sampling", "blank_output", "existing_directory", "missing_parent"} {
		t.Run(mode, func(t *testing.T) {
			cfg := samplerTestConfig(t, server.URL)
			parent := filepath.Dir(cfg.OutputDir)
			ctx := context.Background()
			var want error
			switch mode {
			case "nil_context":
				ctx = nil
			case "zero_duration":
				cfg.Duration = 0
			case "negative_duration":
				cfg.Duration = -time.Second
			case "pre_canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "pre_deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			case "invalid_sampling":
				cfg.Sampling.Interval = 0
			case "blank_output":
				cfg.OutputDir = " \t "
			case "existing_directory":
				if err := os.Mkdir(cfg.OutputDir, 0700); err != nil {
					t.Fatal(err)
				}
				want = os.ErrExist
			case "missing_parent":
				cfg.OutputDir = filepath.Join(cfg.OutputDir, "missing", "run")
				want = os.ErrNotExist
			}
			result, err := run(ctx, cfg)
			if err == nil || result != (loadgen.WorkerRecordingResult{}) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("lost %v: %v", want, err)
			}
			dir := parent
			if mode == "existing_directory" {
				dir = cfg.OutputDir
			}
			if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
				t.Fatalf("created files: %v %v", entries, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("preflight issued %d requests", calls.Load())
	}
}

type samplerUnexpectedTransport struct{}

// RoundTrip 为不符合命令配置预期的 Transport 提供接口实现，正常应在调用前被拒绝。
func (samplerUnexpectedTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected transport use")
}

// TestSamplerRunTransportType 验证默认 Transport 被替换时返回错误，不 panic 或创建文件。
func TestSamplerRunTransportType(t *testing.T) {
	original := http.DefaultTransport
	http.DefaultTransport = samplerUnexpectedTransport{}
	defer func() { http.DefaultTransport = original }()
	cfg := samplerTestConfig(t, "http://unused.invalid/")
	result, err := run(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "http.DefaultTransport") || result != (loadgen.WorkerRecordingResult{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := os.Stat(cfg.OutputDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created output: %v", err)
	}
}

// TestSamplerRunOwnsConnections 验证采样使用私有连接、返回后关闭它，调用方原连接仍可复用。
// 本包不并行运行测试；临时替换 DefaultTransport 后恢复。
func TestSamplerRunOwnsConnections(t *testing.T) {
	original := http.DefaultTransport
	base := original.(*http.Transport).Clone()
	http.DefaultTransport = base
	defer func() { http.DefaultTransport = original; base.CloseIdleConnections() }()
	addresses := make(chan string, 256)
	closed := make(chan string, 256)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		addresses <- r.RemoteAddr
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, samplerSnapshotJSON)
	}))
	server.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- c.RemoteAddr().String()
		}
	}
	server.Start()
	defer server.Close()
	client := &http.Client{Transport: base, Timeout: time.Second}
	query := func() string {
		t.Helper()
		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read=%v close=%v", readErr, closeErr)
		}
		select {
		case addr := <-addresses:
			return addr
		case <-time.After(time.Second):
			t.Fatal("missing request address")
			return ""
		}
	}
	baseAddress := query()
	cfg := samplerTestConfig(t, server.URL)
	cfg.Sampling.Interval = time.Second // 在预算内只查询一次，避免关闭时恰好开始另一条请求。
	result, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	privateAddresses := make(map[string]bool)
	for range result.Recording.SamplesWritten {
		select {
		case addr := <-addresses:
			if addr == baseAddress {
				t.Fatal("sampling reused global connection pool")
			}
			privateAddresses[addr] = true
		case <-time.After(time.Second):
			t.Fatal("missing sampler request")
		}
	}
	if len(privateAddresses) == 0 {
		t.Fatal("no private connection observed")
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for len(privateAddresses) != 0 {
		select {
		case addr := <-closed:
			if addr == baseAddress {
				t.Fatal("closed caller connection")
			}
			delete(privateAddresses, addr)
		case <-deadline.C:
			t.Fatalf("private connections not closed: %v", privateAddresses)
		}
	}
	if addr := query(); addr != baseAddress {
		t.Fatalf("caller connection no longer reusable: %s != %s", addr, baseAddress)
	}
	if http.DefaultTransport != base {
		t.Fatal("changed default transport")
	}
}
