package loadgen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// gatewayManifestReport 构造独立清单夹具，非 UTC 时间用于验证转换不修改输入。
func gatewayManifestReport() loadgen.GatewayRecordingReport {
	at := time.Date(2026, 9, 30, 10, 0, 0, 123, time.FixedZone("fixture", 8*3600))
	return loadgen.GatewayRecordingReport{Config: samplingConfig(), OutputPath: " samples.jsonl ", StartedAt: at, FinishedAt: at.Add(time.Second), SamplesWritten: 3, SuccessfulSamples: 2, FailedSamples: 1}
}

// TestGatewayRecordingJSONContract 校验完整字段、版本、单位、UTC、null 及输入值不变。
func TestGatewayRecordingJSONContract(t *testing.T) {
	report := gatewayManifestReport()
	before := report
	var out bytes.Buffer
	if err := loadgen.WriteGatewayRecordingJSON(&out, report); err != nil {
		t.Fatal(err)
	}
	want := `{
		"schema_version":1,
		"config":{"endpoint":"http://gateway.invalid/debug/gateway","interval_ns":100000000,"request_timeout_ns":300000000},
		"output_path":" samples.jsonl ","started_at":"2026-09-30T02:00:00.000000123Z","finished_at":"2026-09-30T02:00:01.000000123Z",
		"samples_written":3,"successful_samples":2,"failed_samples":1,"stop_reason":"returned",
		"sampling_error":null,"output_error":null,"close_error":null
	}`
	if got := decodeBatchJSON(t, out.Bytes()); !reflect.DeepEqual(got, decodeBatchJSON(t, []byte(want))) {
		t.Fatalf("manifest: %s", out.Bytes())
	}
	if !bytes.HasSuffix(out.Bytes(), []byte("\n")) || bytes.HasSuffix(out.Bytes(), []byte("\n\n")) || !bytes.Contains(out.Bytes(), []byte("\n  \"schema_version\"")) {
		t.Fatalf("manifest must be indented with one ending newline: %q", out.Bytes())
	}
	if report != before {
		t.Fatal("input report changed")
	}
}

// TestGatewayRecordingJSONReasons 验证错误类型分类而非文本匹配，并保留所有错误及空文本。
func TestGatewayRecordingJSONReasons(t *testing.T) {
	outputCanceled := fmt.Errorf("sample output interrupted: %w", context.Canceled)
	closeErr := errors.New("close \"samples\" failed\n关闭失败")
	for _, tc := range []struct {
		name                    string
		sampling, output, close error
		reason                  string
	}{
		{"returned", nil, nil, nil, "returned"},
		{"wrapped_cancel", fmt.Errorf("stop: %w", context.Canceled), nil, nil, "canceled"},
		{"wrapped_deadline", fmt.Errorf("stop: %w", context.DeadlineExceeded), nil, nil, "deadline_exceeded"},
		{"text_is_not_type", errors.New("context canceled"), nil, nil, "sampling_error"},
		{"empty_sampling", errors.New(""), nil, nil, "sampling_error"},
		{"deadline_before_cancel", errors.Join(context.Canceled, context.DeadlineExceeded), nil, nil, "deadline_exceeded"},
		{"output_before_cancel", fmt.Errorf("emit sample 1: %w", outputCanceled), outputCanceled, nil, "output_error"},
		{"all_errors", errors.Join(context.Canceled, context.DeadlineExceeded), errors.New("write failed"), closeErr, "output_error"},
		{"cancel_and_close", context.Canceled, nil, closeErr, "canceled"},
		{"close_only", nil, nil, closeErr, "returned"},
		{"empty_output", errors.New("emit failed"), errors.New(""), nil, "output_error"},
		{"empty_close", nil, nil, errors.New(""), "returned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := gatewayManifestReport()
			report.SamplingErr = tc.sampling
			report.OutputErr = tc.output
			report.CloseErr = tc.close
			before := report
			var out bytes.Buffer
			if err := loadgen.WriteGatewayRecordingJSON(&out, report); err != nil {
				t.Fatal("error facts must remain writable:", err)
			}
			doc := decodeBatchJSON(t, out.Bytes())
			if doc["stop_reason"] != tc.reason {
				t.Fatalf("reason=%v want=%s", doc["stop_reason"], tc.reason)
			}
			for key, value := range map[string]error{"sampling_error": tc.sampling, "output_error": tc.output, "close_error": tc.close} {
				if value == nil {
					assertJSONNull(t, doc, key)
				} else if doc[key] != value.Error() {
					t.Fatalf("%s=%#v want=%q", key, doc[key], value.Error())
				}
			}
			if report != before {
				t.Fatal("input errors or report changed")
			}
		})
	}
}

// TestGatewayRecordingJSONBounds 验证零样本、int64 边界和记录时间不被强制重排。
func TestGatewayRecordingJSONBounds(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		total, success, failed int64
		backwards              bool
	}{
		{"empty", 0, 0, 0, false}, {"max_success", math.MaxInt64, math.MaxInt64, 0, false},
		{"max_failure", math.MaxInt64, 0, math.MaxInt64, false}, {"max_mixed", math.MaxInt64, math.MaxInt64 - 1, 1, false},
		{"wall_clock_backwards", 1, 1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := gatewayManifestReport()
			report.SamplesWritten = tc.total
			report.SuccessfulSamples = tc.success
			report.FailedSamples = tc.failed
			report.Config.Interval = time.Duration(math.MaxInt64)
			report.Config.RequestTimeout = time.Nanosecond
			if tc.backwards {
				report.FinishedAt = report.StartedAt.Add(-time.Second)
			}
			var out bytes.Buffer
			if err := loadgen.WriteGatewayRecordingJSON(&out, report); err != nil {
				t.Fatal(err)
			}
			doc := decodeBatchJSON(t, out.Bytes())
			for key, value := range map[string]int64{"samples_written": tc.total, "successful_samples": tc.success, "failed_samples": tc.failed} {
				if doc[key] != json.Number(strconv.FormatInt(value, 10)) {
					t.Fatalf("lost integer precision: %s", out.Bytes())
				}
			}
			cfg := doc["config"].(map[string]any)
			if cfg["interval_ns"] != json.Number(strconv.FormatInt(math.MaxInt64, 10)) || cfg["request_timeout_ns"] != json.Number("1") {
				t.Fatal("lost duration precision")
			}
			if doc["finished_at"] != report.FinishedAt.UTC().Format(time.RFC3339Nano) {
				t.Fatal("timestamp changed")
			}
		})
	}
}

// TestGatewayRecordingJSONInvalid 拒绝矛盾报告，所有校验/编码失败均零写入。
func TestGatewayRecordingJSONInvalid(t *testing.T) {
	t.Run("nil_writer", func(t *testing.T) {
		if loadgen.WriteGatewayRecordingJSON(nil, gatewayManifestReport()) == nil {
			t.Fatal("accepted nil writer")
		}
	})
	for _, tc := range []struct {
		name     string
		change   func(*loadgen.GatewayRecordingReport)
		encoding bool
	}{
		{"config", func(r *loadgen.GatewayRecordingReport) { r.Config.Interval = 0 }, false},
		{"empty_path", func(r *loadgen.GatewayRecordingReport) { r.OutputPath = "" }, false},
		{"blank_path", func(r *loadgen.GatewayRecordingReport) { r.OutputPath = " \t " }, false},
		{"stdout", func(r *loadgen.GatewayRecordingReport) { r.OutputPath = "-" }, false},
		{"no_start", func(r *loadgen.GatewayRecordingReport) { r.StartedAt = time.Time{} }, false},
		{"no_finish", func(r *loadgen.GatewayRecordingReport) { r.FinishedAt = time.Time{} }, false},
		{"negative_total", func(r *loadgen.GatewayRecordingReport) { r.SamplesWritten = -1 }, false},
		{"negative_success", func(r *loadgen.GatewayRecordingReport) { r.SuccessfulSamples = -1 }, false},
		{"negative_failure", func(r *loadgen.GatewayRecordingReport) { r.FailedSamples = -1 }, false},
		{"success_above_total", func(r *loadgen.GatewayRecordingReport) { r.SuccessfulSamples = 4 }, false},
		{"wrong_sum", func(r *loadgen.GatewayRecordingReport) { r.FailedSamples = 2 }, false},
		{"sum_overflow", func(r *loadgen.GatewayRecordingReport) {
			r.SamplesWritten = math.MaxInt64
			r.SuccessfulSamples = math.MaxInt64
			r.FailedSamples = math.MaxInt64
		}, false},
		{"output_without_sampling_error", func(r *loadgen.GatewayRecordingReport) { r.OutputErr = errors.New("output failed") }, false},
		{"start_encoding", func(r *loadgen.GatewayRecordingReport) { r.StartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
		{"finish_encoding", func(r *loadgen.GatewayRecordingReport) { r.FinishedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := gatewayManifestReport()
			tc.change(&report)
			w := &gatewayLineWriter{}
			err := loadgen.WriteGatewayRecordingJSON(w, report)
			if err == nil || w.calls != 0 || w.closed || w.flushed {
				t.Fatalf("err=%v writer=%+v", err, w)
			}
			if tc.encoding {
				var encodingErr *json.MarshalerError
				if !errors.As(err, &encodingErr) {
					t.Fatalf("lost encoding error: %v", err)
				}
			}
		})
	}
}

// TestGatewayRecordingJSONWriter 验证单次写入、底层错误链与短写，不接管 writer 生命周期。
func TestGatewayRecordingJSONWriter(t *testing.T) {
	failure := errors.New("manifest output failed")
	for _, tc := range []struct {
		name            string
		partial         bool
		writerErr, want error
	}{
		{"success", false, nil, nil}, {"error", false, failure, failure}, {"partial_error", true, failure, failure}, {"short_write", true, nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &gatewayLineWriter{jsonFaultWriter: jsonFaultWriter{partial: tc.partial, err: tc.writerErr}}
			err := loadgen.WriteGatewayRecordingJSON(w, gatewayManifestReport())
			if !errors.Is(err, tc.want) || w.calls != 1 || w.closed || w.flushed {
				t.Fatalf("err=%v writer=%+v", err, w)
			}
		})
	}
}

// TestGatewayRecordingJSONDoesNotAccessSamples 验证清单编码不要求样本文件存在，也不创建目录。
func TestGatewayRecordingJSONDoesNotAccessSamples(t *testing.T) {
	dir := t.TempDir()
	report := gatewayManifestReport()
	report.OutputPath = filepath.Join(dir, "missing", "samples.jsonl")
	var out bytes.Buffer
	if err := loadgen.WriteGatewayRecordingJSON(&out, report); err != nil {
		t.Fatal(err)
	}
	if doc := decodeBatchJSON(t, out.Bytes()); doc["output_path"] != report.OutputPath {
		t.Fatal("path changed")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("sample path accessed for creation: %v %v", entries, err)
	}
}

// TestGatewayRecordingJSONFromRun 验证真实 HTTP -> JSONL -> 运行报告 -> 清单的计数及错误能够互相核对。
func TestGatewayRecordingJSONFromRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, probeValidJSON)
		case 2:
			http.Error(w, "unavailable", 503)
		default:
			cancel()
			<-r.Context().Done()
		}
	}))
	defer srv.Close()
	cfg := samplingConfig()
	cfg.Endpoint = srv.URL
	cfg.Interval = time.Millisecond
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	report, runErr := loadgen.RunGatewaySamplingToFile(ctx, srv.Client(), cfg, path)
	if !errors.Is(runErr, context.Canceled) || report.SamplesWritten != 3 || report.SuccessfulSamples != 1 || report.FailedSamples != 2 || report.OutputErr != nil || report.CloseErr != nil {
		t.Fatalf("report=%+v err=%v", report, runErr)
	}
	rows := readGatewayRecording(t, report)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := loadgen.WriteGatewayRecordingJSON(&out, report); err != nil {
		t.Fatal("canceled report could not be saved:", err)
	}
	doc := decodeBatchJSON(t, out.Bytes())
	if doc["samples_written"] != json.Number(strconv.Itoa(len(rows))) || doc["successful_samples"] != json.Number("1") || doc["failed_samples"] != json.Number("2") || doc["stop_reason"] != "canceled" || doc["sampling_error"] != report.SamplingErr.Error() {
		t.Fatalf("manifest does not match recording: %s", out.Bytes())
	}
	assertJSONNull(t, doc, "output_error")
	assertJSONNull(t, doc, "close_error")
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("manifest encoding changed JSONL")
	}
}
