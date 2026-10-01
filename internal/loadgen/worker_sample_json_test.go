package loadgen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// workerLineSample 构造带非 UTC 时区与纳秒精度的合法样本。
func workerLineSample() loadgen.WorkerSample {
	at := time.Date(2026, 9, 30, 10, 0, 0, 123, time.FixedZone("fixture", 8*3600))
	return loadgen.WorkerSample{Index: 7, StartedAt: at, FinishedAt: at.Add(7 * time.Nanosecond), Duration: 7 * time.Nanosecond,
		State: &loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 64}}}
}

// decodeWorkerLine 要求恰好一行带终止换行的 JSON，保留整数精度。
func decodeWorkerLine(t *testing.T, data []byte) map[string]any {
	t.Helper()
	if bytes.Count(data, []byte("\n")) != 1 || !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("expected exactly one terminated JSON line: %q", data)
	}
	return decodeBatchJSON(t, data)
}

// TestWorkerSampleJSONContract 独立核对三种成功状态和两层 null，不复用生产 DTO。
func TestWorkerSampleJSONContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     loadgen.WorkerState
		stateJSON string
	}{
		{"disabled", loadgen.WorkerState{}, `{"processing_limit_enabled":false,"processing":null}`},
		{"idle", loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 64}}, `{"processing_limit_enabled":true,"processing":{"limit":64,"in_use":0,"waiting":0}}`},
		{"waiting", loadgen.WorkerState{ProcessingLimitEnabled: true, Processing: loadgen.WorkerProcessingState{Limit: 2, InUse: 1, Waiting: 5}}, `{"processing_limit_enabled":true,"processing":{"limit":2,"in_use":1,"waiting":5}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := workerLineSample()
			sample.State = &tc.state
			before, stateBefore := sample, *sample.State
			var out bytes.Buffer
			if err := loadgen.WriteWorkerSampleJSON(&out, sample); err != nil {
				t.Fatal(err)
			}
			want := `{"source_kind":"worker","schema_version":1,"index":7,"started_at":"2026-09-30T02:00:00.000000123Z","finished_at":"2026-09-30T02:00:00.00000013Z","duration_ns":7,"state":` + tc.stateJSON + `,"error":null}`
			if !reflect.DeepEqual(decodeWorkerLine(t, out.Bytes()), decodeBatchJSON(t, []byte(want))) {
				t.Fatalf("line=%s", out.Bytes())
			}
			if sample != before || *sample.State != stateBefore {
				t.Fatal("input changed")
			}
		})
	}
}

// TestWorkerSampleJSONFailures 验证错误是可保存事实，空文本与多行错误不会丢失或拆成多条记录。
func TestWorkerSampleJSONFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"empty", errors.New("")}, {"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
		{"multiline", errors.Join(errors.New("查询 \"worker\" 失败\n下一行"), context.DeadlineExceeded)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := workerLineSample()
			sample.State = nil
			sample.Err = tc.err
			before := sample
			var out bytes.Buffer
			if err := loadgen.WriteWorkerSampleJSON(&out, sample); err != nil {
				t.Fatal("query failure must still be writable:", err)
			}
			doc := decodeWorkerLine(t, out.Bytes())
			assertJSONNull(t, doc, "state")
			if value, ok := doc["error"]; !ok || value != tc.err.Error() {
				t.Fatalf("lost error text: %#v", doc)
			}
			if sample != before {
				t.Fatal("input changed")
			}
		})
	}
}

// TestWorkerSampleJSONDuration 验证记录耗时独立于墙钟差值，保持完整 int64 精度。
func TestWorkerSampleJSONDuration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		duration  time.Duration
		backwards bool
	}{
		{"zero", 0, false}, {"nanoseconds", 7, false}, {"max_int64", time.Duration(math.MaxInt64), false}, {"wall_clock_backwards", 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := workerLineSample()
			sample.Duration = tc.duration
			if tc.backwards {
				sample.FinishedAt = sample.StartedAt.Add(-time.Second)
			}
			var out bytes.Buffer
			if err := loadgen.WriteWorkerSampleJSON(&out, sample); err != nil {
				t.Fatal(err)
			}
			doc := decodeWorkerLine(t, out.Bytes())
			if doc["duration_ns"] != json.Number(strconv.FormatInt(int64(tc.duration), 10)) {
				t.Fatalf("duration changed: %s", out.Bytes())
			}
			if doc["finished_at"] != sample.FinishedAt.UTC().Format(time.RFC3339Nano) {
				t.Fatal("wall clock timestamp changed")
			}
		})
	}
}

type workerLineWriter = gatewayLineWriter

// TestWorkerSampleJSONRejectsBeforeWrite 验证校验及编码失败不会污染已经写出的前缀。
func TestWorkerSampleJSONRejectsBeforeWrite(t *testing.T) {
	t.Run("nil_writer", func(t *testing.T) {
		if loadgen.WriteWorkerSampleJSON(nil, workerLineSample()) == nil {
			t.Fatal("accepted nil writer")
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*loadgen.WorkerSample)
		encode bool
	}{
		{"negative_index", func(s *loadgen.WorkerSample) { s.Index = -1 }, false},
		{"missing_start", func(s *loadgen.WorkerSample) { s.StartedAt = time.Time{} }, false},
		{"missing_finish", func(s *loadgen.WorkerSample) { s.FinishedAt = time.Time{} }, false},
		{"negative_duration", func(s *loadgen.WorkerSample) { s.Duration = -1 }, false},
		{"no_state_or_error", func(s *loadgen.WorkerSample) { s.State = nil }, false},
		{"state_and_error", func(s *loadgen.WorkerSample) { s.Err = context.Canceled }, false},
		{"disabled_counts", func(s *loadgen.WorkerSample) { s.State.ProcessingLimitEnabled = false }, false},
		{"negative_waiting", func(s *loadgen.WorkerSample) { s.State.Processing.Waiting = -1 }, false},
		{"zero_limit", func(s *loadgen.WorkerSample) { s.State.Processing.Limit = 0 }, false},
		{"negative_limit", func(s *loadgen.WorkerSample) { s.State.Processing.Limit = -1 }, false},
		{"negative_active", func(s *loadgen.WorkerSample) { s.State.Processing.InUse = -1 }, false},
		{"above_limit", func(s *loadgen.WorkerSample) { s.State.Processing.InUse = 65 }, false},
		{"unencodable_start", func(s *loadgen.WorkerSample) { s.StartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
		{"unencodable_finish", func(s *loadgen.WorkerSample) { s.FinishedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := workerLineSample()
			tc.mutate(&sample)
			w := &workerLineWriter{}
			err := loadgen.WriteWorkerSampleJSON(w, sample)
			if err == nil || w.calls != 0 || w.closed || w.flushed {
				t.Fatalf("err=%v writer=%+v", err, w)
			}
			if tc.encode {
				var encodeErr *json.MarshalerError
				if !errors.As(err, &encodeErr) {
					t.Fatalf("encoding error chain lost: %v", err)
				}
			}
		})
	}
}

// TestWorkerSampleJSONWriter 验证一次写入、底层错误与短写；调用方保留 Flush/Close 所有权。
func TestWorkerSampleJSONWriter(t *testing.T) {
	sentinel := errors.New("writer failed")
	for _, tc := range []struct {
		name            string
		partial         bool
		writerErr, want error
	}{
		{"success", false, nil, nil}, {"error", false, sentinel, sentinel},
		{"partial_error", true, sentinel, sentinel}, {"short_write", true, nil, io.ErrShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &workerLineWriter{jsonFaultWriter: jsonFaultWriter{partial: tc.partial, err: tc.writerErr}}
			err := loadgen.WriteWorkerSampleJSON(w, workerLineSample())
			if !errors.Is(err, tc.want) || w.calls != 1 || w.closed || w.flushed {
				t.Fatalf("err=%v want=%v writer=%+v", err, tc.want, w)
			}
		})
	}
}

// TestWorkerSampleJSONSampling 验证真实 HTTP 成功、503、在途取消均通过 emit 保存为独立行。
func TestWorkerSampleJSONSampling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, workerProbeJSON)
		case 2:
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		default:
			cancel()
			<-r.Context().Done()
		}
	}))
	defer srv.Close()
	cfg := workerSamplingConfig()
	cfg.Endpoint = srv.URL
	cfg.Interval = time.Millisecond
	var out bytes.Buffer
	var samples []loadgen.WorkerSample
	err := loadgen.RunWorkerSampling(ctx, srv.Client(), cfg, func(s loadgen.WorkerSample) error {
		samples = append(samples, s)
		return loadgen.WriteWorkerSampleJSON(&out, s)
	})
	if !errors.Is(err, context.Canceled) || calls.Load() != 3 || len(samples) != 3 {
		t.Fatalf("err=%v calls=%d samples=%d", err, calls.Load(), len(samples))
	}
	if !errors.Is(samples[2].Err, context.Canceled) {
		t.Fatalf("missing canceled query: %v", samples[2].Err)
	}
	if bytes.Count(out.Bytes(), []byte("\n")) != 3 {
		t.Fatalf("not three JSON lines: %q", out.Bytes())
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	for i, line := range lines {
		doc := decodeWorkerLine(t, []byte(line+"\n"))
		if doc["index"] != json.Number(strconv.Itoa(i)) || doc["duration_ns"] != json.Number(strconv.FormatInt(int64(samples[i].Duration), 10)) {
			t.Fatalf("sample identity/duration lost: %s", line)
		}
		if i == 0 {
			assertJSONNull(t, doc, "error")
			if doc["state"] == nil {
				t.Fatal("success has no state")
			}
		} else {
			assertJSONNull(t, doc, "state")
			if doc["error"] != samples[i].Err.Error() {
				t.Fatal("failure not preserved")
			}
		}
	}
}

// TestWorkerSampleJSONWriteFailureStopsSampling 验证真正的写入失败沿 emit 传播，不重试或追加记录。
func TestWorkerSampleJSONWriteFailureStopsSampling(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return workerSamplingResponse(req, 0), nil }}}
	sentinel := errors.New("sample output failed")
	w := &workerLineWriter{jsonFaultWriter: jsonFaultWriter{partial: true, err: sentinel}}
	err := loadgen.RunWorkerSampling(context.Background(), client, workerSamplingConfig(), func(s loadgen.WorkerSample) error { return loadgen.WriteWorkerSampleJSON(w, s) })
	if !errors.Is(err, sentinel) || calls != 1 || w.calls != 1 || w.closed || w.flushed {
		t.Fatalf("err=%v queries=%d writer=%+v", err, calls, w)
	}
}
