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

// gatewayLineSample 构造带非 UTC 时区与纳秒精度的合法样本。
func gatewayLineSample() loadgen.GatewaySample {
	at := time.Date(2026, 9, 30, 10, 0, 0, 123, time.FixedZone("fixture", 8*3600))
	return loadgen.GatewaySample{Index: 7, StartedAt: at, FinishedAt: at.Add(7 * time.Nanosecond), Duration: 7 * time.Nanosecond,
		State: &loadgen.GatewayState{MaxSessions: 64}}
}

// decodeGatewayLine 要求恰好一行带终止换行的 JSON，保留整数精度。
func decodeGatewayLine(t *testing.T, data []byte) map[string]any {
	t.Helper()
	if bytes.Count(data, []byte("\n")) != 1 || !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("expected exactly one terminated JSON line: %q", data)
	}
	return decodeBatchJSON(t, data)
}

// TestGatewaySampleJSONContract 独立校验完整格式、零值和停止状态，不复用生产 DTO。
func TestGatewaySampleJSONContract(t *testing.T) {
	for _, stopping := range []bool{false, true} {
		t.Run(strconv.FormatBool(stopping), func(t *testing.T) {
			sample := gatewayLineSample()
			sample.State.Stopping = stopping
			active := "0"
			if stopping {
				sample.State.ActiveSessions = 64
				active = "64"
			}
			before, stateBefore := sample, *sample.State
			var out bytes.Buffer
			if err := loadgen.WriteGatewaySampleJSON(&out, sample); err != nil {
				t.Fatal(err)
			}
			want := `{"schema_version":1,"index":7,"started_at":"2026-09-30T02:00:00.000000123Z","finished_at":"2026-09-30T02:00:00.00000013Z","duration_ns":7,"state":{"active_sessions":` + active + `,"max_sessions":64,"stopping":` + strconv.FormatBool(stopping) + `},"error":null}`
			if got := decodeGatewayLine(t, out.Bytes()); !reflect.DeepEqual(got, decodeBatchJSON(t, []byte(want))) {
				t.Fatalf("unexpected line: %s", out.Bytes())
			}
			if sample != before || *sample.State != stateBefore {
				t.Fatal("output modified input or its state")
			}
		})
	}
}

// TestGatewaySampleJSONFailures 验证错误是可保存事实，空文本与多行错误不会丢失或拆成多条记录。
func TestGatewaySampleJSONFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"empty", errors.New("")}, {"canceled", context.Canceled}, {"deadline", context.DeadlineExceeded},
		{"multiline", errors.Join(errors.New("查询 \"gateway\" 失败\n下一行"), context.DeadlineExceeded)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := gatewayLineSample()
			sample.State = nil
			sample.Err = tc.err
			before := sample
			var out bytes.Buffer
			if err := loadgen.WriteGatewaySampleJSON(&out, sample); err != nil {
				t.Fatal("query failure must still be writable:", err)
			}
			doc := decodeGatewayLine(t, out.Bytes())
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

// TestGatewaySampleJSONDuration 验证记录耗时独立于墙钟差值，保持完整 int64 精度。
func TestGatewaySampleJSONDuration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		duration  time.Duration
		backwards bool
	}{
		{"zero", 0, false}, {"nanoseconds", 7, false}, {"max_int64", time.Duration(math.MaxInt64), false}, {"wall_clock_backwards", 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := gatewayLineSample()
			sample.Duration = tc.duration
			if tc.backwards {
				sample.FinishedAt = sample.StartedAt.Add(-time.Second)
			}
			var out bytes.Buffer
			if err := loadgen.WriteGatewaySampleJSON(&out, sample); err != nil {
				t.Fatal(err)
			}
			doc := decodeGatewayLine(t, out.Bytes())
			if doc["duration_ns"] != json.Number(strconv.FormatInt(int64(tc.duration), 10)) {
				t.Fatalf("duration changed: %s", out.Bytes())
			}
			if doc["finished_at"] != sample.FinishedAt.UTC().Format(time.RFC3339Nano) {
				t.Fatal("wall clock timestamp changed")
			}
		})
	}
}

// gatewayLineWriter 观察写入、关闭与刷新；本函数只拥有单次写入。
type gatewayLineWriter struct {
	jsonFaultWriter
	flushed bool
}

func (w *gatewayLineWriter) Flush() error { w.flushed = true; return nil }

// TestGatewaySampleJSONRejectsBeforeWrite 验证校验及编码失败不会污染已经写出的前缀。
func TestGatewaySampleJSONRejectsBeforeWrite(t *testing.T) {
	t.Run("nil_writer", func(t *testing.T) {
		if loadgen.WriteGatewaySampleJSON(nil, gatewayLineSample()) == nil {
			t.Fatal("accepted nil writer")
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*loadgen.GatewaySample)
		encode bool
	}{
		{"negative_index", func(s *loadgen.GatewaySample) { s.Index = -1 }, false},
		{"missing_start", func(s *loadgen.GatewaySample) { s.StartedAt = time.Time{} }, false},
		{"missing_finish", func(s *loadgen.GatewaySample) { s.FinishedAt = time.Time{} }, false},
		{"negative_duration", func(s *loadgen.GatewaySample) { s.Duration = -1 }, false},
		{"no_state_or_error", func(s *loadgen.GatewaySample) { s.State = nil }, false},
		{"state_and_error", func(s *loadgen.GatewaySample) { s.Err = context.Canceled }, false},
		{"zero_limit", func(s *loadgen.GatewaySample) { s.State.MaxSessions = 0 }, false},
		{"negative_limit", func(s *loadgen.GatewaySample) { s.State.MaxSessions = -1 }, false},
		{"negative_active", func(s *loadgen.GatewaySample) { s.State.ActiveSessions = -1 }, false},
		{"above_limit", func(s *loadgen.GatewaySample) { s.State.ActiveSessions = 65 }, false},
		{"unencodable_start", func(s *loadgen.GatewaySample) { s.StartedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
		{"unencodable_finish", func(s *loadgen.GatewaySample) { s.FinishedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sample := gatewayLineSample()
			tc.mutate(&sample)
			w := &gatewayLineWriter{}
			err := loadgen.WriteGatewaySampleJSON(w, sample)
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

// TestGatewaySampleJSONWriter 验证一次写入、底层错误与短写；调用方保留 Flush/Close 所有权。
func TestGatewaySampleJSONWriter(t *testing.T) {
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
			w := &gatewayLineWriter{jsonFaultWriter: jsonFaultWriter{partial: tc.partial, err: tc.writerErr}}
			err := loadgen.WriteGatewaySampleJSON(w, gatewayLineSample())
			if !errors.Is(err, tc.want) || w.calls != 1 || w.closed || w.flushed {
				t.Fatalf("err=%v want=%v writer=%+v", err, tc.want, w)
			}
		})
	}
}

// TestGatewaySampleJSONSampling 验证真实 HTTP 成功、503、在途取消均通过 emit 保存为独立行。
func TestGatewaySampleJSONSampling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, probeValidJSON)
		case 2:
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		default:
			cancel()
			<-r.Context().Done()
		}
	}))
	defer srv.Close()
	cfg := samplingConfig()
	cfg.Endpoint = srv.URL
	cfg.Interval = time.Millisecond
	var out bytes.Buffer
	var samples []loadgen.GatewaySample
	err := loadgen.RunGatewaySampling(ctx, srv.Client(), cfg, func(s loadgen.GatewaySample) error {
		samples = append(samples, s)
		return loadgen.WriteGatewaySampleJSON(&out, s)
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
		doc := decodeGatewayLine(t, []byte(line+"\n"))
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

// TestGatewaySampleJSONWriteFailureStopsSampling 验证真正的写入失败沿 emit 传播，不重试或追加记录。
func TestGatewaySampleJSONWriteFailureStopsSampling(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) { calls++; return samplingResponse(req, 0), nil }}}
	sentinel := errors.New("sample output failed")
	w := &gatewayLineWriter{jsonFaultWriter: jsonFaultWriter{partial: true, err: sentinel}}
	err := loadgen.RunGatewaySampling(context.Background(), client, samplingConfig(), func(s loadgen.GatewaySample) error { return loadgen.WriteGatewaySampleJSON(w, s) })
	if !errors.Is(err, sentinel) || calls != 1 || w.calls != 1 || w.closed || w.flushed {
		t.Fatalf("err=%v queries=%d writer=%+v", err, calls, w)
	}
}
