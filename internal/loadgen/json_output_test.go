package loadgen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/wsclient"
)

// TestBatchJSONContract 校验外部文档契约，包含字段集合、单位、UTC 和缺失值。
// 使用独立 JSON 期望值，避免复用内部输出类型掩盖字段标签错误。
func TestBatchJSONContract(t *testing.T) {
	r := summaryReport(1)
	at := time.Date(2026, 9, 29, 10, 0, 0, 123, time.FixedZone("fixture", 8*3600))
	r.StartedAt, r.FinishedAt = at, at.Add(time.Second)
	s := &r.Results[0].Report
	s.StartedAt, s.FinishedAt = r.StartedAt, r.FinishedAt
	setSummaryTail(&r, 0, time.Second)
	s.Observation = loadgen.SessionObservation{
		AudioBytesWritten: 1000, AudioChunksWritten: 250,
		MaxAudioWriteDuration: 123,
		FirstAudioStartedAt:   at, LastAudioFinishedAt: at,
		StartWrite:  wsclient.WriteEvent{Kind: wsclient.WriteStart, StartedAt: at, FinishedAt: at},
		EndWrite:    wsclient.WriteEvent{Kind: wsclient.WriteEnd, StartedAt: at, FinishedAt: at},
		ResultCount: 3, FinalResultCount: 2,
		FirstResultAt: at, LastResultAt: r.FinishedAt, LastFinalAt: r.FinishedAt,
	}
	before := r
	before.Results = append([]loadgen.SessionResult(nil), r.Results...)
	tailPtr, tailValue := s.TailLatency, *s.TailLatency
	var out bytes.Buffer
	if err := loadgen.WriteBatchJSON(&out, r, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
		t.Fatal("document must end with a newline")
	}
	want := `{
		"schema_version":1,"tail_percentile_method":"nearest_rank",
		"config":{"sessions":1,"session":{"url":"ws://summary.invalid/","audio_bytes":1000,"chunk_bytes":4,"realtime":false,"timeout_ns":3000000000,"expected_final_text":"expected tail"}},
		"started_at":"2026-09-29T02:00:00.000000123Z","finished_at":"2026-09-29T02:00:01.000000123Z","batch_error":null,
		"summary":{"planned_sessions":1,"completed":1,"failed":0,"canceled":0,"timed_out":0,"completion_rate":1,"planned_audio_bytes":1000,"audio_bytes_written":1000,"elapsed_ns":1000000000,"tail":{"samples":1,"min_ns":1000000000,"p50_ns":1000000000,"p95_ns":1000000000,"max_ns":1000000000}},
		"sessions":[{"index":0,"started_at":"2026-09-29T02:00:00.000000123Z","finished_at":"2026-09-29T02:00:01.000000123Z","outcome":"completed","error":null,"tail_latency_ns":1000000000,
		"observation":{"audio_bytes_written":1000,"audio_chunks_written":250,"write_failures":0,"max_audio_write_duration_ns":123,"first_audio_started_at":"2026-09-29T02:00:00.000000123Z","last_audio_finished_at":"2026-09-29T02:00:00.000000123Z",
		"start_write":{"kind":"start","audio_bytes":0,"started_at":"2026-09-29T02:00:00.000000123Z","finished_at":"2026-09-29T02:00:00.000000123Z","error":null},
		"end_write":{"kind":"end","audio_bytes":0,"started_at":"2026-09-29T02:00:00.000000123Z","finished_at":"2026-09-29T02:00:00.000000123Z","error":null},
		"result_count":3,"final_result_count":2,"first_result_at":"2026-09-29T02:00:00.000000123Z","last_result_at":"2026-09-29T02:00:01.000000123Z","last_final_at":"2026-09-29T02:00:01.000000123Z"}}]
	}`
	if got, expected := decodeBatchJSON(t, out.Bytes()), decodeBatchJSON(t, []byte(want)); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected document:\n%s", out.Bytes())
	}
	if !reflect.DeepEqual(r, before) || s.TailLatency != tailPtr || *tailPtr != tailValue {
		t.Fatal("export modified input, time zone, or tail pointer/value")
	}
}

// TestBatchJSONFailureFacts 验证失败记录也能导出，且空错误与缺失错误不混淆。
func TestBatchJSONFailureFacts(t *testing.T) {
	r := summaryReport(3)
	joined := errors.Join(errors.New("write \"end\" failed"), context.Canceled)
	errs := []error{errors.New(""), joined, context.DeadlineExceeded}
	for i, outcome := range []loadgen.SessionOutcome{loadgen.SessionFailed, loadgen.SessionCanceled, loadgen.SessionTimedOut} {
		setSummaryFailure(&r, i, outcome, int64(i*100))
		r.Results[i].Err = errs[i]
	}
	r.Results[1].Report.Observation.WriteFailures = 1
	r.Results[1].Report.Observation.EndWrite = recorderWrite(wsclient.WriteEnd, 0, recorderBaseTime(), 7, joined)
	var out bytes.Buffer
	if err := loadgen.WriteBatchJSON(&out, r, joined); err != nil {
		t.Fatal("batch error must not prevent export:", err)
	}
	doc := decodeBatchJSON(t, out.Bytes())
	if doc["batch_error"] != joined.Error() {
		t.Fatalf("batch error = %#v", doc["batch_error"])
	}
	summary := doc["summary"].(map[string]any)
	wantSummary := decodeBatchJSON(t, []byte(`{"planned_sessions":3,"completed":0,"failed":1,"canceled":1,"timed_out":1,"completion_rate":0,"planned_audio_bytes":3000,"audio_bytes_written":300,"elapsed_ns":1000000000,"tail":null}`))
	if !reflect.DeepEqual(summary, wantSummary) {
		t.Fatalf("summary = %#v", summary)
	}
	for i, value := range doc["sessions"].([]any) {
		session := value.(map[string]any)
		if session["index"] != json.Number(strconv.Itoa(i)) || session["error"] != errs[i].Error() || session["outcome"] != string(r.Results[i].Report.Outcome) {
			t.Fatalf("session %d lost identity or error: %#v", i, session)
		}
		assertJSONNull(t, session, "tail_latency_ns")
		obs := session["observation"].(map[string]any)
		for _, key := range []string{"first_audio_started_at", "last_audio_finished_at", "first_result_at", "last_result_at", "last_final_at", "start_write"} {
			assertJSONNull(t, obs, key)
		}
		if i != 1 {
			assertJSONNull(t, obs, "end_write")
			continue
		}
		end := obs["end_write"].(map[string]any)
		if obs["write_failures"] != json.Number("1") || end["kind"] != "end" || end["error"] != joined.Error() {
			t.Fatalf("failed control event lost: %#v", obs)
		}
	}
}

// TestBatchJSONDurationPrecision 避免整数毫秒或 float64 转换丢失精度。
func TestBatchJSONDurationPrecision(t *testing.T) {
	for _, tc := range []struct {
		name, number string
		duration     time.Duration
	}{
		{"zero", "0", 0}, {"nanoseconds", "7", 7}, {"max_int64", "9223372036854775807", time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := summaryReport(1)
			setSummaryTail(&r, 0, tc.duration)
			var out bytes.Buffer
			if err := loadgen.WriteBatchJSON(&out, r, nil); err != nil {
				t.Fatal(err)
			}
			doc := decodeBatchJSON(t, out.Bytes())
			session := doc["sessions"].([]any)[0].(map[string]any)
			if session["tail_latency_ns"] != json.Number(tc.number) {
				t.Fatalf("tail = %v", session["tail_latency_ns"])
			}
			tail := doc["summary"].(map[string]any)["tail"].(map[string]any)
			for _, key := range []string{"min_ns", "p50_ns", "p95_ns", "max_ns"} {
				if tail[key] != json.Number(tc.number) {
					t.Fatalf("%s = %v", key, tail[key])
				}
			}
		})
	}
}

// TestBatchJSONPrewriteFailures 保证校验和编码错误不会先写出半份文档。
func TestBatchJSONPrewriteFailures(t *testing.T) {
	t.Run("nil_writer", func(t *testing.T) {
		if err := loadgen.WriteBatchJSON(nil, summaryReport(1), nil); err == nil {
			t.Fatal("nil writer accepted")
		}
	})
	for _, tc := range []struct {
		name string
		edit func(*loadgen.BatchReport)
	}{
		{"invalid_report", func(r *loadgen.BatchReport) { r.Results[0].Index = 1 }},
		{"unencodable_timestamp", func(r *loadgen.BatchReport) {
			r.Results[0].Report.Observation.FirstResultAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := summaryReport(1)
			tc.edit(&r)
			w := &jsonFaultWriter{}
			if err := loadgen.WriteBatchJSON(w, r, nil); err == nil {
				t.Fatal("invalid report accepted")
			}
			if w.calls != 0 || w.closed {
				t.Fatalf("writer touched before validation/encoding completed: %+v", w)
			}
		})
	}
}

func TestBatchJSONWriterOutcomes(t *testing.T) {
	sentinel := errors.New("disk unavailable")
	for _, tc := range []struct {
		name      string
		partial   bool
		err, want error
	}{
		{"error", false, sentinel, sentinel},
		{"partial_with_error", true, sentinel, sentinel},
		{"short_without_error", true, nil, io.ErrShortWrite},
		{"success_without_close", false, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &jsonFaultWriter{partial: tc.partial, err: tc.err}
			err := loadgen.WriteBatchJSON(w, summaryReport(1), nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want wrapped %v", err, tc.want)
			}
			if w.calls != 1 || w.closed {
				t.Fatalf("unexpected retry or close: %+v", w)
			}
		})
	}
}

// TestBatchJSONFromNetworkBatch 使用真实 WebSocket 批次，验证混合结果可一起保存。
func TestBatchJSONFromNetworkBatch(t *testing.T) {
	url, wait := batchPeer(t, 2, func(ctx context.Context, conn *websocket.Conn, arrival int64) error {
		if err := readSessionInput(ctx, conn, 10, 4); err != nil {
			return err
		}
		text := "expected tail"
		if arrival == 1 {
			text = "incorrect tail"
		}
		if err := writeSessionJSON(ctx, conn, sessionResult(text, true)); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "done")
	})
	r, batchErr := loadgen.RunBatch(context.Background(), loadgen.BatchConfig{Sessions: 2, Session: sessionConfig(url)})
	wait()
	if batchErr != nil {
		t.Fatal(batchErr)
	}
	var out bytes.Buffer
	if err := loadgen.WriteBatchJSON(&out, r, batchErr); err != nil {
		t.Fatal(err)
	}
	doc := decodeBatchJSON(t, out.Bytes())
	summary := doc["summary"].(map[string]any)
	if summary["completed"] != json.Number("1") || summary["failed"] != json.Number("1") || summary["completion_rate"] != json.Number("0.5") || summary["audio_bytes_written"] != json.Number("20") {
		t.Fatalf("mixed network summary = %#v", summary)
	}
	for i, value := range doc["sessions"].([]any) {
		s := value.(map[string]any)
		if s["outcome"] != string(r.Results[i].Report.Outcome) {
			t.Fatalf("session %d changed outcome", i)
		}
		if r.Results[i].Err != nil && s["error"] != r.Results[i].Err.Error() {
			t.Fatalf("session %d lost error", i)
		}
		obs := s["observation"].(map[string]any)
		if obs["audio_bytes_written"] != json.Number("10") || obs["audio_chunks_written"] != json.Number("3") || obs["final_result_count"] != json.Number("1") {
			t.Fatalf("network observation = %#v", obs)
		}
	}
}

// decodeBatchJSON 保留整数文本精度，并拒绝多个文档或尾部非空垃圾。
func decodeBatchJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("extra JSON content: %v, %v", extra, err)
	}
	return doc
}

// assertJSONNull 同时检查字段存在，避免漏字段被误当成 null。
func assertJSONNull(t *testing.T, object map[string]any, key string) {
	t.Helper()
	if value, ok := object[key]; !ok || value != nil {
		t.Fatalf("%s = %#v, present=%v; want explicit null", key, value, ok)
	}
}

// jsonFaultWriter 模拟外部写入失败；Close 用于发现对调用方资源的越权关闭。
type jsonFaultWriter struct {
	partial bool
	err     error
	calls   int
	closed  bool
}

func (w *jsonFaultWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.partial {
		return len(data) / 2, w.err
	}
	if w.err == nil {
		return len(data), nil
	}
	return 0, w.err
}

func (w *jsonFaultWriter) Close() error { w.closed = true; return nil }
