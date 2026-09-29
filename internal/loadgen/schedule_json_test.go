package loadgen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestBatchJSONScheduleValues 核对 v2 缺失/零值/整数精度，以及失败会话不丢观测。
func TestBatchJSONScheduleValues(t *testing.T) {
	for _, tc := range []struct {
		name    string
		count   int64
		lag     time.Duration
		outcome loadgen.SessionOutcome
	}{
		{"no_samples", 0, 0, loadgen.SessionCompleted},
		{"observed_zero", 1, 0, loadgen.SessionCompleted},
		{"nanoseconds", 3, 7, loadgen.SessionCompleted},
		{"int64_precision", math.MaxInt64, time.Duration(math.MaxInt64), loadgen.SessionCompleted},
		{"failed", 3, 40 * time.Millisecond, loadgen.SessionFailed},
		{"canceled", 3, 40 * time.Millisecond, loadgen.SessionCanceled},
		{"timed_out", 3, 40 * time.Millisecond, loadgen.SessionTimedOut},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := summaryReport(1)
			if tc.outcome != loadgen.SessionCompleted {
				setSummaryFailure(&r, 0, tc.outcome, 4)
			}
			r.Results[0].Report.Observation.AudioScheduleSamples = tc.count
			r.Results[0].Report.Observation.MaxAudioScheduleLag = tc.lag
			before := r.Results[0].Report.Observation
			var out bytes.Buffer
			if err := loadgen.WriteBatchJSON(&out, r, nil); err != nil {
				t.Fatal(err)
			}
			doc := decodeBatchJSON(t, out.Bytes())
			if doc["schema_version"] != json.Number("2") {
				t.Fatal("wrong schema version")
			}
			s := doc["sessions"].([]any)[0].(map[string]any)
			o := s["observation"].(map[string]any)
			if o["audio_schedule_samples"] != json.Number(strconv.FormatInt(tc.count, 10)) {
				t.Fatalf("samples = %v", o["audio_schedule_samples"])
			}
			if tc.count == 0 {
				assertJSONNull(t, o, "max_audio_schedule_lag_ns")
			} else if o["max_audio_schedule_lag_ns"] != json.Number(strconv.FormatInt(int64(tc.lag), 10)) {
				t.Fatalf("max lag = %v", o["max_audio_schedule_lag_ns"])
			}
			if s["outcome"] != string(tc.outcome) || (tc.outcome != loadgen.SessionCompleted && s["error"] == nil) {
				t.Fatal("failure classification or error lost")
			}
			if !reflect.DeepEqual(r.Results[0].Report.Observation, before) {
				t.Fatal("JSON export mutated observation")
			}
		})
	}
}

// TestInvalidScheduleRejectsBeforeOutput 第二场矛盾也必须返回零摘要且不触碰 writer。
func TestInvalidScheduleRejectsBeforeOutput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int64
		lag   time.Duration
	}{
		{"negative_samples", -1, 0}, {"negative_lag", 1, -1}, {"zero_samples_nonzero_lag", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := summaryReport(2)
			r.Results[1].Report.Observation.AudioScheduleSamples = tc.count
			r.Results[1].Report.Observation.MaxAudioScheduleLag = tc.lag
			summary, err := loadgen.SummarizeBatch(r)
			if err == nil || !strings.Contains(err.Error(), "result 1") || !reflect.DeepEqual(summary, loadgen.BatchSummary{}) {
				t.Fatalf("summary=%+v error=%v", summary, err)
			}
			w := &jsonFaultWriter{}
			err = loadgen.WriteBatchJSON(w, r, nil)
			if err == nil || !strings.Contains(err.Error(), "result 1") || w.calls != 0 || w.closed {
				t.Fatalf("export error=%v writer=%+v", err, w)
			}
		})
	}
}

// TestBatchJSONScheduleFromNetwork 从真实实时/非实时批次核对最终导出字段。
func TestBatchJSONScheduleFromNetwork(t *testing.T) {
	for _, realtime := range []bool{false, true} {
		name := "unpaced"
		if realtime {
			name = "realtime"
		}
		t.Run(name, func(t *testing.T) {
			url, wait := batchPeer(t, 2, func(ctx context.Context, conn *websocket.Conn, _ int64) error {
				if err := readSessionInput(ctx, conn, 10, 4); err != nil {
					return err
				}
				if err := writeSessionJSON(ctx, conn, sessionResult("expected tail", true)); err != nil {
					return err
				}
				return conn.Close(websocket.StatusNormalClosure, "done")
			})
			cfg := loadgen.BatchConfig{Sessions: 2, Session: sessionConfig(url)}
			cfg.Session.Realtime = realtime
			r, err := loadgen.RunBatch(context.Background(), cfg)
			wait()
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := loadgen.WriteBatchJSON(&out, r, nil); err != nil {
				t.Fatal(err)
			}
			doc := decodeBatchJSON(t, out.Bytes())
			for i, value := range doc["sessions"].([]any) {
				s := value.(map[string]any)
				if s["outcome"] != "completed" {
					t.Fatalf("unexpected outcome %v", s["outcome"])
				}
				o := s["observation"].(map[string]any)
				if realtime {
					want := strconv.FormatInt(int64(r.Results[i].Report.Observation.MaxAudioScheduleLag), 10)
					if o["audio_schedule_samples"] != json.Number("3") || o["max_audio_schedule_lag_ns"] != json.Number(want) {
						t.Fatalf("schedule output = %v", o)
					}
				} else {
					if o["audio_schedule_samples"] != json.Number("0") {
						t.Fatal("unpaced input produced samples")
					}
					assertJSONNull(t, o, "max_audio_schedule_lag_ns")
				}
			}
		})
	}
}
