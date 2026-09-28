package loadgen_test

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestSummarizeMixedBatch 验证全部计划会话作分母，非完成会话已经成功写出的
// 音频仍贡献实际输入量，但不贡献尾部延迟样本。
func TestSummarizeMixedBatch(t *testing.T) {
	report := summaryReport(5)
	setSummaryTail(&report, 0, 80*time.Millisecond)
	setSummaryTail(&report, 1, 20*time.Millisecond)
	setSummaryFailure(&report, 2, loadgen.SessionFailed, 400)
	setSummaryFailure(&report, 3, loadgen.SessionCanceled, 200)
	setSummaryFailure(&report, 4, loadgen.SessionTimedOut, 100)
	got, err := loadgen.SummarizeBatch(report)
	if err != nil {
		t.Fatal(err)
	}
	want := loadgen.BatchSummary{
		PlannedSessions: 5, Completed: 2, Failed: 1, Canceled: 1, TimedOut: 1,
		CompletionRate: 0.4, PlannedAudioBytes: 5000, AudioBytesWritten: 2700,
		Elapsed: time.Second, Tail: &loadgen.LatencySummary{
			Samples: 2, Min: 20 * time.Millisecond, P50: 20 * time.Millisecond,
			P95: 80 * time.Millisecond, Max: 80 * time.Millisecond,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("summary = %+v, tail = %+v; want %+v", got, got.Tail, want)
	}
}

func TestSummarizeNoCompletedSamples(t *testing.T) {
	report := summaryReport(3)
	for i, outcome := range []loadgen.SessionOutcome{loadgen.SessionFailed, loadgen.SessionCanceled, loadgen.SessionTimedOut} {
		setSummaryFailure(&report, i, outcome, 100)
	}
	got, err := loadgen.SummarizeBatch(report)
	if err != nil {
		t.Fatal(err)
	}
	want := loadgen.BatchSummary{PlannedSessions: 3, Failed: 1, Canceled: 1, TimedOut: 1,
		PlannedAudioBytes: 3000, AudioBytesWritten: 300, Elapsed: time.Second}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("summary = %+v, want %+v with nil tail", got, want)
	}
}

// TestSummarizePercentiles 使用明确的排序位置验证 nearest-rank，
// 包含纳秒、零、极大时长和样本数大于 100 的情况。
func TestSummarizePercentiles(t *testing.T) {
	for _, tc := range []struct {
		name    string
		samples []time.Duration
		want    loadgen.LatencySummary
	}{
		{"single_zero", []time.Duration{0}, loadgen.LatencySummary{Samples: 1}},
		{"single_nanosecond", []time.Duration{7}, loadgen.LatencySummary{Samples: 1, Min: 7, P50: 7, P95: 7, Max: 7}},
		{"single_max_duration", []time.Duration{time.Duration(math.MaxInt64)}, loadgen.LatencySummary{Samples: 1, Min: time.Duration(math.MaxInt64), P50: time.Duration(math.MaxInt64), P95: time.Duration(math.MaxInt64), Max: time.Duration(math.MaxInt64)}},
		{"two_unsorted", []time.Duration{20, 10}, loadgen.LatencySummary{Samples: 2, Min: 10, P50: 10, P95: 20, Max: 20}},
		{"duplicates", []time.Duration{9, 1, 1, 5, 1}, loadgen.LatencySummary{Samples: 5, Min: 1, P50: 1, P95: 9, Max: 9}},
		{"twenty", descendingDurations(20), loadgen.LatencySummary{Samples: 20, Min: 1, P50: 10, P95: 19, Max: 20}},
		{"one_hundred_one", descendingDurations(101), loadgen.LatencySummary{Samples: 101, Min: 1, P50: 51, P95: 96, Max: 101}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := summaryReport(len(tc.samples))
			for i, sample := range tc.samples {
				setSummaryTail(&report, i, sample)
			}
			got, err := loadgen.SummarizeBatch(report)
			if err != nil {
				t.Fatal(err)
			}
			if got.Tail == nil || *got.Tail != tc.want || got.CompletionRate != 1 {
				t.Fatalf("tail = %+v, rate=%v, want %+v and 1", got.Tail, got.CompletionRate, tc.want)
			}
		})
	}
}

// TestSummarizeRejectsInconsistentReports 多数错误放在第二条结果，
// 验证统计已有部分进展后仍返回零摘要，不泄露半份统计。
func TestSummarizeRejectsInconsistentReports(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*loadgen.BatchReport)
	}{
		{"zero_report", func(r *loadgen.BatchReport) { *r = loadgen.BatchReport{} }},
		{"negative_sessions", func(r *loadgen.BatchReport) { r.Config.Sessions = -1 }},
		{"invalid_session_config", func(r *loadgen.BatchReport) { r.Config.Session.Timeout = 0 }},
		{"unaligned_audio_config", func(r *loadgen.BatchReport) { r.Config.Session.AudioBytes = 3 }},
		{"missing_result", func(r *loadgen.BatchReport) { r.Results = r.Results[:1] }},
		{"extra_result", func(r *loadgen.BatchReport) { r.Results = append(r.Results, r.Results[0]) }},
		{"zero_start", func(r *loadgen.BatchReport) { r.StartedAt = time.Time{} }},
		{"zero_finish", func(r *loadgen.BatchReport) { r.FinishedAt = time.Time{} }},
		{"reversed_time", func(r *loadgen.BatchReport) { r.FinishedAt = r.StartedAt.Add(-1) }},
		{"planned_bytes_overflow", func(r *loadgen.BatchReport) {
			r.Config.Session.AudioBytes = math.MaxInt64 - 1
			for i := range r.Results {
				r.Results[i].Report.Config = r.Config.Session
			}
		}},
		{"negative_index", func(r *loadgen.BatchReport) { r.Results[1].Index = -1 }},
		{"duplicate_index", func(r *loadgen.BatchReport) { r.Results[1].Index = 0 }},
		{"out_of_range_index", func(r *loadgen.BatchReport) { r.Results[1].Index = 2 }},
		{"swapped_slots", func(r *loadgen.BatchReport) { r.Results[0], r.Results[1] = r.Results[1], r.Results[0] }},
		{"mismatched_config", func(r *loadgen.BatchReport) { r.Results[1].Report.Config.Realtime = true }},
		{"negative_written", func(r *loadgen.BatchReport) { r.Results[1].Report.Observation.AudioBytesWritten = -1 }},
		{"written_above_plan", func(r *loadgen.BatchReport) { setSummaryFailure(r, 1, loadgen.SessionFailed, 1002) }},
		{"completed_with_error", func(r *loadgen.BatchReport) { r.Results[1].Err = errors.New("unexpected") }},
		{"completed_missing_tail", func(r *loadgen.BatchReport) { r.Results[1].Report.TailLatency = nil }},
		{"completed_negative_tail", func(r *loadgen.BatchReport) { setSummaryTail(r, 1, -1) }},
		{"completed_partial_audio", func(r *loadgen.BatchReport) { r.Results[1].Report.Observation.AudioBytesWritten = 998 }},
		{"empty_outcome", func(r *loadgen.BatchReport) { r.Results[1].Report.Outcome = "" }},
		{"unknown_outcome", func(r *loadgen.BatchReport) { r.Results[1].Report.Outcome = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := summaryReport(2)
			tc.edit(&report)
			assertSummaryRejected(t, report)
		})
	}
	for _, outcome := range []loadgen.SessionOutcome{loadgen.SessionFailed, loadgen.SessionCanceled, loadgen.SessionTimedOut} {
		t.Run(string(outcome)+"_missing_error", func(t *testing.T) {
			report := summaryReport(2)
			setSummaryFailure(&report, 1, outcome, 100)
			report.Results[1].Err = nil
			assertSummaryRejected(t, report)
		})
		t.Run(string(outcome)+"_unexpected_tail", func(t *testing.T) {
			report := summaryReport(2)
			setSummaryFailure(&report, 1, outcome, 100)
			setSummaryTail(&report, 1, 0)
			assertSummaryRejected(t, report)
		})
	}
}

func TestSummarizePreservesInput(t *testing.T) {
	report := summaryReport(3) // 尾部样本按 3、2、1 排列。
	before := report
	before.Results = slices.Clone(report.Results)
	pointers := make([]*time.Duration, len(report.Results))
	for i := range before.Results {
		pointers[i] = report.Results[i].Report.TailLatency
		tail := *before.Results[i].Report.TailLatency
		before.Results[i].Report.TailLatency = &tail
	}
	summary, err := loadgen.SummarizeBatch(report)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(report, before) {
		t.Fatal("summary changed the input report")
	}
	for i := range pointers {
		if report.Results[i].Report.TailLatency != pointers[i] {
			t.Fatal("summary replaced an input tail pointer")
		}
	}
	summary.Tail.Min = 999
	summary.Tail.P50 = 999
	if !reflect.DeepEqual(report, before) {
		t.Fatal("editing summary changed input samples")
	}
}

func TestSummarizeLargeByteCountsAndZeroElapsed(t *testing.T) {
	for _, count := range []int{1, 2} {
		name := "one_session"
		if count == 2 {
			name = "two_sessions"
		}
		t.Run(name, func(t *testing.T) {
			report := summaryReport(count)
			perSession := int64(math.MaxInt64 / int64(count) / 2 * 2)
			report.Config.Session.AudioBytes = perSession
			report.FinishedAt = report.StartedAt
			for i := range report.Results {
				report.Results[i].Report.Config = report.Config.Session
				report.Results[i].Report.Observation.AudioBytesWritten = perSession
			}
			got, err := loadgen.SummarizeBatch(report)
			if err != nil {
				t.Fatal(err)
			}
			wantBytes := perSession * int64(count)
			if got.PlannedAudioBytes != wantBytes || got.AudioBytesWritten != wantBytes || got.Elapsed != 0 {
				t.Fatalf("large byte counts or zero elapsed changed: %+v", got)
			}
		})
	}
}

// summaryReport 构造统计所依赖的字段；不伪装为实际网络执行记录。
func summaryReport(n int) loadgen.BatchReport {
	cfg := loadgen.BatchConfig{Sessions: n, Session: sessionConfig("ws://summary.invalid/")}
	cfg.Session.AudioBytes = 1000
	base := recorderBaseTime()
	report := loadgen.BatchReport{Config: cfg, StartedAt: base, FinishedAt: base.Add(time.Second), Results: make([]loadgen.SessionResult, n)}
	for i := range report.Results {
		report.Results[i] = loadgen.SessionResult{Index: i, Report: loadgen.SessionReport{
			Config: cfg.Session, StartedAt: base, FinishedAt: base.Add(time.Second), Outcome: loadgen.SessionCompleted,
			Observation: loadgen.SessionObservation{AudioBytesWritten: 1000},
		}}
		setSummaryTail(&report, i, time.Duration(n-i))
	}
	return report
}

func setSummaryTail(report *loadgen.BatchReport, index int, duration time.Duration) {
	report.Results[index].Report.TailLatency = &duration
}

func setSummaryFailure(report *loadgen.BatchReport, index int, outcome loadgen.SessionOutcome, written int64) {
	result := &report.Results[index]
	result.Report.Outcome = outcome
	result.Report.TailLatency = nil
	result.Report.Observation.AudioBytesWritten = written
	result.Err = errors.New("fixture failure")
}

func descendingDurations(n int) []time.Duration {
	values := make([]time.Duration, n)
	for i := range values {
		values[i] = time.Duration(n - i)
	}
	return values
}

func assertSummaryRejected(t *testing.T, report loadgen.BatchReport) {
	t.Helper()
	summary, err := loadgen.SummarizeBatch(report)
	if err == nil || !reflect.DeepEqual(summary, loadgen.BatchSummary{}) {
		t.Fatalf("summary = %+v, err = %v; want zero summary and error", summary, err)
	}
}
