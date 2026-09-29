package loadgen_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestRunBatchPreflight 必须在创建会话之前拒绝无效条件；
// 尤其不能把配置错误转成 N 个零会话报告后返回批次成功。
func TestRunBatchPreflight(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	for _, tc := range []struct {
		name string
		edit func(*loadgen.BatchConfig)
	}{
		{"zero_sessions", func(c *loadgen.BatchConfig) { c.Sessions = 0 }},
		{"negative_sessions", func(c *loadgen.BatchConfig) { c.Sessions = -1 }},
		{"zero_timeout", func(c *loadgen.BatchConfig) { c.Session.Timeout = 0 }},
		{"negative_timeout", func(c *loadgen.BatchConfig) { c.Session.Timeout = -1 }},
		{"zero_chunk", func(c *loadgen.BatchConfig) { c.Session.ChunkBytes = 0 }},
		{"negative_chunk", func(c *loadgen.BatchConfig) { c.Session.ChunkBytes = -2 }},
		{"unaligned_chunk", func(c *loadgen.BatchConfig) { c.Session.ChunkBytes = 3 }},
		{"zero_audio", func(c *loadgen.BatchConfig) { c.Session.AudioBytes = 0 }},
		{"negative_audio", func(c *loadgen.BatchConfig) { c.Session.AudioBytes = -2 }},
		{"unaligned_audio", func(c *loadgen.BatchConfig) { c.Session.AudioBytes = 3 }},
		{"empty_tail", func(c *loadgen.BatchConfig) { c.Session.ExpectedFinalText = "" }},
		{"empty_url", func(c *loadgen.BatchConfig) { c.Session.URL = "" }},
		{"planned_total_overflow", func(c *loadgen.BatchConfig) {
			c.Sessions = 2
			c.Session.AudioBytes = math.MaxInt64/2 + 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadgen.BatchConfig{Sessions: 3, Session: sessionConfig("ws" + strings.TrimPrefix(server.URL, "http"))}
			tc.edit(&cfg)
			report, err := loadgen.RunBatch(context.Background(), cfg)
			if err == nil || !reflect.DeepEqual(report, loadgen.BatchReport{}) {
				t.Fatalf("started=%v results=%d err=%v; want preflight error and zero report", report.StartedAt, len(report.Results), err)
			}
		})
	}
	for _, deadline := range []bool{false, true} {
		name := "pre_canceled"
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		want := context.Canceled
		if deadline {
			name = "pre_deadline"
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			cancel()
			want = context.DeadlineExceeded
		}
		t.Run(name, func(t *testing.T) {
			cfg := loadgen.BatchConfig{Sessions: 3, Session: sessionConfig("ws" + strings.TrimPrefix(server.URL, "http"))}
			report, err := loadgen.RunBatch(ctx, cfg)
			if !errors.Is(err, want) || !reflect.DeepEqual(report, loadgen.BatchReport{}) {
				t.Fatalf("report = %+v, err = %v; want zero report and %v", report, err, want)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("preflight failures made %d HTTP requests", got)
	}
}

// TestRunBatchOverlappingSessions 服务端等到全部输入到达才允许任何一场
// 返回尾部；顺序运行会无法越过屏障，真正重叠的会话才能完成。
func TestRunBatchOverlappingSessions(t *testing.T) {
	const count = 8
	arrived := make(chan struct{}, count)
	release := make(chan struct{})
	url, wait := batchPeer(t, count, func(ctx context.Context, conn *websocket.Conn, id int64) error {
		if err := readSessionInput(ctx, conn, 10, 4); err != nil {
			return err
		}
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := writeSessionJSON(ctx, conn, sessionResult("expected tail", true)); err != nil {
			return err
		}
		return conn.Close(websocket.StatusNormalClosure, "complete")
	})
	cfg := loadgen.BatchConfig{Sessions: count, Session: sessionConfig(url)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := startBatch(ctx, cfg)
	for range count {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("sessions did not overlap:", ctx.Err())
		}
	}
	select {
	case result := <-done:
		t.Fatalf("batch returned before server released tails: %+v", result)
	default:
	}
	close(release)
	result := awaitBatch(t, ctx, done)
	if result.err != nil {
		t.Fatal(result.err)
	}
	assertBatchShape(t, result.report, cfg)
	for _, session := range result.report.Results {
		assertCompletedSession(t, session.Report, session.Err)
		if session.Report.Observation.AudioBytesWritten != 10 || session.Report.Observation.AudioChunksWritten != 3 {
			t.Fatalf("input lost: %+v", session)
		}
	}
	// 各场尾部样本必须拥有独立存储，不能共用最后一场的局部变量。
	for i := 1; i < count; i++ {
		if result.report.Results[0].Report.TailLatency == result.report.Results[i].Report.TailLatency {
			t.Fatal("sessions share tail-latency storage")
		}
	}
	wait()
}

// TestRunBatchMixedOutcomes 验证错误完整性检查或单场期限只影响对应结果。
// timeout 场景的其他会话先完成，不将它外推为延后截止时刻的实验。
func TestRunBatchMixedOutcomes(t *testing.T) {
	for _, mode := range []string{"bad_tail", "session_timeout"} {
		t.Run(mode, func(t *testing.T) {
			const count = 5
			url, wait := batchPeer(t, count, func(ctx context.Context, conn *websocket.Conn, id int64) error {
				if err := readSessionInput(ctx, conn, 10, 4); err != nil {
					return err
				}
				text := "expected tail"
				if id == 1 {
					if mode == "session_timeout" {
						_, _, err := conn.Read(ctx)
						if err == nil {
							return errors.New("unexpected input after end")
						}
						return nil
					}
					text = "wrong tail"
				}
				if err := writeSessionJSON(ctx, conn, sessionResult(text, true)); err != nil {
					return err
				}
				return conn.Close(websocket.StatusNormalClosure, "complete")
			})
			cfg := loadgen.BatchConfig{Sessions: count, Session: sessionConfig(url)}
			cfg.Session.Timeout = 500 * time.Millisecond
			report, err := loadgen.RunBatch(context.Background(), cfg)
			if err != nil {
				t.Fatalf("single-session error became batch error: %v", err)
			}
			assertBatchShape(t, report, cfg)
			completed, unsuccessful := 0, 0
			for _, session := range report.Results {
				if session.Report.Outcome == loadgen.SessionCompleted {
					assertCompletedSession(t, session.Report, session.Err)
					completed++
					continue
				}
				unsuccessful++
				want := loadgen.SessionFailed
				if mode == "session_timeout" {
					want = loadgen.SessionTimedOut
				}
				if session.Report.Outcome != want || session.Err == nil || session.Report.TailLatency != nil || session.Report.Observation.AudioBytesWritten != 10 {
					t.Fatalf("invalid failed result: %+v", session)
				}
				if mode == "session_timeout" && !errors.Is(session.Err, context.DeadlineExceeded) {
					t.Fatalf("lost deadline error: %v", session.Err)
				}
			}
			if completed != 4 || unsuccessful != 1 {
				t.Fatalf("completed=%d unsuccessful=%d, want 4/1", completed, unsuccessful)
			}
			summary, summaryErr := loadgen.SummarizeBatch(report)
			if summaryErr != nil || summary.Completed != 4 || summary.CompletionRate != 0.8 || summary.AudioBytesWritten != 50 || summary.Tail == nil || summary.Tail.Samples != 4 {
				t.Fatalf("real batch summary = %+v, err = %v", summary, summaryErr)
			}
			if (mode == "bad_tail" && summary.Failed != 1) || (mode == "session_timeout" && summary.TimedOut != 1) {
				t.Fatalf("failed outcome lost from summary: %+v", summary)
			}
			wait()
		})
	}
}

func TestRunBatchParentStopRetainsAllResults(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			const count = 4
			arrived := make(chan struct{}, count)
			url, wait := batchPeer(t, count, func(ctx context.Context, conn *websocket.Conn, id int64) error {
				if err := readSessionInput(ctx, conn, 10, 4); err != nil {
					return err
				}
				arrived <- struct{}{}
				_, _, err := conn.Read(ctx)
				if err == nil {
					return errors.New("unexpected extra input")
				}
				return nil
			})
			cfg := loadgen.BatchConfig{Sessions: count, Session: sessionConfig(url)}
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			done := startBatch(ctx, cfg)
			guard, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			for range count {
				select {
				case <-arrived:
				case <-guard.Done():
					t.Fatal("input did not arrive")
				}
			}
			if !deadline {
				cancel()
			}
			result := awaitBatch(t, guard, done)
			wantErr, wantOutcome := context.Canceled, loadgen.SessionCanceled
			if deadline {
				wantErr, wantOutcome = context.DeadlineExceeded, loadgen.SessionTimedOut
			}
			if !errors.Is(result.err, wantErr) {
				t.Fatalf("batch error = %v, want %v", result.err, wantErr)
			}
			assertBatchShape(t, result.report, cfg)
			for _, session := range result.report.Results {
				if !errors.Is(session.Err, wantErr) || session.Report.Outcome != wantOutcome || session.Report.Observation.AudioBytesWritten != 10 || session.Report.TailLatency != nil {
					t.Fatalf("incomplete canceled result: %+v", session)
				}
			}
			wait()
		})
	}
}

func TestRunBatchAllRejectedStillCollects(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := loadgen.BatchConfig{Sessions: 3, Session: sessionConfig("ws" + strings.TrimPrefix(server.URL, "http"))}
	report, err := loadgen.RunBatch(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertBatchShape(t, report, cfg)
	if requests.Load() != 3 {
		t.Fatalf("requests = %d, want 3", requests.Load())
	}
	for _, session := range report.Results {
		if session.Err == nil || session.Report.Outcome != loadgen.SessionFailed || session.Report.TailLatency != nil || !reflect.DeepEqual(session.Report.Observation, loadgen.SessionObservation{}) {
			t.Fatalf("rejection not preserved: %+v", session)
		}
	}
	summary, summaryErr := loadgen.SummarizeBatch(report)
	if summaryErr != nil || summary.Failed != 3 || summary.Completed != 0 || summary.CompletionRate != 0 || summary.AudioBytesWritten != 0 || summary.Tail != nil {
		t.Fatalf("all-rejected batch summary = %+v, err = %v", summary, summaryErr)
	}
}

type batchCallResult struct {
	report loadgen.BatchReport
	err    error
}

func startBatch(ctx context.Context, cfg loadgen.BatchConfig) <-chan batchCallResult {
	done := make(chan batchCallResult, 1)
	go func() { report, err := loadgen.RunBatch(ctx, cfg); done <- batchCallResult{report, err} }()
	return done
}

func awaitBatch(t *testing.T, ctx context.Context, done <-chan batchCallResult) batchCallResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal("batch did not finish:", ctx.Err())
		return batchCallResult{}
	}
}

// batchPeer 为每个 HTTP 到达分配测试序号；该序号不是批次 Index。
func batchPeer(t *testing.T, count int, serve func(context.Context, *websocket.Conn, int64) error) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	var arrivals atomic.Int64
	done := make(chan error, count)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := arrivals.Add(1)
		if id > int64(count) {
			http.Error(w, "unexpected retry", http.StatusBadRequest)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer conn.CloseNow()
		done <- serve(ctx, conn, id)
	}))
	t.Cleanup(func() { cancel(); server.Close() })
	return "ws" + strings.TrimPrefix(server.URL, "http"), func() {
		t.Helper()
		for range count {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal("batch peer:", err)
				}
			case <-ctx.Done():
				t.Fatal("batch peer did not finish:", ctx.Err())
			}
		}
		if arrivals.Load() != int64(count) {
			t.Fatalf("HTTP arrivals = %d, want %d", arrivals.Load(), count)
		}
	}
}

func assertBatchShape(t *testing.T, report loadgen.BatchReport, cfg loadgen.BatchConfig) {
	t.Helper()
	if report.Config != cfg || report.StartedAt.IsZero() || report.FinishedAt.Before(report.StartedAt) || len(report.Results) != cfg.Sessions {
		t.Fatalf("invalid batch report: %+v", report)
	}
	for i, session := range report.Results {
		if session.Index != i || session.Report.StartedAt.Before(report.StartedAt) || session.Report.FinishedAt.After(report.FinishedAt) {
			t.Fatalf("result %d not in its slot or batch time window: %+v", i, session)
		}
		assertSessionReport(t, session.Report, cfg.Session, report.StartedAt)
	}
}
