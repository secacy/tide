package loadgen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

// TestGatewayRecordingPreflight 验证前置错误不创建文件、不发起查询并返回零报告。
func TestGatewayRecordingPreflight(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "invalid_interval", "invalid_url", "empty_path", "blank_path", "stdout_path", "canceled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "samples.jsonl")
			cfg := samplingConfig()
			ctx := context.Background()
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected request") }}}
			var want error
			switch mode {
			case "nil_context":
				ctx = nil
			case "nil_client":
				client = nil
			case "invalid_interval":
				cfg.Interval = 0
			case "invalid_url":
				cfg.Endpoint = "ws://gateway.invalid/"
			case "empty_path":
				path = ""
			case "blank_path":
				path = " \t\n "
			case "stdout_path":
				path = "-"
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "expired":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			report, err := loadgen.RunGatewaySamplingToFile(ctx, client, cfg, path)
			if err == nil || report != (loadgen.GatewayRecordingReport{}) || calls != 0 {
				t.Fatalf("report=%+v err=%v calls=%d", report, err, calls)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("lost parent error: %v", err)
			}
			entries, readErr := os.ReadDir(dir)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("preflight created artifacts: %v err=%v", entries, readErr)
			}
		})
	}
}

// TestGatewayRecordingPaths 验证独占创建拒绝旧文件/目录/链接，不修改旧证据或创建父目录。
func TestGatewayRecordingPaths(t *testing.T) {
	for _, mode := range []string{"file", "directory", "symlink", "dangling_symlink", "missing_parent"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "samples.jsonl")
			target := filepath.Join(dir, "original.jsonl")
			original := []byte("keep existing evidence\n")
			switch mode {
			case "file":
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.WriteFile(target, original, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "dangling_symlink":
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "missing_parent":
				path = filepath.Join(dir, "missing", "samples.jsonl")
			}
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected request") }}}
			report, err := loadgen.RunGatewaySamplingToFile(context.Background(), client, samplingConfig(), path)
			if err == nil || report != (loadgen.GatewayRecordingReport{}) || calls != 0 {
				t.Fatalf("report=%+v err=%v calls=%d", report, err, calls)
			}
			want := os.ErrExist
			if mode == "missing_parent" {
				want = os.ErrNotExist
			}
			if !errors.Is(err, want) {
				t.Fatalf("creation error chain: %v", err)
			}
			switch mode {
			case "file", "symlink":
				data, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(data, original) {
					t.Fatalf("existing evidence changed: %q %v", data, err)
				}
			case "directory":
				if info, err := os.Stat(path); err != nil || !info.IsDir() {
					t.Fatalf("directory changed: %v", err)
				}
			case "dangling_symlink":
				if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created target: %v", err)
				}
			case "missing_parent":
				if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created parent: %v", err)
				}
			}
			if mode == "symlink" || mode == "dangling_symlink" {
				if got, err := os.Readlink(path); err != nil || got != target {
					t.Fatalf("link changed: %q %v", got, err)
				}
			}
		})
	}
}

// readGatewayRecording 检查落盘行数、序号、成功/失败分类与内存计数能相互复核。
func readGatewayRecording(t *testing.T, report loadgen.GatewayRecordingReport) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(report.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(report.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("unexpected file mode: %v", info.Mode())
	}
	if report.SamplesWritten != report.SuccessfulSamples+report.FailedSamples {
		t.Fatal("recording count invariant failed")
	}
	if !bytes.HasSuffix(data, []byte("\n")) {
		t.Fatalf("unterminated sample file: %q", data)
	}
	lines := bytes.Split(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	if int64(len(lines)) != report.SamplesWritten {
		t.Fatalf("lines=%d report=%+v", len(lines), report)
	}
	var successes, failures int64
	var docs []map[string]any
	for i, line := range lines {
		doc := decodeGatewayLine(t, append(bytes.Clone(line), '\n'))
		if doc["index"] != json.Number(strconv.Itoa(i)) {
			t.Fatalf("index lost: %#v", doc)
		}
		if doc["error"] == nil {
			successes++
			if doc["state"] == nil {
				t.Fatal("success without state")
			}
		} else {
			failures++
			assertJSONNull(t, doc, "state")
		}
		docs = append(docs, doc)
	}
	if successes != report.SuccessfulSamples || failures != report.FailedSamples {
		t.Fatalf("counts mismatch: successes=%d failures=%d report=%+v", successes, failures, report)
	}
	return docs
}

// TestGatewayRecordingHTTP 验证实际文件与真实 HTTP 的混合结果、全失败和父期限；失败记录不丢弃。
func TestGatewayRecordingHTTP(t *testing.T) {
	for _, mode := range []string{"mixed", "all_failed", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
			}
			defer cancel()
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if mode == "deadline" {
					<-r.Context().Done()
					return
				}
				if n == 3 {
					cancel()
					<-r.Context().Done()
					return
				}
				if mode == "all_failed" || n == 2 {
					http.Error(w, "unavailable", 503)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, probeValidJSON)
			}))
			defer srv.Close()
			cfg := samplingConfig()
			cfg.Endpoint = srv.URL
			cfg.Interval = time.Millisecond
			cfg.RequestTimeout = time.Second
			beforeCfg := cfg
			path := filepath.Join(t.TempDir(), "samples.jsonl")
			started := time.Now()
			report, err := loadgen.RunGatewaySamplingToFile(ctx, srv.Client(), cfg, path)
			returned := time.Now()
			wantErr := context.Canceled
			wantCount := int64(3)
			wantSuccess := int64(1)
			if mode == "all_failed" {
				wantSuccess = 0
			}
			if mode == "deadline" {
				wantErr = context.DeadlineExceeded
				wantCount = 1
				wantSuccess = 0
			}
			if !errors.Is(err, wantErr) || !errors.Is(report.SamplingErr, wantErr) || report.OutputErr != nil || report.CloseErr != nil {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			if report.Config != beforeCfg || cfg != beforeCfg || report.OutputPath != path {
				t.Fatal("configuration or path changed")
			}
			if report.StartedAt.IsZero() || report.StartedAt.Before(started) || report.FinishedAt.Before(report.StartedAt) || report.FinishedAt.After(returned) {
				t.Fatalf("invalid recording time: %+v", report)
			}
			if calls.Load() != wantCount || report.SamplesWritten != wantCount || report.SuccessfulSamples != wantSuccess {
				t.Fatalf("queries=%d report=%+v", calls.Load(), report)
			}
			docs := readGatewayRecording(t, report)
			if docs[len(docs)-1]["error"] == nil {
				t.Fatal("inflight cancellation was not preserved")
			}
		})
	}
}

// TestGatewayRecordingSuccessAtCancellation 验证已成功查询后取消仍写出一条成功记录，路径不被 TrimSpace 改写。
func TestGatewayRecordingSuccessAtCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	transport := &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		calls++
		resp := samplingResponse(req, 0)
		resp.Body = samplingCancelBody{resp.Body, cancel}
		return resp, nil
	}}
	path := filepath.Join(t.TempDir(), " samples.jsonl ")
	report, err := loadgen.RunGatewaySamplingToFile(ctx, &http.Client{Transport: transport}, samplingConfig(), path)
	if !errors.Is(err, context.Canceled) || report.SamplesWritten != 1 || report.SuccessfulSamples != 1 || report.FailedSamples != 0 || report.OutputErr != nil || report.CloseErr != nil || calls != 1 || transport.closed != 0 {
		t.Fatalf("report=%+v err=%v calls=%d", report, err, calls)
	}
	if report.OutputPath != path {
		t.Fatal("rewrote output path")
	}
	readGatewayRecording(t, report)
}

// TestGatewayRecordingExclusiveRace 验证同时争用同一路径时只有一个调用进入采样，另一个返回零报告。
func TestGatewayRecordingExclusiveRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int64
	entered := make(chan struct{}, 2)
	client := &http.Client{Transport: &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	}}}
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	type result struct {
		report loadgen.GatewayRecordingReport
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	cfg := samplingConfig()
	cfg.RequestTimeout = time.Second
	for range 2 {
		go func() {
			<-start
			r, e := loadgen.RunGatewaySamplingToFile(ctx, client, cfg, path)
			results <- result{r, e}
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("winner did not start")
	}
	var loser result
	select {
	case loser = <-results:
	case <-ctx.Done():
		t.Fatal("loser did not return")
	}
	if !errors.Is(loser.err, os.ErrExist) || !reflect.DeepEqual(loser.report, loadgen.GatewayRecordingReport{}) {
		t.Fatalf("loser=%+v", loser)
	}
	cancel()
	var winner result
	select {
	case winner = <-results:
	case <-time.After(3 * time.Second):
		t.Fatal("winner did not stop")
	}
	if !errors.Is(winner.err, context.Canceled) || winner.report.SamplesWritten != 1 || winner.report.FailedSamples != 1 || winner.report.OutputErr != nil || winner.report.CloseErr != nil || calls.Load() != 1 {
		t.Fatalf("winner=%+v requests=%d", winner, calls.Load())
	}
	readGatewayRecording(t, winner.report)
}
