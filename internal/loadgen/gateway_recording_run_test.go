package loadgen_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestGatewayRecordingRunPreflight 验证无效输入在创建目录或查询之前返回零结果。
func TestGatewayRecordingRunPreflight(t *testing.T) {
	for _, mode := range []string{"nil_context", "nil_client", "invalid_url", "invalid_interval", "invalid_timeout", "empty", "blank", "stdout", "canceled", "expired"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "run")
			ctx := context.Background()
			cfg := samplingConfig()
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				calls++
				return nil, errors.New("unexpected request")
			}}}
			var want error
			switch mode {
			case "nil_context":
				ctx = nil
			case "nil_client":
				client = nil
			case "invalid_url":
				cfg.Endpoint = "ws://gateway.invalid/"
			case "invalid_interval":
				cfg.Interval = 0
			case "invalid_timeout":
				cfg.RequestTimeout = 0
			case "empty":
				dir = ""
			case "blank":
				dir = " \t\n "
			case "stdout":
				dir = "-"
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
			result, err := loadgen.RunGatewayRecording(ctx, client, cfg, dir)
			if err == nil || result != (loadgen.GatewayRecordingResult{}) || calls != 0 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
			if want != nil && !errors.Is(err, want) {
				t.Fatalf("lost context cause: %v", err)
			}
			if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
				t.Fatalf("created artifacts: %v %v", entries, err)
			}
		})
	}
}

// TestGatewayRecordingRunPaths 验证旧目录、文件和链接均不被复用，父目录也不会自动创建。
func TestGatewayRecordingRunPaths(t *testing.T) {
	for _, mode := range []string{"empty_directory", "old_recording", "file", "symlink", "dangling_symlink", "missing_parent"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "run")
			target := filepath.Join(parent, "target")
			original := []byte("keep existing evidence\n")
			var protected []string
			mkdir := func(path string) {
				t.Helper()
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "empty_directory", "old_recording":
				mkdir(dir)
				if mode == "old_recording" {
					protected = []string{filepath.Join(dir, "samples.jsonl"), filepath.Join(dir, "manifest.json")}
				}
			case "file":
				protected = []string{dir}
			case "symlink", "dangling_symlink":
				if mode == "symlink" {
					mkdir(target)
					protected = []string{filepath.Join(target, "samples.jsonl"), filepath.Join(target, "manifest.json")}
				}
				if err := os.Symlink(target, dir); err != nil {
					t.Fatal(err)
				}
			case "missing_parent":
				dir = filepath.Join(parent, "missing", "run")
			}
			for _, path := range protected {
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			client := &http.Client{Transport: &probeTransport{roundTrip: func(*http.Request) (*http.Response, error) {
				calls++
				return nil, errors.New("unexpected request")
			}}}
			result, err := loadgen.RunGatewayRecording(context.Background(), client, samplingConfig(), dir)
			want := os.ErrExist
			if mode == "missing_parent" {
				want = os.ErrNotExist
			}
			if !errors.Is(err, want) || result != (loadgen.GatewayRecordingResult{}) || calls != 0 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
			for _, path := range protected {
				if data, err := os.ReadFile(path); err != nil || !bytes.Equal(data, original) {
					t.Fatalf("changed %q: %q %v", path, data, err)
				}
			}
			switch mode {
			case "empty_directory":
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
					t.Fatalf("changed empty directory: %v %v", entries, err)
				}
			case "symlink", "dangling_symlink":
				if link, err := os.Readlink(dir); err != nil || link != target {
					t.Fatalf("changed link: %q %v", link, err)
				}
				if mode == "dangling_symlink" {
					if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("created target: %v", err)
					}
				}
			case "missing_parent":
				if _, err := os.Stat(filepath.Dir(dir)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("created parent: %v", err)
				}
			}
		})
	}
}

// checkGatewayRecordingArtifacts 对照真实 JSONL、返回报告与最终清单；清单仅替换路径为相对路径。
func checkGatewayRecordingArtifacts(t *testing.T, result loadgen.GatewayRecordingResult) {
	t.Helper()
	if !result.ManifestSaved || result.ManifestErr != nil || result.Recording.OutputErr != nil || result.Recording.CloseErr != nil {
		t.Fatalf("artifacts not saved cleanly: %+v", result)
	}
	if !filepath.IsAbs(result.ManifestPath) || !filepath.IsAbs(result.Recording.OutputPath) {
		t.Fatalf("returned paths are not absolute: %+v", result)
	}
	readGatewayRecording(t, result.Recording)
	data, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	copy := result.Recording
	copy.OutputPath = "samples.jsonl"
	var expected bytes.Buffer
	if err := loadgen.WriteGatewayRecordingJSON(&expected, copy); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, expected.Bytes()) {
		t.Fatalf("manifest differs from final report:\n%s\nwant:\n%s", data, expected.Bytes())
	}
	decodeBatchJSON(t, data)
	for _, path := range []string{filepath.Dir(result.ManifestPath), result.ManifestPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0077 != 0 {
			t.Fatalf("unexpected permissions: %s %v", path, info.Mode())
		}
	}
}

// TestGatewayRecordingRunPortable 验证请求前已预留空清单、取消后保存成功样本，以及目录整体移动。
func TestGatewayRecordingRunPortable(t *testing.T) {
	for _, mode := range []string{"absolute", "relative", "spaces"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "run")
			if mode == "spaces" {
				dir = filepath.Join(parent, " run ")
			}
			if mode == "relative" {
				cwd, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				dir, err = filepath.Rel(cwd, dir)
				if err != nil {
					t.Fatal(err)
				}
			}
			absolute, err := filepath.Abs(dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			calls := 0
			transport := &probeTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
				calls++
				for _, name := range []string{"manifest.json", "samples.jsonl"} {
					if data, err := os.ReadFile(filepath.Join(absolute, name)); err != nil || len(data) != 0 {
						t.Errorf("%s not reserved empty before query: %q %v", name, data, err)
					}
				}
				response := samplingResponse(req, 0)
				response.Body = &samplingCancelBody{ReadCloser: response.Body, cancel: cancel}
				return response, nil
			}}
			client := &http.Client{Transport: transport, Timeout: time.Second}
			cfg := samplingConfig()
			result, err := loadgen.RunGatewayRecording(ctx, client, cfg, dir)
			if !errors.Is(err, context.Canceled) || !errors.Is(result.Recording.SamplingErr, context.Canceled) || result.Recording.SamplesWritten != 1 || result.Recording.SuccessfulSamples != 1 || calls != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls)
			}
			if result.Recording.OutputPath != filepath.Join(absolute, "samples.jsonl") || result.ManifestPath != filepath.Join(absolute, "manifest.json") || result.Recording.Config != cfg {
				t.Fatalf("paths/config changed: %+v", result)
			}
			if transport.closed != 0 || client.Transport != transport || client.Timeout != time.Second || client.CheckRedirect != nil {
				t.Fatal("caller client changed or closed")
			}
			checkGatewayRecordingArtifacts(t, result)
			original, err := os.ReadFile(result.Recording.OutputPath)
			if err != nil {
				t.Fatal(err)
			}
			moved := filepath.Join(parent, "moved")
			if err := os.Rename(absolute, moved); err != nil {
				t.Fatal(err)
			}
			manifest, err := os.ReadFile(filepath.Join(moved, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			doc := decodeBatchJSON(t, manifest)
			if doc["output_path"] != "samples.jsonl" {
				t.Fatalf("not portable: %#v", doc)
			}
			if data, err := os.ReadFile(filepath.Join(moved, doc["output_path"].(string))); err != nil || !bytes.Equal(data, original) {
				t.Fatalf("moved evidence unreadable: %q %v", data, err)
			}
			if _, err := os.Stat(absolute); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old directory still exists: %v", err)
			}
		})
	}
}

// TestGatewayRecordingRunHTTP 验证真实 HTTP 失败及取消/到期后，仍保存可复核的最终清单。
func TestGatewayRecordingRunHTTP(t *testing.T) {
	for _, mode := range []string{"mixed", "all_failed", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			limit := 3 * time.Second
			if mode == "deadline" {
				limit = 300 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), limit)
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
			cfg.Endpoint, cfg.Interval, cfg.RequestTimeout = srv.URL, time.Millisecond, time.Second
			result, err := loadgen.RunGatewayRecording(ctx, srv.Client(), cfg, filepath.Join(t.TempDir(), "run"))
			wantErr, wantCount, wantSuccess := error(context.Canceled), int64(3), int64(1)
			if mode == "all_failed" {
				wantSuccess = 0
			}
			if mode == "deadline" {
				wantErr, wantCount, wantSuccess = context.DeadlineExceeded, 1, 0
			}
			if !errors.Is(err, wantErr) || !errors.Is(result.Recording.SamplingErr, wantErr) || result.Recording.SamplesWritten != wantCount || result.Recording.SuccessfulSamples != wantSuccess || calls.Load() != wantCount {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
			}
			checkGatewayRecordingArtifacts(t, result)
		})
	}
}

// TestGatewayRecordingRunExclusiveRace 验证两个调用竞争同一目录时只有一个记录者，另一个不查询、不覆盖。
func TestGatewayRecordingRunExclusiveRace(t *testing.T) {
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
	type outcome struct {
		result loadgen.GatewayRecordingResult
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	dir := filepath.Join(t.TempDir(), "run")
	cfg := samplingConfig()
	cfg.RequestTimeout = time.Second
	for range 2 {
		go func() {
			<-start
			r, err := loadgen.RunGatewayRecording(ctx, client, cfg, dir)
			results <- outcome{r, err}
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("winner did not start")
	}
	var loser outcome
	select {
	case loser = <-results:
	case <-ctx.Done():
		t.Fatal("loser did not return")
	}
	if !errors.Is(loser.err, os.ErrExist) || loser.result != (loadgen.GatewayRecordingResult{}) {
		t.Fatalf("loser=%+v", loser)
	}
	cancel()
	var winner outcome
	select {
	case winner = <-results:
	case <-time.After(3 * time.Second):
		t.Fatal("winner did not stop")
	}
	if !errors.Is(winner.err, context.Canceled) || winner.result.Recording.SamplesWritten != 1 || winner.result.Recording.FailedSamples != 1 || calls.Load() != 1 {
		t.Fatalf("winner=%+v calls=%d", winner, calls.Load())
	}
	checkGatewayRecordingArtifacts(t, winner.result)
}
