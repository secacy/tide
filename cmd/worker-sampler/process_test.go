package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestSamplerProcess 用真实 race 二进制验证退出码和信号收尾，避免混入 go run 的进程行为。
func TestSamplerProcess(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "worker-sampler")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelBuild()
	if output, err := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}
	for _, mode := range []string{"complete", "mixed", "all_failed", "sigint", "sigterm"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			entered := make(chan struct{}, 1)
			signalMode := mode == "sigint" || mode == "sigterm"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if signalMode && n >= 2 {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-r.Context().Done()
					return
				}
				if mode == "all_failed" || (mode == "mixed" && n%2 == 0) {
					http.Error(w, "unavailable", 503)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, samplerSnapshotJSON)
			}))
			defer server.Close()
			dir := filepath.Join(t.TempDir(), "run")
			args := samplerProcessArgs(server.URL, dir)
			if signalMode {
				args = append(args, "-duration=10s", "-request-timeout=5s")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, args...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			if signalMode {
				// 第二个请求进入服务端，说明第一条成功样本已交付，且信号监听已经注册。
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("command never reached second request")
				}
				signal := os.Signal(os.Interrupt)
				if mode == "sigterm" {
					signal = syscall.SIGTERM
				}
				if err := cmd.Process.Signal(signal); err != nil {
					t.Fatal(err)
				}
			}
			wantExit := 0
			if signalMode || mode == "all_failed" {
				wantExit = 1
			}
			assertSamplerExit(t, cmd.Wait(), wantExit, stderr.String())
			if ctx.Err() != nil {
				t.Fatal("command exceeded test deadline")
			}
			if stdout.Len() != 0 {
				t.Fatalf("unexpected stdout: %s", stdout.String())
			}
			for _, text := range []string{"starting worker sampling", "worker sampling summary", "samples_written=", "successful_samples=", "failed_samples=", "manifest_saved=true", dir, "interval=", "request_timeout=", "duration="} {
				if !strings.Contains(stderr.String(), text) {
					t.Fatalf("missing %q in logs:\n%s", text, stderr.String())
				}
			}
			if strings.Contains(stderr.String(), "worker sampling completed") != (wantExit == 0) {
				t.Fatalf("wrong completion log:\n%s", stderr.String())
			}
			doc := readSamplerArtifacts(t, dir)
			wantReason := "deadline_exceeded"
			if signalMode {
				wantReason = "canceled"
			}
			if doc.StopReason != wantReason {
				t.Fatalf("stop reason lost: %+v", doc)
			}
			if mode == "all_failed" {
				if doc.SuccessfulSamples != 0 || doc.FailedSamples < 1 || !strings.Contains(stderr.String(), "no valid worker snapshots") {
					t.Fatalf("all-failed result=%+v logs=%s", doc, stderr.String())
				}
			} else if doc.SuccessfulSamples < 1 {
				t.Fatalf("successful sample lost: %+v", doc)
			}
			if (signalMode || mode == "mixed") && doc.FailedSamples < 1 {
				t.Fatalf("failed sample lost: %+v", doc)
			}
		})
	}

	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); http.Error(w, "unexpected request", 503) }))
	defer server.Close()
	for _, mode := range []string{"help", "invalid", "existing", "missing_parent"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "run")
			if mode == "missing_parent" {
				dir = filepath.Join(parent, "missing", "run")
			}
			args := samplerProcessArgs(server.URL, dir)
			wantExit := 1
			switch mode {
			case "help":
				args = append(args, "-h")
				wantExit = 0
			case "invalid":
				args = append(args, "-duration=0s")
			case "existing":
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte("keep previous evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
			assertSamplerExit(t, err, wantExit, string(output))
			if ctx.Err() != nil {
				t.Fatal("command exceeded test deadline")
			}
			if mode == "existing" {
				if data, err := os.ReadFile(filepath.Join(dir, "manifest.json")); err != nil || string(data) != "keep previous evidence" {
					t.Fatalf("old evidence changed: %q %v", data, err)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
					t.Fatalf("created extra files: %v %v", entries, err)
				}
			} else if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
				t.Fatalf("created artifacts: %v %v", entries, err)
			}
			if mode == "help" && !strings.Contains(string(output), "file finalization may take longer") {
				t.Fatalf("missing duration semantics: %s", output)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("preflight issued %d requests", calls.Load())
	}
}

// samplerProcessArgs 给测试进程明确指定短观察窗口与新目录。
func samplerProcessArgs(endpoint, dir string) []string {
	return []string{"-url", endpoint, "-output-dir", dir, "-interval=10ms", "-request-timeout=100ms", "-duration=300ms"}
}

// assertSamplerExit 区分程序主动退出 1 与被信号强制终止或 race 检测失败。
func assertSamplerExit(t *testing.T, err error, code int, output string) {
	t.Helper()
	if code == 0 {
		if err != nil {
			t.Fatalf("command failed: %v\n%s", err, output)
		}
		return
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != code {
		t.Fatalf("exit=%v want=%d\n%s", err, code, output)
	}
}
