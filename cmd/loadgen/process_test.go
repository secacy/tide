package main

import (
	"bytes"
	"context"
	"errors"
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

// TestLoadgenProcess 构建真实 race 二进制，检查 main 的退出码与信号收尾。
// 不使用 go run，避免把 go 工具进程的信号/退出行为误认为负载进程行为。
func TestLoadgenProcess(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "loadgen")
	buildCtx, stopBuild := context.WithTimeout(context.Background(), 45*time.Second)
	defer stopBuild()
	if output, err := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build loadgen: %v\n%s", err, output)
	}
	for _, mode := range []string{"complete", "mixed", "failed", "sigint", "sigterm"} {
		t.Run(mode, func(t *testing.T) {
			peerMode := mode
			if mode == "sigint" || mode == "sigterm" {
				peerMode = "stall"
			}
			peer := newCommandPeer(t, peerMode, 2)
			path := filepath.Join(t.TempDir(), "report.json")
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, commandArgs(peer.url, path)...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			})
			if peerMode == "stall" {
				peer.awaitInput(t, 2)
				signal := os.Signal(os.Interrupt)
				if mode == "sigterm" {
					signal = syscall.SIGTERM
				}
				if err := cmd.Process.Signal(signal); err != nil {
					t.Fatal(err)
				}
			}
			wantExit := 1
			if mode == "complete" {
				wantExit = 0
			}
			assertCommandExit(t, cmd.Wait(), wantExit, stderr.String())
			if ctx.Err() != nil {
				t.Fatal("command exceeded test deadline")
			}
			peer.wait(t, 2)
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), path) {
				t.Fatalf("unexpected logs: stdout=%s stderr=%s", stdout.String(), stderr.String())
			}
			doc := readCommandReport(t, path)
			assertCommandReport(t, doc, 2)
			switch mode {
			case "complete":
				if doc.Summary.Completed != 2 || doc.BatchError != nil {
					t.Fatalf("wrong success report: %+v", doc)
				}
			case "mixed":
				if doc.Summary.Completed != 1 || doc.Summary.Failed != 1 || doc.BatchError != nil {
					t.Fatalf("wrong mixed report: %+v", doc)
				}
			case "failed":
				if doc.Summary.Failed != 2 || doc.BatchError != nil {
					t.Fatalf("wrong failure report: %+v", doc)
				}
			default:
				if doc.Summary.Canceled != 2 || doc.BatchError == nil || !strings.Contains(*doc.BatchError, "context canceled") {
					t.Fatalf("signal report lost cancellation: %+v", doc)
				}
			}
		})
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", 503)
	}))
	defer server.Close()
	for _, mode := range []string{"help", "invalid", "existing", "missing_parent"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			if mode == "missing_parent" {
				path = filepath.Join(path, "missing", "report.json")
			}
			args := commandArgs("ws"+strings.TrimPrefix(server.URL, "http"), path)
			wantExit := 1
			switch mode {
			case "help":
				args = append(args, "-h")
				wantExit = 0
			case "invalid":
				args = append(args, "-sessions=0")
			case "existing":
				if err := os.WriteFile(path, []byte("previous experiment"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
			assertCommandExit(t, err, wantExit, string(output))
			if ctx.Err() != nil {
				t.Fatal("command exceeded test deadline")
			}
			data, readErr := os.ReadFile(path)
			if mode == "existing" {
				if readErr != nil || string(data) != "previous experiment" {
					t.Fatalf("overwrote result: %q %v", data, readErr)
				}
			} else if !errors.Is(readErr, os.ErrNotExist) {
				t.Fatalf("unexpected output file: %q, %v", data, readErr)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("preflight commands issued %d requests", requests.Load())
	}
}

// commandArgs 显式固定短批次的条件，避免依赖一分钟的命令默认值。
func commandArgs(url, path string) []string {
	return []string{"-url", url, "-output", path, "-sessions=2", "-audio-bytes=10", "-chunk-bytes=4", "-realtime=false", "-session-timeout=5s", "-expected-final-text=expected tail"}
}

// assertCommandExit 核对正常退出码，避免把信号强制终止误当成预期退出 1。
func assertCommandExit(t *testing.T, err error, code int, output string) {
	t.Helper()
	if code == 0 {
		if err != nil {
			t.Fatalf("command failed: %v\n%s", err, output)
		}
		return
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != code {
		t.Fatalf("exit = %v, want %d\n%s", err, code, output)
	}
}
