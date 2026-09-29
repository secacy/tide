package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// TestRunSavesOutcomes 检查业务失败仍落盘，而非误把 batchErr == nil 当作全成功。
func TestRunSavesOutcomes(t *testing.T) {
	for _, tc := range []struct {
		mode                        string
		completed, failed, timedOut int
	}{
		{"complete", 2, 0, 0}, {"mixed", 1, 1, 0}, {"failed", 0, 2, 0}, {"stall", 0, 0, 2},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			peer := newCommandPeer(t, tc.mode, 2)
			cfg := commandRunConfig(t, peer.url)
			if tc.mode == "stall" {
				cfg.Batch.Session.Timeout = 500 * time.Millisecond
			}
			err := run(context.Background(), cfg)
			if (err == nil) != (tc.completed == 2) {
				t.Fatalf("run = %v, completed=%d", err, tc.completed)
			}
			peer.wait(t, 2)
			doc := readCommandReport(t, cfg.OutputPath)
			if doc.BatchError != nil || doc.Summary.Completed != tc.completed || doc.Summary.Failed != tc.failed || doc.Summary.TimedOut != tc.timedOut {
				t.Fatalf("wrong saved outcome: %+v", doc)
			}
			assertCommandReport(t, doc, 2)
		})
	}
}

// TestRunCancellationSavesReport 等到两场输入真正到达再取消，避免测成预取消。
func TestRunCancellationSavesReport(t *testing.T) {
	peer := newCommandPeer(t, "stall", 2)
	cfg := commandRunConfig(t, peer.url)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	peer.awaitInput(t, 2)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run lost cancellation: %v", err)
		}
	case <-peer.ctx.Done():
		t.Fatal("run did not return after cancellation")
	}
	peer.wait(t, 2)
	doc := readCommandReport(t, cfg.OutputPath)
	if doc.BatchError == nil || !strings.Contains(*doc.BatchError, "context canceled") || doc.Summary.Canceled != 2 {
		t.Fatalf("cancellation not saved: %+v", doc)
	}
	assertCommandReport(t, doc, 2)
}

// TestRunRejectsBeforeRequests 检查文件冲突、目录缺失与预取消均不产生网络负载。
func TestRunRejectsBeforeRequests(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	for _, mode := range []string{"existing", "missing_parent", "pre_canceled", "pre_deadline"} {
		t.Run(mode, func(t *testing.T) {
			cfg := commandRunConfig(t, "ws"+strings.TrimPrefix(server.URL, "http"))
			ctx := context.Background()
			want := error(os.ErrExist)
			switch mode {
			case "existing":
				if err := os.WriteFile(cfg.OutputPath, []byte("previous experiment"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing_parent":
				cfg.OutputPath = filepath.Join(t.TempDir(), "missing", "result.json")
				want = os.ErrNotExist
			case "pre_canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "pre_deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				cancel()
				want = context.DeadlineExceeded
			}
			if err := run(ctx, cfg); !errors.Is(err, want) {
				t.Fatalf("run = %v, want %v", err, want)
			}
			data, err := os.ReadFile(cfg.OutputPath)
			if mode == "existing" {
				if err != nil || string(data) != "previous experiment" {
					t.Fatalf("old result changed: %q, %v", data, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected file: %q, %v", data, err)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("preflight caused %d requests", requests.Load())
	}
}

// commandRunConfig 使用小输入避免把正确性测试当作负载实验。
func commandRunConfig(t *testing.T, url string) loadConfig {
	t.Helper()
	return loadConfig{OutputPath: filepath.Join(t.TempDir(), "report.json"), Batch: loadgen.BatchConfig{
		Sessions: 2, Session: loadgen.SessionConfig{URL: url, AudioBytes: 10, ChunkBytes: 4, Timeout: 3 * time.Second, ExpectedFinalText: "expected tail"},
	}}
}

// commandReport 只解码本轮关注的持久化事实；完整 JSON 契约由 loadgen 包测试覆盖。
type commandReport struct {
	BatchError *string `json:"batch_error"`
	Summary    struct {
		Completed         int   `json:"completed"`
		Failed            int   `json:"failed"`
		Canceled          int   `json:"canceled"`
		TimedOut          int   `json:"timed_out"`
		AudioBytesWritten int64 `json:"audio_bytes_written"`
	} `json:"summary"`
	Sessions []struct {
		Index       int     `json:"index"`
		Outcome     string  `json:"outcome"`
		Error       *string `json:"error"`
		TailLatency *int64  `json:"tail_latency_ns"`
	} `json:"sessions"`
}

// readCommandReport 读取落盘结果并要求文件恰好包含一个 JSON 文档。
func readCommandReport(t *testing.T, path string) commandReport {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var doc commandReport
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("invalid report: %v\n%s", err, data)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("extra report content: %v", err)
	}
	return doc
}

// assertCommandReport 核对计划会话全部留存及成功/失败字段的一致性。
func assertCommandReport(t *testing.T, doc commandReport, sessions int) {
	t.Helper()
	if len(doc.Sessions) != sessions || doc.Summary.AudioBytesWritten != int64(sessions*10) {
		t.Fatalf("lost sessions/audio: %+v", doc)
	}
	for i, s := range doc.Sessions {
		if s.Index != i {
			t.Fatalf("index = %d, want %d", s.Index, i)
		}
		if s.Outcome == "completed" {
			if s.Error != nil || s.TailLatency == nil {
				t.Fatalf("completed facts = %+v", s)
			}
		} else if s.Error == nil || s.TailLatency != nil {
			t.Fatalf("failure facts = %+v", s)
		}
	}
}

// commandPeer 提供可控的真实 WebSocket；到达序号不是客户端会话 Index。
type commandPeer struct {
	url   string
	ctx   context.Context
	input chan struct{}
	done  chan error
}

// newCommandPeer 按 mode 返回正确/错误尾部或等待断开；sessions 限制预期连接数。
func newCommandPeer(t *testing.T, mode string, sessions int) *commandPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	p := &commandPeer{ctx: ctx, input: make(chan struct{}, sessions), done: make(chan error, sessions)}
	var arrivals atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := arrivals.Add(1)
		if id > int64(sessions) {
			http.Error(w, "unexpected retry", 503)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			p.done <- err
			return
		}
		defer conn.CloseNow()
		p.done <- func() error {
			if err := readCommandInput(ctx, conn); err != nil {
				return err
			}
			p.input <- struct{}{}
			if mode == "stall" {
				_, _, err := conn.Read(ctx)
				if err == nil {
					return errors.New("unexpected message after end")
				}
				if ctx.Err() != nil {
					return fmt.Errorf("peer timed out waiting for client cleanup: %w", ctx.Err())
				}
				return nil
			}
			text := "expected tail"
			if mode == "failed" || (mode == "mixed" && id == 1) {
				text = "incorrect tail"
			}
			data, err := json.Marshal(wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, Text: text, IsFinal: true, SegmentID: "1"})
			if err != nil {
				return err
			}
			if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
				return err
			}
			return conn.Close(websocket.StatusNormalClosure, "complete")
		}()
	}))
	t.Cleanup(func() { cancel(); server.Close() })
	p.url = "ws" + strings.TrimPrefix(server.URL, "http")
	return p
}

// readCommandInput 检查最小输入协议、分块以及精确静音字节数。
func readCommandInput(ctx context.Context, conn *websocket.Conn) error {
	for i, size := range []int{0, 4, 4, 2, 0} {
		kind, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		if size > 0 {
			if kind != websocket.MessageBinary || !bytes.Equal(data, make([]byte, size)) {
				return fmt.Errorf("unexpected audio frame %d: %v/%x", i, kind, data)
			}
			continue
		}
		var msg struct {
			Type    string `json:"type"`
			Version string `json:"version"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}
		if kind != websocket.MessageText || (i == 0 && (msg.Type != "start" || msg.Version != "v1")) || (i == 4 && msg.Type != "end") {
			return fmt.Errorf("unexpected control: %s", data)
		}
	}
	return nil
}

// awaitInput 等待 n 场输入到达，用作取消与信号测试的同步点。
func (p *commandPeer) awaitInput(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-p.input:
		case <-p.ctx.Done():
			t.Fatal("peer did not receive all input")
		}
	}
}

// wait 收集 n 场服务端处理结果，超时或协议不符均使测试失败。
func (p *commandPeer) wait(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case err := <-p.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-p.ctx.Done():
			t.Fatal("peer did not finish")
		}
	}
}
