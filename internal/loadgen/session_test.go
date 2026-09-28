package loadgen_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/loadgen"
	"github.com/secacy/tide-artisan/internal/mockasr"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRunSessionInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*loadgen.SessionConfig)
	}{
		{"zero_timeout", func(c *loadgen.SessionConfig) { c.Timeout = 0 }},
		{"negative_timeout", func(c *loadgen.SessionConfig) { c.Timeout = -1 }},
		{"zero_chunk", func(c *loadgen.SessionConfig) { c.ChunkBytes = 0 }},
		{"negative_chunk", func(c *loadgen.SessionConfig) { c.ChunkBytes = -2 }},
		{"unaligned_chunk", func(c *loadgen.SessionConfig) { c.ChunkBytes = 3 }},
		{"empty_expected_tail", func(c *loadgen.SessionConfig) { c.ExpectedFinalText = "" }},
		{"zero_audio", func(c *loadgen.SessionConfig) { c.AudioBytes = 0 }},
		{"negative_audio", func(c *loadgen.SessionConfig) { c.AudioBytes = -2 }},
		{"unaligned_audio", func(c *loadgen.SessionConfig) { c.AudioBytes = 3 }},
		{"empty_url", func(c *loadgen.SessionConfig) { c.URL = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sessionConfig("ws://unused.invalid/")
			tc.edit(&cfg)
			report, err := loadgen.RunSession(context.Background(), cfg)
			if err == nil || !reflect.DeepEqual(report, loadgen.SessionReport{}) {
				t.Fatalf("report = %+v, err = %v; want zero report and preflight error", report, err)
			}
		})
	}
}

// TestRunSessionCompletionChecks 在完整上传之后改变结果及关闭行为，
// 区分正常完成、正常关闭但结果不完整、以及明确的协议/网络错误。
func TestRunSessionCompletionChecks(t *testing.T) {
	partial := sessionResult("partial", false)
	final := sessionResult("expected tail", true)
	for _, tc := range []struct {
		name    string
		results []wsprotocol.ResultMessage
		code    websocket.StatusCode
		appErr  bool
		wantErr string
	}{
		{"complete_with_tail_chunk", []wsprotocol.ResultMessage{partial, final}, websocket.StatusNormalClosure, false, ""},
		{"no_result", nil, websocket.StatusNormalClosure, false, "no result received"},
		{"partial_only", []wsprotocol.ResultMessage{partial}, websocket.StatusNormalClosure, false, "last result is not final"},
		{"wrong_tail", []wsprotocol.ResultMessage{sessionResult("wrong", true)}, websocket.StatusNormalClosure, false, "final text mismatch"},
		{"partial_after_final", []wsprotocol.ResultMessage{final, partial}, websocket.StatusNormalClosure, false, "last result is not final"},
		{"abnormal_close_after_final", []wsprotocol.ResultMessage{final}, websocket.StatusInternalError, false, "read websocket message"},
		{"gateway_error_after_partial", []wsprotocol.ResultMessage{partial}, websocket.StatusNormalClosure, true, "gateway error: worker failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url, wait := sessionPeer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := readSessionInput(ctx, conn, 10, 4); err != nil {
					return err
				}
				for _, result := range tc.results {
					if err := writeSessionJSON(ctx, conn, result); err != nil {
						return err
					}
				}
				if tc.appErr {
					return writeSessionJSON(ctx, conn, wsprotocol.ErrorMessage{Type: wsprotocol.MessageTypeError, Message: "worker failed"})
				}
				return conn.Close(tc.code, "test complete")
			})
			cfg := sessionConfig(url)
			before := time.Now()
			report, err := loadgen.RunSession(context.Background(), cfg)
			assertSessionReport(t, report, cfg, before)
			wait()
			if report.Observation.AudioBytesWritten != 10 || report.Observation.AudioChunksWritten != 3 || report.Observation.ResultCount != int64(len(tc.results)) {
				t.Fatalf("lost observations: %+v", report.Observation)
			}
			if report.Observation.EndWrite.Kind != wsclient.WriteEnd || report.Observation.EndWrite.Err != nil {
				t.Fatalf("end write not preserved: %+v", report.Observation.EndWrite)
			}
			if tc.wantErr == "" {
				assertCompletedSession(t, report, err)
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) || report.Outcome != loadgen.SessionFailed || report.TailLatency != nil {
					t.Fatalf("outcome = %s, tail = %v, err = %v; want failed/%q", report.Outcome, report.TailLatency, err, tc.wantErr)
				}
				if tc.code != websocket.StatusNormalClosure && websocket.CloseStatus(err) != tc.code {
					t.Fatalf("close status lost from error chain: %v", err)
				}
			}
		})
	}
}

// TestRunSessionEarlyFinal 用实时发送留下两块之间的间隔，在收到首块后
// 提前返回预期 final；仍读完后续输入并正常关闭，单独检验时间顺序约束。
func TestRunSessionEarlyFinal(t *testing.T) {
	url, wait := sessionPeer(t, func(ctx context.Context, conn *websocket.Conn) error {
		if _, _, err := conn.Read(ctx); err != nil { // start
			return err
		}
		if _, _, err := conn.Read(ctx); err != nil { // first audio
			return err
		}
		if err := writeSessionJSON(ctx, conn, sessionResult("expected tail", true)); err != nil {
			return err
		}
		for range 2 { // second audio, end
			if _, _, err := conn.Read(ctx); err != nil {
				return err
			}
		}
		return conn.Close(websocket.StatusNormalClosure, "complete")
	})
	cfg := sessionConfig(url)
	cfg.AudioBytes, cfg.ChunkBytes, cfg.Realtime = 64000, 32000, true
	report, err := loadgen.RunSession(context.Background(), cfg)
	wait()
	if err == nil || !strings.Contains(err.Error(), "before end write started") || report.Outcome != loadgen.SessionFailed || report.TailLatency != nil {
		t.Fatalf("early final: report = %+v, err = %v", report, err)
	}
	if report.Observation.AudioBytesWritten != cfg.AudioBytes || report.Observation.EndWrite.Err != nil || report.Observation.FinalResultCount != 1 {
		t.Fatalf("early-final test did not complete input and final delivery: %+v", report)
	}
}

// TestRunSessionCancellation 保留完整上传后的观察，在等待尾部期间触发
// 父级取消、会话期限或较早的父级期限；错误链保留对应 context 错误。
func TestRunSessionCancellation(t *testing.T) {
	for _, mode := range []string{"parent_cancel", "session_deadline", "parent_deadline"} {
		t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "parent_deadline" {
				var stop context.CancelFunc
				parent, stop = context.WithTimeout(parent, 500*time.Millisecond)
				defer stop()
			}
			url, wait := sessionPeer(t, func(ctx context.Context, conn *websocket.Conn) error {
				if err := readSessionInput(ctx, conn, 10, 4); err != nil {
					return err
				}
				if mode == "parent_cancel" {
					cancel()
				}
				_, _, err := conn.Read(ctx) // 等待客户端退出，不发送尾部。
				if err == nil {
					return errors.New("unexpected extra client message")
				}
				return nil
			})
			cfg := sessionConfig(url)
			if mode == "session_deadline" {
				cfg.Timeout = 500 * time.Millisecond
			}
			before := time.Now()
			report, err := loadgen.RunSession(parent, cfg)
			assertSessionReport(t, report, cfg, before)
			wait()
			wantOutcome, wantErr := loadgen.SessionTimedOut, context.DeadlineExceeded
			if mode == "parent_cancel" {
				wantOutcome, wantErr = loadgen.SessionCanceled, context.Canceled
			}
			if !errors.Is(err, wantErr) || report.Outcome != wantOutcome || report.TailLatency != nil || report.Observation.AudioBytesWritten != 10 {
				t.Fatalf("report = %+v, err = %v; want %s with preserved input", report, err, wantOutcome)
			}
		})
	}
}

func TestRunSessionUpgradeRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := sessionConfig("ws" + strings.TrimPrefix(server.URL, "http"))
	before := time.Now()
	report, err := loadgen.RunSession(context.Background(), cfg)
	assertSessionReport(t, report, cfg, before)
	if err == nil || report.Outcome != loadgen.SessionFailed || report.TailLatency != nil || !reflect.DeepEqual(report.Observation, loadgen.SessionObservation{}) {
		t.Fatalf("rejection: report = %+v, err = %v", report, err)
	}
}

// TestRunSessionThroughGatewayMock 使用真实 TCP gRPC + WebSocket 验证实际
// Gateway/Mock 链路；这不是子进程启动或容量实验。
func TestRunSessionThroughGatewayMock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := mockasr.New(mockasr.Config{FinalText: "expected tail", PartialTexts: []string{"partial"}, ProcessingConcurrency: 1})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	asrv1.RegisterASRServiceServer(grpcServer, worker)
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); <-serveDone })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "mock", Client: asrv1.NewASRServiceClient(conn)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g, err := gateway.New(ctx, pool, gateway.Config{MaxPendingAudioBytes: 32000})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	t.Cleanup(func() {
		g.StopAccepting()
		cancel()
		server.Close()
		waitCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := g.Wait(waitCtx); err != nil {
			t.Error(err)
		}
	})
	cfg := sessionConfig("ws" + strings.TrimPrefix(server.URL, "http"))
	cfg.AudioBytes, cfg.ChunkBytes = 16002, 3200
	report, err := loadgen.RunSession(context.Background(), cfg)
	assertCompletedSession(t, report, err)
	if report.Observation.AudioBytesWritten != 16002 || report.Observation.AudioChunksWritten != 6 || report.Observation.ResultCount != 2 || report.Observation.FinalResultCount != 1 {
		t.Fatalf("unexpected Gateway/Mock observation: %+v", report)
	}
	// 在主动取消 Gateway 前验证正常会话已经能够完成清理。
	g.StopAccepting()
	waitCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := g.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

func sessionConfig(url string) loadgen.SessionConfig {
	return loadgen.SessionConfig{URL: url, AudioBytes: 10, ChunkBytes: 4, Timeout: 3 * time.Second, ExpectedFinalText: "expected tail"}
}

func sessionResult(text string, final bool) wsprotocol.ResultMessage {
	return wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: text, IsFinal: final}
}

// sessionPeer 在清理前等待测试服务端退出，避免遗留处理协程。
func sessionPeer(t *testing.T, serve func(context.Context, *websocket.Conn) error) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(1 << 20)
		done <- serve(ctx, conn)
	}))
	t.Cleanup(func() { cancel(); server.Close() })
	return "ws" + strings.TrimPrefix(server.URL, "http"), func() {
		t.Helper()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("test peer:", err)
			}
		case <-ctx.Done():
			t.Fatal("test peer did not finish:", ctx.Err())
		}
	}
}

// readSessionInput 检查 start、精确音频长度/分块/静音内容以及 end。
func readSessionInput(ctx context.Context, conn *websocket.Conn, total, chunk int) error {
	kind, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	var start wsprotocol.StartMessage
	if json.Unmarshal(data, &start) != nil || kind != websocket.MessageText || start.Type != wsprotocol.MessageTypeStart || start.Version != "v1" {
		return fmt.Errorf("invalid start: %s", data)
	}
	for remaining := total; remaining > 0; {
		kind, data, err = conn.Read(ctx)
		if err != nil {
			return err
		}
		want := min(chunk, remaining)
		if kind != websocket.MessageBinary || len(data) != want {
			return fmt.Errorf("audio type/size: %v/%d, want binary/%d", kind, len(data), want)
		}
		for _, b := range data {
			if b != 0 {
				return errors.New("non-silent PCM")
			}
		}
		remaining -= len(data)
	}
	kind, data, err = conn.Read(ctx)
	if err != nil {
		return err
	}
	var end wsprotocol.EndMessage
	if json.Unmarshal(data, &end) != nil || kind != websocket.MessageText || end.Type != wsprotocol.MessageTypeEnd {
		return fmt.Errorf("invalid end: %s", data)
	}
	return nil
}

func writeSessionJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func assertSessionReport(t *testing.T, report loadgen.SessionReport, cfg loadgen.SessionConfig, before time.Time) {
	t.Helper()
	if report.Config != cfg || report.StartedAt.Before(before) || report.FinishedAt.Before(report.StartedAt) || report.FinishedAt.After(time.Now()) {
		t.Fatalf("invalid config/time in attempted report: %+v", report)
	}
}

func assertCompletedSession(t *testing.T, report loadgen.SessionReport, err error) {
	t.Helper()
	if err != nil || report.Outcome != loadgen.SessionCompleted || report.TailLatency == nil {
		t.Fatalf("not completed: report = %+v, err = %v", report, err)
	}
	want := report.Observation.LastResultAt.Sub(report.Observation.EndWrite.StartedAt)
	if *report.TailLatency != want || want < 0 {
		t.Fatalf("tail = %v, want %v", *report.TailLatency, want)
	}
}
