package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// TestAudioBacklogConfig 验证配置开关与有符号边界，防止负值转换成巨大预算。
func TestAudioBacklogConfig(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value int64
	}{
		{"disabled", 0}, {"enabled", 32000}, {"negative", -1}, {"minimum_int64", -1 << 63},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := New(context.Background(), &recordingWorker{}, Config{MaxPendingAudioBytes: tc.value})
			if tc.value < 0 {
				if err == nil || g != nil {
					t.Fatal("negative pending audio budget was accepted")
				}
				if errors.Is(err, ErrAudioBacklogExceeded) {
					t.Fatal("invalid configuration classified as runtime backlog failure")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if g.cfg.MaxPendingAudioBytes != tc.value {
				t.Fatalf("budget=%d, want %d", g.cfg.MaxPendingAudioBytes, tc.value)
			}
		})
	}
}

// TestSessionAudioBacklogFailure 检查完整 run 保留超限原因和已读账目，
// 不发送超限块，并等待被取消的下载操作退出；应用本身仍可继续运行。
func TestSessionAudioBacklogFailure(t *testing.T) {
	s, client, _ := newResultWriter(t, 2*time.Second, false)
	worker := &heldProgressClient{ready: make(chan *heldProgressStream, 1)}
	s.worker = worker
	s.startTimeout, s.inputIdleTimeout, s.workerSendTimeout, s.tailTimeout = 3*time.Second, 3*time.Second, 3*time.Second, 3*time.Second
	s.maxPendingAudioBytes = 3199
	appCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.run(appCtx) }()
	joined := false
	defer func() {
		stop()
		if !joined {
			_ = client.CloseNow()
			_ = s.ws.CloseNow()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("session did not exit after test cleanup")
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
		t.Fatal(err)
	}
	var stream *heldProgressStream
	select {
	case stream = <-worker.ready:
	case <-ctx.Done():
		t.Fatal("Worker stream was not created")
	}
	if err := client.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	assertBacklogClose(t, ctx, client, true)
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, ErrAudioBacklogExceeded) || errors.Is(err, ErrWorkerSendTimeout) {
			t.Fatalf("wrong session cause: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("session did not finish")
	}
	if got := s.progress.snapshot(); got != (audioProgressSnapshot{receivedBytes: 3200, pendingBytes: 3200}) {
		t.Fatalf("failure changed received accounting: %+v", got)
	}
	if stream.sendCalls.Load() != 0 {
		t.Fatal("over-budget chunk was sent")
	}
	select {
	case <-stream.recvExited:
	default:
		t.Fatal("download did not exit")
	}
	if stream.ctx.Err() == nil || appCtx.Err() != nil {
		t.Fatal("RPC was not canceled independently of the application")
	}
}

// backlogIntegrationWorker 可选择汇报累计确认；每块之后的文本用作同步标记。
// 客户端看到标记时，网关已经处理了之前的进度，测试不依赖 sleep 猜测时序。
type backlogIntegrationWorker struct {
	asrv1.UnimplementedASRServiceServer
	reportProgress bool
	exits          chan baselineWorkerExit
}

func (w *backlogIntegrationWorker) StreamingRecognize(stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse]) (err error) {
	var chunks, bytes int
	defer func() { w.exits <- baselineWorkerExit{chunks: chunks, bytes: bytes, err: err} }()
	for {
		req, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: "tail", Text: "final", IsFinal: true})
		}
		if recvErr != nil {
			return recvErr
		}
		chunks++
		bytes += len(req.GetData())
		if w.reportProgress {
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: uint64(bytes)}}); err != nil {
				return err
			}
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{SegmentId: fmt.Sprint(chunks), Text: "chunk received"}); err != nil {
			return err
		}
	}
}

// assertBacklogClose 校验超限和正常完成的客户端可见语义。
func assertBacklogClose(t *testing.T, ctx context.Context, client *websocket.Conn, exceeded bool) {
	t.Helper()
	code, reason := websocket.StatusNormalClosure, "completed"
	if exceeded {
		code, reason = websocket.StatusInternalError, "audio backlog exceeded"
	}
	_, _, err := client.Read(ctx)
	var closeErr websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != code || closeErr.Reason != reason {
		t.Fatalf("close=%v, want %v %q", err, code, reason)
	}
}

// TestGatewayAudioBacklog 使用真实 WebSocket 和 TCP gRPC 验证完整构造传参、
// 等额放行、超限块拦截、确认释放预算、关闭开关与清理后的单名额复用。
// 这些是确定性功能测试，不测量处理能力或实际抖动下的性能。
func TestGatewayAudioBacklog(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   int64
		progress bool
		exceeded bool
	}{
		{"no_progress_exceeds_and_reuses_slot", 3200, false, true},
		{"progress_releases_budget", 3200, true, false},
		{"disabled_without_progress", 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := &backlogIntegrationWorker{reportProgress: tc.progress, exits: make(chan baselineWorkerExit, 2)}
			workerClient := newBaselineTCPWorkerClient(t, worker)
			appCtx, stop := context.WithCancel(context.Background())
			defer stop()
			g, err := New(appCtx, workerClient, Config{MaxSessions: 1, MaxPendingAudioBytes: tc.budget})
			if err != nil {
				t.Fatal(err)
			}
			handlers := make(chan struct{}, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { handlers <- struct{}{} }()
				g.ServeHTTP(w, r)
			}))
			var clients []*websocket.Conn
			defer func() {
				stop()
				for _, client := range clients {
					_ = client.CloseNow()
				}
				server.Close()
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			// 首次可触发超限；第二次始终只发一个预算内音频块，证明名额可再次使用。
			for attempt := 0; attempt < 2; attempt++ {
				client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
				if err != nil {
					t.Fatal(err)
				}
				clients = append(clients, client)
				if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
					t.Fatal(err)
				}
				exceeded := tc.exceeded && attempt == 0
				allowed := 3
				if exceeded || attempt == 1 {
					allowed = 1
				}
				for i := 1; i <= allowed; i++ {
					if err := client.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
						t.Fatal(err)
					}
					assertRecognitionResult(t, ctx, client, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: fmt.Sprint(i), Text: "chunk received"})
				}
				if exceeded {
					if err := client.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
						t.Fatal(err)
					}
					assertRecognitionResult(t, ctx, client, wsprotocol.ResultMessage{Type: wsprotocol.MessageTypeResult, SegmentID: "tail", Text: "final", IsFinal: true})
				}
				assertBacklogClose(t, ctx, client, exceeded)
				select {
				case exit := <-worker.exits:
					if exit.chunks != allowed || exit.bytes != allowed*3200 || (exit.err != nil) != exceeded {
						t.Fatalf("Worker exit=%+v, allowed=%d, exceeded=%v", exit, allowed, exceeded)
					}
				case <-ctx.Done():
					t.Fatal("Worker did not exit")
				}
				select {
				case <-handlers:
				case <-ctx.Done():
					t.Fatal("handler did not exit")
				}
				g.tracker.mu.Lock()
				active := g.tracker.active
				g.tracker.mu.Unlock()
				if active != 0 || appCtx.Err() != nil {
					t.Fatalf("cleanup failed: active=%d application=%v", active, appCtx.Err())
				}
			}
		})
	}
}
