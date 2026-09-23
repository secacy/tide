package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// progressResponseStream 依次交付响应和最终错误，供下载分流测试使用。
// 它不模拟传输缓冲或 Worker 处理速度。
type progressResponseStream struct {
	responses []*asrv1.StreamingRecognizeResponse
	terminal  error
	recvCalls int
}

func (s *progressResponseStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	i := s.recvCalls
	s.recvCalls++
	if i < len(s.responses) {
		return s.responses[i], nil
	}
	return nil, s.terminal
}

func (*progressResponseStream) Send(*asrv1.StreamingRecognizeRequest) error {
	return errors.New("unexpected Send in download test")
}

func (*progressResponseStream) CloseSend() error {
	return errors.New("unexpected CloseSend in download test")
}

// TestDownloadProgressContinuesReceiving 验证零值、非零值及重复进度都不会写回
// 或提前结束下载；进度后的 Recv 失败仍保留原始错误。
func TestDownloadProgressContinuesReceiving(t *testing.T) {
	wantErr := errors.New("worker failed after progress")
	stream := &progressResponseStream{terminal: wantErr}
	for _, n := range []uint64{0, 3200, 3200} {
		stream.responses = append(stream.responses, &asrv1.StreamingRecognizeResponse{
			Progress: &asrv1.AudioProgress{ProcessedAudioBytes: n},
		})
	}
	// 没有 WebSocket 和写入预算：合法进度不能进入 writeResult。
	s := &session{}
	if err := s.progress.addReceived(3200); err != nil {
		t.Fatal(err)
	}
	got := s.download(context.Background(), stream)
	if got.kind != resultWorkerFailed || !errors.Is(got.err, wantErr) {
		t.Fatalf("download result=%+v, want worker failure wrapping %v", got, wantErr)
	}
	if stream.recvCalls != 4 || s.resultWriteCtx != nil {
		t.Fatalf("recv calls=%d write context=%v; progress must not write or stop receiving", stream.recvCalls, s.resultWriteCtx)
	}
	if got := s.progress.snapshot(); got != (audioProgressSnapshot{receivedBytes: 3200, processedBytes: 3200}) {
		t.Fatalf("valid progress was not recorded: %+v", got)
	}
}

// TestDownloadProgressRejectsMixedFields 分别验证三个文本字段的互斥约束。
// 非法响应来自 Worker，必须归为 Worker 失败，而非客户端协议错误。
func TestDownloadProgressRejectsMixedFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *asrv1.StreamingRecognizeResponse
	}{
		{"segment_id", &asrv1.StreamingRecognizeResponse{SegmentId: "1"}},
		{"text", &asrv1.StreamingRecognizeResponse{Text: "unexpected text"}},
		{"is_final", &asrv1.StreamingRecognizeResponse{IsFinal: true}},
		{"all_fields", &asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: "text", IsFinal: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.response.Progress = &asrv1.AudioProgress{}
			stream := &progressResponseStream{
				responses: []*asrv1.StreamingRecognizeResponse{tc.response}, terminal: io.EOF,
			}
			s := &session{}
			got := s.download(context.Background(), stream)
			if got.kind != resultWorkerFailed || got.err == nil || !strings.Contains(got.err.Error(), "progress") {
				t.Fatalf("mixed fields result=%+v, want explanatory worker failure", got)
			}
			if stream.recvCalls != 1 || s.resultWriteCtx != nil {
				t.Fatalf("continued after invalid response: recv calls=%d write context=%v", stream.recvCalls, s.resultWriteCtx)
			}
		})
	}
}

// progressRoutingWorker 在真实 gRPC 编解码路径上穿插进度和文本。
// abnormal 指定提前 EOF、混合字段、越界或倒退进度；协议错误后等待网关取消。
type progressRoutingWorker struct {
	asrv1.UnimplementedASRServiceServer
	abnormal string
	finished chan error
}

func (w *progressRoutingWorker) StreamingRecognize(
	stream grpc.BidiStreamingServer[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse],
) (err error) {
	defer func() { w.finished <- err }()
	if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{}}); err != nil {
		return err
	}
	for i, text := range []string{"第一段中间结果", "第一段定稿"} {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if len(req.GetData()) != 3200 {
			return fmt.Errorf("audio %d: got %d bytes, want 3200", i, len(req.GetData()))
		}
		progress := &asrv1.StreamingRecognizeResponse{
			Progress: &asrv1.AudioProgress{ProcessedAudioBytes: uint64(i+1) * 3200},
		}
		if w.abnormal == "mixed_fields" {
			progress.Text = "must not reach client"
		}
		if w.abnormal == "future_progress" {
			progress.Progress.ProcessedAudioBytes = 3201
		}
		if err := stream.Send(progress); err != nil {
			return err
		}
		switch w.abnormal {
		case "backward_progress":
			if err := stream.Send(&asrv1.StreamingRecognizeResponse{
				Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 3199},
			}); err != nil {
				return err
			}
			fallthrough
		case "mixed_fields", "future_progress":
			<-stream.Context().Done()
			return stream.Context().Err()
		case "eof_before_end":
			return nil
		}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{
			SegmentId: "1", Text: text, IsFinal: i == 1,
		}); err != nil {
			return err
		}
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("expected input EOF before tail, got %v", err)
	}
	for _, response := range []*asrv1.StreamingRecognizeResponse{
		{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 6400}},
		{SegmentId: "2", Text: "尾部补充结果", IsFinal: true},
		{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: 6400}},
	} {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

// TestGatewayProgressRouting 验证 WebSocket -> Gateway -> gRPC 完整链路：
// 进度不泄漏成空结果，文本有序且尾部完整；非法混合或提前 EOF 独立失败并清理。
// gRPC 使用已有 bufconn 夹具，WebSocket 使用本机 TCP；本测试不提供性能数据。
func TestGatewayProgressRouting(t *testing.T) {
	for _, mode := range []string{"normal", "mixed_fields", "eof_before_end", "future_progress", "backward_progress"} {
		t.Run(mode, func(t *testing.T) {
			worker := &progressRoutingWorker{abnormal: mode, finished: make(chan error, 1)}
			workerClient := newTestWorkerClient(t, worker)
			appCtx, cancelApp := context.WithCancel(context.Background())
			t.Cleanup(cancelApp)
			g, err := New(appCtx, workerClient, Config{})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(g)
			var client *websocket.Conn
			t.Cleanup(func() {
				g.StopAccepting()
				cancelApp()
				if client != nil {
					_ = client.CloseNow()
				}
				server.Close()
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"start","version":"v1"}`)); err != nil {
				t.Fatal(err)
			}
			for i, text := range []string{"第一段中间结果", "第一段定稿"} {
				if err := client.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
					t.Fatal(err)
				}
				if mode != "normal" {
					break // 不发送 end，直接检查 Worker 失败。
				}
				assertRecognitionResult(t, ctx, client, wsprotocol.ResultMessage{
					Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: text, IsFinal: i == 1,
				})
			}
			wantClose := websocket.StatusInternalError
			if mode == "normal" {
				if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
					t.Fatal(err)
				}
				assertRecognitionResult(t, ctx, client, wsprotocol.ResultMessage{
					Type: wsprotocol.MessageTypeResult, SegmentID: "2", Text: "尾部补充结果", IsFinal: true,
				})
				wantClose = websocket.StatusNormalClosure
			}
			// 下一条必须为关闭：任何泄漏的进度消息都会令此断言失败。
			if _, _, err := client.Read(ctx); websocket.CloseStatus(err) != wantClose {
				t.Fatalf("close=%v, want %v: %v", websocket.CloseStatus(err), wantClose, err)
			}
			select {
			case workerErr := <-worker.finished:
				if mode == "mixed_fields" || mode == "future_progress" || mode == "backward_progress" {
					if !errors.Is(workerErr, context.Canceled) {
						t.Fatalf("malformed response did not cancel Worker: %v", workerErr)
					}
				} else if workerErr != nil {
					t.Fatalf("unexpected Worker error: %v", workerErr)
				}
			case <-ctx.Done():
				t.Fatal("Worker did not exit")
			}
			g.StopAccepting()
			if err := g.Wait(ctx); err != nil {
				t.Fatalf("session cleanup did not finish: %v", err)
			}
			if appCtx.Err() != nil {
				t.Fatal("session cleanup depended on application cancellation")
			}
		})
	}
}
