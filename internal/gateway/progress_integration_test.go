package gateway

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// TestDownloadRejectsInvalidProgress 验证运行路径拒绝非法确认，保留最后的合法计数。
func TestDownloadRejectsInvalidProgress(t *testing.T) {
	for _, tc := range []struct {
		name string
		acks []uint64
		want uint64
	}{
		{"beyond_received", []uint64{3201}, 0},
		{"backward", []uint64{3200, 3199}, 3200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &session{}
			if err := s.progress.addReceived(3200); err != nil {
				t.Fatal(err)
			}
			stream := &progressResponseStream{terminal: io.EOF}
			for _, n := range tc.acks {
				stream.responses = append(stream.responses, &asrv1.StreamingRecognizeResponse{
					Progress: &asrv1.AudioProgress{ProcessedAudioBytes: n},
				})
			}
			got := s.download(context.Background(), stream)
			if got.kind != resultWorkerFailed || got.err == nil || errors.Unwrap(got.err) == nil {
				t.Fatalf("invalid confirmation result=%+v, want wrapped Worker error", got)
			}
			if stream.recvCalls != len(tc.acks) || s.resultWriteCtx != nil {
				t.Fatal("continued receiving or wrote result after invalid progress")
			}
			want := audioProgressSnapshot{receivedBytes: 3200, processedBytes: tc.want, pendingBytes: 3200 - tc.want}
			if got := s.progress.snapshot(); got != want {
				t.Fatalf("invalid confirmation changed counters: %+v, want %+v", got, want)
			}
		})
	}
}

// failingAudioSendStream 用于检查发送失败不回滚已完整接收的音频。
type failingAudioSendStream struct {
	progressResponseStream
	failure error
	calls   int
}

func (s *failingAudioSendStream) Send(*asrv1.StreamingRecognizeRequest) error {
	s.calls++
	return s.failure
}

func TestUploadProgressSendFailure(t *testing.T) {
	cause := errors.New("injected audio send failure")
	type observation struct {
		result   sessionResult
		progress audioProgressSnapshot
		calls    int
	}
	done := make(chan observation, 1)
	client := dialInputReader(t, context.Background(), time.Second, func(ctx context.Context, s *session) {
		rpcCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		stream := &failingAudioSendStream{failure: cause}
		result := s.upload(ctx, rpcCtx, cancel, stream, make(chan struct{}))
		done <- observation{result: result, progress: s.progress.snapshot(), calls: stream.calls}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.result.kind != resultWorkerFailed || !errors.Is(got.result.err, cause) || got.calls != 1 {
			t.Fatalf("wrong send failure: %+v", got)
		}
		if got.progress != (audioProgressSnapshot{receivedBytes: 3200, pendingBytes: 3200}) {
			t.Fatalf("send failure rolled back received bytes: %+v", got.progress)
		}
	case <-ctx.Done():
		t.Fatal("upload did not exit after send failure")
	}
}

// heldProgressClient 在 Send 返回前提供进度和文本，固定并发顺序。
// 这是 gRPC 接口替身，验证网关控制逻辑，不模拟真实网络的性能。
type heldProgressClient struct {
	ready chan *heldProgressStream
}

func (c *heldProgressClient) StreamingRecognize(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	s := &heldProgressStream{
		ctx: ctx, responses: make(chan *asrv1.StreamingRecognizeResponse, 2),
		release: make(chan struct{}), sendReturned: make(chan struct{}), recvExited: make(chan struct{}),
	}
	c.ready <- s
	return s, nil
}

type heldProgressStream struct {
	grpc.ClientStream
	ctx          context.Context
	responses    chan *asrv1.StreamingRecognizeResponse
	release      chan struct{}
	sendReturned chan struct{}
	recvExited   chan struct{}
	sendCalls    atomic.Int32
}

func (s *heldProgressStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	if s.sendCalls.Add(1) != 1 {
		return errors.New("unexpected second Send")
	}
	defer close(s.sendReturned)
	s.responses <- &asrv1.StreamingRecognizeResponse{Progress: &asrv1.AudioProgress{ProcessedAudioBytes: uint64(len(req.GetData()))}}
	s.responses <- &asrv1.StreamingRecognizeResponse{SegmentId: "1", Text: "progress consumed"}
	select {
	case <-s.release:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *heldProgressStream) Recv() (*asrv1.StreamingRecognizeResponse, error) {
	select {
	case response, ok := <-s.responses:
		if !ok {
			close(s.recvExited)
			return nil, io.EOF
		}
		return response, nil
	case <-s.ctx.Done():
		close(s.recvExited)
		return nil, s.ctx.Err()
	}
}

func (s *heldProgressStream) CloseSend() error {
	close(s.responses)
	return nil
}

// TestSessionProgressIntegration 验证完整 run：确认先到时计数已可见；
// 计数溢出时不发送该块，取消接收并以内部错误收尾。
func TestSessionProgressIntegration(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		name := "ack_before_send_returns"
		if overflow {
			name = "overflow_before_send"
		}
		t.Run(name, func(t *testing.T) {
			s, client, _ := newResultWriter(t, 2*time.Second, false)
			worker := &heldProgressClient{ready: make(chan *heldProgressStream, 1)}
			s.worker = worker
			s.startTimeout, s.inputIdleTimeout, s.workerSendTimeout, s.tailTimeout = 3*time.Second, 3*time.Second, 3*time.Second, 3*time.Second
			if overflow {
				if err := s.progress.addReceived(^uint64(0) - 1); err != nil {
					t.Fatal(err)
				}
			}
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
			wantCode, wantReason := websocket.StatusNormalClosure, "completed"
			if overflow {
				wantCode, wantReason = websocket.StatusInternalError, "internal error"
			} else {
				// 文本位于进度之后；收到它证明 download 已先处理过累计确认。
				assertRecognitionResult(t, ctx, client, wsprotocol.ResultMessage{
					Type: wsprotocol.MessageTypeResult, SegmentID: "1", Text: "progress consumed",
				})
				select {
				case <-stream.sendReturned:
					t.Fatal("Send returned before the intended early confirmation check")
				default:
				}
				if got := s.progress.snapshot(); got != (audioProgressSnapshot{receivedBytes: 3200, processedBytes: 3200}) {
					t.Fatalf("early acknowledgement missing or input not recorded: %+v", got)
				}
				close(stream.release)
				if err := client.Write(ctx, websocket.MessageText, []byte(`{"type":"end"}`)); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err := client.Read(ctx)
			var closeErr websocket.CloseError
			if !errors.As(err, &closeErr) || closeErr.Code != wantCode || closeErr.Reason != wantReason {
				t.Fatalf("close=%v, want %v %q", err, wantCode, wantReason)
			}
			select {
			case err := <-done:
				joined = true
				if overflow {
					if err == nil || !strings.Contains(err.Error(), "overflow") || errors.Unwrap(err) == nil {
						t.Fatalf("internal cause lost: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("session did not finish")
			}
			if overflow {
				if stream.sendCalls.Load() != 0 {
					t.Fatal("overflowing chunk was sent to Worker")
				}
				want := audioProgressSnapshot{receivedBytes: ^uint64(0) - 1, pendingBytes: ^uint64(0) - 1}
				if got := s.progress.snapshot(); got != want {
					t.Fatalf("overflow changed counters: %+v", got)
				}
			} else if stream.sendCalls.Load() != 1 {
				t.Fatal("unexpected audio send count")
			}
			select {
			case <-stream.recvExited:
			default:
				t.Fatal("Worker receive operation did not exit")
			}
			if stream.ctx.Err() == nil || appCtx.Err() != nil {
				t.Fatal("RPC was not canceled independently of the application")
			}
		})
	}
}
