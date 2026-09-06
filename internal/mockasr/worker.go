package mockasr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config 描述 Mock ASR Worker 的行为。
//
// Mock Worker 不真正执行 ASR 推理，而是根据收到的音频时长，按固定规则返回可预测的 partial/final 结果。
type Config struct {
	PartialEvery  time.Duration // 每收到多长时间的音频后返回一次 partial result
	ResponseDelay time.Duration // 模拟 ASR 推理延迟，设置为 0 表示立即返回
	PartialTexts  []string      // Mock Worker 依次返回的 partial 文本
	FinalText     string        // 客户端发送完音频后返回的最终结果
}

// Worker 实现 ASRWorker gRPC 服务。
type Worker struct {
	asrv1.UnimplementedASRServiceServer

	cfg Config
}

// New 创建 Mock ASR Worker。
func New(cfg Config) *Worker {
	if cfg.PartialEvery <= 0 {
		cfg.PartialEvery = 500 * time.Millisecond
	}

	if len(cfg.PartialTexts) == 0 {
		cfg.PartialTexts = []string{
			"你好",
			"你好，这里是",
			"你好，这里是 Mock ASR",
		}
	}

	if cfg.FinalText == "" {
		cfg.FinalText = "你好，这里是 Mock ASR 的最终识别结果"
	}

	return &Worker{
		cfg: cfg,
	}
}

// StreamAudio 模拟真实 ASR 的双向流式 RPC。
func (w *Worker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	ctx := stream.Context()

	var (
		totalBytes    int                  // 本次 stream 到目前为止收到的 PCM 总字节数
		partialIndex  int                  // 下一次应该返回哪个 partial 文本
		nextPartialAt = w.cfg.PartialEvery // 下一次 partial 应该出现在音频时间轴的哪个位置
	)

	for {
		req, err := stream.Recv()

		switch {
		case errors.Is(err, io.EOF):
			// 客户端调用 CloseSend 后，服务端的 Recv() 会返回 io.EOF。Mock Worker 利用这个信号判断“本次音频已经发送完成”。
			//
			// 客户端已经调用 CloseSend()，表示本次音频发送完成。
			// 此时发送最终识别结果，然后正常结束 RPC。
			if totalBytes == 0 {
				return status.Error(codes.InvalidArgument, "no audio received")
			}

			return w.sendFinal(ctx, stream)

		case err != nil:
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}

			return status.Errorf(codes.Internal, "receive audio: %v", err)
		}

		audioChunk := req.GetData()
		if len(audioChunk) == 0 {
			continue
		}
		if len(audioChunk)%audio.BytesDepth != 0 {
			return status.Errorf(codes.InvalidArgument, "invalid PCM chunk size %d: must align to %d-byte samples", len(audioChunk), audio.BytesDepth)
		}
		totalBytes += len(audioChunk)
		audioElapsed := audio.DurationFromBytes(totalBytes)

		for partialIndex < len(w.cfg.PartialTexts) && audioElapsed >= nextPartialAt {
			if err := w.sendPartial(ctx, stream, w.cfg.PartialTexts[partialIndex]); err != nil {
				return err
			}
			partialIndex++
			nextPartialAt += w.cfg.PartialEvery
		}
	}
}

// sendPartial 向客户端发送一次模拟的中间识别结果。
func (w *Worker) sendPartial(ctx context.Context, stream asrv1.ASRService_StreamingRecognizeServer, text string) error {
	// 模拟真实 ASR 推理产生的处理延迟。
	if err := wait(ctx, w.cfg.ResponseDelay); err != nil {
		return err
	}

	resp := &asrv1.StreamingRecognizeResponse{
		SegmentId: "1",
		Text:      text,
		IsFinal:   false,
	}

	if err := stream.Send(resp); err != nil {
		return fmt.Errorf("send partial ASR result: %w", err)
	}

	return nil
}

// sendFinal 在客户端音频发送结束后返回最终识别结果。
func (w *Worker) sendFinal(ctx context.Context, stream asrv1.ASRService_StreamingRecognizeServer) error {
	if err := wait(ctx, w.cfg.ResponseDelay); err != nil {
		return err
	}

	resp := &asrv1.StreamingRecognizeResponse{
		SegmentId: "1",
		Text:      w.cfg.FinalText,
		IsFinal:   true,
	}

	if err := stream.Send(resp); err != nil {
		return fmt.Errorf("send final ASR result: %w", err)
	}

	return nil
}

// wait 执行可被 context 取消的延迟
func wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}
