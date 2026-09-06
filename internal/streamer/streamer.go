package streamer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/secacy/tide-artisan/internal/audio"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// Config 描述模拟音频客户端的运行参数。
type Config struct {
	ChunkBytes int // 一次最多读取并发送多少 PCM 字节
}

type Streamer struct {
	client asrv1.ASRServiceClient
	cfg    Config
}

func (c Config) validate() error {
	if c.ChunkBytes <= 0 {
		return fmt.Errorf("chunk bytes must be greater than zero")
	}
	if c.ChunkBytes%audio.BytesDepth != 0 {
		return fmt.Errorf("chunk bytes %d must align to %d-byte PCM samples", c.ChunkBytes, audio.BytesDepth)
	}
	return nil
}

func New(client asrv1.ASRServiceClient, cfg Config) (*Streamer, error) {
	if client == nil {
		return nil, fmt.Errorf("audio service client is nil")
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid streamer config: %w", err)
	}
	return &Streamer{
		client: client,
		cfg:    cfg,
	}, nil
}

// Stream 将 audio 中的 PCM 数据持续发送到服务端， 同时持续接收服务端返回的流式结果。
func (s *Streamer) Stream(ctx context.Context, audio io.Reader) error {
	group, gctx := errgroup.WithContext(ctx)

	// 向服务端发起一次 StreamAudio RPC，并得到 gRPC 流对象
	stream, err := s.client.StreamingRecognize(gctx)
	if err != nil {
		return fmt.Errorf("open audio stream: %w", err)
	}

	// 发送方向
	group.Go(func() error {
		// sendAudio 自己负责发送方向的完整生命周期：
		//
		//   - 本地音频正常结束 -> CloseSend
		//   - Send 返回 io.EOF -> 停止发送，但不取消接收
		//   - 本地读取失败/客户端发送失败 -> 返回错误并取消整个 RPC
		return s.sendAudioStream(gctx, stream, audio)

		//if err := s.sendAudioStream(gctx, stream, audio); err != nil {
		//	return err
		//}
		//// 音频全部发送完成后关闭客户端的发送方向
		//if err := stream.CloseSend(); err != nil {
		//	return fmt.Errorf("close send side: %w", err)
		//}
		//return nil
	})

	// 接收方向
	group.Go(func() error {
		return s.receiveResponses(gctx, stream)
	})

	if err := group.Wait(); err != nil {
		return fmt.Errorf("audio stream failed: %w", err)
	}

	return nil
}

func (s *Streamer) sendAudioStream(ctx context.Context, stream asrv1.ASRService_StreamingRecognizeClient, audio io.Reader) error {
	buf := make([]byte, s.cfg.ChunkBytes)
	pacer := NewPacer()

	for {
		n, readErr := io.ReadFull(audio, buf)
		if n > 0 {
			// 第一块立即发送，后续块按照之前已经发送的 PCM 时长进行调度。
			if err := pacer.WaitBeforeSend(ctx); err != nil {
				return fmt.Errorf("wait before sending audio: %w", err)
			}
			err := s.sendChunk(stream, buf[:n])
			switch {
			case errors.Is(err, io.EOF): // 发送失败返回io.EOF可能是服务端造成的，gRPC status需要通过Recv获取，返回nil这样可以让receiveResponses继续读取服务端最终状态
				return nil
			case err != nil: // 非 EOF 的 Send 错误属于客户端发送失败
				return err
			}
			// 推进时间轴
			pacer.Advance(n)
		}
		switch {
		case readErr == nil: // 成功读取完整 chunk，继续读取下一块
			continue
		case errors.Is(readErr, io.EOF), // 没有剩余数据
			errors.Is(readErr, io.ErrUnexpectedEOF): // 最后一个 chunk 不足 ChunkBytes
			// 此时应该关闭客户端发送方向
			if err := stream.CloseSend(); err != nil {
				return fmt.Errorf("close audio send stream: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("read audio: %w", readErr)
		}
	}
}

// sendChunk 把一个 PCM chunk 封装成 protobuf message 并发送。
func (s *Streamer) sendChunk(stream asrv1.ASRService_StreamingRecognizeClient, audio []byte) error {
	req := &asrv1.StreamingRecognizeRequest{
		Data: bytes.Clone(audio), // 避免后续 ReadFull 改写已交给 gRPC 的 message 数据
	}
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("send audio chunk (%d bytes): %w", len(audio), err)
	}
	return nil
}

// receiveResponses 持续读取服务端的流式返回。
func (s *Streamer) receiveResponses(ctx context.Context, stream asrv1.ASRService_StreamingRecognizeClient) error {
	for {
		resp, err := stream.Recv()

		switch {
		case errors.Is(err, io.EOF): // 服务端正常关闭返回流
			return nil

		case err != nil:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive stream response: %w", err)
		}

		// 示例阶段先直接输出。
		// 在真正项目中，更推荐通过 callback、channel 或 ResultHandler 把结果交给上层业务， 避免基础设施层直接依赖 stdout。
		fmt.Printf("text=%q final=%v\n", resp.GetText(), resp.GetIsFinal())
	}
}
