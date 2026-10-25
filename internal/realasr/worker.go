package realasr

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxAudioBytes = 960000 // 30 秒音频
	defaultMaxChunkBytes = 3200   // 100ms 音频
	defaultMaxTextBytes  = 4096
)

// Config 的零值采用首轮短音频开发默认值；负值非法。
type Config struct {
	MaxAudioBytes int // 整场原始 PCM 上限，默认 960000；须为偶数。
	MaxChunkBytes int // 单个请求的 PCM 上限，默认 3200；须为偶数。
	MaxTextBytes  int // 单条完整快照上限，默认 4096。
}

// Worker 实现流式识别服务；通过 New 创建后以指针使用，不得复制。
type Worker struct {
	asrv1.UnimplementedASRServiceServer
	cfg     Config
	factory EngineFactory // 只在取得名额后调用。
	active  chan struct{} // 容量固定 1；占位覆盖创建、运行及实际释放全过程。
}

// New 校验并复制配置，创建一个名额；不加载模型、不启动 goroutine。
func New(cfg Config, factory EngineFactory) (*Worker, error) {
	if factory == nil {
		return nil, errors.New("engine factory must not be nil")
	}
	if cfg.MaxAudioBytes < 0 || cfg.MaxChunkBytes < 0 || cfg.MaxTextBytes < 0 {
		return nil, errors.New("worker limits must not be negative")
	}
	if cfg.MaxAudioBytes == 0 {
		cfg.MaxAudioBytes = defaultMaxAudioBytes
	}
	if cfg.MaxChunkBytes == 0 {
		cfg.MaxChunkBytes = defaultMaxChunkBytes
	}
	if cfg.MaxTextBytes == 0 {
		cfg.MaxTextBytes = defaultMaxTextBytes
	}
	if cfg.MaxAudioBytes%2 != 0 || cfg.MaxChunkBytes%2 != 0 {
		return nil, errors.New("PCM byte limits must be even")
	}
	return &Worker{
		cfg:     cfg,
		factory: factory,
		active:  make(chan struct{}, 1),
	}, nil
}

// contextFailure 将取消或超时转换为对应的 gRPC 状态错误。
func contextFailure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

// StreamingRecognize 拥有本 RPC 的 Engine、mapper 和输入计数。
// 正常尾部刷新及 final 发送成功后返回 nil；异常路径不补发 final。
func (w *Worker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	ctx := stream.Context()

	if err := contextFailure(ctx); err != nil {
		return err
	}

	// 非阻塞准入：Worker 忙时立即拒绝，不排队等待。
	select {
	case w.active <- struct{}{}:
		// 先注册名额释放。
		// Engine.Close 的 defer 将在后面注册，
		// 因此 Close 完成后才会归还名额。
		defer func() {
			<-w.active
		}()

	default:
		if err := contextFailure(ctx); err != nil {
			return err
		}
		return status.Error(
			codes.ResourceExhausted,
			"worker is busy",
		)
	}

	if err := contextFailure(ctx); err != nil {
		return err
	}

	// 获取名额后才创建模型资源。
	engine, factoryErr := w.factory(ctx)

	// 成功取得 Engine 后立即注册清理。
	// 即使已经取消，也必须等待 Close 执行完成。
	if factoryErr == nil && engine != nil {
		defer engine.Close()
	}

	if err := contextFailure(ctx); err != nil {
		return err
	}
	if factoryErr != nil {
		// 创建失败时，Factory 负责清理部分创建的资源。
		return status.Error(codes.Internal, "model creation failed")
	}
	if engine == nil {
		return status.Error(codes.Internal, "model factory returned nil engine")
	}
	// 每场 RPC 使用独立映射器
	mapper, err := newSnapshotMapper("1", w.cfg.MaxTextBytes)
	if err != nil {
		return status.Error(codes.Internal, "snapshot mapper initialization failed")
	}

	// total 只记录收到并成功提交给模型的原始 PCM 字节。
	// 它是输入预算计数，不是模型处理进度。
	total := 0

	for {
		if err := contextFailure(ctx); err != nil {
			return err
		}

		req, recvErr := stream.Recv()

		if err := contextFailure(ctx); err != nil {
			return err
		}
		// 只有合法输入 EOF 才执行正常尾部刷新。
		if recvErr == io.EOF {
			if total == 0 {
				return status.Error(codes.InvalidArgument, "audio stream is empty")
			}

			if err := contextFailure(ctx); err != nil {
				return err
			}

			// 提交尾部 padding 并声明输入结束。
			// 具体模型操作由 Engine 实现负责。
			finishErr := engine.Finish()

			if err := contextFailure(ctx); err != nil {
				return err
			}
			if finishErr != nil {
				return status.Error(codes.Internal, "model finish failed")
			}

			// 继续处理 Finish 后剩余的可执行解码步骤。
			if err := w.drain(stream, engine, mapper); err != nil {
				return err
			}

			// 最后读取一次完整文本快照。
			// 即使文本为空或与上一条相同，也生成 final。
			if err := w.sendSnapshot(stream, engine, mapper, true); err != nil {
				return err
			}

			return contextFailure(ctx)
		}

		// Recv 失败且未取消时保留原始传输错误。
		if recvErr != nil {
			return recvErr
		}

		if req == nil {
			return status.Error(codes.InvalidArgument, "nil audio request")
		}

		data := req.GetData()

		// 空数据块不进入模型。
		if len(data) == 0 {
			continue
		}

		// PCM S16LE 每个样本占两个字节。
		if len(data)%2 != 0 {
			return status.Error(codes.InvalidArgument, "unaligned PCM audio")
		}

		if len(data) > w.cfg.MaxChunkBytes {
			return status.Error(codes.ResourceExhausted, "audio chunk limit exceeded")
		}

		// 超限块不得进入模型。
		if len(data) > w.cfg.MaxAudioBytes-total {
			return status.Error(codes.ResourceExhausted, "audio stream limit exceeded")
		}

		// 将 PCM S16LE 转换为独立的 float32 样本切片。
		samples := pcm16Samples(data)

		if err := contextFailure(ctx); err != nil {
			return err
		}

		acceptErr := engine.Accept(samples)

		if err := contextFailure(ctx); err != nil {
			return err
		}
		if acceptErr != nil {
			return status.Error(codes.Internal, "model accept failed")
		}

		// Accept 成功后才计入本场原始音频预算。
		// 模型内部的前置 padding 不计入这里。
		total += len(data)

		// 每次提交音频后，处理当前所有可执行解码步骤。
		if err := w.drain(stream, engine, mapper); err != nil {
			return err
		}
	}

}

// drain 执行当前可运行的解码步骤，每步之后尝试发送 partial。
// 这里的排空仅指暂时没有可执行解码步骤，不是已处理字节确认。
func (w *Worker) drain(stream asrv1.ASRService_StreamingRecognizeServer,
	engine Engine, mapper *snapshotMapper) error {
	ctx := stream.Context()

	for {
		if err := contextFailure(ctx); err != nil {
			return err
		}

		ready, readyErr := engine.Ready()

		if err := contextFailure(ctx); err != nil {
			return err
		}
		if readyErr != nil {
			return status.Error(codes.Internal, "model ready check failed")
		}

		if !ready {
			return nil
		}

		if err := contextFailure(ctx); err != nil {
			return err
		}

		decodeErr := engine.Decode()

		// 即使 Decode 期间发生取消，也要等待它返回，
		// 再退出并执行 Engine.Close。
		if err := contextFailure(ctx); err != nil {
			return err
		}
		if decodeErr != nil {
			return status.Error(codes.Internal, "model decode failed")
		}

		if err := w.sendSnapshot(
			stream,
			engine,
			mapper,
			false,
		); err != nil {
			return err
		}
	}
}

// sendSnapshot 读取完整模型快照，经 mapper 转换；nil 响应不发送。
// final=true 只用于 Finish 和最终 drain 均成功之后。
func (w *Worker) sendSnapshot(stream asrv1.ASRService_StreamingRecognizeServer,
	engine Engine, mapper *snapshotMapper, final bool) error {
	ctx := stream.Context()

	if err := contextFailure(ctx); err != nil {
		return err
	}

	text, resultErr := engine.Result()

	if err := contextFailure(ctx); err != nil {
		return err
	}
	if resultErr != nil {
		return status.Error(
			codes.Internal,
			"model result failed",
		)
	}

	resp, err := mapper.mapSnapshot(text, final)
	if err != nil {
		// 不向客户端错误中暴露识别原文或原生错误信息。
		return status.Error(codes.Internal, "model snapshot mapping failed")
	}

	if resp == nil {
		return nil
	}

	if err := contextFailure(ctx); err != nil {
		return err
	}

	sendErr := stream.Send(resp)

	if err := contextFailure(ctx); err != nil {
		return err
	}

	// context 未取消时保留原始传输错误。
	// 发送失败后不得重试或补发 final。
	return sendErr
}

// pcm16Samples 将已经校验对齐的 PCM S16LE 转为新建 float32 切片。
// 调用方保证非空且长度为偶数；本函数不解析 WAV、不重采样。
func pcm16Samples(data []byte) []float32 {
	samples := make([]float32, len(data)/2)

	for i := 0; i < len(data); i += 2 {
		value := int16(
			binary.LittleEndian.Uint16(data[i : i+2]),
		)
		samples[i/2] = float32(value) / 32768
	}

	return samples
}
