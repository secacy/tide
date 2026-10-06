package gateway

import (
	"context"
	"errors"
	"time"
)

var (
	errInvalidSessionUploadConfig = errors.New("invalid session upload config") // 上传配置缺少必要资源或使用了非法的单次发送期限
	errWorkerUploadResultMismatch = errors.New("worker upload result mismatch") // 上传结果与当前等待的任务不匹配
	errSessionUploadUnavailable   = errors.New("session upload unavailable")    // 当前协调循环没有配置上传资源
	errSessionNotAttached         = errors.New("session is not attached")       // 当前逻辑会话没有连接处于 attached 状态
	errSessionGenerationMismatch  = errors.New("session generation mismatch")   // 输入来自非当前连接代次
)

// sessionUploadConfig 描述一场逻辑会话已经建立的 Worker RPC 及输入预算。
// rpcCtx/cancelRPC 必须属于创建 stream 的同一 RPC，继承逻辑会话生命周期。
// 不能绑定某一代 WebSocket；Go 类型无法验证这层关联，启动方负责保证。
type sessionUploadConfig struct {
	rpcCtx         context.Context         // 原 Worker RPC 的 context。
	cancelRPC      context.CancelCauseFunc // 取消原 RPC，不关闭共享 gRPC ClientConn。
	stream         workerStream            // 有效、响应 RPC 取消的流。
	sendTimeout    time.Duration           // 单次 Send 期限，必须为正。
	maxAudioBytes  uint64                  // 缓冲字节预算，包含在途块，必须为正。
	maxAudioChunks int                     // 缓冲槽位预算，包含在途块，必须为正。
}

// sessionUpload 组合协调者独占的缓冲和唯一上传任务。
// 构造后只交给一个 runWithUpload；不得复用或并发读取可变字段。
type sessionUpload struct {
	config   sessionUploadConfig // 初始化后不变。
	input    *audioInputBuffer   // 只有协调者调用；上传退出后解除引用。
	uploader *workerUploader     // 通过命令/结果通道交接，run 只启动一次。
}

// sessionUploadPhase 表示协调者眼中的上传操作阶段，不是整场会话状态。
type sessionUploadPhase uint8

const (
	uploadIdle       sessionUploadPhase = iota // 无未完成操作，可检查缓冲。
	uploadPending                              // 已准备任务，尚未交付给 uploader。
	uploadWaiting                              // 已交付，等待并处理其唯一结果。
	uploadHalfClosed                           // CloseSend 成功，上传结束；响应仍可能继续。
)

// newSessionUpload 校验配置并创建空缓冲及任务通道，不启动 I/O 或 goroutine。
// 失败返回 nil 和错误；必要配置缺失使用 errInvalidSessionUploadConfig，
// 缓冲预算错误保留 errInvalidAudioBufferLimits 的 errors.Is 身份。
// stream 的 typed nil 属于调用方违反有效流契约，不要求反射检测。
// 构造成功或失败均不接管 RPC 清理；只有交给 runWithUpload 才转交责任。
func newSessionUpload(config sessionUploadConfig) (*sessionUpload, error) {
	if config.rpcCtx == nil || config.cancelRPC == nil || config.stream == nil || config.sendTimeout <= 0 {
		return nil, errInvalidSessionUploadConfig
	}

	input, err := newAudioInputBuffer(
		config.maxAudioBytes,
		config.maxAudioChunks,
	)
	if err != nil {
		return nil, err
	}

	return &sessionUpload{
		config:   config,
		input:    input,
		uploader: newWorkerUploader(),
	}, nil
}

// runWithUpload 运行同一个协调循环及唯一上传任务，恰好调用一次。
// ctx 属于逻辑会话，now 与 Timer 同步；必要参数 nil 属于编程错误。
// 开始运行后接管 RPC 的取消和 uploader 的等待责任。
// 返回前已取消 RPC、观察到 uploader.done，并解除剩余缓冲引用。
// 返回值是本步协调/上传路径的退出原因，不是整场 ASR 结论。
func (s *resumableSession) runWithUpload(ctx context.Context, now func() time.Time, upload *sessionUpload) error {
	if ctx == nil {
		panic("gateway: nil session context")
	}
	if now == nil {
		panic("gateway: nil time source")
	}
	if upload == nil {
		panic("gateway: nil session upload")
	}

	go upload.uploader.run(
		upload.config.rpcCtx,
		upload.config.cancelRPC,
		upload.config.stream,
		upload.config.sendTimeout,
	)

	err := s.runCoordinator(ctx, now, upload)

	// 从这里开始不再安排新的上传任务。
	//
	// 无论 coordinator 为什么结束，都取消原 Worker RPC，
	// 让可能阻塞中的 Send 能够退出。
	upload.config.cancelRPC(err)

	// 必须确认 uploader 真正退出以后才能释放 buffer，
	// 因为 uploader 可能仍持有 take 出去的 data 引用。
	<-upload.uploader.done

	upload.input = nil

	return err
}
