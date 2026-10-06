package gateway

import (
	"context"
	"errors"
	"time"
)

var (
	errInvalidSessionWorkerConfig = errors.New("invalid session worker config")       // Worker RPC、期限或必要资源配置非法。
	errInvalidWorkerProgress      = errors.New("invalid worker progress")             // Worker 报告的累计处理位置非法，例如倒退或超过已经实际交付给uploader 的音频位置。
	errWorkerEndedEarly           = errors.New("worker ended before input completed") // Worker 接收方向在完整合法输入完成之前正常 EOF。
	errWorkerStatusTimeout        = errors.New("worker status timeout")               // uploader 已返回 EOF，但在规定期限内没有观察到真实 Recv 终态。
	errResultRetentionExpired     = errors.New("result retention expired")            // Worker 已正常计算结束，但结果保留/恢复窗口已经耗尽。
	errWorkerInputStopped         = errors.New("worker input stopped")                // Worker 输入方向已经停止，不能再接纳新的音频或新的 end。
	errSessionWorkerUnavailable   = errors.New("session worker unavailable")          // 当前协调循环没有 Worker 资源。
	errWorkerUploadResultMismatch = errors.New("worker upload result mismatch")       // 上传结果与当前等待的任务不匹配
	errSessionNotAttached         = errors.New("session is not attached")             // 当前逻辑会话没有连接处于 attached 状态
	errSessionGenerationMismatch  = errors.New("session generation mismatch")         // 输入来自非当前连接代次
)

// sessionWorkerConfig 描述一场逻辑会话的原 RPC、缓冲预算和等待期限。
// rpcCtx/cancelRPC 必须属于 stream，不绑定某代 WebSocket。
type sessionWorkerConfig struct {
	rpcCtx                 context.Context         // 原 Worker RPC 的生命周期。
	cancelRPC              context.CancelCauseFunc // 仅取消该 RPC，不关闭共享 ClientConn。
	stream                 workerStream            // 有效且响应原 RPC 取消的双向流。
	sendTimeout            time.Duration           // 单次 Send 期限，必须为正。
	tailTimeout            time.Duration           // 首次合法 end 到 Worker 完成的预算，必须为正。
	statusTimeout          time.Duration           // Send/CloseSend 返回 EOF 后等待 Recv 终态的预算，必须为正。
	resultRetentionTimeout time.Duration           // Worker 正常完成后的结果保留预算，必须为正。
	maxAudioBytes          uint64                  // 含在途块的音频存储预算，必须为正。
	maxAudioChunks         int                     // 含在途块的槽位预算，必须为正。
	maxResultBytes         uint64                  // 未确认结果的字符串字节预算，必须为正。
	maxResults             int                     // 未确认结果的条目预算，必须为正。
	maxPendingAudioBytes   uint64                  // 接纳但未确认处理的音频预算；0 关闭。
}

// sessionWorkerPhase 表示计算与结果保留阶段，不描述连接是否 attached。
type sessionWorkerPhase uint8

const (
	workerRunning   sessionWorkerPhase = iota // 上传/识别尚未正常完成。
	workerRetaining                           // 正常计算完成；继续保留结果及恢复资格，期限不续期。
)

// sessionUploadPhase 表示协调者眼中的上传操作阶段，不是整场会话状态。
type sessionUploadPhase uint8

const (
	uploadIdle           sessionUploadPhase = iota // 无未完成操作，可检查缓冲。
	uploadPending                                  // 已准备任务，尚未交付给 uploader。
	uploadWaiting                                  // 已交付，等待并处理其唯一结果。
	uploadHalfClosed                               // CloseSend 成功，上传结束；响应仍可能继续。
	uploadAwaitingStatus                           // Send/CloseSend 返回了 io.EOF。EOF 不能证明上传成功，因此停止继续上传，只等待 Recv 的真实终态。
)

// sessionWorker 组合原 Worker RPC 的双向任务与协调者独占状态。
// 通过构造器创建，以指针使用，一次运行；运行中禁止外部读取可变字段。
type sessionWorker struct {
	config          sessionWorkerConfig // 构造后不变。
	input           *audioInputBuffer   // 协调者独占；I/O 任务全退出后清理。
	results         *resultBuffer       // 协调者独占；整个协调路径结束后清理。
	uploader        *workerUploader     // 唯一 Send/CloseSend 任务。
	receiver        *workerReceiver     // 唯一 Recv 任务。
	phase           sessionWorkerPhase  // 计算成功只推进一次到 retaining。
	dispatchedBytes uint64              // 已交付 uploader 的音频末端，可能仍在 Send 中。
	processedBytes  uint64              // Worker 合法累计处理位置。
}

// newSessionWorker 校验 RPC、四项正期限及两组缓冲预算，创建资源但不启动任务。
// 失败返回 nil；保留音频/结果预算错误的 errors.Is 身份。
// 构造不转移 RPC 清理责任；只有 runWithWorker 启动后接管。
func newSessionWorker(config sessionWorkerConfig) (*sessionWorker, error) {
	if config.rpcCtx == nil ||
		config.cancelRPC == nil ||
		config.stream == nil ||
		config.sendTimeout <= 0 ||
		config.tailTimeout <= 0 ||
		config.statusTimeout <= 0 ||
		config.resultRetentionTimeout <= 0 {
		return nil, errInvalidSessionWorkerConfig
	}

	input, err := newAudioInputBuffer(
		config.maxAudioBytes,
		config.maxAudioChunks,
	)
	if err != nil {
		return nil, err
	}

	results, err := newResultBuffer(
		config.maxResultBytes,
		config.maxResults,
	)
	if err != nil {
		return nil, err
	}

	return &sessionWorker{
		config:   config,
		input:    input,
		results:  results,
		uploader: newWorkerUploader(),
		receiver: newWorkerReceiver(),
		phase:    workerRunning,
	}, nil
}

// runWithWorker 恰好启动一次 uploader、receiver 与同一协调循环。
// ctx 属于逻辑会话；now 与 Timer 同步，不可阻塞。
// 返回前取消原 RPC，等待两个 done，再解除 input/results 引用。
// 返回值是生命周期退出原因；nil 仍可表示明确 controlClose，不能单独视为 ASR 成功。
func (s *resumableSession) runWithWorker(ctx context.Context, now func() time.Time, worker *sessionWorker) error {
	if ctx == nil {
		panic("gateway: nil session context")
	}
	if now == nil {
		panic("gateway: nil time source")
	}
	if worker == nil {
		panic("gateway: nil session worker")
	}

	go worker.uploader.run(
		worker.config.rpcCtx,
		worker.config.cancelRPC,
		worker.config.stream,
		worker.config.sendTimeout,
	)

	go worker.receiver.run(
		worker.config.rpcCtx,
		worker.config.stream,
	)

	err := s.runCoordinator(ctx, now, worker)

	// coordinator 已经停止安排新 I/O。
	// 必须先取消 RPC，再等待可能阻塞在 Send/Recv 中的任务。
	worker.config.cancelRPC(err)

	<-worker.uploader.done
	<-worker.receiver.done

	// I/O 任务退出后才可以解除应用缓冲引用；
	// uploader/receiver 此前可能仍持有其中数据。
	worker.input = nil
	worker.results = nil

	return err
}

// acknowledgeProgress 校验累计处理位置，不操作 I/O 或分配结果序号。
// n 不得倒退或超过 dispatchedBytes；重复确认合法。失败不修改状态。
func (w *sessionWorker) acknowledgeProgress(n uint64) error {
	if n < w.processedBytes {
		return errInvalidWorkerProgress
	}

	if n > w.dispatchedBytes {
		return errInvalidWorkerProgress
	}

	w.processedBytes = n
	return nil
}

// offerAudio 先检查音频范围与未处理预算，再接管存储。
// offset 是字节起点，payload 在调用期间只读；失败不推进接纳位置。
// 完整历史重发不增加任何计量；代次/附着资格由协调者在调用前校验。
func (w *sessionWorker) offerAudio(offset uint64, payload []byte) (bool, error) {
	size := uint64(len(payload))

	kind, err := w.input.input.classifyAudio(offset, size)
	if err != nil {
		return false, err
	}

	switch kind {
	case audioChunkDuplicate:
		return w.input.offer(offset, payload)

	case audioChunkNew:
	default:
		panic("gateway: invalid audio chunk classification")
	}

	if w.config.maxPendingAudioBytes != 0 {
		// classifyAudio 已保证新块连续且 offset+size 不溢出。
		end := offset + size

		// 正常状态下 processedBytes 不会超过 nextOffset。
		// 这里避免内部状态损坏导致 uint64 下溢。
		if end < w.processedBytes {
			return false, errInvalidWorkerProgress
		}

		if end-w.processedBytes > w.config.maxPendingAudioBytes {
			return false, ErrAudioBacklogExceeded
		}
	}

	return w.input.offer(offset, payload)
}

// offerHistoricalAudio 用于 Worker 已停止输入后的幂等重放。
// 只有已经完整接纳过的历史块可以继续返回成功。
func (w *sessionWorker) offerHistoricalAudio(
	offset uint64,
	payload []byte,
) (bool, error) {
	kind, err := w.input.input.classifyAudio(
		offset,
		uint64(len(payload)),
	)
	if err != nil {
		return false, err
	}

	if kind != audioChunkDuplicate {
		return false, errWorkerInputStopped
	}

	return w.input.offer(offset, payload)
}
