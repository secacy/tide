package gateway

import (
	"context"
	"errors"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

var (
	// errInvalidWorkerUploadCommand 表示上传任务收到内部非法命令：未知 kind、空音频，或 CloseSend 命令携带非零 chunk。
	errInvalidWorkerUploadCommand = errors.New("invalid worker upload command")
)

// workerUploadKind 表示上传任务支持的操作。
type workerUploadKind uint8

const (
	workerUploadInvalid   workerUploadKind = iota // 无效零值。
	workerUploadAudio                             // 发送一块音频。
	workerUploadCloseSend                         // 半关闭输入。
)

// workerUploadCommand 是协调者交付的一次操作。
type workerUploadCommand struct {
	kind  workerUploadKind // 操作类型。
	chunk bufferedAudio    // 音频命令使用；半关闭命令必须为零值。
}

// workerUploadResult 是一个已接收操作的唯一结果。
type workerUploadResult struct {
	kind   workerUploadKind // 对应的操作类型。
	offset uint64           // 音频起点；合法半关闭命令为 0。
	err    error            // 操作错误、取消原因或内部命令错误。
}

// workerUploader 管理一场会话的串行上传任务。
// 通过构造器创建，以指针传递；run 恰好启动一次。
// 协调者处理完前一个结果后，才可提交下一任务。
type workerUploader struct {
	jobs    chan workerUploadCommand // 无缓冲，协调者发送。
	results chan workerUploadResult  // 容量为 1，上传任务发送。
	done    chan struct{}            // 上传任务退出时关闭，不表示 Worker 响应流完成。
}

// newWorkerUploader 创建三个独立通道，不启动任务，也不执行 I/O。
func newWorkerUploader() *workerUploader {
	return &workerUploader{
		jobs:    make(chan workerUploadCommand),
		results: make(chan workerUploadResult, 1),
		done:    make(chan struct{}),
	}
}

// run 串行执行发送和半关闭，由生命周期拥有者恰好启动一次。
// rpcCtx/cancelRPC 属于创建 stream 的同一 RPC，不能绑定某次 WebSocket。
// stream 必须有效且响应 RPC 取消；sendTimeout 必须为正。
// 必要参数为 nil 或期限非法时入口 panic；每次启动仅有一个结果写入者。
// 每个已接收任务发布一次结果；退出时关闭 done。
// 不读取 Worker 响应，不操作缓冲，不在正常半关闭后取消 RPC。
func (u *workerUploader) run(
	rpcCtx context.Context,
	cancelRPC context.CancelCauseFunc,
	stream workerStream,
	sendTimeout time.Duration,
) {
	if rpcCtx == nil {
		panic("gateway: nil worker uploader RPC context")
	}
	if cancelRPC == nil {
		panic("gateway: nil worker uploader RPC cancel function")
	}
	if stream == nil {
		panic("gateway: nil worker uploader stream")
	}
	if sendTimeout <= 0 {
		panic("gateway: non-positive worker uploader send timeout")
	}

	defer close(u.done)

	for {
		if context.Cause(rpcCtx) != nil {
			return
		}

		var job workerUploadCommand

		select {
		case <-rpcCtx.Done():
			return

		case received, ok := <-u.jobs:
			if !ok {
				panic("gateway: worker uploader jobs channel closed")
			}
			job = received
		}

		result := workerUploadResult{
			kind:   job.kind,
			offset: job.chunk.offset,
		}

		// 一旦接收了 job，就必须为它发布一次结果。
		if cause := context.Cause(rpcCtx); cause != nil {
			result.err = cause
			job.chunk = bufferedAudio{}
			u.results <- result
			return
		}

		stop := false

		switch job.kind {
		case workerUploadAudio:
			if len(job.chunk.data) == 0 {
				result.err = errInvalidWorkerUploadCommand
				stop = true
				break
			}

			req := &asrv1.StreamingRecognizeRequest{
				Data: job.chunk.data,
			}

			result.err = sendWithTimeout(rpcCtx, cancelRPC, stream, req, sendTimeout)

			if result.err != nil {
				stop = true
			}

		case workerUploadCloseSend:
			if job.chunk.offset != 0 || job.chunk.data != nil {
				result.err = errInvalidWorkerUploadCommand
			} else {
				result.err = stream.CloseSend()

				if cause := context.Cause(rpcCtx); cause != nil {
					result.err = cause
				}
			}

			// CloseSend 是上传方向最后一个操作。
			stop = true

		default:
			result.err = errInvalidWorkerUploadCommand
			stop = true
		}

		// uploader 不再主动保留音频引用。
		job.chunk = bufferedAudio{}

		// 已接收任务必须发布结果。
		// 这里不能再和 rpcCtx.Done 做 select。
		u.results <- result

		if stop {
			return
		}
	}
}
