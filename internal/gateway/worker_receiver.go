package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// errInvalidWorkerResponse 标识成功读取但无法解释为合法事件的 Worker 响应。
var errInvalidWorkerResponse = errors.New("invalid worker response")

// workerReceiveKind 区分普通响应事件，零值无效。
type workerReceiveKind uint8

const (
	workerReceiveInvalid  workerReceiveKind = iota // 非法零值，不交付协调者。
	workerReceiveResult                            // 一次识别文本更新。
	workerReceiveProgress                          // 累计音频处理进度。
)

// workerReceiveEvent 是解析后的响应值。
// 字符串只读；结果序号由协调者保存成功后分配。
type workerReceiveEvent struct {
	kind           workerReceiveKind // 响应类别。
	segmentID      string            // result 的片段标识。
	text           string            // result 的此次文本更新。
	isFinal        bool              // result 的片段定稿标志。
	processedBytes uint64            // progress 的累计处理字节数。
}

// workerReceiver 独占一条 Worker 流的 Recv。
// 使用构造器创建，以指针使用，run 恰好启动一次。
type workerReceiver struct {
	events chan workerReceiveEvent // 无缓冲，向唯一协调者交付。
	done   chan struct{}           // run 写好 err 后关闭。
	err    error                   // 仅 run 写；done 关闭后才能读。
}

// newWorkerReceiver 创建独立通道，不启动任务或 I/O。
func newWorkerReceiver() *workerReceiver {
	return &workerReceiver{
		events: make(chan workerReceiveEvent),
		done:   make(chan struct{}),
	}
}

// decodeWorkerResponse 解析一条成功读取的响应。
// nil 响应或 progress 混带文本字段时返回错误。
// 不修改响应，不操作缓冲，不分配结果序号。
func decodeWorkerResponse(response *asrv1.StreamingRecognizeResponse) (workerReceiveEvent, error) {
	if response == nil {
		return workerReceiveEvent{}, fmt.Errorf("%w: nil response", errInvalidWorkerResponse)
	}

	if response.Progress != nil {
		if response.SegmentId != "" {
			return workerReceiveEvent{}, fmt.Errorf("%w: progress response contains segment id", errInvalidWorkerResponse)
		}

		if response.Text != "" {
			return workerReceiveEvent{}, fmt.Errorf("%w: progress response contains text", errInvalidWorkerResponse)
		}

		if response.IsFinal {
			return workerReceiveEvent{}, fmt.Errorf("%w: progress response marked final", errInvalidWorkerResponse)
		}

		return workerReceiveEvent{
			kind:           workerReceiveProgress,
			processedBytes: response.Progress.ProcessedAudioBytes,
		}, nil
	}

	return workerReceiveEvent{
		kind:      workerReceiveResult,
		segmentID: response.SegmentId,
		text:      response.Text,
		isFinal:   response.IsFinal,
	}, nil
}

// run 串行读取并交付事件，退出前设置 err 并关闭 done。
// rpcCtx 属于创建 stream 的原 RPC，不能绑定某代 WebSocket。
// stream 必须响应 RPC 取消；本方法不主动取消 RPC。
func (r *workerReceiver) run(rpcCtx context.Context, stream workerStream) {
	if rpcCtx == nil {
		panic("gateway: workerReceiver.run called with nil rpcCtx")
	}
	if stream == nil {
		panic("gateway: workerReceiver.run called with nil stream")
	}

	defer close(r.done)

	for {
		// 在进入可能阻塞的 Recv 前先尊重已经发生的取消。
		if cause := context.Cause(rpcCtx); cause != nil {
			r.err = cause
			return
		}

		response, err := stream.Recv()

		// Recv 可能因为 RPC 取消而返回 EOF、nil 或某个底层错误。
		// 这里优先保留原 RPC 的取消原因。
		if cause := context.Cause(rpcCtx); cause != nil {
			r.err = cause
			return
		}

		// Recv 的错误优先于 response。
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.err = nil
				return
			}

			r.err = err
			return
		}

		event, err := decodeWorkerResponse(response)
		if err != nil {
			r.err = err
			return
		}

		select {
		case r.events <- event:
			// 当前事件已经完成所有权交接。
			//
			// 清零主要是及时解除这里对字符串的引用；
			// 下一轮不会依赖旧 event。
			event = workerReceiveEvent{}

		case <-rpcCtx.Done():
			r.err = context.Cause(rpcCtx)
			return
		}
	}
}
