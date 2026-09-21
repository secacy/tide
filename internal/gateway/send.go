package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

var ErrWorkerSendTimeout = errors.New("worker send timeout") // 单次向 Worker 发送音频的等待超过期限

// sendWithTimeout 同步发送一块音频，等待超时后取消整个 RPC。
// rpcCtx 和 cancelRPC 必须来自创建该 stream 的同一组 WithCancelCause。
// stream 必须响应 RPC context 取消。
// 不能与该 stream 的另一次 Send 或 CloseSend 并发调用。
func sendWithTimeout(
	rpcCtx context.Context,
	cancelRPC context.CancelCauseFunc,
	stream workerStream,
	request *asrv1.StreamingRecognizeRequest,
	timeout time.Duration,
) error {
	// 检查参数和取消状态
	if timeout <= 0 {
		return fmt.Errorf("send timeout must be positive: %s", timeout)
	}
	if cause := context.Cause(rpcCtx); cause != nil {
		return cause
	}
	// 创建回调完成channel
	timeoutDone := make(chan struct{})

	timer := time.AfterFunc(timeout, func() {
		cancelRPC(ErrWorkerSendTimeout)
		close(timeoutDone)
	})

	sendErr := stream.Send(request)

	// 停止计时器，并处理 Send 返回与超时回调之间的竞争。
	if !timer.Stop() {
		<-timeoutDone
	}

	if cause := context.Cause(rpcCtx); cause != nil {
		return cause
	}
	return sendErr
}
