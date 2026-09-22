package gateway

import (
	"context"
	"errors"
	"time"
)

// ErrTailTimeout 表示输入结束后，未能在预算内完成结果转发。
var ErrTailTimeout = errors.New("tail timeout")

// waitSessionEvent 等待会话退出事件。
// 观察到合法 end 通知后，启动一次性的尾部完成计时器。
// end 本身不是退出事件；后续结果不会延长期限。
// tailTimeout 必须为正值。
// 本函数只返回原因，不取消 RPC、不关闭连接，也不释放会话名额。
func waitSessionEvent(ctx context.Context, events <-chan sessionResult, inputEnded <-chan struct{}, tailTimeout time.Duration) sessionResult {
	var (
		endSignal = inputEnded     // 本函数使用的接收入口，处理一次后禁用
		timer     *time.Timer      // 尚未观察到 end 时，不创建计时器
		timeoutC  <-chan time.Time // nil channel 在 select 中不会被选中，相当于暂时关闭超时分支
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case event := <-events:
			return event
		case <-ctx.Done():
			return sessionResult{
				kind: resultServerStopping,
				err:  ctx.Err(),
			}
		case <-endSignal:
			// 创建计时器
			timer = time.NewTimer(tailTimeout)
			timeoutC = timer.C
			endSignal = nil
		case <-timeoutC:
			return sessionResult{
				kind: resultTailTimeout,
				err:  ErrTailTimeout,
			}
		}
	}
}
