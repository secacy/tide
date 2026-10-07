package gateway

import (
	"context"
	"errors"
)

var (
	errCompletionNotReady   = errors.New("session completion not ready")      // 正常计算或本代结果写入尚未完成。
	errCompletionNotOffered = errors.New("session completion not offered")    // 当前代尚未获完成通知授权。
	errCompletionMismatch   = errors.New("session completion range mismatch") // 确认范围与服务器固定终态不同。
)

// completionSnapshot 是正常计算完成后的固定业务范围。
type completionSnapshot struct {
	finalOffset uint64 // 已接纳的合法输入终点。
	lastSeq     uint64 // 最后一条结果序号，允许 0。
}

// completionState 只由协调者修改，放在 sessionWorker 中。
// 外层运行器只能在协调者结束后读取 acknowledged；缓冲清理不抹去此业务事实。
type completionState struct {
	offeredGeneration uint64 // 已授权发送完成通知的代次；0 表示没有。
	acknowledged      bool   // 已接纳整场确认，随后进入最终清理。
}

// requestCompletion 取得当前 attached 代完成范围，并登记发送授权。
// ctx 控制本次请求，generation 为服务端固定代次；命令交付后等待明确回复。
// 失败返回零值，不改变结果确认或期限。
func (s *resumableSession) requestCompletion(ctx context.Context, generation uint64) (completionSnapshot, error) {
	result := s.submitCommand(ctx, sessionControlCommand{kind: controlCompletion, generation: generation})
	if result.err != nil {
		return completionSnapshot{}, result.err
	}
	return result.completion, nil
}

// requestCompletionAck 接纳本代完整确认。
// ctx 控制本次请求，generation 为 reader 固定代次；finalOffset/lastSeq 必须匹配终态。
// nil 表示确认已提交，不代表连接和任务已经清理完成。
func (s *resumableSession) requestCompletionAck(ctx context.Context, generation, finalOffset, lastSeq uint64) error {
	return s.submitCommand(ctx, sessionControlCommand{
		kind: controlCompletionAck, generation: generation, offset: finalOffset, resultSeq: lastSeq,
	}).err
}

// offerCompletion 由协调者在当前代附着校验后调用。
// generation 是服务端固定代次；先登记授权再回复，允许确认早于实际 Write 返回。
// 不推进结果确认，也不改变保留期限。
func (w *sessionWorker) offerCompletion(generation uint64) (completionSnapshot, error) {
	if w.phase != workerRetaining || !w.input.input.ended ||
		w.delivery.inFlightSeq != 0 || w.delivery.cursor != w.results.lastSeq {
		return completionSnapshot{}, errCompletionNotReady
	}
	w.completion.offeredGeneration = generation
	return completionSnapshot{finalOffset: w.input.input.nextOffset, lastSeq: w.results.lastSeq}, nil
}

// acknowledgeCompletion 由协调者在当前代附着校验后调用。
// finalOffset/lastSeq 必须覆盖固定终态；全部校验通过后才提交累计结果确认。
// acknowledged 即使在结果已单独确认时仍置 true，随后协调者结束。
func (w *sessionWorker) acknowledgeCompletion(generation, finalOffset, lastSeq uint64) error {
	if w.phase != workerRetaining || !w.input.input.ended {
		return errCompletionNotReady
	}
	if w.completion.offeredGeneration != generation {
		return errCompletionNotOffered
	}
	if finalOffset != w.input.input.nextOffset || lastSeq != w.results.lastSeq {
		return errCompletionMismatch
	}
	if w.delivery.inFlightSeq != 0 || w.delivery.cursor != lastSeq || lastSeq > w.delivery.offeredSeq {
		return errCompletionNotReady
	}
	if _, err := w.acknowledgeResult(lastSeq); err != nil {
		return err
	}
	w.completion.acknowledged = true
	return nil
}
