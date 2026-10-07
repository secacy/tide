package gateway

import (
	"context"
	"errors"
)

var (
	errResultWriteInFlight = errors.New("result write in flight") // 当前连接代已经有一条结果交给写任务，在它报告完成或连接失效前不能再授权下一条。
	errResultWriteMismatch = errors.New("result write mismatch")  // 写成功报告与当前代唯一在途结果不匹配。
	errResultAckAhead      = errors.New("result ack ahead")       // 客户端确认了尚未授权给任何写任务的结果位置。
)

// resultDeliveryState 保存结果交付状态，由唯一协调者串行操作。
// newSessionWorker 初始化 changed；使用后不得复制或并发读取。
type resultDeliveryState struct {
	offeredSeq  uint64        // 跨代次保留：曾授权交给内部写任务的最大连续序号。
	cursor      uint64        // 当前代已写成功或已获应用确认的位置；下一次从此处读取。
	inFlightSeq uint64        // 当前代已借出且未报告写成功的序号；0 表示没有。
	changed     chan struct{} // 输入接纳或结果状态变化通知；协调者独占关闭/替换，调用方仅等待。
}

// resultOffer 是一次取结果操作的值快照。
// available=true 时同时建立唯一写入授权，调用方负责写入或报告连接失效。
// 字符串只读，确认释放缓冲后已经取得的值仍可使用。
// input 与 changed 同时取得；没有结果时也可用于发送累计输入确认。
type resultOffer struct {
	result          retainedResult  // available=true 时有效；否则为零值。
	available       bool            // 本次是否实际取得并授权了一条结果。
	workerCompleted bool            // Worker 已正常完成，不表示客户端已收到终态。
	lastSeq         uint64          // 快照时最后已保存的结果序号。
	ackedSeq        uint64          // 快照时已接纳的累计应用确认。
	changed         <-chan struct{} // 一次性状态变化通知；唤醒后重新查询。
	input           inputAcceptance // 本次查询时的输入接纳状态。
}

// offerResult 读取并授权 cursor 后紧邻的一条结果。
// 已有 inFlight 时返回零值和 errResultWriteInFlight。
// 没有下一条时返回 available=false 的快照，不等待、不移动位置。
// 成功交付前登记 inFlightSeq 和 offeredSeq，不释放存储或推进 cursor。
func (w *sessionWorker) offerResult() (resultOffer, error) {
	if w.delivery.inFlightSeq != 0 {
		return resultOffer{}, errResultWriteInFlight
	}

	result, ok, err := w.results.peekAfter(w.delivery.cursor)
	if err != nil {
		return resultOffer{}, err
	}

	offer := resultOffer{
		input: inputAcceptance{
			nextOffset: w.input.input.nextOffset,
			inputEnded: w.input.input.ended,
		},
		workerCompleted: w.phase == workerRetaining,
		lastSeq:         w.results.lastSeq,
		ackedSeq:        w.results.ackedSeq,
		changed:         w.delivery.changed,
	}

	if !ok {
		return offer, nil
	}

	// 必须在回复调用方之前登记授权。
	// 写任务只有拿到这个方法的结果后才可能真正执行 Write。
	w.delivery.inFlightSeq = result.seq

	if result.seq > w.delivery.offeredSeq {
		w.delivery.offeredSeq = result.seq
	}

	offer.result = result
	offer.available = true

	return offer, nil
}

// completeResultWrite 接纳当前代的一次写成功报告。
// seq 必须非零且等于 inFlightSeq；失败返回 errResultWriteMismatch，不修改状态。
// 成功清除 inFlightSeq，并将 cursor 推进到 max(cursor,seq)，不释放结果。
func (w *sessionWorker) completeResultWrite(seq uint64) error {
	if seq == 0 || seq != w.delivery.inFlightSeq {
		return errResultWriteMismatch
	}

	w.delivery.inFlightSeq = 0

	if seq > w.delivery.cursor {
		w.delivery.cursor = seq
	}

	return nil
}

// acknowledgeResult 接纳客户端已连续应用至 seq 的声明。
// seq>offeredSeq 返回 errResultAckAhead，不修改状态。
// 其余交给 resultBuffer.acknowledge；推进确认后令 cursor>=ackedSeq。
// 不清除在途写入，即使 ACK 已覆盖它也要等待写任务的报告或断开。
func (w *sessionWorker) acknowledgeResult(seq uint64) (bool, error) {
	if seq > w.delivery.offeredSeq {
		return false, errResultAckAhead
	}

	advanced, err := w.results.acknowledge(seq)
	if err != nil {
		return false, err
	}

	// ACK 可以早于 Write 成功报告，也可以跨过当前重放中的结果。
	if w.results.ackedSeq > w.delivery.cursor {
		w.delivery.cursor = w.results.ackedSeq
	}

	return advanced, nil
}

// resetResultDelivery 在有效断开或成功恢复时重置本代写入状态。
// cursor 回到当前 ackedSeq，inFlightSeq 和完成通知授权清零；offeredSeq 跨代次保留。
// 只撤销本代授权元数据，旧写任务持有值的释放仍由该任务负责。
func (w *sessionWorker) resetResultDelivery() {
	w.delivery.cursor = w.results.ackedSeq
	w.delivery.inFlightSeq = 0
	w.completion.offeredGeneration = 0
}

// notifyOutputChange 在已提交输入接纳或结果变化后关闭旧通知，并创建新通道。
// 只能在协调者运行期间调用；最终退出关闭当前通道且不再替换。
func (w *sessionWorker) notifyOutputChange() {
	if w.delivery.changed == nil {
		panic("gateway: nil result change channel")
	}

	close(w.delivery.changed)
	w.delivery.changed = make(chan struct{})
}

// requestResult 请求当前代下一条结果，并取得本次写入授权。
// ctx 只控制本次命令；generation 必须是当前 attached 代次。
// 成功返回的 available=false 表示暂时没有下一条，调用方可等待 changed。
// 错误返回零值；调用方不得把该入口当成无副作用的窥视操作。
func (s *resumableSession) requestResult(ctx context.Context, generation uint64) (resultOffer, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlTakeResult,
		generation: generation,
	})

	if result.err != nil {
		return resultOffer{}, result.err
	}

	return result.offer, nil
}

// reportResultWritten 报告此前取得的 seq 已成功写入该代连接。
// 仅成功 Write 后调用；失败由连接拥有者按错误类别报告断开或结束会话。
// stale/detached 报告返回 false,nil；当前代序号不匹配返回错误。
// 报告使用独立的有效请求 ctx；不能默默丢弃已经成功交付的授权。
func (s *resumableSession) reportResultWritten(ctx context.Context, generation, seq uint64) (bool, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlResultWritten,
		generation: generation,
		resultSeq:  seq,
	})

	if result.err != nil {
		return false, result.err
	}

	return result.handled, nil
}

// requestResultAck 报告客户端已连续应用至 seq。
// 必须先校验当前附着资格和 generation，再进行幂等/上限判定。
// 新确认返回 true；旧/重复确认 false,nil；错误返回 false。
func (s *resumableSession) requestResultAck(ctx context.Context, generation, seq uint64) (bool, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlResultAck,
		generation: generation,
		resultSeq:  seq,
	})

	if result.err != nil {
		return false, result.err
	}

	return result.advanced, nil
}

// requestResume 在同一串行处理段校验结果恢复位置并切换连接代次。
// appliedSeq 是客户端已连续应用的位置；不得低于 ackedSeq 或超过 offeredSeq。
// 成功时接纳该累计确认并以它作为新代投递起点，返回新 generation。
// 纯控制模式没有 Worker，只允许 appliedSeq=0。
// 真实候选连接的安装与凭据检查在后续接入时纳入同一个提交边界。
func (s *resumableSession) requestResume(ctx context.Context, appliedSeq uint64) (uint64, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:      controlResume,
		resultSeq: appliedSeq,
	})

	if result.err != nil {
		return 0, result.err
	}

	return result.generation, nil
}
