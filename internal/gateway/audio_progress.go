package gateway

import (
	"fmt"
	"sync"
)

// audioProgress 保存一个会话的音频接收量与处理确认量。
// 零值可用，支持并发调用；开始使用后不可复制。
type audioProgress struct {
	mu sync.Mutex // 保护两个计数及其组合校验。

	receivedBytes  uint64 // 网关已完整接收的原始音频字节数。
	processedBytes uint64 // Worker 已累计确认处理的字节数。
}

// audioProgressSnapshot 是同一时刻取得的计量快照。
// 只保存数值，不引用计量器内部状态。
type audioProgressSnapshot struct {
	receivedBytes  uint64 // 已接收量。
	processedBytes uint64 // 已确认处理量。
	pendingBytes   uint64 // 已接收但尚未确认处理的音频量。
}

// addReceived 将 n 个新接收的原始音频字节加入本会话的累计接收量。
// n 是本次增量，不是累计值；允许为零。计数溢出时返回错误，所有计数保持不变。
// 接入上传流程时应在完整读取音频之后、向 Worker 发送之前调用；
// 它记录的是已接收量，不代表发送成功，发送失败也不应回滚该计数。
// 与其他计量方法通过同一把锁同步；锁仅覆盖检查和计数更新。
func (a *audioProgress) addReceived(n uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	// 检查溢出
	if n > ^uint64(0)-a.receivedBytes {
		return fmt.Errorf("receivedBytes overflow")
	}
	a.receivedBytes += n
	return nil
}

// acknowledge 将 Worker 汇报的累计处理字节数 n 记为本会话的最新确认位置。
// n 不是本次新增处理量；允许重复确认，包括初始的零值确认。
// 确认不能倒退，也不能超过累计接收量；校验失败时返回错误，所有计数保持不变。
// 校验与更新在同一临界区内完成，支持与 addReceived、snapshot 并发调用。
func (a *audioProgress) acknowledge(n uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n < a.processedBytes {
		return fmt.Errorf("acknowledge %d bytes, but already acknowledged %d bytes", n, a.processedBytes)
	}
	if n > a.receivedBytes {
		return fmt.Errorf("acknowledge %d bytes, but only received %d bytes", n, a.receivedBytes)
	}
	a.processedBytes = n
	return nil
}

// snapshot 在同一临界区内读取接收量与处理确认量，并计算未确认处理量。
// 返回独立的值快照，调用方修改它或后续更新计量器均不会影响另一方。
// pendingBytes 包含尚未获得处理确认的音频，不等于 Worker 队列大小或实际等待时长。
func (a *audioProgress) snapshot() audioProgressSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return audioProgressSnapshot{
		receivedBytes:  a.receivedBytes,
		processedBytes: a.processedBytes,
		pendingBytes:   a.receivedBytes - a.processedBytes,
	}
}
