package realasr

import (
	"errors"
	"fmt"
)

var (
	errSegmentCursorBudget        = errors.New("invalid segment cursor budget")
	errSegmentCursorInput         = errors.New("invalid segment cursor input length")
	errSegmentCursorTotalLimit    = errors.New("total audio limit exceeded")
	errSegmentCursorSegmentLimit  = errors.New("segment audio limit exceeded")
	errSegmentCursorCannotAdvance = errors.New("segment cursor cannot advance")
)

// segmentCursor 记录单场 RPC 的原始音频提交进度。
// 每个 RPC 独占一个实例，所有操作串行执行，无需加锁。
// 满段时停留在原编号，直到调用者完成旧段收尾并准备好下一段。
type segmentCursor struct {
	maxTotalBytes   int // 整场原始输入上限；不包含 padding。
	maxSegmentBytes int // 单段原始输入上限；不大于整场上限。
	totalBytes      int // 本 RPC 已成功 Accept 的原始字节总数。
	segmentBytes    int // 当前段已成功 Accept 的原始字节数。
	segmentNumber   int // 从 1 开始；只在 advance 成功时加一。
}

// newSegmentCursor 创建原始音频分段计数器。
// 整场和单段上限必须为正偶数，且单段上限不得超过整场上限。
// 不采用隐式默认值；默认配置由后续 Worker 构造入口负责。
func newSegmentCursor(maxTotal, maxSegment int) (*segmentCursor, error) {
	if maxTotal <= 0 || maxSegment <= 0 || maxTotal%2 != 0 || maxSegment%2 != 0 || maxSegment > maxTotal {
		return nil, fmt.Errorf("%w: total=%d segment=%d", errSegmentCursorBudget, maxTotal, maxSegment)
	}
	return &segmentCursor{
		maxTotalBytes:   maxTotal,
		maxSegmentBytes: maxSegment,
		segmentNumber:   1,
	}, nil
}

// plan 计算当前段能够接收的原始音频前缀长度。
// 校验本次尚未提交的请求余量 bytes（正偶数）能完全容纳于整场。
// 返回当前段可接收的前缀长度，不改变状态、不分配音频副本。
// 满段但整场仍能容纳该请求时返回 0,nil，提示调用者先完成换段。
// 单请求大小由 Worker 在拆分前校验，本组件不重复承担该配置。
func (c *segmentCursor) plan(bytes int) (int, error) {
	if bytes <= 0 || bytes%2 != 0 {
		return 0, errSegmentCursorInput
	}

	if bytes > c.maxTotalBytes-c.totalBytes {
		return 0, errSegmentCursorTotalLimit
	}

	available := c.maxSegmentBytes - c.segmentBytes

	if bytes < available {
		return bytes, nil
	}

	return available, nil
}

// commit 记录已经成功提交给模型的原始音频字节数。
// 仅在这一份前缀 Accept 成功后调用；增加整场和当前段计数。
func (c *segmentCursor) commit(bytes int) error {
	if bytes <= 0 || bytes%2 != 0 {
		return errSegmentCursorInput
	}
	if bytes > c.maxTotalBytes-c.totalBytes {
		return errSegmentCursorTotalLimit
	}
	if bytes > c.maxSegmentBytes-c.segmentBytes {
		return errSegmentCursorSegmentLimit
	}

	c.totalBytes += bytes
	c.segmentBytes += bytes
	return nil
}

// advance 将计数器推进到下一段。
// 要求当前段恰好满额，且整场仍有剩余额度。
// 调用者保证旧段已成功刷新/发送 final/释放，且下一段模型流已就绪。
func (c *segmentCursor) advance() error {
	if c.segmentBytes != c.maxSegmentBytes || c.totalBytes >= c.maxTotalBytes {
		return errSegmentCursorCannotAdvance
	}
	c.segmentNumber++
	c.segmentBytes = 0
	return nil
}
