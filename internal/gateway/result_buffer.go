package gateway

import (
	"errors"
	"math"
	"strings"
)

var (
	// errInvalidResultBufferLimits 表示字节或条目预算不为正。
	errInvalidResultBufferLimits = errors.New("invalid result buffer limits")
	// errResultBufferFull 表示追加结果会超过字节或条目预算。
	errResultBufferFull = errors.New("result buffer full")
	// errResultSequenceExhausted 表示结果序号已达最大值，不能继续分配。
	errResultSequenceExhausted = errors.New("result sequence exhausted")
	// errResultSequenceAhead 表示确认或读取位置超过最后已保存序号。
	errResultSequenceAhead = errors.New("result sequence ahead")
	// errResultReplayGap 表示读取位置落在已获确认并清理的范围内。
	errResultReplayGap = errors.New("result replay gap")
)

// retainedResult 是一次识别结果更新。
// seq 标识此次更新；多个更新可以属于同一个 segmentID。
type retainedResult struct {
	seq       uint64 // 逻辑会话内连续序号，从 1 开始。
	segmentID string // Worker 的片段标识。
	text      string // 此次更新的文本，保持原值。
	isFinal   bool   // 此片段是否定稿，不表示整场结束。
}

// resultBuffer 保存尚未获得累计确认的连续结果。
// 由唯一协调者串行操作，必须通过构造器创建。
// 通过指针使用，使用后不得复制或并发调用；不启动任务或执行 I/O。
type resultBuffer struct {
	slots         []retainedResult // 固定长度环形槽位。
	head          int              // 最早未确认结果的位置。
	count         int              // 当前保留条目数。
	retainedBytes uint64           // segmentID 和 text 的字节总数。
	maxBytes      uint64           // 字符串负载字节上限。
	lastSeq       uint64           // 最后成功保存的序号。
	ackedSeq      uint64           // 已接纳的累计确认位置。
}

// newResultBuffer 创建空缓冲。
// maxBytes 和 maxResults 必须为正；只预分配槽位。
// maxBytes 计量两个字符串字段的原始字节；maxResults 决定固定条目上限。
// 非法预算返回 nil 和 errInvalidResultBufferLimits。
func newResultBuffer(
	maxBytes uint64,
	maxResults int,
) (*resultBuffer, error) {
	if maxBytes == 0 || maxResults <= 0 {
		return nil, errInvalidResultBufferLimits
	}

	return &resultBuffer{
		slots:    make([]retainedResult, maxResults),
		maxBytes: maxBytes,
	}, nil
}

// append 保存一条更新的字符串副本，返回新序号。
// 容量不足或序号耗尽时返回错误，原状态保持不变。
// 同一 segmentID 的每次更新分别保存；isFinal 表示片段定稿。
// 空字符串允许进入，仍占一个槽位；字段合法性由响应解析层负责。
func (b *resultBuffer) append(
	segmentID, text string,
	isFinal bool,
) (uint64, error) {
	// 序号不能回绕。
	if b.lastSeq == math.MaxUint64 {
		return 0, errResultSequenceExhausted
	}

	// 固定槽位已经用完。
	if b.count == len(b.slots) {
		return 0, errResultBufferFull
	}

	// 不直接计算 len(segmentID)+len(text)，避免 int 加法溢出；
	// 也不直接做 retainedBytes+payload，避免 uint64 加法溢出。
	remaining := b.maxBytes - b.retainedBytes

	segmentBytes := uint64(len(segmentID))
	if segmentBytes > remaining {
		return 0, errResultBufferFull
	}
	remaining -= segmentBytes

	textBytes := uint64(len(text))
	if textBytes > remaining {
		return 0, errResultBufferFull
	}

	// 所有可能失败的检查完成后，再取得独立字符串副本。
	segmentID = strings.Clone(segmentID)
	text = strings.Clone(text)

	nextSeq := b.lastSeq + 1
	tail := (b.head + b.count) % len(b.slots)

	b.slots[tail] = retainedResult{
		seq:       nextSeq,
		segmentID: segmentID,
		text:      text,
		isFinal:   isFinal,
	}

	b.count++
	b.retainedBytes += segmentBytes
	b.retainedBytes += textBytes
	b.lastSeq = nextSeq

	return nextSeq, nil
}

// acknowledge 接纳累计确认，并释放对应前缀。
// 新确认返回 true；相同或更旧确认返回 false,nil。
// 超过 lastSeq 时返回错误，不修改状态。
// seq 表示客户端已连续应用的位置，调用方须先校验凭据、代次和投递范围。
func (b *resultBuffer) acknowledge(seq uint64) (bool, error) {
	if seq > b.lastSeq {
		return false, errResultSequenceAhead
	}

	if seq <= b.ackedSeq {
		return false, nil
	}

	release := seq - b.ackedSeq

	// 按 resultBuffer 的不变量： uint64(count) == lastSeq - ackedSeq 且 seq <= lastSeq，因此 release 一定 <= count。
	// 这里的转换因此是安全的。
	releaseCount := int(release)

	for i := 0; i < releaseCount; i++ {
		result := b.slots[b.head]

		b.retainedBytes -= uint64(len(result.segmentID))
		b.retainedBytes -= uint64(len(result.text))

		// 主动清掉字符串引用，否则槽位复用前仍会持有底层字符串。
		b.slots[b.head] = retainedResult{}

		b.head++
		if b.head == len(b.slots) {
			b.head = 0
		}

		b.count--
	}

	b.ackedSeq = seq

	return true, nil
}

// peekAfter 读取 after 之后紧邻的一条结果，不修改状态。
// 没有下一条时返回零值,false,nil；游标越界时返回错误。
// after<ackedSeq 是重放缺口；after>lastSeq 是位置超前。
// 返回值的字符串只读，确认清理缓冲后，已取得的值仍可安全读取。
func (b *resultBuffer) peekAfter(
	after uint64,
) (retainedResult, bool, error) {
	if after < b.ackedSeq {
		return retainedResult{}, false, errResultReplayGap
	}

	if after > b.lastSeq {
		return retainedResult{}, false, errResultSequenceAhead
	}

	// 必须在做 after+1 一类运算前处理这个边界，
	// 尤其 lastSeq == math.MaxUint64 时不能产生回绕。
	if after == b.lastSeq {
		return retainedResult{}, false, nil
	}

	// 保留区：ackedSeq+1 ... lastSeq
	// after 后面的结果相对于 head 的偏移为：after - ackedSeq
	relative := after - b.ackedSeq

	// 前面的范围检查已经保证 relative < uint64(count)。
	index := (b.head + int(relative)) % len(b.slots)

	return b.slots[index], true, nil
}
