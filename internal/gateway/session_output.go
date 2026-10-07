package gateway

// inputAcceptance 是协调者已提交的输入接纳状态。
// 两字段构成一个值快照，可以直接比较。
// inputEnded=true 时，nextOffset 已固定为最终输入位置。
type inputAcceptance struct {
	nextOffset uint64 // 连续已接纳字节位置。
	inputEnded bool   // 是否已接纳合法 end。
}
