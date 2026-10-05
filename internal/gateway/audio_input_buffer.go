package gateway

import "errors"

var (
	// errInvalidAudioBufferLimits 表示音频缓冲区的字节数或块数上限非法。
	errInvalidAudioBufferLimits = errors.New("invalid audio buffer limits")

	// errAudioBufferFull 表示新音频会超过缓冲区的字节数或块数上限。
	errAudioBufferFull = errors.New("audio buffer full")

	// errAudioCompletionMismatch 表示发送完成事件与当前在途队首不匹配。
	errAudioCompletionMismatch = errors.New("audio completion mismatch")
)

// bufferedAudio 是缓冲区拥有的一块音频。
// 借给发送任务后，data 必须保持只读。
type bufferedAudio struct {
	offset uint64 // 音频在逻辑会话中的字节起点。
	data   []byte // 独立副本，不与 offer 的调用方共享底层数组。
}

// audioInputBuffer 组合连续输入状态与有界音频存储。
// 由协调者串行操作，必须通过构造器创建，使用后不能复制。
type audioInputBuffer struct {
	input         audioInputState // 连续接纳位置与输入结束状态。
	slots         []bufferedAudio // 固定长度的环形槽位。
	head          int             // 最早尚未完成发送的块所在位置。
	count         int             // 已占槽位数，包含正在发送的队首。
	retainedBytes uint64          // 已占槽位的音频字节总数，包含在途块。
	maxBytes      uint64          // 音频字节上限，创建后不变。
	inFlight      bool            // 队首是否已借给发送任务。
}

// newAudioInputBuffer 创建空缓冲区。
// maxBytes 和 maxChunks 必须为正；仅预分配槽位，不预分配音频。
// 非法配置返回 nil 和 errInvalidAudioBufferLimits，不启动任何任务。
func newAudioInputBuffer(maxBytes uint64, maxChunks int) (*audioInputBuffer, error) {
	if maxBytes == 0 || maxChunks <= 0 {
		return nil, errInvalidAudioBufferLimits
	}
	return &audioInputBuffer{
		slots:    make([]bufferedAudio, maxChunks),
		maxBytes: maxBytes,
	}, nil
}

// offer 尝试接管音频，长度取 len(payload)。
// 新数据成功返回 true；完整重发返回 false,nil。
// 范围错误或容量不足时，全部状态保持不变。
// 非阻塞；调用期间不得并发修改 payload，返回后不持有原切片的底层数组。
func (b *audioInputBuffer) offer(offset uint64, payload []byte) (bool, error) {
	size := uint64(len(payload))

	kind, err := b.input.classifyAudio(offset, size)
	if err != nil {
		return false, err
	}

	switch kind {
	case audioChunkDuplicate:
		// 历史完整重发不需要新增存储。
		// 即使当前缓冲区已经满，也仍然成功识别为重复。
		return false, nil

	case audioChunkNew:
		// 继续执行容量检查。

	default:
		// classifyAudio 的契约保证 nil error 时只可能返回
		// audioChunkNew 或 audioChunkDuplicate。
		panic("gateway: invalid audio chunk classification")
	}

	// count 包含正在发送的队首，因此在途块仍占槽位预算。
	if b.count == len(b.slots) {
		return false, errAudioBufferFull
	}

	// retainedBytes 同样包含正在发送的块。
	// 使用减法比较避免 retainedBytes+size 的 uint64 溢出。
	if size > b.maxBytes-b.retainedBytes {
		return false, errAudioBufferFull
	}

	// 到这里才复制，避免错误范围、重复块或容量不足时发生分配。
	data := make([]byte, len(payload))
	copy(data, payload)

	// 正式提交输入位置。
	//
	// classifyAudio 与这里之间由同一个协调者串行执行，
	// 没有其他代码修改 input，因此刚才的新块必须仍然是新块。
	accepted, err := b.input.acceptAudio(offset, size)
	if err != nil {
		return false, err
	}
	if !accepted {
		panic("gateway: audio input changed between classifyAudio and acceptAudio")
	}

	tail := (b.head + b.count) % len(b.slots)

	b.slots[tail] = bufferedAudio{
		offset: offset,
		data:   data,
	}
	b.count++
	b.retainedBytes += size

	return true, nil
}

// take 借出唯一在途队首，不出队、不归还预算。
// 空队列或已有在途块时返回零值,false；返回的 data 只读。
func (b *audioInputBuffer) take() (bufferedAudio, bool) {
	if b.count == 0 || b.inFlight {
		return bufferedAudio{}, false
	}

	b.inFlight = true
	return b.slots[b.head], true
}

// complete 确认匹配的在途队首已成功发送，释放槽位与预算。
// 无在途块或 offset 不匹配时返回错误，不修改状态。
// offset 来自 take 结果；发送失败不调用，交由会话失败清理。
func (b *audioInputBuffer) complete(offset uint64) error {
	if !b.inFlight || b.count == 0 {
		return errAudioCompletionMismatch
	}

	current := b.slots[b.head]
	if current.offset != offset {
		return errAudioCompletionMismatch
	}

	b.retainedBytes -= uint64(len(current.data))

	// 去掉对底层音频数组的引用，使已经完成的块可以被回收。
	b.slots[b.head] = bufferedAudio{}

	b.head++
	if b.head == len(b.slots) {
		b.head = 0
	}

	b.count--
	b.inFlight = false

	return nil
}

// acceptEnd 沿用输入状态的终点规则，不占槽位、不直接关闭 Worker 输入。
func (b *audioInputBuffer) acceptEnd(finalOffset uint64) (bool, error) {
	return b.input.acceptEnd(finalOffset)
}

// inputDrained 表示已接纳 end，且所有音频都已 complete。
// 是状态条件，不是一次性事件；不表示已执行 CloseSend、Worker 已处理完成或整场结束。
func (b *audioInputBuffer) inputDrained() bool {
	return b.input.ended && b.count == 0
}
