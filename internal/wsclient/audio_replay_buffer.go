package wsclient

import (
	"errors"
	"math"
)

var (
	errInvalidReplayBufferLimits = errors.New("invalid audio replay buffer limits")
	errReplayBufferFull          = errors.New("audio replay buffer full")
	errReplayBufferSealed        = errors.New("audio replay buffer sealed")
	errReplayAudioEmpty          = errors.New("empty replay audio chunk")
	errReplayOffsetOverflow      = errors.New("audio replay offset overflow")
	errAudioReplayGap            = errors.New("audio replay gap")
	errInvalidReplayOffset       = errors.New("invalid audio replay offset")
)

// replayAudioChunk 保存原始分块及其逻辑字节位置。
// 缓存内部和 copyChunkAt 返回的负载各自拥有独立数组。
type replayAudioChunk struct {
	offset uint64 // 本块在本场音频中的起点。
	data   []byte // 独立保存的非空音频负载。
}

// audioReplayBuffer 由客户端协调者串行访问。
// 必须通过构造器创建，使用后不复制；自身不保证并发安全。
type audioReplayBuffer struct {
	slots       []replayAudioChunk // 固定数量的环形槽位。
	head        int                // 最早未确认块的位置。
	count       int                // 当前保留的块数。
	maxBytes    uint64             // 缓存自身负载上限，不包含返回给写任务的副本。
	ackedOffset uint64             // 已确认位置，也是可重放起点。
	nextOffset  uint64             // 已缓存末端，下一块从这里开始。
	sealed      bool               // 本地输入已正常结束，禁止追加。
}

// newAudioReplayBuffer 创建空缓存，两项上限必须为正。
// 只预分配槽位，音频随 append 分配；非法配置返回 nil 和配置错误。
func newAudioReplayBuffer(
	maxBytes uint64,
	maxChunks int,
) (*audioReplayBuffer, error) {
	if maxBytes == 0 || maxChunks <= 0 {
		return nil, errInvalidReplayBufferLimits
	}
	return &audioReplayBuffer{
		slots:    make([]replayAudioChunk, maxChunks),
		maxBytes: maxBytes,
	}, nil
}

// append 复制新音频，返回本块起点，并推进 nextOffset。
// 调用期间 payload 不得被并发修改；返回后不持有其底层数组。
// nextOffset 仅表示已缓存的末端，不表示网络已经发送或对端已接纳。
// 空块、封口、溢出或预算不足时拒绝，失败不修改状态。
func (b *audioReplayBuffer) append(payload []byte) (uint64, error) {
	if b.sealed {
		return 0, errReplayBufferSealed
	}
	if len(payload) == 0 {
		return 0, errReplayAudioEmpty
	}
	size := uint64(len(payload))
	if size > math.MaxUint64-b.nextOffset {
		return 0, errReplayOffsetOverflow
	}
	if b.count == len(b.slots) || size > b.maxBytes-(b.nextOffset-b.ackedOffset) {
		return 0, errReplayBufferFull
	}

	// 所有检查通过后才复制和提交，负载长度即计入预算的实际分配长度。
	data := make([]byte, len(payload))
	copy(data, payload)
	offset := b.nextOffset
	b.slots[(b.head+b.count)%len(b.slots)] = replayAudioChunk{offset: offset, data: data}
	b.count++
	b.nextOffset += size
	return offset, nil
}

// checkReplayOffset 校验恢复位置，不修改状态。
// 早于 ackedOffset 表示缺口；超前或落在块中间表示非法。
// 等于 nextOffset 合法，表示没有待重放数据。
func (b *audioReplayBuffer) checkReplayOffset(offset uint64) error {
	_, err := b.chunkIndexAt(offset)
	return err
}

// copyChunkAt 返回指定起点的完整原块副本，不出队。
// 到达 nextOffset 时返回零值、false、nil。
// 返回的 data 归调用方；修改副本或 ACK 释放原块均不影响另一方。
// 后续运行器须限制待写/在途副本数量，副本不计入缓存自身的 maxBytes。
func (b *audioReplayBuffer) copyChunkAt(offset uint64) (replayAudioChunk, bool, error) {
	index, err := b.chunkIndexAt(offset)
	if err != nil {
		return replayAudioChunk{}, false, err
	}
	if index < 0 {
		return replayAudioChunk{}, false, nil
	}
	chunk := b.slots[index]
	data := make([]byte, len(chunk.data))
	copy(data, chunk.data)
	return replayAudioChunk{offset: chunk.offset, data: data}, true, nil
}

// acknowledge 提交累计确认，并释放完整的已确认前缀。
// 重复或旧确认返回 false、nil；错误不修改状态。
// 调用方须先验证身份、代次及发送授权上界；缓存只能检查本地位置范围。
// 恢复 ready 须先 checkReplayOffset，不能将缺口当作旧 ACK 忽略。
func (b *audioReplayBuffer) acknowledge(nextOffset uint64) (bool, error) {
	if nextOffset <= b.ackedOffset {
		return false, nil
	}
	// 完整检查目标，不能先释放前几块后才发现目标落在后续块中间。
	if err := b.checkReplayOffset(nextOffset); err != nil {
		return false, err
	}
	for b.count > 0 && b.slots[b.head].offset < nextOffset {
		b.slots[b.head] = replayAudioChunk{}
		b.head = (b.head + 1) % len(b.slots)
		b.count--
	}
	b.ackedOffset = nextOffset
	return true, nil
}

// seal 固定本地输入终点，允许重复调用。
// 封口后仍可确认和重放，不再允许追加。
// 不证明 end 已发送或被接纳；源异常不得被调用方当作正常 EOF 封口。
func (b *audioReplayBuffer) seal() uint64 {
	b.sealed = true
	return b.nextOffset
}

// chunkIndexAt 只查原块边界，不切分音频、不修改状态。
// 末端返回 -1,nil；已释放范围是缺口，超前和块内部是非法位置。
func (b *audioReplayBuffer) chunkIndexAt(offset uint64) (int, error) {
	if offset < b.ackedOffset {
		return -1, errAudioReplayGap
	}
	if offset > b.nextOffset {
		return -1, errInvalidReplayOffset
	}
	if offset == b.nextOffset {
		return -1, nil
	}
	for i := range b.count {
		index := (b.head + i) % len(b.slots)
		if b.slots[index].offset == offset {
			return index, nil
		}
	}
	return -1, errInvalidReplayOffset
}
