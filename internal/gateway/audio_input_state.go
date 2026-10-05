package gateway

import (
	"errors"
	"math"
)

var (
	// errAudioEmptyChunk 表示音频块负载长度为零。
	errAudioEmptyChunk = errors.New("audio chunk is empty")

	// errAudioPositionOverflow 表示 offset+size 会超出 uint64 范围。
	errAudioPositionOverflow = errors.New("audio position overflows uint64")

	// errAudioGap 表示新音频起点晚于当前连续接纳位置，中间存在缺口。
	errAudioGap = errors.New("audio chunk has a gap")

	// errAudioOverlap 表示音频范围一部分已接纳、一部分尚未接纳。
	errAudioOverlap = errors.New("audio chunk partially overlaps accepted audio")

	// errAudioInputEnded 表示输入已经结束，但请求仍包含尚未接纳的新音频。
	errAudioInputEnded = errors.New("audio input has ended")

	// errAudioEndMismatch 表示 end 声明的最终位置与当前连续接纳位置不一致。
	errAudioEndMismatch = errors.New("audio end offset does not match accepted position")
)

// audioChunkKind 表示音频范围相对连续接纳位置的关系。
type audioChunkKind uint8

const (
	audioChunkInvalid   audioChunkKind = iota // 校验失败，必须检查 error。
	audioChunkNew                             // 连续的新音频。
	audioChunkDuplicate                       // 整个范围均已接纳。
)

// audioInputState 管理逻辑会话的连续接纳位置和输入结束状态。
// 零值可用，由会话协调者串行访问，自身不保证并发安全。
// 只保存元数据，不保存音频，也不操作 Worker。
type audioInputState struct {
	nextOffset uint64 // [0,nextOffset) 已接纳，也是下一段新音频的起点。
	ended      bool   // 已接纳合法 end，此后 nextOffset 不再增加。
}

// classifyAudio 检查 [offset,offset+size) 是新数据还是完整重发。
// offset 是音频字节起点，size 是实际负载长度，必须为正。
// 错误返回 audioChunkInvalid；本方法不修改状态。
func (s *audioInputState) classifyAudio(offset, size uint64) (audioChunkKind, error) {
	if size == 0 {
		return audioChunkInvalid, errAudioEmptyChunk
	}

	if size > math.MaxUint64-offset {
		return audioChunkInvalid, errAudioPositionOverflow
	}

	endOffset := offset + size

	// 完整落在已经接纳的连续范围内，属于重发。
	// 这个判断必须在 ended 之前，因此输入结束后仍允许历史音频重发确认。
	if endOffset <= s.nextOffset {
		return audioChunkDuplicate, nil
	}

	// 到这里说明该范围至少包含一个尚未接纳的新字节。
	if s.ended {
		return audioChunkInvalid, errAudioInputEnded
	}

	if offset == s.nextOffset {
		return audioChunkNew, nil
	}

	if offset > s.nextOffset {
		return audioChunkInvalid, errAudioGap
	}

	// 前面的条件均不成立时，只可能是：
	//
	//	offset < nextOffset < endOffset
	//
	// 即部分范围已接纳，部分范围是新数据。
	return audioChunkInvalid, errAudioOverlap
}

// acceptAudio 重新校验范围并记录接纳结果。
// 新数据推进位置并返回 true；完整重发返回 false,nil。
// 错误不修改状态。真实接入时，调用方须先取得新音频的有界存储所有权。
func (s *audioInputState) acceptAudio(offset, size uint64) (bool, error) {
	kind, err := s.classifyAudio(offset, size)
	if err != nil {
		return false, err
	}

	switch kind {
	case audioChunkDuplicate:
		return false, nil

	case audioChunkNew:
		// classifyAudio 已经确认：
		//
		//	offset == nextOffset
		//	offset + size 不溢出
		//
		// 因此这里推进 nextOffset 是安全的。
		s.nextOffset += size
		return true, nil

	default:
		// 按当前 classifyAudio 契约，nil error 时只可能返回
		// audioChunkNew 或 audioChunkDuplicate。
		panic("gateway: invalid audio chunk classification")
	}
}

// acceptEnd 校验并记录最终音频位置。
// finalOffset 必须等于 nextOffset；首次返回 true,nil，重复返回 false,nil。
// 错误不修改状态；不直接半关闭 Worker，也不操作尾部计时器。
func (s *audioInputState) acceptEnd(finalOffset uint64) (bool, error) {
	if finalOffset != s.nextOffset {
		return false, errAudioEndMismatch
	}

	if s.ended {
		return false, nil
	}

	s.ended = true
	return true, nil
}
