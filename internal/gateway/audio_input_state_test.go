package gateway

import (
	"errors"
	"math"
	"testing"
)

// TestAudioInputStateRanges 检查每种范围的分类、错误身份及接纳后的完整状态。
// 极大范围只是元数据夹具，不分配音频，也不表示网络层允许这么大的消息。
func TestAudioInputStateRanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before audioInputState
		offset uint64
		size   uint64
		kind   audioChunkKind
		err    error
	}{
		{name: "initial_new", size: 3200, kind: audioChunkNew},
		{name: "first_byte", size: 1, kind: audioChunkNew},
		{name: "empty_initial", kind: audioChunkInvalid, err: errAudioEmptyChunk},
		{name: "empty_history", before: audioInputState{nextOffset: 6400}, offset: 3200, kind: audioChunkInvalid, err: errAudioEmptyChunk},
		{name: "exact_replay", before: audioInputState{nextOffset: 6400}, offset: 3200, size: 3200, kind: audioChunkDuplicate},
		{name: "subrange_replay", before: audioInputState{nextOffset: 6400}, offset: 100, size: 37, kind: audioChunkDuplicate},
		{name: "whole_prefix_replay", before: audioInputState{nextOffset: 6400}, size: 6400, kind: audioChunkDuplicate},
		{name: "new_after_prefix", before: audioInputState{nextOffset: 6400}, offset: 6400, size: 3200, kind: audioChunkNew},
		{name: "gap", before: audioInputState{nextOffset: 6400}, offset: 9600, size: 3200, kind: audioChunkInvalid, err: errAudioGap},
		{name: "one_byte_gap", before: audioInputState{nextOffset: 6400}, offset: 6401, size: 1, kind: audioChunkInvalid, err: errAudioGap},
		{name: "partial_overlap", before: audioInputState{nextOffset: 6400}, offset: 4800, size: 3200, kind: audioChunkInvalid, err: errAudioOverlap},
		{name: "one_byte_overlap", before: audioInputState{nextOffset: 6400}, offset: 6399, size: 2, kind: audioChunkInvalid, err: errAudioOverlap},
		{name: "prefix_plus_new_byte", before: audioInputState{nextOffset: 6400}, size: 6401, kind: audioChunkInvalid, err: errAudioOverlap},
		{name: "ended_replay", before: audioInputState{nextOffset: 6400, ended: true}, offset: 3200, size: 3200, kind: audioChunkDuplicate},
		{name: "ended_new", before: audioInputState{nextOffset: 6400, ended: true}, offset: 6400, size: 1, kind: audioChunkInvalid, err: errAudioInputEnded},
		{name: "ended_gap", before: audioInputState{nextOffset: 6400, ended: true}, offset: 6401, size: 1, kind: audioChunkInvalid, err: errAudioInputEnded},
		{name: "ended_overlap", before: audioInputState{nextOffset: 6400, ended: true}, offset: 6399, size: 2, kind: audioChunkInvalid, err: errAudioInputEnded},
		{name: "ended_empty_input_new_byte", before: audioInputState{ended: true}, size: 1, kind: audioChunkInvalid, err: errAudioInputEnded},
		{name: "empty_before_ended_check", before: audioInputState{nextOffset: math.MaxUint64, ended: true}, offset: math.MaxUint64, kind: audioChunkInvalid, err: errAudioEmptyChunk},
		{name: "overflow_initial", offset: math.MaxUint64, size: 1, kind: audioChunkInvalid, err: errAudioPositionOverflow},
		{name: "overflow_would_look_duplicate", before: audioInputState{nextOffset: 6400}, offset: math.MaxUint64, size: 2, kind: audioChunkInvalid, err: errAudioPositionOverflow},
		{name: "overflow_before_ended_check", before: audioInputState{nextOffset: math.MaxUint64, ended: true}, offset: math.MaxUint64, size: 1, kind: audioChunkInvalid, err: errAudioPositionOverflow},
		{name: "last_representable_byte", before: audioInputState{nextOffset: math.MaxUint64 - 1}, offset: math.MaxUint64 - 1, size: 1, kind: audioChunkNew},
		{name: "maximum_prefix_replay", before: audioInputState{nextOffset: math.MaxUint64, ended: true}, size: math.MaxUint64, kind: audioChunkDuplicate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.before
			kind, err := s.classifyAudio(tc.offset, tc.size)
			if kind != tc.kind || !errors.Is(err, tc.err) {
				t.Fatalf("classification = (%d, %v), want (%d, %v)", kind, err, tc.kind, tc.err)
			}
			if s != tc.before {
				t.Fatalf("classification mutated state: %+v, want %+v", s, tc.before)
			}

			accepted, err := s.acceptAudio(tc.offset, tc.size)
			wantAccepted := tc.kind == audioChunkNew
			if accepted != wantAccepted || !errors.Is(err, tc.err) {
				t.Fatalf("acceptance = (%v, %v), want (%v, %v)", accepted, err, wantAccepted, tc.err)
			}
			want := tc.before
			if wantAccepted {
				want.nextOffset = tc.offset + tc.size
			}
			if s != want {
				t.Fatalf("state = %+v, want %+v", s, want)
			}
		})
	}
}

func TestAudioInputStateEndPositions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		before   audioInputState
		final    uint64
		accepted bool
		err      error
	}{
		{name: "empty_input", accepted: true},
		{name: "empty_input_duplicate", before: audioInputState{ended: true}},
		{name: "empty_input_future", final: 1, err: errAudioEndMismatch},
		{name: "exact_end", before: audioInputState{nextOffset: 6400}, final: 6400, accepted: true},
		{name: "end_behind", before: audioInputState{nextOffset: 6400}, final: 6399, err: errAudioEndMismatch},
		{name: "end_ahead", before: audioInputState{nextOffset: 6400}, final: 6401, err: errAudioEndMismatch},
		{name: "duplicate_end", before: audioInputState{nextOffset: 6400, ended: true}, final: 6400},
		{name: "changed_end_behind", before: audioInputState{nextOffset: 6400, ended: true}, final: 6399, err: errAudioEndMismatch},
		{name: "changed_end_ahead", before: audioInputState{nextOffset: 6400, ended: true}, final: 6401, err: errAudioEndMismatch},
		{name: "maximum_end", before: audioInputState{nextOffset: math.MaxUint64}, final: math.MaxUint64, accepted: true},
		{name: "maximum_duplicate", before: audioInputState{nextOffset: math.MaxUint64, ended: true}, final: math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.before
			accepted, err := s.acceptEnd(tc.final)
			if accepted != tc.accepted || !errors.Is(err, tc.err) {
				t.Fatalf("end = (%v, %v), want (%v, %v)", accepted, err, tc.accepted, tc.err)
			}
			want := tc.before
			if tc.accepted {
				want.ended = true
			}
			if s != want {
				t.Fatalf("end changed position or violated state: %+v, want %+v", s, want)
			}
		})
	}
}

// TestAudioInputStateReplaySequence 使用同一状态经历接纳、失败、重放及重复结束。
// acceptedBytes 只统计元数据接纳结果，不模拟或声称 Worker 实际接收量。
func TestAudioInputStateReplaySequence(t *testing.T) {
	var s audioInputState
	var acceptedBytes uint64
	for _, chunk := range []struct{ offset, size uint64 }{{0, 3200}, {3200, 1}, {3201, 3199}} {
		accepted, err := s.acceptAudio(chunk.offset, chunk.size)
		if !accepted || err != nil {
			t.Fatalf("contiguous input %+v = (%v, %v)", chunk, accepted, err)
		}
		acceptedBytes += chunk.size
	}
	for _, chunk := range []struct{ offset, size uint64 }{{0, 6400}, {3000, 400}, {3201, 3199}} {
		if accepted, err := s.acceptAudio(chunk.offset, chunk.size); accepted || err != nil {
			t.Fatalf("replayed range %+v = (%v, %v)", chunk, accepted, err)
		}
	}
	if accepted, err := s.acceptAudio(6401, 1); accepted || !errors.Is(err, errAudioGap) {
		t.Fatalf("gap = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptEnd(9600); accepted || !errors.Is(err, errAudioEndMismatch) {
		t.Fatalf("premature end = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptAudio(6400, 3200); !accepted || err != nil {
		t.Fatalf("valid input after failures = (%v, %v)", accepted, err)
	}
	acceptedBytes += 3200
	for i := range 3 {
		if accepted, err := s.acceptEnd(9600); accepted != (i == 0) || err != nil {
			t.Fatalf("end attempt %d = (%v, %v)", i, accepted, err)
		}
	}
	if accepted, err := s.acceptAudio(0, 9600); accepted || err != nil {
		t.Fatalf("replay after end = (%v, %v)", accepted, err)
	}
	if s != (audioInputState{nextOffset: 9600, ended: true}) || acceptedBytes != 9600 {
		t.Fatalf("final state = %+v, acceptedBytes = %d", s, acceptedBytes)
	}
}

// TestAudioInputStateInspectionDoesNotCommit 模拟只完成检查、尚未接管音频的边界。
// 这里不提供缓存实现，仅验证未调用 acceptAudio 时不会推进接纳位置。
func TestAudioInputStateInspectionDoesNotCommit(t *testing.T) {
	var s audioInputState
	for range 2 {
		if kind, err := s.classifyAudio(0, 3200); kind != audioChunkNew || err != nil {
			t.Fatalf("inspection = (%d, %v)", kind, err)
		}
		if s != (audioInputState{}) {
			t.Fatalf("inspection committed input: %+v", s)
		}
	}
	if accepted, err := s.acceptEnd(3200); accepted || !errors.Is(err, errAudioEndMismatch) {
		t.Fatalf("unaccepted range counted by end = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptAudio(0, 3200); !accepted || err != nil {
		t.Fatalf("subsequent acceptance = (%v, %v)", accepted, err)
	}
	if s != (audioInputState{nextOffset: 3200}) {
		t.Fatalf("state = %+v", s)
	}
}

func TestAudioInputStateMaximumPositionSequence(t *testing.T) {
	var s audioInputState
	if accepted, err := s.acceptAudio(0, math.MaxUint64-1); !accepted || err != nil {
		t.Fatalf("large metadata range = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptAudio(math.MaxUint64-1, 2); accepted || !errors.Is(err, errAudioPositionOverflow) {
		t.Fatalf("overflow = (%v, %v)", accepted, err)
	}
	if s != (audioInputState{nextOffset: math.MaxUint64 - 1}) {
		t.Fatalf("overflow mutated state: %+v", s)
	}
	if accepted, err := s.acceptAudio(math.MaxUint64-1, 1); !accepted || err != nil {
		t.Fatalf("last valid byte = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptEnd(math.MaxUint64); !accepted || err != nil {
		t.Fatalf("maximum final offset = (%v, %v)", accepted, err)
	}
	if accepted, err := s.acceptAudio(math.MaxUint64-1, 1); accepted || err != nil {
		t.Fatalf("last byte replay = (%v, %v)", accepted, err)
	}
	if s != (audioInputState{nextOffset: math.MaxUint64, ended: true}) {
		t.Fatalf("maximum position was not preserved: %+v", s)
	}
}

func TestAudioInputStateInstancesAreIndependent(t *testing.T) {
	var a, b audioInputState
	if accepted, err := a.acceptAudio(0, 3200); !accepted || err != nil {
		t.Fatalf("first session audio = (%v, %v)", accepted, err)
	}
	if accepted, err := a.acceptEnd(3200); !accepted || err != nil {
		t.Fatalf("first session end = (%v, %v)", accepted, err)
	}
	if b != (audioInputState{}) {
		t.Fatalf("second session changed: %+v", b)
	}
	if accepted, err := b.acceptAudio(0, 1); !accepted || err != nil {
		t.Fatalf("second session input = (%v, %v)", accepted, err)
	}
	if a != (audioInputState{nextOffset: 3200, ended: true}) || b != (audioInputState{nextOffset: 1}) {
		t.Fatalf("session states interfere: a=%+v, b=%+v", a, b)
	}
}
