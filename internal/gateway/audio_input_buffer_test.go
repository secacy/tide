package gateway

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
)

// newTestAudioInputBuffer 创建有效夹具，不启动任务或模拟实际 Worker Send。
func newTestAudioInputBuffer(t *testing.T, maxBytes uint64, maxChunks int) *audioInputBuffer {
	t.Helper()
	b, err := newAudioInputBuffer(maxBytes, maxChunks)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// audioBufferTestSnapshot 深拷贝状态和槽位内容，用于验证拒绝操作没有任何副作用。
type audioBufferTestSnapshot struct {
	input         audioInputState
	head, count   int
	retainedBytes uint64
	maxBytes      uint64
	inFlight      bool
	slots         []bufferedAudio
}

func snapshotTestAudioBuffer(b *audioInputBuffer) audioBufferTestSnapshot {
	s := audioBufferTestSnapshot{
		input: b.input, head: b.head, count: b.count,
		retainedBytes: b.retainedBytes, maxBytes: b.maxBytes, inFlight: b.inFlight,
		slots: make([]bufferedAudio, len(b.slots)),
	}
	for i, slot := range b.slots {
		s.slots[i] = bufferedAudio{offset: slot.offset, data: bytes.Clone(slot.data)}
	}
	return s
}

func assertTestAudioBufferUnchanged(t *testing.T, b *audioInputBuffer, before audioBufferTestSnapshot) {
	t.Helper()
	if after := snapshotTestAudioBuffer(b); !reflect.DeepEqual(after, before) {
		t.Fatalf("buffer changed: after=%+v, before=%+v", after, before)
	}
}

// assertTestAudioBufferInvariant 检查预算、连续队列、在途与未占槽位的引用。
// 测试同样遵守单一所有者，只在串行调用完成后读取字段。
func assertTestAudioBufferInvariant(t *testing.T, b *audioInputBuffer) {
	t.Helper()
	if len(b.slots) == 0 || b.head < 0 || b.head >= len(b.slots) || b.count < 0 || b.count > len(b.slots) {
		t.Fatalf("invalid ring: head=%d count=%d slots=%d", b.head, b.count, len(b.slots))
	}
	if b.retainedBytes > b.maxBytes || (b.inFlight && b.count == 0) {
		t.Fatalf("invalid budget/in-flight: bytes=%d max=%d count=%d inFlight=%v", b.retainedBytes, b.maxBytes, b.count, b.inFlight)
	}
	occupied := make([]bool, len(b.slots))
	var total, end uint64
	for i := range b.count {
		index := (b.head + i) % len(b.slots)
		occupied[index] = true
		chunk := b.slots[index]
		if len(chunk.data) == 0 || (i > 0 && chunk.offset != end) {
			t.Fatalf("invalid contiguous slot %d: %+v, previous end=%d", index, chunk, end)
		}
		end = chunk.offset + uint64(len(chunk.data))
		total += uint64(len(chunk.data))
	}
	if total != b.retainedBytes || (b.count > 0 && end != b.input.nextOffset) {
		t.Fatalf("accounting mismatch: total=%d retained=%d end=%d next=%d", total, b.retainedBytes, end, b.input.nextOffset)
	}
	for i, slot := range b.slots {
		if !occupied[i] && (slot.offset != 0 || slot.data != nil) {
			t.Fatalf("unused slot %d retains audio: %+v", i, slot)
		}
	}
}

func TestAudioInputBufferConstruction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bytes  uint64
		chunks int
		valid  bool
	}{
		{"zero_bytes", 0, 1, false},
		{"zero_chunks", 1, 0, false},
		{"negative_chunks", 1, -1, false},
		{"minimum_int_chunks", 1, math.MinInt, false},
		{"both_zero", 0, 0, false},
		{"smallest_limits", 1, 1, true},
		{"maximum_byte_budget_without_payload_allocation", math.MaxUint64, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := newAudioInputBuffer(tc.bytes, tc.chunks)
			if !tc.valid {
				if b != nil || !errors.Is(err, errInvalidAudioBufferLimits) {
					t.Fatalf("construction = (%v, %v)", b, err)
				}
				return
			}
			if err != nil || b == nil {
				t.Fatalf("valid construction = (%v, %v)", b, err)
			}
			if len(b.slots) != tc.chunks || b.maxBytes != tc.bytes || b.input != (audioInputState{}) || b.count != 0 || b.retainedBytes != 0 || b.inFlight || b.inputDrained() {
				t.Fatal("constructor did not create an empty buffer with requested limits")
			}
			assertTestAudioBufferInvariant(t, b)
		})
	}
}

func TestAudioInputBufferOwnsPayloadCopy(t *testing.T) {
	b := newTestAudioInputBuffer(t, 8, 2)
	original := bytes.Repeat([]byte{9}, 64)
	copy(original[8:12], []byte("tide"))
	if accepted, err := b.offer(0, original[8:12]); !accepted || err != nil {
		t.Fatalf("offer = (%v, %v)", accepted, err)
	}
	for i := range original {
		original[i] = 0 // 调用结束后可修改原数组，包括 payload 的全部字节。
	}
	chunk, ok := b.take()
	if !ok || chunk.offset != 0 || !bytes.Equal(chunk.data, []byte("tide")) {
		t.Fatalf("accepted data aliases caller storage: chunk=%+v ok=%v", chunk, ok)
	}
	if b.retainedBytes != 4 || b.input.nextOffset != 4 {
		t.Fatal("retained/accepted size must use payload length, not source capacity")
	}
	assertTestAudioBufferInvariant(t, b)
}

func TestAudioInputBufferOfferRejectionAndReplay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ended   bool
		offset  uint64
		payload []byte
		err     error
	}{
		{name: "nil_payload", err: errAudioEmptyChunk},
		{name: "empty_payload", payload: []byte{}, err: errAudioEmptyChunk},
		{name: "new_when_full", offset: 6, payload: []byte("g"), err: errAudioBufferFull},
		{name: "gap_before_capacity_check", offset: 7, payload: []byte("h"), err: errAudioGap},
		{name: "overlap_before_capacity_check", offset: 5, payload: []byte("fg"), err: errAudioOverlap},
		{name: "position_overflow", offset: math.MaxUint64, payload: []byte("g"), err: errAudioPositionOverflow},
		{name: "full_prefix_replay_when_full", payload: []byte("abcdef")},
		{name: "subrange_replay_when_full", offset: 1, payload: []byte("bc")},
		{name: "replay_crossing_original_chunk_boundary", offset: 2, payload: []byte("cd")},
		{name: "ended_new", ended: true, offset: 6, payload: []byte("g"), err: errAudioInputEnded},
		{name: "ended_overlap", ended: true, offset: 5, payload: []byte("fg"), err: errAudioInputEnded},
		{name: "ended_replay_when_full", ended: true, payload: []byte("abcdef")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioInputBuffer(t, 6, 2)
			for i, data := range [][]byte{[]byte("abc"), []byte("def")} {
				if accepted, err := b.offer(uint64(i*3), data); !accepted || err != nil {
					t.Fatalf("initial offer = (%v, %v)", accepted, err)
				}
			}
			if tc.ended {
				if accepted, err := b.acceptEnd(6); !accepted || err != nil {
					t.Fatalf("initial end = (%v, %v)", accepted, err)
				}
			}
			before := snapshotTestAudioBuffer(b)
			if accepted, err := b.offer(tc.offset, tc.payload); accepted || !errors.Is(err, tc.err) {
				t.Fatalf("offer = (%v, %v), want (false, %v)", accepted, err, tc.err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			assertTestAudioBufferInvariant(t, b)
		})
	}
}

func TestAudioInputBufferSeparateLimitsAndEquality(t *testing.T) {
	for _, tc := range []struct {
		name      string
		maxBytes  uint64
		maxChunks int
		first     string
		second    string
	}{
		{"byte_limit_with_spare_slots", 4, 3, "abc", "d"},
		{"slot_limit_with_spare_bytes", 100, 2, "a", "b"},
		{"both_limits", 4, 2, "ab", "cd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioInputBuffer(t, tc.maxBytes, tc.maxChunks)
			for _, data := range []string{tc.first, tc.second} {
				if accepted, err := b.offer(b.input.nextOffset, []byte(data)); !accepted || err != nil {
					t.Fatalf("budget equality should be allowed: (%v, %v)", accepted, err)
				}
			}
			before := snapshotTestAudioBuffer(b)
			if accepted, err := b.offer(b.input.nextOffset, []byte("x")); accepted || !errors.Is(err, errAudioBufferFull) {
				t.Fatalf("limit exceeded = (%v, %v)", accepted, err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			assertTestAudioBufferInvariant(t, b)
		})
	}
}

func TestAudioInputBufferInFlightKeepsBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		maxBytes  uint64
		maxChunks int
	}{
		{"byte_budget", 3, 2},
		{"slot_budget", 10, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioInputBuffer(t, tc.maxBytes, tc.maxChunks)
			if accepted, err := b.offer(0, []byte("abc")); !accepted || err != nil {
				t.Fatalf("offer = (%v, %v)", accepted, err)
			}
			chunk, ok := b.take()
			if !ok || chunk.offset != 0 || !bytes.Equal(chunk.data, []byte("abc")) || b.count != 1 || b.retainedBytes != 3 {
				t.Fatal("take released the in-flight budget or returned incorrect data")
			}
			before := snapshotTestAudioBuffer(b)
			if chunk, ok := b.take(); ok || chunk.offset != 0 || chunk.data != nil {
				t.Fatal("second take must not borrow another block")
			}
			if accepted, err := b.offer(3, []byte("d")); accepted || !errors.Is(err, errAudioBufferFull) {
				t.Fatalf("in-flight budget was bypassed: (%v, %v)", accepted, err)
			}
			if accepted, err := b.offer(0, []byte("abc")); accepted || err != nil {
				t.Fatalf("in-flight replay = (%v, %v)", accepted, err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			if err := b.complete(0); err != nil {
				t.Fatal(err)
			}
			if b.retainedBytes != 0 || b.count != 0 || b.input.nextOffset != 3 {
				t.Fatal("completion must free storage without rewinding acceptance")
			}
			if accepted, err := b.offer(3, []byte("d")); !accepted || err != nil {
				t.Fatalf("budget not reusable after completion: (%v, %v)", accepted, err)
			}
			assertTestAudioBufferInvariant(t, b)
		})
	}
}

func TestAudioInputBufferCompletionRejectsStaleEvents(t *testing.T) {
	b := newTestAudioInputBuffer(t, 6, 2)
	before := snapshotTestAudioBuffer(b)
	if err := b.complete(0); !errors.Is(err, errAudioCompletionMismatch) {
		t.Fatalf("empty completion = %v", err)
	}
	assertTestAudioBufferUnchanged(t, b, before)
	if chunk, ok := b.take(); ok || chunk.data != nil || chunk.offset != 0 {
		t.Fatal("empty take must return zero value")
	}
	for i, data := range []string{"abc", "def"} {
		if accepted, err := b.offer(uint64(i*3), []byte(data)); !accepted || err != nil {
			t.Fatalf("offer = (%v, %v)", accepted, err)
		}
	}
	before = snapshotTestAudioBuffer(b)
	if err := b.complete(0); !errors.Is(err, errAudioCompletionMismatch) {
		t.Fatalf("completion without take = %v", err)
	}
	assertTestAudioBufferUnchanged(t, b, before)
	if _, ok := b.take(); !ok {
		t.Fatal("first take failed")
	}
	before = snapshotTestAudioBuffer(b)
	if err := b.complete(3); !errors.Is(err, errAudioCompletionMismatch) {
		t.Fatalf("wrong in-flight offset = %v", err)
	}
	assertTestAudioBufferUnchanged(t, b, before)
	if err := b.complete(0); err != nil {
		t.Fatal(err)
	}
	before = snapshotTestAudioBuffer(b)
	if err := b.complete(0); !errors.Is(err, errAudioCompletionMismatch) {
		t.Fatalf("duplicate completion = %v", err)
	}
	assertTestAudioBufferUnchanged(t, b, before)
	chunk, ok := b.take()
	if !ok || chunk.offset != 3 || !bytes.Equal(chunk.data, []byte("def")) {
		t.Fatal("incorrect next head")
	}
	before = snapshotTestAudioBuffer(b)
	if err := b.complete(0); !errors.Is(err, errAudioCompletionMismatch) {
		t.Fatalf("old completion must not release new head: %v", err)
	}
	assertTestAudioBufferUnchanged(t, b, before)
	if err := b.complete(3); err != nil {
		t.Fatal(err)
	}
	assertTestAudioBufferInvariant(t, b)
}

func TestAudioInputBufferFIFOAndRingReuse(t *testing.T) {
	for _, maxChunks := range []int{1, 3} {
		name := "one_slot"
		if maxChunks == 3 {
			name = "three_slots"
		}
		t.Run(name, func(t *testing.T) {
			b := newTestAudioInputBuffer(t, 9, maxChunks)
			var expected []bufferedAudio
			var next uint64
			serial := 0
			offerNext := func() {
				data := bytes.Repeat([]byte{byte(serial + 1)}, serial%3+1)
				if accepted, err := b.offer(next, data); !accepted || err != nil {
					t.Fatalf("ring offer = (%v, %v)", accepted, err)
				}
				expected = append(expected, bufferedAudio{offset: next, data: data})
				next += uint64(len(data))
				serial++
				assertTestAudioBufferInvariant(t, b)
			}
			completeNext := func() {
				chunk, ok := b.take()
				want := expected[0]
				if !ok || chunk.offset != want.offset || !bytes.Equal(chunk.data, want.data) {
					t.Fatalf("FIFO take = (%+v, %v), want %+v", chunk, ok, want)
				}
				oldHead := b.head
				if err := b.complete(chunk.offset); err != nil {
					t.Fatal(err)
				}
				if b.slots[oldHead].data != nil || b.slots[oldHead].offset != 0 {
					t.Fatal("completed slot still holds audio reference")
				}
				expected = expected[1:]
				assertTestAudioBufferInvariant(t, b)
			}
			for range maxChunks {
				offerNext()
			}
			for range 24 {
				completeNext()
				offerNext()
			}
			if accepted, err := b.acceptEnd(next); !accepted || err != nil || b.inputDrained() {
				t.Fatalf("end with pending data = (%v, %v)", accepted, err)
			}
			for len(expected) > 0 {
				completeNext()
			}
			if !b.inputDrained() || b.input.nextOffset != next || len(b.slots) != maxChunks || cap(b.slots) != maxChunks {
				t.Fatal("ring storage grew or final state was lost")
			}
		})
	}
}

func TestAudioInputBufferEndAndDrain(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "empty_input"
		if queued {
			name = "full_buffer"
		}
		t.Run(name, func(t *testing.T) {
			b := newTestAudioInputBuffer(t, 3, 1)
			if queued {
				if accepted, err := b.offer(0, []byte("abc")); !accepted || err != nil {
					t.Fatalf("offer = (%v, %v)", accepted, err)
				}
			}
			if b.inputDrained() {
				t.Fatal("empty/pending input without end is not drained")
			}
			before := snapshotTestAudioBuffer(b)
			if accepted, err := b.acceptEnd(b.input.nextOffset + 1); accepted || !errors.Is(err, errAudioEndMismatch) {
				t.Fatalf("wrong end = (%v, %v)", accepted, err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			if accepted, err := b.acceptEnd(b.input.nextOffset); !accepted || err != nil {
				t.Fatalf("valid end = (%v, %v)", accepted, err)
			}
			before.input.ended = true
			assertTestAudioBufferUnchanged(t, b, before) // end 不消耗槽位或字节。
			if accepted, err := b.acceptEnd(b.input.nextOffset); accepted || err != nil {
				t.Fatalf("duplicate end = (%v, %v)", accepted, err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			if accepted, err := b.offer(b.input.nextOffset, []byte("x")); accepted || !errors.Is(err, errAudioInputEnded) {
				t.Fatalf("new audio after end = (%v, %v)", accepted, err)
			}
			assertTestAudioBufferUnchanged(t, b, before)
			if queued {
				if b.inputDrained() {
					t.Fatal("pending data must prevent drain")
				}
				chunk, ok := b.take()
				if !ok || b.inputDrained() {
					t.Fatal("in-flight data must prevent drain")
				}
				if err := b.complete(chunk.offset); err != nil {
					t.Fatal(err)
				}
				before = snapshotTestAudioBuffer(b)
				if accepted, err := b.offer(0, []byte("abc")); accepted || err != nil {
					t.Fatalf("sent audio replay after end = (%v, %v)", accepted, err)
				}
				assertTestAudioBufferUnchanged(t, b, before)
			}
			if !b.inputDrained() || !b.inputDrained() {
				t.Fatal("drain should remain true without consuming an event")
			}
			assertTestAudioBufferInvariant(t, b)
		})
	}
}

func TestAudioInputBufferInstancesAreIndependent(t *testing.T) {
	a := newTestAudioInputBuffer(t, 3, 1)
	b := newTestAudioInputBuffer(t, 3, 1)
	if accepted, err := a.offer(0, []byte("abc")); !accepted || err != nil {
		t.Fatalf("first offer = (%v, %v)", accepted, err)
	}
	if accepted, err := a.acceptEnd(3); !accepted || err != nil {
		t.Fatalf("first end = (%v, %v)", accepted, err)
	}
	if chunk, ok := b.take(); ok || chunk.data != nil || b.input != (audioInputState{}) || b.retainedBytes != 0 {
		t.Fatal("second instance shares first instance's state")
	}
	if accepted, err := b.offer(0, []byte("def")); !accepted || err != nil {
		t.Fatalf("second offer = (%v, %v)", accepted, err)
	}
	aChunk, aOK := a.take()
	bChunk, bOK := b.take()
	if !aOK || !bOK || !bytes.Equal(aChunk.data, []byte("abc")) || !bytes.Equal(bChunk.data, []byte("def")) {
		t.Fatal("instances share payload or lease state")
	}
	if err := a.complete(0); err != nil {
		t.Fatal(err)
	}
	if !a.inputDrained() || b.inputDrained() || !b.inFlight || b.count != 1 || b.retainedBytes != 3 {
		t.Fatal("completing one instance affected the other")
	}
	assertTestAudioBufferInvariant(t, a)
	assertTestAudioBufferInvariant(t, b)
}
