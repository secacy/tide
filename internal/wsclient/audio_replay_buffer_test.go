package wsclient

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
)

func newTestAudioReplayBuffer(t *testing.T, maxBytes uint64, maxChunks int) *audioReplayBuffer {
	t.Helper()
	b, err := newAudioReplayBuffer(maxBytes, maxChunks)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// snapshotAudioReplay 深拷贝全部状态，用来检查拒绝和只读操作的原子性。
func snapshotAudioReplay(b *audioReplayBuffer) audioReplayBuffer {
	s := *b
	s.slots = make([]replayAudioChunk, len(b.slots))
	for i, chunk := range b.slots {
		s.slots[i] = replayAudioChunk{offset: chunk.offset, data: bytes.Clone(chunk.data)}
	}
	return s
}

func assertAudioReplayUnchanged(t *testing.T, b *audioReplayBuffer, before audioReplayBuffer) {
	t.Helper()
	if after := snapshotAudioReplay(b); !reflect.DeepEqual(after, before) {
		t.Fatalf("state changed: before=%+v after=%+v", before, after)
	}
}

// assertAudioReplayInvariant 检查连续覆盖、双预算及空槽位不保留引用。
// 仅由单一测试所有者串行调用，不暗示部件支持并发访问。
func assertAudioReplayInvariant(t *testing.T, b *audioReplayBuffer) {
	t.Helper()
	if len(b.slots) == 0 || b.head < 0 || b.head >= len(b.slots) || b.count < 0 || b.count > len(b.slots) {
		t.Fatalf("invalid ring: head=%d count=%d slots=%d", b.head, b.count, len(b.slots))
	}
	if b.ackedOffset > b.nextOffset || b.nextOffset-b.ackedOffset > b.maxBytes {
		t.Fatalf("invalid range/budget: acked=%d next=%d max=%d", b.ackedOffset, b.nextOffset, b.maxBytes)
	}
	occupied := make([]bool, len(b.slots))
	end := b.ackedOffset
	for i := range b.count {
		index := (b.head + i) % len(b.slots)
		occupied[index] = true
		chunk := b.slots[index]
		if chunk.offset != end || len(chunk.data) == 0 || uint64(len(chunk.data)) > math.MaxUint64-end {
			t.Fatalf("invalid contiguous chunk at slot %d: %+v, expected start=%d", index, chunk, end)
		}
		end += uint64(len(chunk.data))
	}
	if end != b.nextOffset {
		t.Fatalf("retained chunks end at %d, want %d", end, b.nextOffset)
	}
	for i, chunk := range b.slots {
		if !occupied[i] && (chunk.offset != 0 || chunk.data != nil) {
			t.Fatalf("unused slot %d retains payload: %+v", i, chunk)
		}
	}
}

func appendTestReplayAudio(t *testing.T, b *audioReplayBuffer, wantOffset uint64, payload string) {
	t.Helper()
	if offset, err := b.append([]byte(payload)); err != nil || offset != wantOffset {
		t.Fatalf("append %q = (%d, %v), want offset=%d", payload, offset, err, wantOffset)
	}
	assertAudioReplayInvariant(t, b)
}

func assertReplayChunk(t *testing.T, b *audioReplayBuffer, offset uint64, want string) replayAudioChunk {
	t.Helper()
	before := snapshotAudioReplay(b)
	chunk, ok, err := b.copyChunkAt(offset)
	if err != nil || !ok || chunk.offset != offset || string(chunk.data) != want {
		t.Fatalf("copy at %d = (%+v, %v, %v), want %q", offset, chunk, ok, err, want)
	}
	assertAudioReplayUnchanged(t, b, before)
	return chunk
}

func TestAudioReplayConstruction(t *testing.T) {
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
		{"max_bytes_without_payload_allocation", math.MaxUint64, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := newAudioReplayBuffer(tc.bytes, tc.chunks)
			if !tc.valid {
				if b != nil || !errors.Is(err, errInvalidReplayBufferLimits) {
					t.Fatalf("constructor = (%v, %v)", b, err)
				}
				return
			}
			if err != nil || b == nil {
				t.Fatalf("valid constructor = (%v, %v)", b, err)
			}
			if len(b.slots) != tc.chunks || b.maxBytes != tc.bytes || b.head != 0 || b.count != 0 || b.ackedOffset != 0 || b.nextOffset != 0 || b.sealed {
				t.Fatalf("unexpected initial state: %+v", b)
			}
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplayPayloadOwnership(t *testing.T) {
	b := newTestAudioReplayBuffer(t, 4, 1)
	source := bytes.Repeat([]byte{9}, 128)
	copy(source[10:14], "tide")
	if offset, err := b.append(source[10:14]); offset != 0 || err != nil {
		t.Fatalf("append = (%d, %v)", offset, err)
	}
	clear(source) // 源可复用，预算和副本均只使用 payload 长度。
	first := assertReplayChunk(t, b, 0, "tide")
	first.data[0] = 'x'
	second := assertReplayChunk(t, b, 0, "tide")
	if advanced, err := b.acknowledge(4); !advanced || err != nil {
		t.Fatalf("acknowledge = (%v, %v)", advanced, err)
	}
	assertAudioReplayInvariant(t, b)
	appendTestReplayAudio(t, b, 4, "new!") // 同一槽位复用，不应改变任何已返回副本。
	if string(first.data) != "xide" || string(second.data) != "tide" {
		t.Fatalf("copies changed after release/reuse: first=%q second=%q", first.data, second.data)
	}
	clear(second.data)
	assertReplayChunk(t, b, 4, "new!")
}

func TestAudioReplayAppendRejectionIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		sealed  bool
		wantErr error
	}{
		{"nil", nil, false, errReplayAudioEmpty},
		{"empty", []byte{}, false, errReplayAudioEmpty},
		{"too_large_for_remaining_bytes", []byte("12345"), false, errReplayBufferFull},
		{"sealed", []byte("a"), true, errReplayBufferSealed},
		{"sealed_empty", nil, true, errReplayBufferSealed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, 8, 3)
			appendTestReplayAudio(t, b, 0, "tide")
			if tc.sealed {
				b.seal()
			}
			before := snapshotAudioReplay(b)
			if offset, err := b.append(tc.payload); offset != 0 || !errors.Is(err, tc.wantErr) {
				t.Fatalf("rejected append = (%d, %v), want %v", offset, err, tc.wantErr)
			}
			assertAudioReplayUnchanged(t, b, before)
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplayIndependentBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bytes  uint64
		chunks int
	}{
		{"byte_limit_with_spare_slots", 6, 4},
		{"chunk_limit_with_spare_bytes", 100, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, tc.bytes, tc.chunks)
			appendTestReplayAudio(t, b, 0, "abc")
			appendTestReplayAudio(t, b, 3, "def")
			before := snapshotAudioReplay(b)
			if offset, err := b.append([]byte("g")); offset != 0 || !errors.Is(err, errReplayBufferFull) {
				t.Fatalf("append past limit = (%d, %v)", offset, err)
			}
			assertAudioReplayUnchanged(t, b, before)
			assertReplayChunk(t, b, 0, "abc")
			assertReplayChunk(t, b, 3, "def")
			// Write/copy 不释放容量；只有有效接纳确认才能腾出空间。
			if advanced, err := b.acknowledge(0); advanced || err != nil {
				t.Fatalf("unchanged ack = (%v, %v)", advanced, err)
			}
			if _, err := b.append([]byte("g")); !errors.Is(err, errReplayBufferFull) {
				t.Fatalf("copy/stale ack freed budget: %v", err)
			}
			if advanced, err := b.acknowledge(3); !advanced || err != nil {
				t.Fatalf("prefix ack = (%v, %v)", advanced, err)
			}
			appendTestReplayAudio(t, b, 6, "ghi")
			assertReplayChunk(t, b, 3, "def")
			assertReplayChunk(t, b, 6, "ghi")
		})
	}
}

func TestAudioReplayPositions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset uint64
		want   string
		err    error
	}{
		{"released_start", 0, "", errAudioReplayGap},
		{"released_interior", 1, "", errAudioReplayGap},
		{"retained_start", 3, "def", nil},
		{"first_retained_interior", 4, "", errInvalidReplayOffset},
		{"second_retained_start", 6, "ghij", nil},
		{"last_retained_interior", 9, "", errInvalidReplayOffset},
		{"end", 10, "", nil},
		{"ahead", 11, "", errInvalidReplayOffset},
		{"max_position", math.MaxUint64, "", errInvalidReplayOffset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, 10, 3)
			appendTestReplayAudio(t, b, 0, "abc")
			appendTestReplayAudio(t, b, 3, "def")
			appendTestReplayAudio(t, b, 6, "ghij")
			if advanced, err := b.acknowledge(3); !advanced || err != nil {
				t.Fatalf("initial ack = (%v, %v)", advanced, err)
			}
			before := snapshotAudioReplay(b)
			if err := b.checkReplayOffset(tc.offset); !errors.Is(err, tc.err) {
				t.Fatalf("check %d = %v, want %v", tc.offset, err, tc.err)
			}
			chunk, ok, err := b.copyChunkAt(tc.offset)
			if !errors.Is(err, tc.err) || ok != (tc.want != "") {
				t.Fatalf("copy = (%+v, %v, %v), want payload=%q err=%v", chunk, ok, err, tc.want, tc.err)
			}
			if ok {
				if chunk.offset != tc.offset || string(chunk.data) != tc.want {
					t.Fatalf("wrong original block: %+v", chunk)
				}
			} else if chunk.offset != 0 || chunk.data != nil {
				t.Fatalf("no chunk must return zero value: %+v", chunk)
			}
			assertAudioReplayUnchanged(t, b, before)
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplayAcknowledge(t *testing.T) {
	for _, tc := range []struct {
		name     string
		offset   uint64
		advanced bool
		wantErr  error
	}{
		{"old_zero", 0, false, nil},
		{"old_interior_is_noop", 1, false, nil},
		{"duplicate", 3, false, nil},
		{"middle_of_first_retained_block", 4, false, errInvalidReplayOffset},
		{"middle_of_later_block_without_partial_release", 8, false, errInvalidReplayOffset},
		{"ahead_without_partial_release", 11, false, errInvalidReplayOffset},
		{"prefix", 6, true, nil},
		{"all_retained_blocks", 10, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, 10, 3)
			appendTestReplayAudio(t, b, 0, "abc")
			appendTestReplayAudio(t, b, 3, "def")
			appendTestReplayAudio(t, b, 6, "ghij")
			if advanced, err := b.acknowledge(3); !advanced || err != nil {
				t.Fatalf("initial ack = (%v, %v)", advanced, err)
			}
			before := snapshotAudioReplay(b)
			if advanced, err := b.acknowledge(tc.offset); advanced != tc.advanced || !errors.Is(err, tc.wantErr) {
				t.Fatalf("ack %d = (%v, %v), want (%v, %v)", tc.offset, advanced, err, tc.advanced, tc.wantErr)
			}
			if !tc.advanced {
				assertAudioReplayUnchanged(t, b, before)
			} else {
				if b.ackedOffset != tc.offset || b.nextOffset != 10 {
					t.Fatalf("ack changed wrong positions: %+v", b)
				}
				if tc.offset == 6 {
					if b.count != 1 {
						t.Fatalf("retained count=%d, want 1", b.count)
					}
					assertReplayChunk(t, b, 6, "ghij")
				} else if b.count != 0 {
					t.Fatalf("all acked but count=%d", b.count)
				}
			}
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplayLostAckRecovery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ready uint64
	}{
		{"lost_second_ack", 8},
		{"lost_all_remaining_acks", 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, 12, 3)
			appendTestReplayAudio(t, b, 0, "abcd")
			appendTestReplayAudio(t, b, 4, "efgh")
			appendTestReplayAudio(t, b, 8, "ijkl")
			if advanced, err := b.acknowledge(4); !advanced || err != nil {
				t.Fatalf("known ack = (%v, %v)", advanced, err)
			}
			// 模拟未来协调者已经验证身份、代次和 offeredOffset 的 ready 位置。
			// 这里只证明本地校准范围，不模拟网络或承诺自动恢复。
			if err := b.checkReplayOffset(tc.ready); err != nil {
				t.Fatal(err)
			}
			if advanced, err := b.acknowledge(tc.ready); !advanced || err != nil {
				t.Fatalf("ready ack = (%v, %v)", advanced, err)
			}
			if tc.ready == 8 {
				if b.count != 1 || b.nextOffset-b.ackedOffset != 4 {
					t.Fatalf("wrong retained range after ready: %+v", b)
				}
				assertReplayChunk(t, b, 8, "ijkl")
			} else {
				if chunk, ok, err := b.copyChunkAt(12); err != nil || ok || chunk.data != nil || b.count != 0 {
					t.Fatalf("ready at end = (%+v, %v, %v), count=%d", chunk, ok, err, b.count)
				}
			}
			// 普通旧 ACK 可忽略；同一个位置作为恢复起点必须明确缺口。
			before := snapshotAudioReplay(b)
			if advanced, err := b.acknowledge(4); advanced || err != nil {
				t.Fatalf("late old ack = (%v, %v)", advanced, err)
			}
			if err := b.checkReplayOffset(4); !errors.Is(err, errAudioReplayGap) {
				t.Fatalf("released recovery range = %v", err)
			}
			assertAudioReplayUnchanged(t, b, before)
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplaySeal(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"empty_input", ""},
		{"unconfirmed_input", "tide"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestAudioReplayBuffer(t, 4, 1)
			if tc.data != "" {
				appendTestReplayAudio(t, b, 0, tc.data)
			}
			final := uint64(len(tc.data))
			if end := b.seal(); end != final || !b.sealed {
				t.Fatalf("seal = %d, state=%+v", end, b)
			}
			before := snapshotAudioReplay(b)
			if end := b.seal(); end != final {
				t.Fatalf("repeat seal = %d", end)
			}
			if _, err := b.append([]byte("x")); !errors.Is(err, errReplayBufferSealed) {
				t.Fatalf("sealed append = %v", err)
			}
			assertAudioReplayUnchanged(t, b, before)
			if tc.data != "" {
				assertReplayChunk(t, b, 0, tc.data)
			}
			if advanced, err := b.acknowledge(final); advanced != (final != 0) || err != nil {
				t.Fatalf("sealed ack = (%v, %v)", advanced, err)
			}
			if chunk, ok, err := b.copyChunkAt(final); err != nil || ok || chunk.offset != 0 || chunk.data != nil {
				t.Fatalf("empty sealed replay = (%+v, %v, %v)", chunk, ok, err)
			}
			if b.seal() != final || !b.sealed {
				t.Fatal("ack changed sealed final offset")
			}
			assertAudioReplayInvariant(t, b)
		})
	}
}

func TestAudioReplayMaximumOffset(t *testing.T) {
	b := newTestAudioReplayBuffer(t, math.MaxUint64, 2)
	// 构造接近上限的合法空缓存，避免为累计位置分配天文数量的音频。
	b.ackedOffset, b.nextOffset = math.MaxUint64-3, math.MaxUint64-3
	before := snapshotAudioReplay(b)
	if offset, err := b.append([]byte("four")); offset != 0 || !errors.Is(err, errReplayOffsetOverflow) {
		t.Fatalf("overflow = (%d, %v)", offset, err)
	}
	assertAudioReplayUnchanged(t, b, before)
	appendTestReplayAudio(t, b, math.MaxUint64-3, "end")
	assertReplayChunk(t, b, math.MaxUint64-3, "end")
	if b.nextOffset != math.MaxUint64 {
		t.Fatalf("exact fit end=%d", b.nextOffset)
	}
	if advanced, err := b.acknowledge(math.MaxUint64); !advanced || err != nil {
		t.Fatalf("maximum ack = (%v, %v)", advanced, err)
	}
	before = snapshotAudioReplay(b)
	if offset, err := b.append([]byte("x")); offset != 0 || !errors.Is(err, errReplayOffsetOverflow) {
		t.Fatalf("overflow after release = (%d, %v)", offset, err)
	}
	assertAudioReplayUnchanged(t, b, before)
	if chunk, ok, err := b.copyChunkAt(math.MaxUint64); err != nil || ok || chunk.data != nil {
		t.Fatalf("maximum end replay = (%+v, %v, %v)", chunk, ok, err)
	}
	if b.seal() != math.MaxUint64 {
		t.Fatal("seal changed maximum final offset")
	}
	assertAudioReplayInvariant(t, b)
}

func TestAudioReplayRepeatedRingReuse(t *testing.T) {
	b := newTestAudioReplayBuffer(t, 9, 3)
	// 用普通切片记录参考序列，不依赖被测环形队列的索引算法。
	var model []replayAudioChunk
	var acked, next uint64
	check := func() {
		t.Helper()
		assertAudioReplayInvariant(t, b)
		if b.ackedOffset != acked || b.nextOffset != next || b.count != len(model) {
			t.Fatalf("reference mismatch: buffer=%+v acked=%d next=%d count=%d", b, acked, next, len(model))
		}
		for _, chunk := range model {
			assertReplayChunk(t, b, chunk.offset, string(chunk.data))
		}
	}
	appendModel := func(payload []byte) {
		t.Helper()
		if offset, err := b.append(payload); err != nil || offset != next {
			t.Fatalf("model append = (%d, %v), want %d", offset, err, next)
		}
		model = append(model, replayAudioChunk{offset: next, data: bytes.Clone(payload)})
		next += uint64(len(payload))
		check()
	}
	ackPrefix := func(n int) {
		t.Helper()
		last := model[n-1]
		acked = last.offset + uint64(len(last.data))
		if advanced, err := b.acknowledge(acked); !advanced || err != nil {
			t.Fatalf("model ack = (%v, %v)", advanced, err)
		}
		model = model[n:]
		check()
	}
	for cycle := range 256 {
		for i := range 3 {
			appendModel(bytes.Repeat([]byte{byte(cycle + i)}, 1+(cycle+i)%3))
		}
		copyBeforeAck := assertReplayChunk(t, b, model[0].offset, string(model[0].data))
		wantOldCopy := bytes.Clone(copyBeforeAck.data)
		before := snapshotAudioReplay(b)
		if _, err := b.append([]byte{0}); !errors.Is(err, errReplayBufferFull) {
			t.Fatalf("full ring in cycle %d: %v", cycle, err)
		}
		assertAudioReplayUnchanged(t, b, before)
		ackPrefix(2)
		for i := range 2 {
			appendModel(bytes.Repeat([]byte{byte(cycle + i + 3)}, 1+(cycle+i+3)%3))
		}
		if !bytes.Equal(copyBeforeAck.data, wantOldCopy) {
			t.Fatalf("copy changed during ring reuse in cycle %d", cycle)
		}
		ackPrefix(3)
	}
	// 1280 次追加/释放后，绝对位置继续增长；全部槽位为空且没有负载引用。
	if b.count != 0 || b.nextOffset == 0 || b.ackedOffset != b.nextOffset {
		t.Fatalf("unexpected final state: %+v", b)
	}
}
