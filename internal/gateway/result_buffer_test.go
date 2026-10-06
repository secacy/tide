package gateway

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

// newTestResultBuffer 创建只由测试方串行操作的有效缓冲。
func newTestResultBuffer(t *testing.T, maxBytes uint64, maxResults int) *resultBuffer {
	t.Helper()
	b, err := newResultBuffer(maxBytes, maxResults)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// resultBufferSnapshot 记录元数据和槽位值；字符串不可变，无须复制其负载。
type resultBufferSnapshot struct {
	head, count             int
	retainedBytes, maxBytes uint64
	lastSeq, ackedSeq       uint64
	slots                   []retainedResult
}

func snapshotTestResultBuffer(b *resultBuffer) resultBufferSnapshot {
	return resultBufferSnapshot{
		head: b.head, count: b.count, retainedBytes: b.retainedBytes, maxBytes: b.maxBytes,
		lastSeq: b.lastSeq, ackedSeq: b.ackedSeq, slots: append([]retainedResult(nil), b.slots...),
	}
}

func assertTestResultBufferUnchanged(t *testing.T, b *resultBuffer, before resultBufferSnapshot) {
	t.Helper()
	if got := snapshotTestResultBuffer(b); !reflect.DeepEqual(got, before) {
		t.Fatalf("result buffer changed: got=%+v, before=%+v", got, before)
	}
}

// assertTestResultBufferInvariant 同时检查连续序号、负载预算和空槽位引用。
func assertTestResultBufferInvariant(t *testing.T, b *resultBuffer) {
	t.Helper()
	if len(b.slots) == 0 || b.head < 0 || b.head >= len(b.slots) || b.count < 0 || b.count > len(b.slots) {
		t.Fatalf("invalid ring: head=%d count=%d slots=%d", b.head, b.count, len(b.slots))
	}
	if b.ackedSeq > b.lastSeq || uint64(b.count) != b.lastSeq-b.ackedSeq || b.retainedBytes > b.maxBytes {
		t.Fatalf("invalid positions/budget: ack=%d last=%d count=%d retained=%d max=%d", b.ackedSeq, b.lastSeq, b.count, b.retainedBytes, b.maxBytes)
	}
	occupied := make([]bool, len(b.slots))
	var total uint64
	for i := range b.count {
		index := (b.head + i) % len(b.slots)
		occupied[index] = true
		item := b.slots[index]
		if want := b.ackedSeq + uint64(i) + 1; item.seq != want {
			t.Fatalf("slot %d seq=%d, want %d", index, item.seq, want)
		}
		total += uint64(len(item.segmentID))
		total += uint64(len(item.text))
	}
	if total != b.retainedBytes {
		t.Fatalf("payload accounting = %d, retained=%d", total, b.retainedBytes)
	}
	for i, item := range b.slots {
		if !occupied[i] && item != (retainedResult{}) {
			t.Fatalf("unused slot %d retains result: %+v", i, item)
		}
	}
}

func appendTestResult(t *testing.T, b *resultBuffer, segment, text string, final bool, wantSeq uint64) {
	t.Helper()
	if seq, err := b.append(segment, text, final); seq != wantSeq || err != nil {
		t.Fatalf("append = (%d, %v), want seq %d", seq, err, wantSeq)
	}
	assertTestResultBufferInvariant(t, b)
}

func assertTestResultRead(t *testing.T, b *resultBuffer, after uint64, want retainedResult, wantOK bool, wantErr error) {
	t.Helper()
	before := snapshotTestResultBuffer(b)
	got, ok, err := b.peekAfter(after)
	if got != want || ok != wantOK || !errors.Is(err, wantErr) {
		t.Fatalf("peekAfter(%d) = (%+v, %v, %v), want (%+v, %v, %v)", after, got, ok, err, want, wantOK, wantErr)
	}
	assertTestResultBufferUnchanged(t, b, before)
}

func TestResultBufferConstruction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bytes   uint64
		results int
		valid   bool
	}{
		{"zero_bytes", 0, 1, false},
		{"zero_results", 1, 0, false},
		{"negative_results", 1, -1, false},
		{"minimum_int_results", 1, math.MinInt, false},
		{"both_zero", 0, 0, false},
		{"smallest_limits", 1, 1, true},
		{"maximum_byte_budget_without_payload", math.MaxUint64, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := newResultBuffer(tc.bytes, tc.results)
			if !tc.valid {
				if b != nil || !errors.Is(err, errInvalidResultBufferLimits) {
					t.Fatalf("construction = (%v, %v)", b, err)
				}
				return
			}
			if err != nil || b == nil {
				t.Fatalf("valid construction = (%v, %v)", b, err)
			}
			if b.lastSeq != 0 || b.ackedSeq != 0 || b.count != 0 || b.retainedBytes != 0 || b.maxBytes != tc.bytes || len(b.slots) != tc.results || cap(b.slots) != tc.results {
				t.Fatal("construction did not create empty fixed storage with requested limits")
			}
			assertTestResultBufferInvariant(t, b)
			assertTestResultRead(t, b, 0, retainedResult{}, false, nil)
		})
	}
}

func TestResultBufferRetainsEverySegmentUpdate(t *testing.T) {
	b := newTestResultBuffer(t, 100, 4)
	want := []retainedResult{
		{seq: 1, segmentID: "A", text: "患者"},
		{seq: 2, segmentID: "B", text: "旁述"},
		{seq: 3, segmentID: "A", text: "患者头痛"},
		{seq: 4, segmentID: "A", text: "患者头痛三天。", isFinal: true},
	}
	for _, item := range want {
		appendTestResult(t, b, item.segmentID, item.text, item.isFinal, item.seq)
	}
	for _, item := range want {
		assertTestResultRead(t, b, item.seq-1, item, true, nil)
		assertTestResultRead(t, b, item.seq-1, item, true, nil)
	}
	if b.count != 4 || b.ackedSeq != 0 {
		t.Fatal("reading or final result released/overwrote unconfirmed updates")
	}
	assertTestResultRead(t, b, 4, retainedResult{}, false, nil)
}

func TestResultBufferOwnsShortStringCopies(t *testing.T) {
	b := newTestResultBuffer(t, 8, 1)
	source := strings.Repeat("0123456789", 8192)
	segment, text := source[8:12], source[100:104]
	appendTestResult(t, b, segment, text, false, 1)
	got, ok, err := b.peekAfter(0)
	if !ok || err != nil || got.segmentID != segment || got.text != text || b.retainedBytes != 8 {
		t.Fatalf("copied result = (%+v, %v, %v)", got, ok, err)
	}
	// 只读比较非空字符串地址，避免以 unsafe 修改 Go 字符串。
	// 这项约束防止短子串继续引用调用方的大字符串。
	if unsafe.StringData(got.segmentID) == unsafe.StringData(segment) || unsafe.StringData(got.text) == unsafe.StringData(text) {
		t.Fatal("retained result still aliases the original substrings")
	}
	borrowed := got
	got.text = "changed record value"
	assertTestResultRead(t, b, 0, borrowed, true, nil)
	if changed, err := b.acknowledge(1); !changed || err != nil {
		t.Fatalf("ack = (%v, %v)", changed, err)
	}
	appendTestResult(t, b, "n", "new", true, 2)
	if borrowed.segmentID != segment || borrowed.text != text {
		t.Fatal("ack or slot reuse changed an already borrowed immutable value")
	}
}

func TestResultBufferAppendFailureKeepsStateAndSequence(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		maxBytes               uint64
		maxResults             int
		initialID, initialText string
		id, text               string
	}{
		{"id_exceeds_remaining", 4, 3, "s", "a", "xyz", ""},
		{"text_exceeds_remaining", 4, 3, "s", "a", "", "xyz"},
		{"combined_exceeds_remaining", 4, 3, "s", "a", "x", "yz"},
		{"byte_full_with_spare_slots", 2, 3, "s", "a", "", "x"},
		{"slot_full_with_spare_bytes", 100, 1, "s", "a", "", ""},
		{"both_limits_full", 2, 1, "s", "a", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestResultBuffer(t, tc.maxBytes, tc.maxResults)
			appendTestResult(t, b, tc.initialID, tc.initialText, false, 1)
			before := snapshotTestResultBuffer(b)
			if seq, err := b.append(tc.id, tc.text, true); seq != 0 || !errors.Is(err, errResultBufferFull) {
				t.Fatalf("rejection = (%d, %v)", seq, err)
			}
			assertTestResultBufferUnchanged(t, b, before)
			assertTestResultBufferInvariant(t, b)
			if changed, err := b.acknowledge(1); !changed || err != nil {
				t.Fatalf("ack = (%v, %v)", changed, err)
			}
			appendTestResult(t, b, "", "x", false, 2) // 拒绝没有留下序号空洞。
		})
	}
}

func TestResultBufferByteEqualityAndEmptyResults(t *testing.T) {
	t.Run("utf8_byte_equality", func(t *testing.T) {
		b := newTestResultBuffer(t, 9, 2)
		appendTestResult(t, b, "片", "头痛", true, 1) // 3+6 字节，不是三个字符。
		if b.retainedBytes != 9 {
			t.Fatalf("UTF-8 bytes = %d", b.retainedBytes)
		}
		before := snapshotTestResultBuffer(b)
		if seq, err := b.append("", "a", false); seq != 0 || !errors.Is(err, errResultBufferFull) {
			t.Fatalf("UTF-8 budget rejection = (%d, %v)", seq, err)
		}
		assertTestResultBufferUnchanged(t, b, before)
		appendTestResult(t, b, "", "", false, 2) // 无字节负载仍占一个槽位。
		if b.retainedBytes != 9 || b.count != 2 {
			t.Fatal("empty result bypassed slot accounting")
		}
	})
	t.Run("empty_results_still_fill_slots", func(t *testing.T) {
		b := newTestResultBuffer(t, 1, 2)
		appendTestResult(t, b, "", "", false, 1)
		appendTestResult(t, b, "", "", true, 2)
		before := snapshotTestResultBuffer(b)
		if seq, err := b.append("", "", false); seq != 0 || !errors.Is(err, errResultBufferFull) {
			t.Fatalf("empty-result limit = (%d, %v)", seq, err)
		}
		assertTestResultBufferUnchanged(t, b, before)
		if b.retainedBytes != 0 {
			t.Fatal("empty results charged nonexistent bytes")
		}
	})
}

func TestResultBufferCumulativeAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name            string
		empty           bool
		initialAck, seq uint64
		changed         bool
		err             error
	}{
		{name: "empty_zero", empty: true},
		{name: "empty_ahead", empty: true, seq: 1, err: errResultSequenceAhead},
		{name: "zero_is_idempotent"},
		{name: "first", seq: 1, changed: true},
		{name: "skip_to_second", seq: 2, changed: true},
		{name: "all", seq: 3, changed: true},
		{name: "ahead", seq: 4, err: errResultSequenceAhead},
		{name: "maximum_ahead", seq: math.MaxUint64, err: errResultSequenceAhead},
		{name: "older", initialAck: 2, seq: 1},
		{name: "duplicate", initialAck: 2, seq: 2},
		{name: "advance_after_partial", initialAck: 2, seq: 3, changed: true},
		{name: "ahead_after_partial", initialAck: 2, seq: 4, err: errResultSequenceAhead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestResultBuffer(t, 20, 3)
			if !tc.empty {
				for i := uint64(1); i <= 3; i++ {
					appendTestResult(t, b, "s", strings.Repeat("x", int(i)), i == 3, i)
				}
			}
			if tc.initialAck != 0 {
				if changed, err := b.acknowledge(tc.initialAck); !changed || err != nil {
					t.Fatalf("initial ack = (%v, %v)", changed, err)
				}
			}
			before := snapshotTestResultBuffer(b)
			changed, err := b.acknowledge(tc.seq)
			if changed != tc.changed || !errors.Is(err, tc.err) {
				t.Fatalf("ack = (%v, %v), want (%v, %v)", changed, err, tc.changed, tc.err)
			}
			if !tc.changed {
				assertTestResultBufferUnchanged(t, b, before)
			} else if b.ackedSeq != tc.seq || b.lastSeq != 3 || b.count != int(3-tc.seq) {
				t.Fatal("cumulative ack did not release precisely its prefix")
			}
			assertTestResultBufferInvariant(t, b)
		})
	}
}

func TestResultBufferReadCursorBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name       string
		empty      bool
		ack, after uint64
		wantSeq    uint64
		err        error
	}{
		{name: "empty_at_zero", empty: true},
		{name: "empty_ahead", empty: true, after: 1, err: errResultSequenceAhead},
		{name: "first", wantSeq: 1},
		{name: "middle", after: 1, wantSeq: 2},
		{name: "at_last", after: 3},
		{name: "ahead_of_last", after: 4, err: errResultSequenceAhead},
		{name: "before_ack_zero", ack: 2, err: errResultReplayGap},
		{name: "before_ack_one", ack: 2, after: 1, err: errResultReplayGap},
		{name: "at_ack", ack: 2, after: 2, wantSeq: 3},
		{name: "cleared_at_last", ack: 3, after: 3},
		{name: "cleared_old_cursor", ack: 3, after: 2, err: errResultReplayGap},
		{name: "maximum_ahead", after: math.MaxUint64, err: errResultSequenceAhead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestResultBuffer(t, 20, 3)
			if !tc.empty {
				for i := uint64(1); i <= 3; i++ {
					appendTestResult(t, b, "s", fmt.Sprint(i), i == 3, i)
				}
			}
			if tc.ack > 0 {
				if changed, err := b.acknowledge(tc.ack); !changed || err != nil {
					t.Fatalf("ack = (%v, %v)", changed, err)
				}
			}
			var want retainedResult
			if tc.wantSeq != 0 {
				want = retainedResult{seq: tc.wantSeq, segmentID: "s", text: fmt.Sprint(tc.wantSeq), isFinal: tc.wantSeq == 3}
			}
			assertTestResultRead(t, b, tc.after, want, tc.wantSeq != 0, tc.err)
			assertTestResultBufferInvariant(t, b)
		})
	}
}

func TestResultBufferReplayAndLostAcknowledgement(t *testing.T) {
	b := newTestResultBuffer(t, 100, 6)
	for i := uint64(1); i <= 6; i++ {
		appendTestResult(t, b, "s", fmt.Sprint(i), i == 6, i)
	}
	if changed, err := b.acknowledge(2); !changed || err != nil {
		t.Fatalf("ack 2 = (%v, %v)", changed, err)
	}
	assertTestResultRead(t, b, 4, retainedResult{seq: 5, segmentID: "s", text: "5"}, true, nil)
	if changed, err := b.acknowledge(4); !changed || err != nil {
		t.Fatalf("ack 4 = (%v, %v)", changed, err)
	}
	if b.count != 2 || b.retainedBytes != 4 {
		t.Fatal("ack 4 did not retain exactly 5 and 6")
	}
	before := snapshotTestResultBuffer(b)
	if changed, err := b.acknowledge(3); changed || err != nil {
		t.Fatalf("old ack = (%v, %v)", changed, err)
	}
	assertTestResultBufferUnchanged(t, b, before)
	assertTestResultRead(t, b, 3, retainedResult{}, false, errResultReplayGap)
	assertTestResultRead(t, b, 4, retainedResult{seq: 5, segmentID: "s", text: "5"}, true, nil)
	// 客户端已经应用 6，但确认丢失：内部恢复游标 6 合法，无需重复应用。
	// 真正握手仍须以后接入凭据、代次和投递范围校验。
	assertTestResultRead(t, b, 6, retainedResult{}, false, nil)
	if changed, err := b.acknowledge(6); !changed || err != nil {
		t.Fatalf("recovered ack = (%v, %v)", changed, err)
	}
	if b.count != 0 || b.retainedBytes != 0 || b.lastSeq != 6 || b.ackedSeq != 6 {
		t.Fatal("confirmed results not released or sequence reset")
	}
	appendTestResult(t, b, "s", "7", false, 7)
	assertTestResultRead(t, b, 6, retainedResult{seq: 7, segmentID: "s", text: "7"}, true, nil)
}

func TestResultBufferFIFOAndRingReuse(t *testing.T) {
	for _, maxResults := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d_slots", maxResults), func(t *testing.T) {
			b := newTestResultBuffer(t, 100, maxResults)
			var model []retainedResult
			var next uint64
			add := func() {
				next++
				item := retainedResult{seq: next, segmentID: "s", text: fmt.Sprintf("结果%d", next), isFinal: next%3 == 0}
				appendTestResult(t, b, item.segmentID, item.text, item.isFinal, item.seq)
				model = append(model, item)
			}
			check := func() {
				for _, item := range model {
					assertTestResultRead(t, b, item.seq-1, item, true, nil)
				}
				assertTestResultBufferInvariant(t, b)
				if len(b.slots) != maxResults || cap(b.slots) != maxResults {
					t.Fatal("ring storage expanded")
				}
			}
			for range maxResults {
				add()
			}
			for cycle := range 32 {
				check()
				release := cycle%maxResults + 1
				if changed, err := b.acknowledge(model[release-1].seq); !changed || err != nil {
					t.Fatalf("ring ack = (%v, %v)", changed, err)
				}
				model = model[release:]
				check()
				for range release {
					add()
				}
			}
			check()
			if changed, err := b.acknowledge(next); !changed || err != nil {
				t.Fatalf("final ack = (%v, %v)", changed, err)
			}
			assertTestResultBufferInvariant(t, b)
			if b.count != 0 || b.retainedBytes != 0 || b.ackedSeq != next || b.lastSeq != next {
				t.Fatal("final ring accounting invalid")
			}
			add()
			assertTestResultRead(t, b, next-1, model[len(model)-1], true, nil)
		})
	}
}

func TestResultBufferMaximumSequenceDoesNotWrap(t *testing.T) {
	for _, clear := range []bool{false, true} {
		name := "maximum_with_full_buffer"
		if clear {
			name = "maximum_after_ack"
		}
		t.Run(name, func(t *testing.T) {
			b := newTestResultBuffer(t, 4, 2)
			// 合法的已清空累计位置，用于直接覆盖极限，不生成 2^64 条记录。
			b.lastSeq, b.ackedSeq = math.MaxUint64-2, math.MaxUint64-2
			appendTestResult(t, b, "s", "a", false, math.MaxUint64-1)
			appendTestResult(t, b, "s", "b", true, math.MaxUint64)
			assertTestResultRead(t, b, math.MaxUint64-1, retainedResult{seq: math.MaxUint64, segmentID: "s", text: "b", isFinal: true}, true, nil)
			assertTestResultRead(t, b, math.MaxUint64, retainedResult{}, false, nil)
			if clear {
				if changed, err := b.acknowledge(math.MaxUint64); !changed || err != nil {
					t.Fatalf("max ack = (%v, %v)", changed, err)
				}
				assertTestResultRead(t, b, math.MaxUint64-1, retainedResult{}, false, errResultReplayGap)
			}
			before := snapshotTestResultBuffer(b)
			if seq, err := b.append("", "", false); seq != 0 || !errors.Is(err, errResultSequenceExhausted) {
				t.Fatalf("sequence exhaustion = (%d, %v)", seq, err)
			}
			assertTestResultBufferUnchanged(t, b, before)
			assertTestResultBufferInvariant(t, b)
		})
	}
}

func TestResultBufferInstancesAreIndependent(t *testing.T) {
	a, b := newTestResultBuffer(t, 4, 1), newTestResultBuffer(t, 4, 1)
	appendTestResult(t, a, "s", "a", false, 1)
	appendTestResult(t, b, "s", "b", true, 1)
	before := snapshotTestResultBuffer(b)
	if changed, err := a.acknowledge(1); !changed || err != nil {
		t.Fatalf("ack = (%v, %v)", changed, err)
	}
	appendTestResult(t, a, "s", "aa", true, 2)
	assertTestResultBufferUnchanged(t, b, before)
	assertTestResultRead(t, b, 0, retainedResult{seq: 1, segmentID: "s", text: "b", isFinal: true}, true, nil)
}
