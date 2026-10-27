package realasr

import (
	"bytes"
	"errors"
	"testing"
)

func testSegmentCursor(t *testing.T, total, segment int) *segmentCursor {
	t.Helper()
	c, err := newSegmentCursor(total, segment)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSegmentCursorConstruction(t *testing.T) {
	for _, tc := range []struct {
		name           string
		total, segment int
	}{
		{"zero_total", 0, 2}, {"zero_segment", 2, 0},
		{"negative_total", -2, 2}, {"negative_segment", 2, -2},
		{"odd_total", 3, 2}, {"odd_segment", 4, 3},
		{"segment_exceeds_total", 2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := newSegmentCursor(tc.total, tc.segment)
			if c != nil || !errors.Is(err, errSegmentCursorBudget) {
				t.Fatalf("constructor = (%+v, %v), want nil and budget error", c, err)
			}
		})
	}
	for _, limits := range [][2]int{{2, 2}, {24, 10}} {
		c := testSegmentCursor(t, limits[0], limits[1])
		want := segmentCursor{maxTotalBytes: limits[0], maxSegmentBytes: limits[1], segmentNumber: 1}
		if *c != want {
			t.Fatalf("initial state = %+v, want %+v", *c, want)
		}
	}
}

func TestSegmentCursorPlanningAndAdvance(t *testing.T) {
	c := testSegmentCursor(t, 24, 10)
	if err := c.commit(8); err != nil {
		t.Fatal(err)
	}
	before := *c
	for i := 0; i < 3; i++ {
		if n, err := c.plan(6); n != 2 || err != nil || *c != before {
			t.Fatalf("read-only plan = (%d, %v), state %+v", n, err, *c)
		}
	}
	if err := c.commit(2); err != nil {
		t.Fatal(err)
	}
	before = *c
	if n, err := c.plan(4); n != 0 || err != nil || *c != before || c.segmentNumber != 1 {
		t.Fatalf("full segment plan = (%d, %v), state %+v", n, err, *c)
	}
	if err := c.advance(); err != nil {
		t.Fatal(err)
	}
	if c.totalBytes != 10 || c.segmentBytes != 0 || c.segmentNumber != 2 {
		t.Fatalf("after advance: %+v", *c)
	}
	if n, err := c.plan(4); n != 4 || err != nil {
		t.Fatalf("next segment plan = (%d, %v)", n, err)
	}
	if err := c.commit(4); err != nil {
		t.Fatal(err)
	}
	before = *c
	// 本段还有 6B，但整个请求的 12B 超出整场剩余 10B，不能接纳前缀。
	if n, err := c.plan(12); n != 0 || !errors.Is(err, errSegmentCursorTotalLimit) || *c != before {
		t.Fatalf("oversized request plan = (%d, %v), state %+v", n, err, *c)
	}
}

func TestSegmentCursorRejectedOperationsPreserveState(t *testing.T) {
	for _, tc := range []struct {
		name           string
		total, segment int
		committed      int
	}{
		{"empty", 24, 10, 0}, {"partial", 24, 10, 8},
		{"full", 24, 10, 10}, {"total_exhausted", 10, 10, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testSegmentCursor(t, tc.total, tc.segment)
			if tc.committed > 0 {
				if err := c.commit(tc.committed); err != nil {
					t.Fatal(err)
				}
			}
			before := *c
			for _, invalid := range []int{-2, -1, 0, 1, 3} {
				if n, err := c.plan(invalid); n != 0 || !errors.Is(err, errSegmentCursorInput) || *c != before {
					t.Fatalf("plan(%d) = (%d, %v), state %+v", invalid, n, err, *c)
				}
				if err := c.commit(invalid); !errors.Is(err, errSegmentCursorInput) || *c != before {
					t.Fatalf("commit(%d) = %v, state %+v", invalid, err, *c)
				}
			}
			beyondTotal := c.maxTotalBytes - c.totalBytes + 2
			if n, err := c.plan(beyondTotal); n != 0 || !errors.Is(err, errSegmentCursorTotalLimit) || *c != before {
				t.Fatalf("plan beyond total = (%d, %v), state %+v", n, err, *c)
			}
			if err := c.commit(beyondTotal); !errors.Is(err, errSegmentCursorTotalLimit) || *c != before {
				t.Fatalf("commit beyond total = %v, state %+v", err, *c)
			}
			beyondSegment := c.maxSegmentBytes - c.segmentBytes + 2
			if beyondSegment <= c.maxTotalBytes-c.totalBytes {
				if err := c.commit(beyondSegment); !errors.Is(err, errSegmentCursorSegmentLimit) || *c != before {
					t.Fatalf("commit beyond segment = %v, state %+v", err, *c)
				}
			}
			if tc.name != "full" {
				if err := c.advance(); !errors.Is(err, errSegmentCursorCannotAdvance) || *c != before {
					t.Fatalf("invalid advance = %v, state %+v", err, *c)
				}
			}
		})
	}
}

func TestSegmentCursorLargeIntegerBudgets(t *testing.T) {
	maxEven := int(^uint(0)>>1) - 1
	c := testSegmentCursor(t, maxEven, maxEven-2)
	if err := c.commit(maxEven - 4); err != nil {
		t.Fatal(err)
	}
	before := *c
	if n, err := c.plan(maxEven); n != 0 || !errors.Is(err, errSegmentCursorTotalLimit) || *c != before {
		t.Fatalf("large plan = (%d, %v), state %+v", n, err, *c)
	}
	if err := c.commit(maxEven); !errors.Is(err, errSegmentCursorTotalLimit) || *c != before {
		t.Fatalf("large commit = %v, state %+v", err, *c)
	}
	if err := c.commit(2); err != nil {
		t.Fatal(err)
	}
	if err := c.advance(); err != nil {
		t.Fatal(err)
	}
	if n, err := c.plan(2); n != 2 || err != nil {
		t.Fatalf("last sample plan = (%d, %v)", n, err)
	}
	if err := c.commit(2); err != nil {
		t.Fatal(err)
	}
	if c.totalBytes != maxEven || c.segmentBytes != 2 || c.segmentNumber != 2 {
		t.Fatalf("large final state: %+v", *c)
	}
	before = *c
	if err := c.advance(); !errors.Is(err, errSegmentCursorCannotAdvance) || *c != before {
		t.Fatalf("advance after large total = %v, state %+v", err, *c)
	}
}

// 遍历每个 PCM 样本边界是否划分请求，与固定位置切片的独立期望比较。
// 此处不模拟模型收尾和 final；它们属于后续 Worker 集成验收。
func TestSegmentCursorConservesAudioAcrossRequestPartitions(t *testing.T) {
	for _, length := range []int{20, 24} {
		source := make([]byte, length)
		for i := range source {
			source[i] = byte(i + 1)
		}
		for partition := 0; partition < 1<<(length/2-1); partition++ {
			c := testSegmentCursor(t, length, 10)
			var segments [][]byte
			requestStart := 0
			for end := 2; end <= length; end += 2 {
				if end != length && partition&(1<<(end/2-1)) == 0 {
					continue
				}
				remaining := source[requestStart:end]
				for len(remaining) > 0 {
					n, err := c.plan(len(remaining))
					if err != nil {
						t.Fatalf("length %d partition %d: %v", length, partition, err)
					}
					if n == 0 {
						if err := c.advance(); err != nil {
							t.Fatal(err)
						}
						continue
					}
					if len(segments) < c.segmentNumber {
						segments = append(segments, nil)
					}
					segments[c.segmentNumber-1] = append(segments[c.segmentNumber-1], remaining[:n]...)
					if err := c.commit(n); err != nil {
						t.Fatal(err)
					}
					remaining = remaining[n:]
				}
				requestStart = end
			}
			if !bytes.Equal(bytes.Join(segments, nil), source) || len(segments) != (length+9)/10 {
				t.Fatalf("length %d partition %d: lost, duplicated, or extra segment", length, partition)
			}
			for i, segment := range segments {
				want := source[i*10 : min((i+1)*10, length)]
				if !bytes.Equal(segment, want) {
					t.Fatalf("length %d partition %d segment %d: %v, want %v", length, partition, i+1, segment, want)
				}
			}
			if c.totalBytes != length || c.segmentNumber != len(segments) || c.segmentBytes != len(segments[len(segments)-1]) {
				t.Fatalf("length %d partition %d: final state %+v", length, partition, *c)
			}
		}
	}
}
