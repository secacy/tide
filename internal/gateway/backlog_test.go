package gateway

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

// TestPendingAudioLimitBoundaries 检查开关、严格大于边界和 uint64 极值。
// 预算约束的是未确认量，不是会话累计接收量。
func TestPendingAudioLimitBoundaries(t *testing.T) {
	const max = ^uint64(0)
	for _, tc := range []struct {
		name      string
		received  uint64
		processed uint64
		limit     uint64
		wantError bool
	}{
		{name: "disabled_with_large_pending", received: max},
		{name: "empty", limit: 32000},
		{name: "below", received: 31999, limit: 32000},
		{name: "equal", received: 32000, limit: 32000},
		{name: "one_byte_above", received: 32001, limit: 32000, wantError: true},
		{name: "large_received_but_equal_pending", received: 1000000, processed: 968000, limit: 32000},
		{name: "all_processed", received: max, processed: max, limit: 1},
		{name: "maximum_equal", received: max, limit: max},
		{name: "maximum_above", received: max, limit: max - 1, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := audioProgressSnapshot{
				receivedBytes: tc.received, processedBytes: tc.processed,
				pendingBytes: tc.received - tc.processed,
			}
			before := p
			err := checkPendingAudioLimit(p, tc.limit)
			if (err != nil) != tc.wantError {
				t.Fatalf("pending=%d limit=%d: error=%v, want error=%v", p.pendingBytes, tc.limit, err, tc.wantError)
			}
			if p != before {
				t.Fatalf("budget check changed snapshot: before=%+v after=%+v", before, p)
			}
			if tc.wantError {
				if !errors.Is(err, ErrAudioBacklogExceeded) || errors.Unwrap(err) == nil {
					t.Fatalf("backlog error did not preserve wrapped cause: %v", err)
				}
				for _, value := range []uint64{p.pendingBytes, tc.limit} {
					if !strings.Contains(err.Error(), strconv.FormatUint(value, 10)) {
						t.Fatalf("error omits byte count %d: %v", value, err)
					}
				}
				if errors.Is(err, ErrWorkerSendTimeout) {
					t.Fatal("backlog error classified as send timeout")
				}
			}
		})
	}
}

// TestPendingAudioLimitDoesNotRollBackAccounting 验证预算判断与事实计量分离：
// 触发超限的已读块仍在账目里；新确认改变未确认量后，判断使用新快照。
// 本测试没有接入 upload，也不模拟超限会话的继续运行。
func TestPendingAudioLimitDoesNotRollBackAccounting(t *testing.T) {
	var p audioProgress
	if err := p.addReceived(32000); err != nil {
		t.Fatal(err)
	}
	if err := checkPendingAudioLimit(p.snapshot(), 32000); err != nil {
		t.Fatalf("exact budget rejected: %v", err)
	}
	if err := p.addReceived(3200); err != nil {
		t.Fatal(err)
	}
	over := p.snapshot()
	if err := checkPendingAudioLimit(over, 32000); !errors.Is(err, ErrAudioBacklogExceeded) {
		t.Fatalf("missing excess-budget error: %v", err)
	}
	if got := p.snapshot(); got != (audioProgressSnapshot{receivedBytes: 35200, pendingBytes: 35200}) {
		t.Fatalf("budget check changed received audio accounting: %+v", got)
	}
	if err := p.acknowledge(3200); err != nil {
		t.Fatal(err)
	}
	if err := checkPendingAudioLimit(p.snapshot(), 32000); err != nil {
		t.Fatalf("new acknowledgement not reflected in fresh check: %v", err)
	}
	if err := checkPendingAudioLimit(over, 32000); !errors.Is(err, ErrAudioBacklogExceeded) {
		t.Fatal("historical snapshot changed after new acknowledgement")
	}
}
