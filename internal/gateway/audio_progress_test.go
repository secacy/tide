package gateway

import (
	"sync"
	"testing"
)

// TestAudioProgressAccounting 验证累计确认与增量输入的不同含义、非法确认不改账目，
// 以及随后仍可继续记录；零值对象无需构造函数。
func TestAudioProgressAccounting(t *testing.T) {
	var p audioProgress
	for _, tc := range []struct {
		name      string
		ack       bool
		n         uint64
		wantErr   bool
		received  uint64
		processed uint64
		pending   uint64
	}{
		{name: "initial_zero_ack", ack: true},
		{name: "ack_without_input", ack: true, n: 1, wantErr: true},
		{name: "first_chunk", n: 3200, received: 3200, pending: 3200},
		{name: "empty_chunk", received: 3200, pending: 3200},
		{name: "partial_ack", ack: true, n: 1600, received: 3200, processed: 1600, pending: 1600},
		{name: "duplicate_ack", ack: true, n: 1600, received: 3200, processed: 1600, pending: 1600},
		{name: "backward_ack", ack: true, n: 1599, wantErr: true, received: 3200, processed: 1600, pending: 1600},
		{name: "future_ack", ack: true, n: 3201, wantErr: true, received: 3200, processed: 1600, pending: 1600},
		{name: "second_chunk", n: 6400, received: 9600, processed: 1600, pending: 8000},
		{name: "cumulative_ack", ack: true, n: 6400, received: 9600, processed: 6400, pending: 3200},
		{name: "fully_acknowledged", ack: true, n: 9600, received: 9600, processed: 9600},
		{name: "input_after_ack", n: 2, received: 9602, processed: 9600, pending: 2},
	} {
		// 步骤有意顺序执行；前一阶段的计数是下一阶段的输入。
		var err error
		if tc.ack {
			err = p.acknowledge(tc.n)
		} else {
			err = p.addReceived(tc.n)
		}
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: error=%v, want error=%v", tc.name, err, tc.wantErr)
		}
		want := audioProgressSnapshot{receivedBytes: tc.received, processedBytes: tc.processed, pendingBytes: tc.pending}
		if got := p.snapshot(); got != want {
			t.Fatalf("%s: snapshot=%+v, want %+v", tc.name, got, want)
		}
	}
}

// TestAudioProgressOverflow 验证接收量不会绕回零，拒绝溢出后仍能使用剩余范围，
// 且 uint64 最大值本身和最大值下的零增量合法。
func TestAudioProgressOverflow(t *testing.T) {
	const max = ^uint64(0)
	var p audioProgress
	if err := p.addReceived(max - 3); err != nil {
		t.Fatal(err)
	}
	if err := p.acknowledge(max - 5); err != nil {
		t.Fatal(err)
	}
	before := p.snapshot()
	if err := p.addReceived(4); err == nil {
		t.Fatal("overflow accepted")
	}
	if got := p.snapshot(); got != before {
		t.Fatalf("overflow changed counters: got %+v, before %+v", got, before)
	}
	if err := p.addReceived(3); err != nil {
		t.Fatal(err)
	}
	if err := p.acknowledge(max); err != nil {
		t.Fatal(err)
	}
	if err := p.addReceived(0); err != nil {
		t.Fatalf("zero addition at maximum failed: %v", err)
	}
	if err := p.addReceived(1); err == nil {
		t.Fatal("addition past maximum accepted")
	}
	want := audioProgressSnapshot{receivedBytes: max, processedBytes: max}
	if got := p.snapshot(); got != want {
		t.Fatalf("maximum snapshot=%+v, want %+v", got, want)
	}
}

// TestAudioProgressSnapshotsAreIndependent 验证历史快照保持原值，修改返回值不能改动账目；
// 两个计量器也不会共享会话数据。
func TestAudioProgressSnapshotsAreIndependent(t *testing.T) {
	var p, other audioProgress
	initial := p.snapshot()
	if err := p.addReceived(3200); err != nil {
		t.Fatal(err)
	}
	old := p.snapshot()
	if err := p.acknowledge(3200); err != nil {
		t.Fatal(err)
	}
	if initial != (audioProgressSnapshot{}) || old != (audioProgressSnapshot{receivedBytes: 3200, pendingBytes: 3200}) {
		t.Fatalf("historical snapshot changed: initial=%+v old=%+v", initial, old)
	}
	old.receivedBytes = 99
	old.processedBytes = 88
	old.pendingBytes = 11
	if got := p.snapshot(); got != (audioProgressSnapshot{receivedBytes: 3200, processedBytes: 3200}) {
		t.Fatalf("editing returned value changed counters: %+v", got)
	}
	if got := other.snapshot(); got != (audioProgressSnapshot{}) {
		t.Fatalf("independent meter changed: %+v", got)
	}
}

// TestAudioProgressConcurrent 验证并发接收、累计确认和快照读取不会丢计数，
// 每次快照仍满足同一时刻的不变量；这是功能与 race 检查，不是容量实验。
func TestAudioProgressConcurrent(t *testing.T) {
	var p audioProgress
	var wg sync.WaitGroup
	start := make(chan struct{})
	const writers, iterations = 4, 1000
	for writer := 1; writer <= writers; writer++ {
		wg.Add(1)
		go func(n uint64) {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				if err := p.addReceived(n); err != nil {
					t.Errorf("concurrent addition failed: %v", err)
					return
				}
			}
		}(uint64(writer))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		// 只有一个确认者，模拟 download 的顺序确认；接收量同时继续增长。
		for i := 0; i < iterations; i++ {
			n := p.snapshot().receivedBytes
			if err := p.acknowledge(n); err != nil {
				t.Errorf("confirmation of previously received bytes failed: %v", err)
				return
			}
		}
	}()
	for reader := 0; reader < 2; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var previous audioProgressSnapshot
			for i := 0; i < iterations; i++ {
				got := p.snapshot()
				if got.processedBytes > got.receivedBytes || got.pendingBytes != got.receivedBytes-got.processedBytes ||
					got.receivedBytes < previous.receivedBytes || got.processedBytes < previous.processedBytes {
					t.Errorf("inconsistent or regressing snapshot: previous=%+v now=%+v", previous, got)
					return
				}
				previous = got
			}
		}()
	}
	close(start)
	wg.Wait()
	const total = (1 + 2 + 3 + 4) * iterations
	if got := p.snapshot().receivedBytes; got != total {
		t.Fatalf("lost concurrent additions: got %d, want %d", got, total)
	}
	if err := p.acknowledge(total); err != nil {
		t.Fatal(err)
	}
	if got := p.snapshot(); got != (audioProgressSnapshot{receivedBytes: total, processedBytes: total}) {
		t.Fatalf("final snapshot=%+v", got)
	}
}
