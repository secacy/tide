package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

// selectionOnlyClient 为选择器提供可辨识的客户端；选择阶段不允许调用 RPC。
type selectionOnlyClient struct{ name string }

func (c *selectionOnlyClient) StreamingRecognize(context.Context, ...grpc.CallOption) (grpc.BidiStreamingClient[asrv1.StreamingRecognizeRequest, asrv1.StreamingRecognizeResponse], error) {
	panic("selector must not open a Worker RPC")
}

// testWorkers 使用独立客户端，以便同时校验 ID 和客户端归属。
func testWorkers() []Worker {
	return []Worker{
		{ID: "A", Client: &selectionOnlyClient{name: "A"}},
		{ID: "B", Client: &selectionOnlyClient{name: "B"}},
		{ID: "C", Client: &selectionOnlyClient{name: "C"}},
	}
}

func mustRoundRobin(t *testing.T, workers []Worker) *RoundRobin {
	t.Helper()
	r, err := NewRoundRobin(workers)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestNewRoundRobinInvalidWorkers 覆盖非法列表，包括相邻及非相邻重复 ID。
func TestNewRoundRobinInvalidWorkers(t *testing.T) {
	workers := testWorkers()
	for _, tc := range []struct {
		name    string
		workers []Worker
	}{
		{"nil_list", nil},
		{"empty_list", []Worker{}},
		{"empty_id", []Worker{{ID: "", Client: workers[0].Client}}},
		{"blank_id", []Worker{{ID: " \t\n", Client: workers[0].Client}}},
		{"duplicate_adjacent", []Worker{workers[0], {ID: "A", Client: workers[1].Client}}},
		{"duplicate_nonadjacent", []Worker{workers[0], workers[1], {ID: "A", Client: workers[2].Client}}},
		{"nil_client", []Worker{{ID: "A"}}},
		{"nil_client_later", []Worker{workers[0], {ID: "B"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRoundRobin(tc.workers)
			if err == nil || r != nil {
				t.Fatalf("invalid list accepted: selector=%v err=%v", r, err)
			}
		})
	}
}

// TestRoundRobinSequence 检查单 Worker 与多 Worker 的顺序及多次回绕。
func TestRoundRobinSequence(t *testing.T) {
	for _, n := range []int{1, 3} {
		name := "single"
		if n == 3 {
			name = "three_workers"
		}
		t.Run(name, func(t *testing.T) {
			workers := testWorkers()[:n]
			r := mustRoundRobin(t, workers)
			for i := 0; i < 10; i++ {
				got, want := r.Pick(), workers[i%n]
				if got != want {
					t.Fatalf("selection %d: got %+v, want %+v", i, got, want)
				}
			}
		})
	}
}

// TestRoundRobinPreservesIDs 按约定将原始 ID 作为标识，不静默修剪或改写。
// 共享 Client 的不同 ID 允许存在；选择器不探测地址或推断是否同一个后端。
func TestRoundRobinPreservesIDs(t *testing.T) {
	client := &selectionOnlyClient{name: "shared"}
	workers := []Worker{{ID: "A", Client: client}, {ID: " A ", Client: client}}
	r := mustRoundRobin(t, workers)
	for _, want := range workers {
		if got := r.Pick(); got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
}

// TestRoundRobinListIsolation 修改传入切片和返回值不会改变内部固定列表；Client 引用仍共享。
func TestRoundRobinListIsolation(t *testing.T) {
	workers := testWorkers()
	original := append([]Worker(nil), workers...)
	r := mustRoundRobin(t, workers)
	workers[0] = Worker{ID: "replacement", Client: &selectionOnlyClient{name: "replacement"}}
	workers[1].ID = "changed"
	workers[2].Client = nil
	selected := r.Pick()
	if selected != original[0] {
		t.Fatalf("input mutation changed selection: %+v", selected)
	}
	selected.ID = "caller mutation"
	selected.Client = nil
	for i := 1; i <= 6; i++ {
		if got, want := r.Pick(), original[i%len(original)]; got != want {
			t.Fatalf("selection %d: got %+v, want %+v", i, got, want)
		}
	}
}

// TestRoundRobinConcurrentSelections 验证并发选择总数和精确轮询分布；不测吞吐或锁公平性。
func TestRoundRobinConcurrentSelections(t *testing.T) {
	const callers, perCaller = 32, 100
	workers := testWorkers()
	r := mustRoundRobin(t, workers)
	indices := map[string]int{"A": 0, "B": 1, "C": 2}
	var counts [3]atomic.Int64
	var invalid atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perCaller {
				got := r.Pick()
				i, ok := indices[got.ID]
				if !ok || got.Client != workers[i].Client {
					invalid.Add(1)
					continue
				}
				counts[i].Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if invalid.Load() != 0 {
		t.Fatalf("invalid Worker selections: %d", invalid.Load())
	}
	total := callers * perCaller
	var observed int64
	for i := range workers {
		want := total / len(workers)
		if i < total%len(workers) {
			want++
		}
		got := counts[i].Load()
		observed += got
		if got != int64(want) {
			t.Errorf("Worker %s selected %d times, want %d", workers[i].ID, got, want)
		}
	}
	if observed != int64(total) {
		t.Errorf("total=%d want=%d", observed, total)
	}
	if got, want := r.Pick(), workers[total%len(workers)]; got != want {
		t.Errorf("next selection=%+v want=%+v", got, want)
	}
	t.Logf("callers=%d selections=%d counts=[%d %d %d]", callers, total, counts[0].Load(), counts[1].Load(), counts[2].Load())
}
