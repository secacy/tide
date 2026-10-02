package workerpool

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

// weightedTestWorkers 复用禁止 RPC 调用的客户端，生成独立的选择器配置。
func weightedTestWorkers(weights ...int64) []WeightedWorker {
	workers := testWorkers()
	result := make([]WeightedWorker, len(weights))
	for i, weight := range weights {
		result[i] = WeightedWorker{Worker: workers[i], Weight: weight}
	}
	return result
}

func mustWeightedRoundRobin(t *testing.T, workers []WeightedWorker) *WeightedRoundRobin {
	t.Helper()
	r, err := NewWeightedRoundRobin(workers)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// TestNewWeightedRoundRobinInvalidConfig 检查错误输入不产生可用选择器，含加法溢出边界。
func TestNewWeightedRoundRobinInvalidConfig(t *testing.T) {
	for _, name := range []string{
		"nil_list", "empty_list", "empty_id", "blank_id", "duplicate_adjacent",
		"duplicate_nonadjacent", "nil_client_first", "nil_client_later",
		"zero_weight", "negative_weight", "min_int_weight", "single_over_limit",
		"sum_over_limit", "max_int_first", "max_int_later",
	} {
		t.Run(name, func(t *testing.T) {
			workers := weightedTestWorkers(2, 1, 1)
			switch name {
			case "nil_list":
				workers = nil
			case "empty_list":
				workers = []WeightedWorker{}
			case "empty_id":
				workers[0].Worker.ID = ""
			case "blank_id":
				workers[1].Worker.ID = " \t\n"
			case "duplicate_adjacent":
				workers[1].Worker.ID = workers[0].Worker.ID
			case "duplicate_nonadjacent":
				workers[2].Worker.ID = workers[0].Worker.ID
			case "nil_client_first":
				workers[0].Worker.Client = nil
			case "nil_client_later":
				workers[2].Worker.Client = nil
			case "zero_weight":
				workers[1].Weight = 0
			case "negative_weight":
				workers[1].Weight = -1
			case "min_int_weight":
				workers[1].Weight = math.MinInt64
			case "single_over_limit":
				workers = weightedTestWorkers(1_000_001)
			case "sum_over_limit":
				workers = weightedTestWorkers(500_000, 500_001)
			case "max_int_first":
				workers[0].Weight = math.MaxInt64
			case "max_int_later":
				workers[2].Weight = math.MaxInt64
			}
			r, err := NewWeightedRoundRobin(workers)
			if err == nil || r != nil {
				t.Fatalf("invalid config accepted: selector=%v err=%v", r, err)
			}
		})
	}
}

// TestWeightedRoundRobinSequence 使用手工推导的顺序验证平滑分配、稳定同分选择及周期重复。
func TestWeightedRoundRobinSequence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []int64
		cycle   string
	}{
		{"single", []int64{1}, "A"},
		{"two_to_one", []int64{2, 1}, "ABA"},
		{"one_to_two", []int64{1, 2}, "BAB"},
		{"five_one_one", []int64{5, 1, 1}, "AABACAA"},
		{"equal", []int64{1, 1, 1}, "ABC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := weightedTestWorkers(tc.weights...)
			r := mustWeightedRoundRobin(t, workers)
			for i := 0; i < len(tc.cycle)*20; i++ {
				want := workers[int(tc.cycle[i%len(tc.cycle)]-'A')].Worker
				if got := r.Pick(); got != want {
					t.Fatalf("selection %d: got %+v want %+v", i, got, want)
				}
			}
		})
	}
}

// TestWeightedRoundRobinEqualWeightsMatchRoundRobin 验证等权时保留配置顺序，与原策略选择一致。
func TestWeightedRoundRobinEqualWeightsMatchRoundRobin(t *testing.T) {
	for _, weight := range []int64{1, 7, 333_333} {
		workers := weightedTestWorkers(weight, weight, weight)
		workers[0], workers[2] = workers[2], workers[0]
		r := mustWeightedRoundRobin(t, workers)
		ordinary := mustRoundRobin(t, []Worker{workers[0].Worker, workers[1].Worker, workers[2].Worker})
		for i := 0; i < 100; i++ {
			if got, want := r.Pick(), ordinary.Pick(); got != want {
				t.Fatalf("weight=%d selection=%d got=%+v want=%+v", weight, i, got, want)
			}
		}
	}
}

// TestWeightedRoundRobinFullCycleShares 验证完整权重周期的数量、身份和极端合法配置。
// 百万次选择仅用于验证稀有实例仍能被选中，不记录或宣称吞吐性能。
func TestWeightedRoundRobinFullCycleShares(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []int64
		cycles  int64
	}{
		{"coprime", []int64{2, 3, 7}, 100},
		{"common_factor", []int64{4, 2, 2}, 100},
		{"single_at_limit", []int64{1_000_000}, 1},
		{"skew_at_limit", []int64{999_999, 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := weightedTestWorkers(tc.weights...)
			r := mustWeightedRoundRobin(t, workers)
			var total int64
			for _, w := range workers {
				total += w.Weight
			}
			counts := make([]int64, len(workers))
			for i := int64(0); i < total*tc.cycles; i++ {
				got := r.Pick()
				index := int(got.ID[0] - 'A')
				if index < 0 || index >= len(workers) || got != workers[index].Worker {
					t.Fatalf("invalid selection: %+v", got)
				}
				counts[index]++
			}
			for i, w := range workers {
				if want := w.Weight * tc.cycles; counts[i] != want {
					t.Errorf("Worker %s selected %d times, want %d", w.Worker.ID, counts[i], want)
				}
			}
			fresh := mustWeightedRoundRobin(t, workers)
			for i := 0; i < 20; i++ {
				if got, want := r.Pick(), fresh.Pick(); got != want {
					t.Fatalf("post-cycle selection %d: got %+v want %+v", i, got, want)
				}
			}
		})
	}
}

// TestWeightedRoundRobinScaleInvariance 权重同比放大到合法总量边界，选择序列应保持一致。
func TestWeightedRoundRobinScaleInvariance(t *testing.T) {
	small := mustWeightedRoundRobin(t, weightedTestWorkers(2, 1, 1))
	large := mustWeightedRoundRobin(t, weightedTestWorkers(500_000, 250_000, 250_000))
	for i := 0; i < 1_000; i++ {
		if a, b := small.Pick(), large.Pick(); a.ID != b.ID {
			t.Fatalf("selection %d: scaled sequence differs: %s / %s", i, a.ID, b.ID)
		}
	}
}

// TestWeightedRoundRobinIsolation 调用方修改输入、返回值或推进另一个选择器，不影响原实例。
func TestWeightedRoundRobinIsolation(t *testing.T) {
	workers := weightedTestWorkers(2, 1)
	original := append([]WeightedWorker(nil), workers...)
	r := mustWeightedRoundRobin(t, workers)
	independent := mustWeightedRoundRobin(t, workers)
	workers[0] = WeightedWorker{}
	workers[1].Worker.ID = "changed"
	workers[1].Worker.Client = nil
	workers[1].Weight = 999_999
	got := r.Pick()
	if got != original[0].Worker {
		t.Fatalf("input mutation changed selection: %+v", got)
	}
	got.ID, got.Client = "caller mutation", nil
	if first := independent.Pick(); first != original[0].Worker {
		t.Fatalf("selectors share progress: %+v", first)
	}
	for _, index := range []int{1, 0, 0, 1, 0} {
		if selected := r.Pick(); selected != original[index].Worker {
			t.Fatalf("mutation or other selector changed sequence: %+v", selected)
		}
	}
}

// TestWeightedRoundRobinPreservesIdentity ID 原值保留；同一 Client 的不同 ID 不自动合并。
func TestWeightedRoundRobinPreservesIdentity(t *testing.T) {
	client := &selectionOnlyClient{name: "shared"}
	workers := []WeightedWorker{
		{Worker: Worker{ID: "A", Client: client}, Weight: 1},
		{Worker: Worker{ID: " A ", Client: client}, Weight: 1},
	}
	r := mustWeightedRoundRobin(t, workers)
	for i := 0; i < 10; i++ {
		if got, want := r.Pick(), workers[i%2].Worker; got != want {
			t.Fatalf("identity changed: got %+v want %+v", got, want)
		}
	}
}

// TestWeightedRoundRobinConcurrentSelections 检查并发下精确比例及后续选择位置，不假设调用方获锁顺序。
func TestWeightedRoundRobinConcurrentSelections(t *testing.T) {
	const callers, perCaller = 32, 700
	workers := weightedTestWorkers(5, 1, 1)
	r := mustWeightedRoundRobin(t, workers)
	indices := map[string]int{"A": 0, "B": 1, "C": 2}
	var counts [3]atomic.Int64
	var invalid atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perCaller {
				got := r.Pick()
				i, ok := indices[got.ID]
				if !ok || got != workers[i].Worker {
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
	for i, w := range workers {
		if got, want := counts[i].Load(), int64(callers*perCaller/7)*w.Weight; got != want {
			t.Errorf("Worker %s selected %d times, want %d", w.Worker.ID, got, want)
		}
	}
	for _, id := range "AABACAA" {
		if got := r.Pick(); got != workers[indices[string(id)]].Worker {
			t.Fatalf("post-concurrency sequence differs: got %+v want ID %c", got, id)
		}
	}
	t.Logf("callers=%d selections=%d counts=[%d %d %d]", callers, callers*perCaller, counts[0].Load(), counts[1].Load(), counts[2].Load())
}
