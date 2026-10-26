package workerpool

import (
	"errors"
	"sync"
	"testing"
)

func mustCapacityPool(t *testing.T, limits ...int) (*CapacityRoundRobin, []WorkerCapacity) {
	t.Helper()
	workers := testWorkers()
	config := make([]WorkerCapacity, len(limits))
	for i, limit := range limits {
		config[i] = WorkerCapacity{Worker: workers[i], Limit: limit}
	}
	pool, err := NewCapacityRoundRobin(config)
	if err != nil {
		t.Fatal(err)
	}
	return pool, config
}

// acquireCapacity 校验 Worker/Client 配对；替身客户端在任何 RPC 调用时 panic。
func acquireCapacity(t *testing.T, pool *CapacityRoundRobin, want Worker) *WorkerLease {
	t.Helper()
	lease, err := pool.TryAcquire()
	if err != nil || lease == nil {
		t.Fatalf("acquire %s: lease=%v err=%v", want.ID, lease, err)
	}
	t.Cleanup(lease.Release)
	if got := lease.Worker(); got != want {
		t.Fatalf("acquire: got %+v, want %+v", got, want)
	}
	return lease
}

func requireCapacityFull(t *testing.T, pool *CapacityRoundRobin) {
	t.Helper()
	lease, err := pool.TryAcquire()
	if lease != nil {
		lease.Release()
		t.Fatal("acquired a lease from a full pool")
	}
	if !errors.Is(err, ErrNoWorkerCapacity) {
		t.Fatalf("full pool error: %v", err)
	}
}

func TestCapacityRoundRobinRejectsInvalidConfig(t *testing.T) {
	a, b := testWorkers()[0], testWorkers()[1]
	for _, tc := range []struct {
		name   string
		config []WorkerCapacity
	}{
		{"nil", nil},
		{"empty", []WorkerCapacity{}},
		{"empty_id", []WorkerCapacity{{Worker: Worker{Client: a.Client}, Limit: 1}}},
		{"blank_id", []WorkerCapacity{{Worker: Worker{ID: " \t\n", Client: a.Client}, Limit: 1}}},
		{"duplicate", []WorkerCapacity{{Worker: a, Limit: 1}, {Worker: a, Limit: 2}}},
		{"duplicate_later", []WorkerCapacity{{Worker: a, Limit: 1}, {Worker: b, Limit: 1}, {Worker: a, Limit: 1}}},
		{"nil_client", []WorkerCapacity{{Worker: Worker{ID: "A"}, Limit: 1}}},
		{"zero_limit", []WorkerCapacity{{Worker: a}}},
		{"negative_limit", []WorkerCapacity{{Worker: a, Limit: -1}}},
		{"invalid_later", []WorkerCapacity{{Worker: a, Limit: 1}, {Worker: b}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := NewCapacityRoundRobin(tc.config)
			if pool != nil || err == nil {
				t.Fatalf("invalid config accepted: pool=%v err=%v", pool, err)
			}
		})
	}
}

func TestCapacityRoundRobinSkipsFullEntriesAndReusesFreedWorker(t *testing.T) {
	pool, config := mustCapacityPool(t, 1, 1)
	a := acquireCapacity(t, pool, config[0].Worker)
	oldB := acquireCapacity(t, pool, config[1].Worker)
	requireCapacityFull(t, pool)
	oldB.Release()
	newB := acquireCapacity(t, pool, config[1].Worker)
	requireCapacityFull(t, pool)
	// 旧 B 凭证的归还动作已经执行；不能误归还新 B 的名额。
	oldB.Release()
	requireCapacityFull(t, pool)
	a.Release()
	newB.Release()
	acquireCapacity(t, pool, config[0].Worker)
	acquireCapacity(t, pool, config[1].Worker)
	requireCapacityFull(t, pool)
}

func TestCapacityRoundRobinHeterogeneousLimits(t *testing.T) {
	pool, config := mustCapacityPool(t, 2, 1, 3)
	// 上限是同时持有数，不是加权选择比例；每次仍从上个条目之后扫描。
	var leases []*WorkerLease
	for _, index := range []int{0, 1, 2, 0, 2, 2} {
		leases = append(leases, acquireCapacity(t, pool, config[index].Worker))
	}
	requireCapacityFull(t, pool)
	for _, lease := range leases {
		lease.Release()
	}
	// 最后选中 C，下一次从 A 开始；释放不修改选择顺序。
	acquireCapacity(t, pool, config[0].Worker)
	acquireCapacity(t, pool, config[1].Worker)
	acquireCapacity(t, pool, config[2].Worker)
}

func TestCapacityRoundRobinReleaseAndFailedAcquirePreserveCursor(t *testing.T) {
	t.Run("release", func(t *testing.T) {
		pool, config := mustCapacityPool(t, 1, 1, 1)
		acquireCapacity(t, pool, config[0].Worker).Release()
		acquireCapacity(t, pool, config[1].Worker)
		acquireCapacity(t, pool, config[2].Worker)
		acquireCapacity(t, pool, config[0].Worker)
	})
	t.Run("full", func(t *testing.T) {
		pool, config := mustCapacityPool(t, 1, 1, 1)
		var leases []*WorkerLease
		for _, worker := range config {
			leases = append(leases, acquireCapacity(t, pool, worker.Worker))
		}
		for range 2 {
			requireCapacityFull(t, pool)
		}
		for _, lease := range leases {
			lease.Release()
		}
		acquireCapacity(t, pool, config[0].Worker)
	})
}

func TestCapacityRoundRobinCopiesConfigAndReturnedWorker(t *testing.T) {
	pool, config := mustCapacityPool(t, 1, 1)
	wantA, wantB := config[0].Worker, config[1].Worker
	config[0].Worker = Worker{ID: "replacement"}
	config[0].Limit = 100
	config[1] = WorkerCapacity{}
	a := acquireCapacity(t, pool, wantA)
	returned := a.Worker()
	returned.ID, returned.Client = "changed", nil
	if a.Worker() != wantA {
		t.Fatal("changing returned Worker mutated the lease")
	}
	acquireCapacity(t, pool, wantB)
	requireCapacityFull(t, pool)
	a.Release()
	acquireCapacity(t, pool, wantA)
	requireCapacityFull(t, pool)
}

func TestCapacityRoundRobinPreservesOriginalIDs(t *testing.T) {
	client := &selectionOnlyClient{name: "shared"}
	config := []WorkerCapacity{
		{Worker: Worker{ID: "A", Client: client}, Limit: 1},
		{Worker: Worker{ID: " A ", Client: client}, Limit: 1},
	}
	pool, err := NewCapacityRoundRobin(config)
	if err != nil {
		t.Fatal(err)
	}
	for _, worker := range config {
		acquireCapacity(t, pool, worker.Worker)
	}
	requireCapacityFull(t, pool)
}

// 每轮同时争抢，全部调用返回前不释放；不依赖调度顺序或 sleep。
// 第二轮继续使用同一池，可检查归还后恢复容量及游标环绕。
func TestCapacityRoundRobinConcurrentAdmissionAndReuse(t *testing.T) {
	pool, config := mustCapacityPool(t, 2, 1, 3)
	const callers = 96
	type outcome struct {
		lease *WorkerLease
		err   error
	}
	for round := range 8 {
		start := make(chan struct{})
		outcomes := make(chan outcome, callers)
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				<-start
				lease, err := pool.TryAcquire()
				outcomes <- outcome{lease, err}
			})
		}
		close(start)
		wg.Wait()
		close(outcomes)
		counts := make(map[string]int)
		var leases []*WorkerLease
		failed := 0
		for got := range outcomes {
			if got.lease == nil {
				if !errors.Is(got.err, ErrNoWorkerCapacity) {
					t.Errorf("round %d: unexpected acquire error %v", round, got.err)
				}
				failed++
				continue
			}
			t.Cleanup(got.lease.Release)
			if got.err != nil {
				t.Errorf("round %d: returned both lease and error %v", round, got.err)
			}
			worker := got.lease.Worker()
			counts[worker.ID]++
			leases = append(leases, got.lease)
			valid := false
			for _, entry := range config {
				valid = valid || worker == entry.Worker
			}
			if !valid {
				t.Errorf("round %d: unconfigured Worker %+v", round, worker)
			}
		}
		if len(leases) != 6 || failed != callers-6 {
			t.Fatalf("round %d: acquired=%d rejected=%d", round, len(leases), failed)
		}
		for _, entry := range config {
			if counts[entry.Worker.ID] != entry.Limit {
				t.Fatalf("round %d: %s acquired=%d limit=%d", round, entry.Worker.ID, counts[entry.Worker.ID], entry.Limit)
			}
		}
		requireCapacityFull(t, pool)
		// 同时归还不同 Worker 的不同名额；池锁保护共享计数。
		for _, lease := range leases {
			wg.Go(lease.Release)
		}
		wg.Wait()
	}
}

func TestCapacityRoundRobinConcurrentReleaseCannotFreeNewLease(t *testing.T) {
	pool, config := mustCapacityPool(t, 1)
	old := acquireCapacity(t, pool, config[0].Worker)
	start := make(chan struct{})
	returned := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		old.Release()
		close(returned)
	})
	for range 64 {
		wg.Go(func() {
			<-start
			for range 16 {
				old.Release()
			}
		})
	}
	close(start)
	<-returned // 一次 Release 已真正返回，允许新持有者立即取得该名额。
	newLease := acquireCapacity(t, pool, config[0].Worker)
	wg.Wait()
	old.Release()
	requireCapacityFull(t, pool)
	newLease.Release()
	acquireCapacity(t, pool, config[0].Worker)
	requireCapacityFull(t, pool)
}
