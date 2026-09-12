package gateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

// testWorkerPool 构造无需建流的容量测试池。
func testWorkerPool(t *testing.T, policy WorkerSelectionPolicy, capacities ...int) *WorkerPool {
	t.Helper()
	configs := make([]WorkerConfig, len(capacities))
	for i, c := range capacities {
		configs[i] = WorkerConfig{ID: fmt.Sprint(i), Client: &unusedGatewayWorker{}, Capacity: c}
	}
	p, err := NewWorkerPool(configs, policy)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWorkerPoolValidation(t *testing.T) {
	good := WorkerConfig{ID: "a", Client: &unusedGatewayWorker{}, Capacity: 1}
	for _, configs := range [][]WorkerConfig{nil, {{ID: "", Client: good.Client, Capacity: 1}}, {{ID: "a", Capacity: 1}}, {{ID: "a", Client: good.Client, Capacity: 0}}, {good, good}} {
		if _, err := NewWorkerPool(configs, RoundRobin); err == nil {
			t.Fatalf("accepted invalid config: %+v", configs)
		}
	}
	if _, err := NewWorkerPool([]WorkerConfig{good}, ""); err == nil {
		t.Fatal("missing policy accepted")
	}
	if _, err := NewWithPool(context.Background(), &WorkerPool{}, nil, Config{MaxSessions: 1}); err == nil {
		t.Fatal("zero pool accepted")
	}
	configs := []WorkerConfig{good}
	p, err := NewWorkerPool(configs, RoundRobin)
	if err != nil {
		t.Fatal(err)
	}
	configs[0].Capacity = 100
	s := p.Snapshot()
	s[0].Reserved = 100
	if got := p.Snapshot()[0]; got.Capacity != 1 || got.Reserved != 0 {
		t.Fatal("configuration or snapshot aliases pool")
	}
}

func TestWorkerPoolSelectionAndDrain(t *testing.T) {
	for _, policy := range []WorkerSelectionPolicy{RoundRobin, LeastReservedRatio} {
		t.Run(string(policy), func(t *testing.T) {
			p := testWorkerPool(t, policy, 2, 6)
			var leases []*WorkerLease
			for range 4 {
				l, err := p.TryAcquire()
				if err != nil {
					t.Fatal(err)
				}
				leases = append(leases, l)
			}
			want := 2
			if policy == LeastReservedRatio {
				want = 1
			}
			if p.Snapshot()[0].Reserved != want {
				t.Fatalf("wrong distribution: %+v", p.Snapshot())
			}
			if err := p.StopAccepting("0"); err != nil {
				t.Fatal(err)
			}
			if err := p.StopAccepting("missing"); !errors.Is(err, ErrUnknownWorker) {
				t.Fatal(err)
			}
			for _, l := range leases {
				l.Release()
			}
			for range 6 {
				l, err := p.TryAcquire()
				if err != nil {
					t.Fatal(err)
				}
				if l.WorkerID() != "1" {
					t.Fatal("drained Worker selected")
				}
				leases = append(leases, l)
			}
			if _, err := p.TryAcquire(); !errors.Is(err, ErrNoWorkerCapacity) {
				t.Fatal("full pool admitted")
			}
			for _, l := range leases {
				l.Release()
			}
			for _, s := range p.Snapshot() {
				if s.Reserved != 0 {
					t.Fatal(s)
				}
			}
		})
	}
}

func TestWorkerPoolConcurrentReservationsAndRelease(t *testing.T) {
	for _, policy := range []WorkerSelectionPolicy{RoundRobin, LeastReservedRatio} {
		t.Run(string(policy), func(t *testing.T) {
			p := testWorkerPool(t, policy, 3, 7)
			start := make(chan struct{})
			out := make(chan *WorkerLease, 128)
			var wg sync.WaitGroup
			for range 128 {
				wg.Go(func() {
					<-start
					l, err := p.TryAcquire()
					if err != nil && !errors.Is(err, ErrNoWorkerCapacity) {
						t.Error(err)
					}
					out <- l
				})
			}
			close(start)
			wg.Wait()
			close(out)
			var leases []*WorkerLease
			for l := range out {
				if l != nil {
					leases = append(leases, l)
				}
			}
			if len(leases) != 10 {
				t.Fatalf("admitted %d, want 10", len(leases))
			}
			for _, s := range p.Snapshot() {
				if s.Reserved != s.Capacity {
					t.Fatal(s)
				}
			}
			for _, l := range leases {
				for range 8 {
					wg.Go(l.Release)
				}
			}
			wg.Wait()
			for _, s := range p.Snapshot() {
				if s.Reserved != 0 {
					t.Fatal(s)
				}
			}
		})
	}
}

func TestWorkerPoolStopRacesWithAcquire(t *testing.T) {
	p := testWorkerPool(t, LeastReservedRatio, 16)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 100 {
				l, err := p.TryAcquire()
				if err == nil {
					l.Release()
				}
			}
		})
	}
	if err := p.StopAccepting("0"); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if l, err := p.TryAcquire(); l != nil || !errors.Is(err, ErrNoWorkerCapacity) {
			t.Fatalf("new lease after stop: %v %v", l, err)
		}
	}
	wg.Wait()
	if s := p.Snapshot()[0]; s.Reserved != 0 || s.Accepting {
		t.Fatal(s)
	}
}

func TestWorkerRatioLargeCapacity(t *testing.T) {
	a := workerState{cfg: WorkerConfig{Capacity: math.MaxInt}, reserved: math.MaxInt - 2}
	b := workerState{cfg: WorkerConfig{Capacity: math.MaxInt}, reserved: math.MaxInt - 1}
	if !lessReservedRatio(&a, &b) || lessReservedRatio(&b, &a) {
		t.Fatal("large ratio comparison overflowed or rounded away the difference")
	}
}
