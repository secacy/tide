package gateway

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestNewSessionRegistry(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(fmt.Sprintf("invalid_limit_%d", limit), func(t *testing.T) {
			r, err := newSessionRegistry(limit)
			if r != nil || !errors.Is(err, errInvalidMaxSessions) {
				t.Fatalf("newSessionRegistry(%d) = (%v, %v), want nil and invalid limit", limit, r, err)
			}
		})
	}

	r := newTestRegistry(t, 1)
	assertRegistrySessions(t, r)
	assertRegistryNotDrained(t, r)
	registerTestSession(t, r, "s1")
}

// 登记被拒绝时，原有会话与剩余容量都必须保持不变。
func TestSessionRegistryRegisterValidation(t *testing.T) {
	r := newTestRegistry(t, 2)
	first := registerTestSession(t, r, "s1")
	for _, tc := range []struct {
		name string
		s    *session
		want error
	}{
		{"nil_session", nil, errInvalidSession},
		{"empty_id", &session{}, errInvalidSession},
		{"same_object", first, errDuplicateSession},
		{"same_id_different_object", &session{id: "s1"}, errDuplicateSession},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := r.register(tc.s); !errors.Is(err, tc.want) {
				t.Fatalf("register() = %v, want %v", err, tc.want)
			}
			assertRegistrySessions(t, r, first)
		})
	}

	second := registerTestSession(t, r, "s2")
	if err := r.register(&session{id: "s3"}); !errors.Is(err, errSessionLimit) {
		t.Fatalf("register at capacity = %v, want session limit", err)
	}
	assertRegistrySessions(t, r, first, second)
}

// 注销必须匹配具体对象；只有相同 ID 不能代表同一个登记。
func TestSessionRegistryUnregisterIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		candidate  func(*session) *session
		wantRemain bool
	}{
		{"nil", func(*session) *session { return nil }, true},
		{"empty_id", func(*session) *session { return &session{} }, true},
		{"unknown_id", func(*session) *session { return &session{id: "unknown"} }, true},
		{"different_object", func(s *session) *session { return &session{id: s.id} }, true},
		{"registered_object", func(s *session) *session { return s }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRegistry(t, 1)
			s := registerTestSession(t, r, "s1")
			r.unregister(tc.candidate(s))
			if tc.wantRemain {
				assertRegistrySessions(t, r, s)
			} else {
				assertRegistrySessions(t, r)
			}
		})
	}
}

// 名额在注销后可复用；旧对象迟到的清理不能删除替换后的对象。
func TestSessionRegistryCapacityReuseAndStaleUnregister(t *testing.T) {
	r := newTestRegistry(t, 1)
	old := registerTestSession(t, r, "s1")
	r.unregister(old)
	r.unregister(old)
	assertRegistrySessions(t, r)
	assertRegistryNotDrained(t, r) // 暂时为空不代表停止接入。

	replacement := registerTestSession(t, r, "s1")
	r.unregister(old)
	assertRegistrySessions(t, r, replacement)
}

func TestSessionRegistrySnapshotIsIndependent(t *testing.T) {
	r := newTestRegistry(t, 2)
	first := registerTestSession(t, r, "s1")
	second := registerTestSession(t, r, "s2")
	assertRegistrySessions(t, r, first, second)

	view := r.snapshot()
	view[0] = &session{id: "outside"}
	view = append(view, nil)
	assertRegistrySessions(t, r, first, second)
}

func TestSessionRegistryStopEmpty(t *testing.T) {
	r := newTestRegistry(t, 1)
	r.stopAccepting()
	r.stopAccepting()
	if err := r.register(&session{id: "late"}); !errors.Is(err, errRegistryStopping) {
		t.Fatalf("register after stop = %v, want registry stopping", err)
	}
	assertRegistrySessions(t, r)
	assertRegistryDrained(t, r)
	assertRegistryWaitCompleted(t, r)
}

// 所有等待者都必须等到最后一个会话注销；等待本身不能阻塞注销。
func TestSessionRegistryWaitForAllSessions(t *testing.T) {
	r := newTestRegistry(t, 2)
	first := registerTestSession(t, r, "s1")
	second := registerTestSession(t, r, "s2")
	r.stopAccepting()
	r.stopAccepting()
	assertRegistrySessions(t, r, first, second)
	assertRegistryNotDrained(t, r)
	if err := r.register(&session{id: "late"}); !errors.Is(err, errRegistryStopping) {
		t.Fatalf("register after stop = %v, want registry stopping", err)
	}

	const waiters = 8
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	results := make(chan error, waiters)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- r.wait(ctx)
		}()
	}

	r.unregister(first)
	assertRegistrySessions(t, r, second)
	assertRegistryNotDrained(t, r)
	r.unregister(second)
	assertRegistrySessions(t, r)
	assertRegistryDrained(t, r)
	for range waiters {
		if err := <-results; err != nil {
			t.Fatalf("wait() = %v, want successful completion", err)
		}
	}
	r.unregister(second)
	r.stopAccepting()
	assertRegistryWaitCompleted(t, r)
}

// 等待取消或超时只结束本次等待，不能移除会话或改变停止接入状态。
func TestSessionRegistryWaitContext(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline_exceeded"
		}
		t.Run(name, func(t *testing.T) {
			r := newTestRegistry(t, 1)
			s := registerTestSession(t, r, "s1")
			r.stopAccepting()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			want := context.Canceled
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				want = context.DeadlineExceeded
			}
			if err := r.wait(ctx); !errors.Is(err, want) {
				t.Fatalf("wait() = %v, want %v", err, want)
			}
			assertRegistrySessions(t, r, s)
			assertRegistryNotDrained(t, r)
			if err := r.register(&session{id: "late"}); !errors.Is(err, errRegistryStopping) {
				t.Fatalf("register after canceled wait = %v, want registry stopping", err)
			}
		})
	}
}

// 所有登记尝试完成前不释放名额，以验证并发准入不会突破上限。
func TestSessionRegistryConcurrentCapacity(t *testing.T) {
	const limit, callers = 8, 64
	r := newTestRegistry(t, limit)
	start := make(chan struct{})
	type result struct {
		s   *session
		err error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := &session{id: fmt.Sprintf("s%d", i)}
			<-start
			results <- result{s, r.register(s)}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var admitted []*session
	for result := range results {
		if result.err == nil {
			admitted = append(admitted, result.s)
		} else if !errors.Is(result.err, errSessionLimit) {
			t.Fatalf("register() = %v, want success or session limit", result.err)
		}
	}
	if len(admitted) != limit {
		t.Fatalf("admitted = %d, want %d", len(admitted), limit)
	}
	assertRegistrySessions(t, r, admitted...)
}

// 并发停止与登记允许两种先后顺序，但每个成功登记都必须仍被跟踪。
func TestSessionRegistryConcurrentRegisterAndStop(t *testing.T) {
	const callers = 32
	r := newTestRegistry(t, callers+1)
	first := registerTestSession(t, r, "existing")
	start := make(chan struct{})
	admitted := make(chan *session, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := &session{id: fmt.Sprintf("s%d", i)}
			<-start
			if err := r.register(s); err == nil {
				admitted <- s
			} else if !errors.Is(err, errRegistryStopping) {
				t.Errorf("register() = %v, want success or registry stopping", err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		r.stopAccepting()
	}()
	close(start)
	wg.Wait()
	close(admitted)
	want := []*session{first}
	for s := range admitted {
		want = append(want, s)
	}
	assertRegistrySessions(t, r, want...)
	assertRegistryNotDrained(t, r)
	if err := r.register(&session{id: "after_stop"}); !errors.Is(err, errRegistryStopping) {
		t.Fatalf("register after stop returned = %v, want registry stopping", err)
	}
	for _, s := range want {
		r.unregister(s)
	}
	assertRegistrySessions(t, r)
	assertRegistryWaitCompleted(t, r)
}

// 多个停止调用与最后一批注销竞争时，退出通知也只能关闭一次。
func TestSessionRegistryConcurrentStopAndUnregister(t *testing.T) {
	const count = 32
	r := newTestRegistry(t, count)
	var sessions []*session
	for i := range count {
		sessions = append(sessions, registerTestSession(t, r, fmt.Sprintf("s%d", i)))
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r.unregister(s)
			r.unregister(s)
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r.stopAccepting()
		}()
	}
	close(start)
	wg.Wait()
	assertRegistrySessions(t, r)
	assertRegistryWaitCompleted(t, r)
}

// 快照与登记并发执行，既验证引用完整性，也让 -race 检查 map 的所有读取。
func TestSessionRegistrySnapshotConcurrentRegistration(t *testing.T) {
	const writers, perWriter, readers = 4, 32, 4
	r := newTestRegistry(t, writers*perWriter)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range perWriter {
				s := &session{id: fmt.Sprintf("%d-%d", writer, i)}
				if err := r.register(s); err != nil {
					t.Errorf("register() = %v", err)
				}
				runtime.Gosched()
			}
		}()
	}
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perWriter * 2 {
				seen := make(map[string]bool)
				for _, s := range r.snapshot() {
					if s == nil || s.id == "" {
						t.Error("snapshot contains an invalid session")
						continue
					}
					if seen[s.id] {
						t.Errorf("snapshot contains duplicate session %q", s.id)
					}
					seen[s.id] = true
				}
				runtime.Gosched()
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := len(r.snapshot()); got != writers*perWriter {
		t.Fatalf("registered sessions = %d, want %d", got, writers*perWriter)
	}
}

// newTestRegistry 创建测试用注册表；测试中的 session 不建立网络连接。
func newTestRegistry(t *testing.T, limit int) *sessionRegistry {
	t.Helper()
	r, err := newSessionRegistry(limit)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func registerTestSession(t *testing.T, r *sessionRegistry, id string) *session {
	t.Helper()
	s := &session{id: id}
	if err := r.register(s); err != nil {
		t.Fatalf("register(%q): %v", id, err)
	}
	return s
}

// assertRegistrySessions 按 ID 和对象身份比较快照，不依赖 map 遍历顺序。
func assertRegistrySessions(t *testing.T, r *sessionRegistry, want ...*session) {
	t.Helper()
	got := r.snapshot()
	if len(got) != len(want) {
		t.Fatalf("snapshot has %d sessions, want %d", len(got), len(want))
	}
	expected := make(map[string]*session, len(want))
	for _, s := range want {
		expected[s.id] = s
	}
	for _, s := range got {
		if s == nil {
			t.Fatal("snapshot contains nil session")
		}
		if expected[s.id] != s {
			t.Fatalf("snapshot contains unexpected object for ID %q", s.id)
		}
		delete(expected, s.id)
	}
}

func assertRegistryNotDrained(t *testing.T, r *sessionRegistry) {
	t.Helper()
	select {
	case <-r.drained:
		t.Fatal("registry announced completion before stopping and removing every session")
	default:
	}
}

func assertRegistryDrained(t *testing.T, r *sessionRegistry) {
	t.Helper()
	select {
	case <-r.drained:
	default:
		t.Fatal("registry did not announce completion after stopping and removing every session")
	}
}

func assertRegistryWaitCompleted(t *testing.T, r *sessionRegistry) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.wait(ctx); err != nil {
		t.Fatalf("wait() = %v, want successful completion", err)
	}
}
