package gateway

import (
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func assertRegisteredSession(t *testing.T, r *sessionRegistry, id string, want *resumableSession) {
	t.Helper()
	got, ok := r.lookup(id)
	if got != want || ok != (want != nil) {
		t.Fatalf("lookup reference mismatch: found=%v, want found=%v", ok, want != nil)
	}
}

func TestSessionRegistryLookupIsExact(t *testing.T) {
	r := newSessionRegistry()
	s := newTestResumableSession(t, identityTestMaterial())
	assertRegisteredSession(t, r, s.identity.id, nil)
	if err := r.add(s); err != nil {
		t.Fatal(err)
	}
	assertRegisteredSession(t, r, s.identity.id, s)
	for _, id := range []string{"", "missing", " " + s.identity.id, s.identity.id + " ", strings.ToUpper(s.identity.id)} {
		assertRegisteredSession(t, r, id, nil)
	}
	// 相同身份在另一个注册表中没有隐式登记。
	assertRegisteredSession(t, newSessionRegistry(), s.identity.id, nil)
}

func TestSessionRegistryRejectsInvalidObjects(t *testing.T) {
	for _, name := range []string{"nil", "zero", "nil_state", "empty_id", "short_id", "long_id", "empty_token", "short_token", "long_token"} {
		t.Run(name, func(t *testing.T) {
			r := newSessionRegistry()
			sentinel := newTestResumableSession(t, identityTestMaterial())
			if err := r.add(sentinel); err != nil {
				t.Fatal(err)
			}
			s := newTestResumableSession(t, identityTestMaterial())
			switch name {
			case "nil":
				s = nil
			case "zero":
				s = &resumableSession{}
			case "nil_state":
				s.resume = nil
			case "empty_id":
				s.identity.id = ""
			case "short_id":
				s.identity.id = s.identity.id[:31]
			case "long_id":
				s.identity.id += "0"
			case "empty_token":
				s.identity.resumeToken = ""
			case "short_token":
				s.identity.resumeToken = s.identity.resumeToken[:42]
			case "long_token":
				s.identity.resumeToken += "A"
			}
			if err := r.add(s); !errors.Is(err, errInvalidResumableSession) {
				t.Fatalf("add invalid object = %v", err)
			}
			assertRegisteredSession(t, r, sentinel.identity.id, sentinel)
			// 所有调用均已返回，这里不存在并发访问。
			if len(r.entries) != 1 {
				t.Fatal("invalid insertion changed registry size")
			}
		})
	}
}

func TestSessionRegistryDuplicateDoesNotOverwrite(t *testing.T) {
	r := newSessionRegistry()
	material := identityTestMaterial()
	a := newTestResumableSession(t, material)
	material[16] ^= 255 // 同 ID、不同凭据的另一个对象。
	b := newTestResumableSession(t, material)
	if err := r.add(a); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []*resumableSession{a, b} {
		if err := r.add(candidate); !errors.Is(err, errSessionAlreadyRegistered) {
			t.Fatalf("duplicate add = %v", err)
		}
		assertRegisteredSession(t, r, a.identity.id, a)
	}
}

func TestSessionRegistryConditionalRemoveAndLateCleanup(t *testing.T) {
	r := newSessionRegistry()
	material := identityTestMaterial()
	a := newTestResumableSession(t, material)
	b := newTestResumableSession(t, material) // 内容相同，指针不同。
	material[0] ^= 255
	other := newTestResumableSession(t, material)
	for _, s := range []*resumableSession{a, other} {
		if err := r.add(s); err != nil {
			t.Fatal(err)
		}
	}
	if r.remove(a.identity.id, nil) || r.remove("missing", a) || r.remove(a.identity.id, b) || r.remove(other.identity.id, a) {
		t.Fatal("wrong expected object must not remove an entry")
	}
	assertRegisteredSession(t, r, a.identity.id, a)
	if !r.remove(a.identity.id, a) || r.remove(a.identity.id, a) {
		t.Fatal("first removal must succeed and repetition must fail")
	}
	assertRegisteredSession(t, r, a.identity.id, nil)
	// 移除仅改变映射，调用方持有的状态仍保持原值。
	assertResumeState(t, a.resume, resumeAttached, 1, time.Time{})
	if err := r.add(b); err != nil {
		t.Fatal(err)
	}
	if r.remove(a.identity.id, a) {
		t.Fatal("late cleanup removed the replacement")
	}
	assertRegisteredSession(t, r, b.identity.id, b)
	assertRegisteredSession(t, r, other.identity.id, other)
	if !r.remove(b.identity.id, b) || !r.remove(other.identity.id, other) {
		t.Fatal("matching references must remove their own entries")
	}
}

func TestSessionRegistryConcurrentSameIDHasOneWinner(t *testing.T) {
	const callers = 32
	r := newSessionRegistry()
	sessions := make([]*resumableSession, callers)
	for i := range sessions {
		sessions[i] = newTestResumableSession(t, identityTestMaterial())
	}
	start := make(chan struct{})
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for _, s := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- r.add(s)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, errSessionAlreadyRegistered) {
			t.Fatalf("unexpected add result: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful registrations = %d, want 1", wins)
	}
	id := sessions[0].identity.id
	winner, ok := r.lookup(id)
	if !ok || winner == nil {
		t.Fatal("winner missing")
	}
	belongs := false
	for _, s := range sessions {
		belongs = belongs || s == winner
	}
	if !belongs {
		t.Fatal("registry did not retain an original reference")
	}
	// 每个清理者只持有自己的对象，恰好获胜者能删除。
	start = make(chan struct{})
	removed := make(chan bool, callers)
	for _, s := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			removed <- r.remove(id, s)
		}()
	}
	close(start)
	wg.Wait()
	close(removed)
	wins = 0
	for success := range removed {
		if success {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("successful removals = %d, want 1", wins)
	}
	assertRegisteredSession(t, r, id, nil)
}

func TestSessionRegistryConcurrentIndependentLifecycles(t *testing.T) {
	const callers, rounds = 16, 32
	r := newSessionRegistry()
	sentinel := newTestResumableSession(t, identityTestMaterial())
	if err := r.add(sentinel); err != nil {
		t.Fatal(err)
	}
	sessions := make([][]*resumableSession, callers)
	for i := range sessions {
		for j := range rounds {
			material := identityTestMaterial()
			binary.LittleEndian.PutUint64(material[:8], uint64(i+1))
			binary.LittleEndian.PutUint64(material[8:16], uint64(j+1))
			sessions[i] = append(sessions[i], newTestResumableSession(t, material))
		}
	}
	start := make(chan struct{})
	failures := make(chan string, callers)
	var wg sync.WaitGroup
	for _, owned := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, s := range owned {
				if err := r.add(s); err != nil {
					failures <- "independent add failed"
					return
				}
				got, ok := r.lookup(s.identity.id)
				if !ok || got != s {
					failures <- "lookup lost own reference"
					return
				}
				if r.remove(sentinel.identity.id, s) || !r.remove(s.identity.id, s) {
					failures <- "conditional removal violated ownership"
					return
				}
				got, ok = r.lookup(s.identity.id)
				if ok || got != nil {
					failures <- "own entry remains after removal"
					return
				}
				got, ok = r.lookup(sentinel.identity.id)
				if !ok || got != sentinel {
					failures <- "another key was affected"
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	assertRegisteredSession(t, r, sentinel.identity.id, sentinel)
	if !r.remove(sentinel.identity.id, sentinel) || len(r.entries) != 0 {
		t.Fatal("registry must be empty after all lifecycles finish")
	}
}
