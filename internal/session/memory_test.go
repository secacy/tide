package session

import (
	"errors"
	"testing"
	"time"
)

func newTestSession(id, tenant, tokenHash string) Session {
	now := time.Now()
	return Session{
		ID: id, TenantID: tenant, State: StateCreated,
		TokenHash: tokenHash, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
}

func TestMemoryStoreTokenRotationAndReplacement(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Create(t.Context(), newTestSession("one", "clinic", "first"), 2); err != nil {
		t.Fatal(err)
	}
	stream, err := store.Attach(t.Context(), "one", "clinic", 0, "first", "second")
	if err != nil {
		t.Fatal(err)
	}
	if stream.Generation != 1 || stream.State != StateAttached {
		t.Fatalf("unexpected attached stream: %+v", stream)
	}
	if _, err := store.Attach(t.Context(), "one", "clinic", 0, "first", "replay"); !errors.Is(err, ErrTokenConsumed) {
		t.Fatalf("replayed token returned %v", err)
	}
	stream, err = store.Attach(t.Context(), "one", "clinic", 1, "second", "third")
	if err != nil || stream.Generation != 2 {
		t.Fatalf("replacement attach: stream=%+v err=%v", stream, err)
	}
}

func TestMemoryStoreOwnerFailoverIncrementsEpoch(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Create(t.Context(), newTestSession("one", "clinic", "token"), 2); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	stream, acquired, changed, err := store.AcquireOwner(t.Context(), "one", "node-a", "a:9090", now, 10*time.Second)
	if err != nil || !acquired || changed || stream.Epoch != 1 {
		t.Fatalf("initial owner: stream=%+v acquired=%v changed=%v err=%v", stream, acquired, changed, err)
	}
	stream, acquired, changed, err = store.AcquireOwner(t.Context(), "one", "node-b", "b:9090", now.Add(5*time.Second), 10*time.Second)
	if err != nil || acquired || changed || stream.OwnerID != "node-a" {
		t.Fatalf("active owner replaced: stream=%+v acquired=%v changed=%v err=%v", stream, acquired, changed, err)
	}
	stream, acquired, changed, err = store.AcquireOwner(t.Context(), "one", "node-b", "b:9090", now.Add(11*time.Second), 10*time.Second)
	if err != nil || !acquired || !changed || stream.OwnerID != "node-b" || stream.Epoch != 2 {
		t.Fatalf("failover: stream=%+v acquired=%v changed=%v err=%v", stream, acquired, changed, err)
	}
}

func TestMemoryStoreQuota(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Create(t.Context(), newTestSession("one", "clinic", "a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), newTestSession("two", "clinic", "b"), 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected quota error, got %v", err)
	}
	if err := store.End(t.Context(), "one", "clinic", "test", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), newTestSession("two", "clinic", "b"), 1); err != nil {
		t.Fatalf("quota not released: %v", err)
	}
}

func TestMemoryStoreRejectsExpiredSession(t *testing.T) {
	store := NewMemoryStore()
	stream := newTestSession("expired", "clinic", "token")
	stream.ExpiresAt = time.Now().Add(-time.Second)
	if err := store.Create(t.Context(), stream, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "token", "next"); !errors.Is(err, ErrExpired) {
		t.Fatalf("expected expiration error, got %v", err)
	}
}
