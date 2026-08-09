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
	stream, err := store.Attach(t.Context(), "one", "clinic", 0, "first", "second", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Generation != 1 || stream.State != StateAttached {
		t.Fatalf("unexpected attached stream: %+v", stream)
	}
	if _, err := store.Attach(t.Context(), "one", "clinic", 0, "first", "replay", time.Now(), time.Minute); !errors.Is(err, ErrTokenConsumed) {
		t.Fatalf("replayed token returned %v", err)
	}
	stream, err = store.Attach(t.Context(), "one", "clinic", 1, "second", "third", time.Now(), time.Minute)
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
	if _, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "token", "next", time.Now(), time.Minute); !errors.Is(err, ErrExpired) {
		t.Fatalf("expected expiration error, got %v", err)
	}
}

func TestMemoryStoreResumeWindowAndTokenRotation(t *testing.T) {
	store := NewMemoryStore()
	stream := newTestSession("resume", "clinic", "first")
	if err := store.Create(t.Context(), stream, 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	attached, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "second", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDetached(t.Context(), stream.ID, attached.Generation, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RotateToken(t.Context(), stream.ID, stream.TenantID, "third", now, time.Minute); err != nil {
		t.Fatalf("rotate token inside resume window: %v", err)
	}
	if _, err := store.Attach(t.Context(), stream.ID, stream.TenantID, attached.Generation, "third", "fourth", now.Add(2*time.Second), time.Minute); !errors.Is(err, ErrResumeExpired) {
		t.Fatalf("attach outside resume window returned %v", err)
	}
	if _, err := store.RotateToken(t.Context(), stream.ID, stream.TenantID, "fifth", now.Add(2*time.Second), time.Minute); !errors.Is(err, ErrResumeExpired) {
		t.Fatalf("token rotation outside resume window returned %v", err)
	}
}

func TestMemoryStoreRejectsAttachAfterDeadOwnerWindow(t *testing.T) {
	store := NewMemoryStore()
	stream := newTestSession("dead-owner", "clinic", "first")
	if err := store.Create(t.Context(), stream, 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	attached, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "second", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, acquired, _, err := store.AcquireOwner(t.Context(), stream.ID, "node/boot", "node:9090", now, time.Second); err != nil || !acquired {
		t.Fatalf("acquire owner: acquired=%v err=%v", acquired, err)
	}
	if _, err := store.Attach(t.Context(), stream.ID, stream.TenantID, attached.Generation, "second", "third", now.Add(3*time.Second), time.Second); !errors.Is(err, ErrResumeExpired) {
		t.Fatalf("attach after dead owner window returned %v", err)
	}
}

func TestMemoryStoreOwnerChangeResetsAcceptedToCommitted(t *testing.T) {
	store := NewMemoryStore()
	stream := newTestSession("offset-reset", "clinic", "first")
	if err := store.Create(t.Context(), stream, 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	_, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "second", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, acquired, _, err := store.AcquireOwner(t.Context(), stream.ID, "node/boot-1", "node:9090", now, time.Second); err != nil || !acquired {
		t.Fatalf("acquire first owner: acquired=%v err=%v", acquired, err)
	}
	if err := store.UpdateAcceptedOffset(t.Context(), stream.ID, "node/boot-1", 1280); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateCommittedOffset(t.Context(), stream.ID, "node/boot-1", 640); err != nil {
		t.Fatal(err)
	}
	owned, acquired, changed, err := store.AcquireOwner(t.Context(), stream.ID, "node/boot-2", "node:9090", now.Add(2*time.Second), time.Second)
	if err != nil || !acquired || !changed || owned.AcceptedOffset != 640 || owned.CommittedOffset != 640 {
		t.Fatalf("owner change did not reset watermarks: stream=%+v acquired=%v changed=%v err=%v", owned, acquired, changed, err)
	}
	if err := store.UpdateCommittedOffset(t.Context(), stream.ID, "node/boot-1", 1280); !errors.Is(err, ErrOwnerConflict) {
		t.Fatalf("stale owner updated committed watermark: %v", err)
	}
}
