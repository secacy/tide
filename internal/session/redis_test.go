package session

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisStoreLifecycleAndOwnerLease(t *testing.T) {
	redisServer := miniredis.RunT(t)
	store := NewRedisStore(redisServer.Addr(), "", 0)
	t.Cleanup(func() { _ = store.Close() })

	stream := newTestSession("redis-one", "clinic", "first")
	if err := store.Create(t.Context(), stream, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), newTestSession("redis-two", "clinic", "other"), 1); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected quota error, got %v", err)
	}
	attached, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "second", time.Now(), time.Minute)
	if err != nil || attached.Generation != 1 {
		t.Fatalf("attach: stream=%+v err=%v", attached, err)
	}
	if _, err := store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "replay", time.Now(), time.Minute); !errors.Is(err, ErrTokenConsumed) {
		t.Fatalf("token replay returned %v", err)
	}

	now := time.Now()
	owned, acquired, changed, err := store.AcquireOwner(t.Context(), stream.ID, "node-a", "a:9090", now, 10*time.Second)
	if err != nil || !acquired || changed || owned.Epoch != 1 {
		t.Fatalf("initial owner: stream=%+v acquired=%v changed=%v err=%v", owned, acquired, changed, err)
	}
	if err := store.ReleaseOwner(t.Context(), stream.ID, "node-a"); err != nil {
		t.Fatal(err)
	}
	owned, acquired, changed, err = store.AcquireOwner(t.Context(), stream.ID, "node-b", "b:9090", now.Add(time.Second), 10*time.Second)
	if err != nil || !acquired || !changed || owned.Epoch != 2 {
		t.Fatalf("owner handoff: stream=%+v acquired=%v changed=%v err=%v", owned, acquired, changed, err)
	}
	if err := store.UpdateOffset(t.Context(), stream.ID, attached.Generation, 640); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Get(t.Context(), stream.ID)
	if err != nil || loaded.NextOffset != 640 {
		t.Fatalf("stored offset: stream=%+v err=%v", loaded, err)
	}
	if err := store.End(t.Context(), stream.ID, "clinic", "test", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(t.Context(), newTestSession("redis-two", "clinic", "other"), 1); err != nil {
		t.Fatalf("quota was not released: %v", err)
	}
}

func TestRedisStoreResumeWindowAndTokenRotation(t *testing.T) {
	redisServer := miniredis.RunT(t)
	store := NewRedisStore(redisServer.Addr(), "", 0)
	t.Cleanup(func() { _ = store.Close() })

	stream := newTestSession("redis-resume", "clinic", "first")
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

func TestRedisStoreRejectsAttachAfterDeadOwnerWindow(t *testing.T) {
	redisServer := miniredis.RunT(t)
	store := NewRedisStore(redisServer.Addr(), "", 0)
	t.Cleanup(func() { _ = store.Close() })
	stream := newTestSession("redis-dead-owner", "clinic", "first")
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
