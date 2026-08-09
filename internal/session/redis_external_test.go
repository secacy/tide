package session

import (
	"os"
	"testing"
	"time"

	"github.com/secacy/tide/internal/id"
)

func TestExternalRedisLifecycle(t *testing.T) {
	addr := os.Getenv("TIDE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TIDE_TEST_REDIS_ADDR is not configured")
	}
	store := NewRedisStore(addr, "", 0)
	t.Cleanup(func() { _ = store.Close() })
	ctx := t.Context()
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	suffix, err := id.New()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	stream := Session{
		ID: "external-" + suffix, TenantID: "tenant-" + suffix,
		State: StateCreated, TokenHash: "initial",
		CreatedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := store.Create(ctx, stream, 1); err != nil {
		t.Fatal(err)
	}
	stream, err = store.Attach(ctx, stream.ID, stream.TenantID, 0, "initial", "next")
	if err != nil {
		t.Fatal(err)
	}
	stream, acquired, _, err := store.AcquireOwner(ctx, stream.ID, "node-a", "node-a:9090", now, 10*time.Second)
	if err != nil || !acquired || stream.Epoch != 1 {
		t.Fatalf("acquire owner: stream=%+v acquired=%v err=%v", stream, acquired, err)
	}
	if err := store.UpdateOffset(ctx, stream.ID, stream.Generation, 640); err != nil {
		t.Fatal(err)
	}
	if err := store.End(ctx, stream.ID, stream.TenantID, "test", time.Second); err != nil {
		t.Fatal(err)
	}
}
