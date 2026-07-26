package auth

import (
	"testing"
	"time"
)

func TestStreamTokenRoundTrip(t *testing.T) {
	service := NewStreamTokenService("01234567890123456789012345678901")
	raw, hash, _, err := service.Issue("stream-1", "tenant-1", TokenResume, 3, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := service.Verify(raw)
	if err != nil {
		t.Fatal(err)
	}
	if claims.StreamID != "stream-1" || claims.TenantID != "tenant-1" ||
		claims.Generation != 3 || claims.Epoch != 2 || claims.Kind != TokenResume {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	if HashTokenID(claims.ID) != hash {
		t.Fatal("stored token hash does not match jti")
	}
}

func TestBearer(t *testing.T) {
	if got := Bearer("Bearer abc.def"); got != "abc.def" {
		t.Fatalf("got %q", got)
	}
	if got := Bearer("Basic abc"); got != "" {
		t.Fatalf("accepted non-bearer token %q", got)
	}
}

func TestAccessJWT(t *testing.T) {
	verifier := NewVerifier("01234567890123456789012345678901", "", "issuer", "audience")
	raw, err := verifier.SignDevelopment(Identity{TenantID: "clinic", Subject: "doctor"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if identity.TenantID != "clinic" || identity.Subject != "doctor" {
		t.Fatalf("unexpected identity: %+v", identity)
	}
}
