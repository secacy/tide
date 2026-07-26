package config

import (
	"strings"
	"testing"
	"time"
)

func TestProductionRequiresExternalSecurityConfiguration(t *testing.T) {
	cfg := Config{
		Environment:       "production",
		NodeID:            "node-a",
		PeerAdvertiseAddr: "node-a:9090",
		ASRAddr:           "asr:9091",
		RedisAddr:         "redis:6379",
		PublicWSBaseURL:   "wss://gateway.example.com",
		TokenSecret:       developmentTokenSecret,
		JWKSURL:           "https://identity.example.com/jwks",
		TenantMaxStreams:  1,
		MaxConnections:    1,
		CreateRatePerMin:  1,
		ConnectRatePerMin: 1,
		OwnerRenew:        time.Second,
		OwnerLease:        2 * time.Second,
		ASRTLSCA:          "ca",
		ASRTLSCert:        "cert",
		ASRTLSKey:         "key",
		PeerTLSCA:         "ca",
		PeerTLSCert:       "cert",
		PeerTLSKey:        "key",
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "TOKEN_SECRET") {
		t.Fatalf("expected production token secret error, got %v", err)
	}
}

func TestLoadRejectsMalformedEnvironment(t *testing.T) {
	t.Setenv("TIDE_MAX_CONNECTIONS", "many")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "TIDE_MAX_CONNECTIONS") {
		t.Fatalf("expected a named parse error, got %v", err)
	}
}
