package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	developmentTokenSecret = "development-only-token-secret-change-me"
	developmentJWTSecret   = "development-only-jwt-secret-change-me"
)

type Config struct {
	Environment       string
	HTTPAddr          string
	MetricsAddr       string
	PublicWSBaseURL   string
	PeerAddr          string
	PeerAdvertiseAddr string
	ASRAddr           string
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	NodeID            string
	TokenSecret       string
	JWKSURL           string
	JWTSecret         string
	JWTIssuer         string
	JWTAudience       string
	AllowedOrigins    []string
	TenantMaxStreams  int
	MaxConnections    int64
	CreateRatePerMin  int
	ConnectRatePerMin int
	AttachTimeout     time.Duration
	DetachWindow      time.Duration
	MaxSessionAge     time.Duration
	EndedRetention    time.Duration
	OwnerLease        time.Duration
	OwnerRenew        time.Duration
	ASRTLSCA          string
	ASRTLSCert        string
	ASRTLSKey         string
	PeerTLSCA         string
	PeerTLSCert       string
	PeerTLSKey        string
	OTLPEndpoint      string
}

func Load() (Config, error) {
	hostname, _ := os.Hostname()
	redisDB, err := envInt("TIDE_REDIS_DB", 0)
	if err != nil {
		return Config{}, err
	}
	tenantMaxStreams, err := envInt("TIDE_TENANT_MAX_STREAMS", 1000)
	if err != nil {
		return Config{}, err
	}
	maxConnections, err := envInt("TIDE_MAX_CONNECTIONS", 10000)
	if err != nil {
		return Config{}, err
	}
	createRate, err := envInt("TIDE_CREATE_RATE_PER_MIN", 120)
	if err != nil {
		return Config{}, err
	}
	connectRate, err := envInt("TIDE_CONNECT_RATE_PER_MIN", 6000)
	if err != nil {
		return Config{}, err
	}
	attachTimeout, err := envDuration("TIDE_ATTACH_TIMEOUT", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	detachWindow, err := envDuration("TIDE_DETACH_WINDOW", 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxSessionAge, err := envDuration("TIDE_MAX_SESSION_AGE", 4*time.Hour)
	if err != nil {
		return Config{}, err
	}
	endedRetention, err := envDuration("TIDE_ENDED_RETENTION", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	ownerLease, err := envDuration("TIDE_OWNER_LEASE", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	ownerRenew, err := envDuration("TIDE_OWNER_RENEW", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	c := Config{
		Environment:       env("TIDE_ENV", "development"),
		HTTPAddr:          env("TIDE_HTTP_ADDR", ":8080"),
		MetricsAddr:       env("TIDE_METRICS_ADDR", ":8081"),
		PublicWSBaseURL:   os.Getenv("TIDE_PUBLIC_WS_BASE_URL"),
		PeerAddr:          env("TIDE_PEER_ADDR", ":9090"),
		PeerAdvertiseAddr: env("TIDE_PEER_ADVERTISE_ADDR", "127.0.0.1:9090"),
		ASRAddr:           env("TIDE_ASR_ADDR", "127.0.0.1:9091"),
		RedisAddr:         os.Getenv("TIDE_REDIS_ADDR"),
		RedisPassword:     os.Getenv("TIDE_REDIS_PASSWORD"),
		NodeID:            env("TIDE_NODE_ID", hostname),
		TokenSecret:       env("TIDE_TOKEN_SECRET", developmentTokenSecret),
		JWKSURL:           os.Getenv("TIDE_JWT_JWKS_URL"),
		JWTSecret:         env("TIDE_JWT_HS256_SECRET", developmentJWTSecret),
		JWTIssuer:         env("TIDE_JWT_ISSUER", "tide-dev"),
		JWTAudience:       env("TIDE_JWT_AUDIENCE", "tide-gateway"),
		AllowedOrigins:    split(env("TIDE_ALLOWED_ORIGINS", "localhost:*,127.0.0.1:*")),
		TenantMaxStreams:  tenantMaxStreams,
		MaxConnections:    int64(maxConnections),
		CreateRatePerMin:  createRate,
		ConnectRatePerMin: connectRate,
		AttachTimeout:     attachTimeout,
		DetachWindow:      detachWindow,
		MaxSessionAge:     maxSessionAge,
		EndedRetention:    endedRetention,
		OwnerLease:        ownerLease,
		OwnerRenew:        ownerRenew,
		ASRTLSCA:          os.Getenv("TIDE_ASR_TLS_CA"),
		ASRTLSCert:        os.Getenv("TIDE_ASR_TLS_CERT"),
		ASRTLSKey:         os.Getenv("TIDE_ASR_TLS_KEY"),
		PeerTLSCA:         os.Getenv("TIDE_PEER_TLS_CA"),
		PeerTLSCert:       os.Getenv("TIDE_PEER_TLS_CERT"),
		PeerTLSKey:        os.Getenv("TIDE_PEER_TLS_KEY"),
		OTLPEndpoint:      os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
	c.RedisDB = redisDB
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) Validate() error {
	if c.NodeID == "" || c.PeerAdvertiseAddr == "" || c.ASRAddr == "" {
		return errors.New("node id, peer advertise address, and ASR address are required")
	}
	if c.OwnerRenew <= 0 || c.OwnerLease <= c.OwnerRenew {
		return errors.New("owner lease must be longer than the positive renew interval")
	}
	if c.TenantMaxStreams <= 0 {
		return errors.New("tenant stream quota must be positive")
	}
	if c.MaxConnections <= 0 || c.CreateRatePerMin <= 0 || c.ConnectRatePerMin <= 0 {
		return errors.New("connection and stream creation limits must be positive")
	}
	if len(c.TokenSecret) < 32 {
		return errors.New("TIDE_TOKEN_SECRET must be at least 32 bytes")
	}
	if c.Environment == "production" {
		switch {
		case c.RedisAddr == "":
			return errors.New("TIDE_REDIS_ADDR is required in production")
		case c.TokenSecret == developmentTokenSecret:
			return errors.New("TIDE_TOKEN_SECRET must be explicitly configured in production")
		case c.PublicWSBaseURL == "":
			return errors.New("TIDE_PUBLIC_WS_BASE_URL is required in production")
		case c.JWKSURL == "":
			return errors.New("TIDE_JWT_JWKS_URL is required in production")
		case c.ASRTLSCA == "" || c.ASRTLSCert == "" || c.ASRTLSKey == "":
			return errors.New("ASR mTLS is required in production")
		case c.PeerTLSCA == "" || c.PeerTLSCert == "" || c.PeerTLSKey == "":
			return errors.New("peer mTLS is required in production")
		}
	}
	return nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	return parsed, nil
}

func split(value string) []string {
	var values []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
