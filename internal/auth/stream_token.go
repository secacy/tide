package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/secacy/tide/internal/id"
)

const (
	TokenAttach = "attach"
	TokenResume = "resume"
)

type StreamClaims struct {
	StreamID   string `json:"stream_id"`
	TenantID   string `json:"tenant_id"`
	Kind       string `json:"kind"`
	Generation uint64 `json:"generation"`
	Epoch      uint64 `json:"epoch"`
	jwt.RegisteredClaims
}

type StreamTokenService struct {
	secret []byte
	issuer string
}

func NewStreamTokenService(secret string) *StreamTokenService {
	return &StreamTokenService{secret: []byte(secret), issuer: "tide-gateway"}
}

func (s *StreamTokenService) Issue(streamID, tenantID, kind string, generation, epoch uint64, ttl time.Duration) (raw, tokenHash string, expiresAt time.Time, err error) {
	jti, err := id.New()
	if err != nil {
		return "", "", time.Time{}, err
	}
	raw, expiresAt, err = s.IssueWithID(streamID, tenantID, kind, jti, generation, epoch, ttl)
	return raw, HashTokenID(jti), expiresAt, err
}

func (s *StreamTokenService) NewTokenID() (jti, tokenHash string, err error) {
	jti, err = id.New()
	if err != nil {
		return "", "", err
	}
	return jti, HashTokenID(jti), nil
}

func (s *StreamTokenService) IssueWithID(streamID, tenantID, kind, jti string, generation, epoch uint64, ttl time.Duration) (raw string, expiresAt time.Time, err error) {
	now := time.Now()
	expiresAt = now.Add(ttl)
	claims := StreamClaims{
		StreamID: streamID, TenantID: tenantID, Kind: kind,
		Generation: generation, Epoch: epoch,
		RegisteredClaims: jwt.RegisteredClaims{
			ID: jti, Issuer: s.issuer, Subject: tenantID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}
	raw, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign stream token: %w", err)
	}
	return raw, expiresAt, nil
}

func (s *StreamTokenService) Verify(raw string) (StreamClaims, error) {
	claims := StreamClaims{}
	token, err := jwt.ParseWithClaims(raw, &claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected signing algorithm %s", token.Method.Alg())
		}
		return s.secret, nil
	}, jwt.WithExpirationRequired(), jwt.WithIssuer(s.issuer))
	if err != nil {
		return StreamClaims{}, fmt.Errorf("invalid stream token: %w", err)
	}
	if !token.Valid {
		return StreamClaims{}, errors.New("invalid stream token")
	}
	if claims.StreamID == "" || claims.TenantID == "" || claims.ID == "" {
		return StreamClaims{}, errors.New("stream token is missing required claims")
	}
	if claims.Kind != TokenAttach && claims.Kind != TokenResume {
		return StreamClaims{}, errors.New("unknown stream token kind")
	}
	return claims, nil
}

func HashTokenID(jti string) string {
	sum := sha256.Sum256([]byte(jti))
	return hex.EncodeToString(sum[:])
}
