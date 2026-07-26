package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Identity struct {
	TenantID string
	Subject  string
}

type Claims struct {
	TenantID string `json:"tenant_id"`
	jwt.RegisteredClaims
}

type Verifier struct {
	secret   []byte
	issuer   string
	audience string
	jwks     *jwksCache
}

func NewVerifier(secret, jwksURL, issuer, audience string) *Verifier {
	verifier := &Verifier{
		secret: []byte(secret), issuer: issuer, audience: audience,
	}
	if jwksURL != "" {
		verifier.jwks = &jwksCache{
			url:    jwksURL,
			client: &http.Client{Timeout: 3 * time.Second},
			keys:   make(map[string]*rsa.PublicKey),
		}
	}
	return verifier
}

func (v *Verifier) Verify(ctx context.Context, raw string) (Identity, error) {
	if raw == "" {
		return Identity{}, errors.New("missing bearer token")
	}
	claims := &Claims{}
	options := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
	}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		if v.jwks != nil {
			if token.Method.Alg() != "RS256" {
				return nil, fmt.Errorf("unexpected signing algorithm %s", token.Method.Alg())
			}
			kid, _ := token.Header["kid"].(string)
			return v.jwks.key(ctx, kid)
		}
		if token.Method.Alg() != "HS256" {
			return nil, fmt.Errorf("unexpected signing algorithm %s", token.Method.Alg())
		}
		return v.secret, nil
	}, options...)
	if err != nil {
		return Identity{}, fmt.Errorf("invalid bearer token: %w", err)
	}
	if !token.Valid {
		return Identity{}, errors.New("invalid bearer token")
	}
	if claims.TenantID == "" || claims.Subject == "" {
		return Identity{}, errors.New("token requires tenant_id and subject claims")
	}
	return Identity{TenantID: claims.TenantID, Subject: claims.Subject}, nil
}

func (v *Verifier) SignDevelopment(identity Identity, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		TenantID: identity.TenantID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: v.issuer, Subject: identity.Subject,
			Audience:  jwt.ClaimStrings{v.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(v.secret)
}

func Bearer(header string) string {
	scheme, value, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(value)
}

type jwksCache struct {
	mu        sync.Mutex
	url       string
	client    *http.Client
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
}

type jwkSet struct {
	Keys []struct {
		KID string `json:"kid"`
		KTY string `json:"kty"`
		Alg string `json:"alg"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func (c *jwksCache) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key := c.keys[kid]; key != nil && time.Now().Before(c.expiresAt) {
		return key, nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch JWKS: HTTP %d", response.StatusCode)
	}
	var set jwkSet
	if err := json.NewDecoder(response.Body).Decode(&set); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, item := range set.Keys {
		if item.KTY != "RSA" || (item.Alg != "" && item.Alg != "RS256") {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(item.N)
		if err != nil {
			continue
		}
		e, err := base64.RawURLEncoding.DecodeString(item.E)
		if err != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		exponent := 0
		for _, value := range e {
			exponent = exponent<<8 + int(value)
		}
		keys[item.KID] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exponent}
	}
	c.keys = keys
	c.expiresAt = time.Now().Add(10 * time.Minute)
	key := keys[kid]
	if key == nil {
		return nil, fmt.Errorf("JWKS has no key %q", kid)
	}
	return key, nil
}
