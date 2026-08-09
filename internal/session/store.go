package session

import (
	"context"
	"errors"
	"time"
)

type State string

const (
	StateCreated  State = "created"
	StateAttached State = "attached"
	StateDetached State = "detached"
	StateEnding   State = "ending"
	StateEnded    State = "ended"
	StateFailed   State = "failed"
)

var (
	ErrNotFound      = errors.New("stream not found")
	ErrExpired       = errors.New("stream expired")
	ErrForbidden     = errors.New("stream belongs to another tenant")
	ErrTokenConsumed = errors.New("attach token is expired or already consumed")
	ErrResumeExpired = errors.New("stream resume window has expired")
	ErrEnded         = errors.New("stream is already ended")
	ErrQuotaExceeded = errors.New("tenant stream quota exceeded")
	ErrOwnerConflict = errors.New("stream has another active owner")
)

type Session struct {
	ID            string
	TenantID      string
	LanguageCode  string
	State         State
	Generation    uint64
	Epoch         uint64
	OwnerID       string
	OwnerAddr     string
	OwnerLeaseEnd time.Time
	NextOffset    uint64
	TokenHash     string
	CreatedAt     time.Time
	ExpiresAt     time.Time
	DetachedUntil time.Time
}

type Store interface {
	Health(ctx context.Context) error
	Create(ctx context.Context, stream Session, tenantLimit int) error
	Get(ctx context.Context, streamID string) (Session, error)
	Attach(ctx context.Context, streamID, tenantID string, expectedGeneration uint64, tokenHash, nextTokenHash string, now time.Time, detachWindow time.Duration) (Session, error)
	RotateToken(ctx context.Context, streamID, tenantID, nextTokenHash string, now time.Time, detachWindow time.Duration) (Session, error)
	MarkDetached(ctx context.Context, streamID string, generation uint64, until time.Time) error
	UpdateOffset(ctx context.Context, streamID string, generation, nextOffset uint64) error
	AcquireOwner(ctx context.Context, streamID, nodeID, nodeAddr string, now time.Time, lease time.Duration) (stream Session, acquired bool, changed bool, err error)
	RenewOwner(ctx context.Context, streamID, nodeID string, now time.Time, lease time.Duration) error
	ReleaseOwner(ctx context.Context, streamID, nodeID string) error
	End(ctx context.Context, streamID, tenantID, reason string, retention time.Duration) error
	Close() error
}

func resumeExpired(stream Session, now time.Time, detachWindow time.Duration) bool {
	switch stream.State {
	case StateDetached:
		return stream.DetachedUntil.IsZero() || !now.Before(stream.DetachedUntil)
	case StateAttached:
		return !stream.OwnerLeaseEnd.IsZero() && !now.Before(stream.OwnerLeaseEnd.Add(detachWindow))
	default:
		return false
	}
}
