package session

import (
	"context"
	"sync"
	"time"
)

type MemoryStore struct {
	mu       sync.Mutex
	sessions map[string]Session
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{sessions: make(map[string]Session)}
}

func (s *MemoryStore) Health(context.Context) error { return nil }

func (s *MemoryStore) Create(_ context.Context, stream Session, tenantLimit int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	count := 0
	for id, existing := range s.sessions {
		if now.After(existing.ExpiresAt) {
			delete(s.sessions, id)
			continue
		}
		if existing.TenantID == stream.TenantID && existing.State != StateEnded && existing.State != StateFailed {
			count++
		}
	}
	if count >= tenantLimit {
		return ErrQuotaExceeded
	}
	s.sessions[stream.ID] = stream
	return nil
}

func (s *MemoryStore) Get(_ context.Context, streamID string) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return Session{}, ErrNotFound
	}
	if time.Now().After(stream.ExpiresAt) {
		delete(s.sessions, streamID)
		return Session{}, ErrExpired
	}
	return stream, nil
}

func (s *MemoryStore) Attach(_ context.Context, streamID, tenantID string, expectedGeneration uint64, tokenHash, nextTokenHash string) (Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return Session{}, ErrNotFound
	}
	if time.Now().After(stream.ExpiresAt) {
		return Session{}, ErrExpired
	}
	if stream.TenantID != tenantID {
		return Session{}, ErrForbidden
	}
	if stream.State == StateEnded || stream.State == StateFailed {
		return Session{}, ErrEnded
	}
	if stream.Generation != expectedGeneration || stream.TokenHash != tokenHash {
		return Session{}, ErrTokenConsumed
	}
	stream.Generation++
	stream.TokenHash = nextTokenHash
	stream.State = StateAttached
	stream.DetachedUntil = time.Time{}
	s.sessions[streamID] = stream
	return stream, nil
}

func (s *MemoryStore) MarkDetached(_ context.Context, streamID string, generation uint64, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return ErrNotFound
	}
	if stream.Generation != generation || stream.State == StateEnded {
		return nil
	}
	stream.State = StateDetached
	stream.DetachedUntil = until
	s.sessions[streamID] = stream
	return nil
}

func (s *MemoryStore) UpdateOffset(_ context.Context, streamID string, generation, nextOffset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return ErrNotFound
	}
	if stream.Generation == generation && nextOffset > stream.NextOffset {
		stream.NextOffset = nextOffset
		s.sessions[streamID] = stream
	}
	return nil
}

func (s *MemoryStore) AcquireOwner(_ context.Context, streamID, nodeID, nodeAddr string, now time.Time, lease time.Duration) (Session, bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return Session{}, false, false, ErrNotFound
	}
	if now.After(stream.ExpiresAt) {
		return Session{}, false, false, ErrExpired
	}
	if stream.State == StateEnded || stream.State == StateFailed {
		return Session{}, false, false, ErrEnded
	}
	if stream.OwnerID != "" && stream.OwnerID != nodeID && stream.OwnerLeaseEnd.After(now) {
		return stream, false, false, nil
	}
	changed := stream.OwnerID != "" && stream.OwnerID != nodeID
	if stream.OwnerID == "" {
		stream.Epoch = 1
	} else if changed {
		stream.Epoch++
	}
	stream.OwnerID = nodeID
	stream.OwnerAddr = nodeAddr
	stream.OwnerLeaseEnd = now.Add(lease)
	s.sessions[streamID] = stream
	return stream, true, changed, nil
}

func (s *MemoryStore) RenewOwner(_ context.Context, streamID, nodeID string, now time.Time, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return ErrNotFound
	}
	if stream.OwnerID != nodeID {
		return ErrOwnerConflict
	}
	if stream.State == StateEnded || stream.State == StateFailed {
		return ErrEnded
	}
	if now.After(stream.ExpiresAt) {
		return ErrExpired
	}
	stream.OwnerLeaseEnd = now.Add(lease)
	s.sessions[streamID] = stream
	return nil
}

func (s *MemoryStore) ReleaseOwner(_ context.Context, streamID, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return ErrNotFound
	}
	if stream.OwnerID != nodeID {
		return ErrOwnerConflict
	}
	stream.OwnerLeaseEnd = time.Time{}
	s.sessions[streamID] = stream
	return nil
}

func (s *MemoryStore) End(_ context.Context, streamID, tenantID, _ string, retention time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.sessions[streamID]
	if !ok {
		return nil
	}
	if tenantID != "" && stream.TenantID != tenantID {
		return ErrForbidden
	}
	stream.State = StateEnded
	stream.ExpiresAt = time.Now().Add(retention)
	stream.DetachedUntil = time.Time{}
	s.sessions[streamID] = stream
	return nil
}

func (s *MemoryStore) Close() error { return nil }
