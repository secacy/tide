package gateway

import (
	"errors"
	"sync"
)

var (
	errInvalidResumableSession  = errors.New("invalid resumable session")
	errSessionAlreadyRegistered = errors.New("session already registered")
)

// sessionRegistry 按 ID 保存逻辑会话引用，支持并发调用。
// 必须通过构造器创建，使用后不能复制。
// 锁只保护映射，不保护会话内部状态。
type sessionRegistry struct {
	mu      sync.Mutex                   // 保护映射及复合操作。
	entries map[string]*resumableSession // 已登记的会话对象。
}

// newSessionRegistry 创建空注册表。
func newSessionRegistry() *sessionRegistry {
	return &sessionRegistry{
		entries: make(map[string]*resumableSession),
	}
}

// add 登记新会话；非法对象或重复 ID 返回错误。
// ID 已存在时保留原记录，即使传入的是同一指针。
func (r *sessionRegistry) add(s *resumableSession) error {
	if s == nil {
		return errInvalidResumableSession
	}
	if s.resume == nil {
		return errInvalidResumableSession
	}
	if len(s.identity.id) != sessionIDLength {
		return errInvalidResumableSession
	}
	if len(s.identity.resumeToken) != resumeTokenLength {
		return errInvalidResumableSession
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.entries[s.identity.id]; exists {
		return errSessionAlreadyRegistered
	}

	r.entries[s.identity.id] = s
	return nil
}

// lookup 精确查找 ID，缺失时返回 nil, false。
// 返回共享对象引用；找到对象不表示当前允许恢复。
func (r *sessionRegistry) lookup(id string) (*resumableSession, bool) {
	r.mu.Lock()
	s, ok := r.entries[id]
	r.mu.Unlock()

	return s, ok
}

// remove 仅在 id 当前对应 expected 对象时删除。
// 缺失、对象不匹配或 expected 为 nil 时返回 false。
// 删除映射不负责关闭会话或释放准入名额。
func (r *sessionRegistry) remove(
	id string,
	expected *resumableSession,
) bool {
	if expected == nil {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	current, ok := r.entries[id]
	if !ok {
		return false
	}
	if current != expected {
		return false
	}

	delete(r.entries, id)
	return true
}
