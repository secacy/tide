package gateway

import (
	"context"
	"errors"
	"sync"
)

var (
	errInvalidSession     = errors.New("invalid session")
	errDuplicateSession   = errors.New("duplicate session")
	errRegistryStopping   = errors.New("session registry stopping")
	errSessionLimit       = errors.New("session limit reached")
	errInvalidMaxSessions = errors.New("max sessions must be greater than zero")
)

// sessionRegistry 管理已接纳、但尚未完成清理的会话。
//
// 所有实例必须通过 newSessionRegistry 创建。
// 注册表停止接入后不能重新开启，使用后不能复制。
// 连接关闭和会话取消由会话生命周期逻辑负责。
type sessionRegistry struct {
	// mu 保护 sessions、stopping 和 drained 的关闭判断。
	// 持锁期间不能进行网络操作或等待会话退出。
	mu sync.Mutex

	// sessions 保存当前仍受管理的会话。
	// 会话完成全部清理后，调用方才能将其注销。
	sessions map[string]*session

	// maxSessions 是允许同时登记的最大会话数。
	// 包括准备、运行和清理阶段，初始化后保持不变。
	maxSessions int

	// stopping 表示已经永久停止接纳新会话。
	// 它不表示已有会话已经退出。
	stopping bool

	// drained 是“停止接入且所有会话均已注销”的通知。
	//
	// 初始化为打开的 channel，不发送数据，只关闭一次。
	// 正常运行时，即使 sessions 暂时为空，也不能关闭它。
	drained chan struct{}
}

// newSessionRegistry 创建允许接入新会话的注册表。
//
// maxSessions 必须大于 0，否则返回错误。
// 必须初始化 sessions 和 drained。
func newSessionRegistry(maxSessions int) (*sessionRegistry, error) {
	if maxSessions <= 0 {
		return nil, errInvalidMaxSessions
	}
	return &sessionRegistry{
		sessions:    make(map[string]*session),
		maxSessions: maxSessions,
		drained:     make(chan struct{}),
	}, nil
}

// register 登记一个尚未登记的会话。
//
// 拒绝 nil 会话、空 ID、重复 ID、停止接入和容量不足。
// 登记失败时，不得改变已有记录。
//
// 检查接入状态、重复 ID、容量和插入记录，
// 必须在同一次加锁期间完成。
func (r *sessionRegistry) register(s *session) error {
	if s == nil || len(s.id) == 0 {
		return errInvalidSession
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[s.id]; ok {
		return errDuplicateSession
	}
	if r.stopping {
		return errRegistryStopping
	}
	if len(r.sessions) >= r.maxSessions {
		return errSessionLimit
	}
	r.sessions[s.id] = s
	return nil
}

// unregister 注销一个已经完成全部清理的会话。
//
// 只有 ID 对应的记录恰好是 s 时，才能删除。
// nil、不存在或对象不匹配时，不修改注册表。
// 重复注销同一个对象不会产生额外影响。
//
// 已停止接入且最后一条记录被删除时，关闭 drained。
func (r *sessionRegistry) unregister(s *session) {
	if s == nil || len(s.id) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	vs, ok := r.sessions[s.id]
	if !ok || vs != s {
		return
	}
	delete(r.sessions, s.id)
	if r.stopping && len(r.sessions) == 0 {
		close(r.drained)
	}
}

// snapshot 返回当前会话引用的快照，顺序不作保证。
//
// 返回新建的切片，不暴露内部 map。
// 调用方修改切片不会改变注册表，但切片中的会话对象仍共享。
// 获取引用不代表可以直接并发修改会话字段。
func (r *sessionRegistry) snapshot() []*session {
	r.mu.Lock()
	defer r.mu.Unlock()
	sessions := make([]*session, 0, len(r.sessions))
	for _, session := range r.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

// stopAccepting 永久停止接纳新会话，允许重复调用。
//
// 当前没有会话时，关闭 drained。
// 当前还有会话时，等待后续注销操作触发通知。
// 此方法不执行会话取消或网络关闭。
func (r *sessionRegistry) stopAccepting() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return
	}
	r.stopping = true
	if len(r.sessions) == 0 {
		close(r.drained)
	}
}

// wait 等待注册表进入“停止接入且全部会话已注销”的状态。
//
// 观察到 drained 关闭时返回 nil。
// 等待被 ctx 取消时返回 ctx.Err()。
// 取消等待不会取消会话或修改注册表。
//
// 调用方应先执行 stopAccepting；等待期间不能持有 mu。
func (r *sessionRegistry) wait(ctx context.Context) error {
	select {
	case <-r.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
