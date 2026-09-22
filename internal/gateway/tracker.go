package gateway

import (
	"context"
	"errors"
	"sync"
)

// sessionTracker 跟踪已接纳但尚未完成清理的会话数量。
//
// 它负责接入登记、退出计数、停止接入和等待全部退出。
// 会话内部的取消与资源清理由 Session 自己负责。
//
// 必须通过 newSessionTracker 初始化，使用后不能复制。
type sessionTracker struct {
	// mu 保护 active、stopping 和关闭 drained 的判断。
	mu sync.Mutex

	// active 表示已成功登记、但尚未完成清理的会话数量。
	// 包括连接升级、等待 start、识别和收尾阶段。
	active int

	// stopping 表示已经永久停止接纳新会话。
	stopping bool

	// drained 在 stopping 为 true 且 active 为 0 时关闭。
	// 它只用于通知全部退出，不发送数据，且只能关闭一次。
	drained chan struct{}

	// maxActive 是允许同时登记的会话上限。
	// 构造时设置，使用期间不修改。
	maxActive int
}

var (
	// errGatewayStopping 表示 Gateway 已永久停止接入。
	errGatewayStopping = errors.New("service is stopping")

	// errSessionLimit 表示尚未清理完成的会话数量已达到上限。
	errSessionLimit = errors.New("session limit reached")
)

// newSessionTracker 创建一个允许接入新会话的跟踪器。
// maxActive 必须为正值，由 Gateway.New 校验后传入。
func newSessionTracker(maxActive int) *sessionTracker {
	return &sessionTracker{
		drained:   make(chan struct{}),
		maxActive: maxActive,
	}
}

// tryEnter 尝试登记一个会话。
// 成功返回 nil，并增加 active；失败返回拒绝原因，不改变计数。
// 每次成功登记都必须在资源清理后对应一次 leave。
func (t *sessionTracker) tryEnter() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return errGatewayStopping
	}
	if t.active >= t.maxActive {
		return errSessionLimit
	}
	t.active++
	return nil
}

// leave 表示一个已登记会话完成全部清理。
// 减少计数；如果已经停止接入且计数归零，则关闭 drained。
// 只能与成功的 tryEnter 配对，不能提前或重复调用。
func (t *sessionTracker) leave() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == 0 {
		panic("no active session but leave")
	}
	t.active--
	if t.stopping && t.active == 0 {
		close(t.drained)
	}
}

// stopAccepting 永久停止接纳新会话，允许重复调用。
// 如果当前没有活跃会话，立即关闭 drained。
// 此方法不会主动取消已有会话。
func (t *sessionTracker) stopAccepting() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return
	}
	t.stopping = true
	if t.active == 0 {
		close(t.drained)
	}
}

// wait 等待停止接入且所有已登记会话退出。
// 观察到 drained 关闭时返回 nil；等待被取消时返回 ctx.Err()。
// 调用方应先调用 stopAccepting。
// 等待取消不会改变跟踪器状态，也不会取消已有会话。
// 允许多个调用者同时等待，等待期间不持有 mu。
func (t *sessionTracker) wait(ctx context.Context) error {
	select {
	case <-t.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
