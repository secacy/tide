package gateway

import (
	"context"
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
}

// newSessionTracker 创建一个允许接入新会话的跟踪器。
// 初始计数为零，drained 已创建但尚未关闭。
func newSessionTracker() *sessionTracker {
	return &sessionTracker{
		drained: make(chan struct{}),
	}
}

// tryEnter 尝试登记一个会话。
// 成功时增加计数并返回 true；停止接入后返回 false。
// 每次成功调用，必须在清理完成后恰好对应一次 leave。
func (t *sessionTracker) tryEnter() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopping {
		return false
	}
	t.active++
	return true
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
