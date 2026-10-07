package gateway

import (
	"context"
	"errors"
	"sync"
)

var (
	errInvalidEntryGateConfig = errors.New("invalid entry gate config") // 缺少共享 tracker 或握手上限非正。
	errHandshakeLimit         = errors.New("handshake limit reached")   // 临时握手名额已满，不进入等待队列。
)

// entryGate 管理临时握手预算，并协调最终接管与停服。
// 构造后以指针使用，不得复制；方法允许并发调用。
type entryGate struct {
	mu sync.Mutex // 保护握手计数、停止状态和最终提交。

	// 与 Gateway 原 tracker 为同一对象。
	// 会话计数仍由 tracker 自己的锁保护。
	tracker *sessionTracker

	maxHandshakes    int  // 临时握手上限，构造后不变。
	activeHandshakes int  // 尚未交接或完成失败清理的入口数量。
	stopping         bool // 是否永久停止接入和提交。

	// stopping=true 且 activeHandshakes=0 时关闭一次。
	// 它只说明临时握手已退出。
	handshakesDrained chan struct{}
}

// newEntryGate 在 Gateway 开始接入前构造入口协调部件。
// tracker 必须非 nil，maxHandshakes 必须为正。
// 构造不修改 tracker，也不消耗名额。
func newEntryGate(tracker *sessionTracker, maxHandshakes int) (*entryGate, error) {
	if tracker == nil || maxHandshakes <= 0 {
		return nil, errInvalidEntryGateConfig
	}
	return &entryGate{
		tracker:           tracker,
		maxHandshakes:     maxHandshakes,
		handshakesDrained: make(chan struct{}),
	}, nil
}

// tryEnterHandshake 尝试占用一个临时握手名额。
// 停止返回 errGatewayStopping，满额返回 errHandshakeLimit。
// 失败不改变计数。
func (g *entryGate) tryEnterHandshake() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return errGatewayStopping
	}
	if g.activeHandshakes >= g.maxHandshakes {
		return errHandshakeLimit
	}
	g.activeHandshakes++
	return nil
}

// leaveHandshake 归还一次成功申请的临时名额。
// 交接结果已经确定，或失败清理已经完成后才能调用。
// 每次申请恰好对应一次释放。
func (g *entryGate) leaveHandshake() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.activeHandshakes == 0 {
		panic("gateway: no active handshake to leave")
	}
	g.activeHandshakes--
	if g.stopping && g.activeHandshakes == 0 {
		close(g.handshakesDrained)
	}
}

// tryEnterSession 仅用于 v2 新建。
// 调用方已经取得握手名额并完成合法 start 解析。
// 成功后，最终拥有者负责在清理完成后调用 tracker.leave。
func (g *entryGate) tryEnterSession() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return errGatewayStopping
	}
	// 锁顺序始终为 entryGate.mu → tracker.mu，不另存一份会话计数。
	return g.tracker.tryEnter()
}

// withCommit 在与停服互斥的短临界区执行最终提交。
// ctx 属于本次入口或附着操作。
// commit 只能执行短小的本地校验和状态提交。
// commit 禁止网络 I/O、等待任务或再次调用 entryGate。
// 调用方在提交结果确定前持续持有握手名额。
// nil ctx/commit 属于编程错误。成功提交后不因随后取消而改报失败。
func (g *entryGate) withCommit(ctx context.Context, commit func() error) error {
	if ctx == nil || commit == nil {
		panic("gateway: nil entry commit dependency")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return errGatewayStopping
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return commit()
}

// stopAccepting 永久停止新握手、新建占位及最终提交。
// 同时停止共享 tracker 接入；允许重复调用。
// 已有连接和 RPC 的取消仍由生命周期拥有者负责。
func (g *entryGate) stopAccepting() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return
	}
	g.stopping = true
	g.tracker.stopAccepting()
	if g.activeHandshakes == 0 {
		close(g.handshakesDrained)
	}
}

// wait 等待握手和逻辑会话两类资源都清理完成。
// 调用方先 stopAccepting；ctx 只限制本次等待。
// 两次等待共用同一预算，不持 gate 锁，不改变已有资源的生命周期。
func (g *entryGate) wait(ctx context.Context) error {
	select {
	case <-g.handshakesDrained:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.tracker.wait(ctx)
}
