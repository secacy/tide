package gateway

// GatewaySnapshot 是单个 Gateway 在一次加锁读取时的活动状态。
// 返回值不引用内部可变数据，取出后不会随 Gateway 运行而变化。
type GatewaySnapshot struct {
	// ActiveSessions 是已接纳但尚未完成清理的会话数。
	// 包括连接升级、等待 start、识别和收尾阶段。
	ActiveSessions int

	// MaxSessions 是实际采用的会话接纳上限，已包含默认值处理。
	// 它是配置限制，不是实测稳定容量。
	MaxSessions int

	// Stopping 表示已永久停止接纳新会话。
	// 为 true 时，已有会话仍可能运行或清理中。
	Stopping bool
}

// Snapshot 返回本 Gateway 的一致状态快照，允许并发调用。
// 不等待会话结束，也不改变会话状态；g 必须由 New 成功创建。
func (g *Gateway) Snapshot() GatewaySnapshot {
	return g.tracker.snapshot()
}
