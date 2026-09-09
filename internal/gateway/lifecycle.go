package gateway

import "context"

// StopAccepting 永久停止接纳新会话，允许并发和重复调用。
//
// 已成功登记的会话继续运行，包括尚未完成 WebSocket 升级的会话。
// 此方法不取消会话、不关闭连接，也不等待已有会话退出。
func (g *Gateway) StopAccepting() {
	g.registry.stopAccepting()
}

// Wait 等待 Gateway 停止接入，且所有已登记会话完成清理和注销。
//
// 调用方应先调用 StopAccepting，并传入非 nil 的 ctx。
// 等待被 ctx 取消时返回 ctx.Err()，只结束本次等待，不取消会话。
// 支持多个调用方同时等待，也支持一次等待超时后重新等待。
func (g *Gateway) Wait(ctx context.Context) error {
	return g.registry.wait(ctx)
}

// Abort 永久停止接纳新会话，并请求强制停止剩余会话。
// 允许并发和重复调用。
//
// 它会取消识别工作并尝试关闭底层连接，不等待会话完成注销。
// 调用方应随后使用 Wait 确认所有会话已完成清理。
func (g *Gateway) Abort() {
	g.StopAccepting()
	snapshots := g.registry.snapshot()
	for _, snapshot := range snapshots {
		snapshot.abort()
	}
}
