package main

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/secacy/tide-artisan/internal/gateway"
)

// gatewaySnapshotJSON 是 Gateway 状态查询接口的 v1 响应。
// 与内部快照分开定义，避免 HTTP 格式影响会话管理。
type gatewaySnapshotJSON struct {
	SchemaVersion  int  `json:"schema_version"`  // 本接口格式版本，固定为 1。
	ActiveSessions int  `json:"active_sessions"` // 已接纳、尚未完成清理的会话数。
	MaxSessions    int  `json:"max_sessions"`    // 实际采用的会话接纳上限。
	Stopping       bool `json:"stopping"`        // 是否已永久停止接纳新会话。
}

// gatewaySnapshotHandler 创建只读的状态查询处理函数。
// g 必须由 gateway.New 成功创建。
// 每个请求读取一次快照，查询本身不占用会话名额。
func gatewaySnapshotHandler(g *gateway.Gateway) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot := g.Snapshot()

		response := gatewaySnapshotJSON{
			SchemaVersion:  1,
			ActiveSessions: snapshot.ActiveSessions,
			MaxSessions:    snapshot.MaxSessions,
			Stopping:       snapshot.Stopping,
		}

		data, err := json.Marshal(response)
		if err != nil {
			slog.Error("marshal gateway snapshot", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)

		if _, err := w.Write(data); err != nil {
			slog.Error("write gateway snapshot response", "error", err)
		}
	}
}
