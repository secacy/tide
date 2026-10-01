package main

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/secacy/tide-artisan/internal/mockasr"
)

// workerProcessingJSON 表示共享限制启用时的处理名额状态。
type workerProcessingJSON struct {
	Limit   int `json:"limit"`   // 实际名额上限，不是实测稳定容量。
	InUse   int `json:"in_use"`  // 已登记持有、尚未归还的名额数。
	Waiting int `json:"waiting"` // 尚未完成获取或取消收尾的等待请求数。
}

// workerSnapshotJSON 定义 Worker 查询接口的 v1 响应。
type workerSnapshotJSON struct {
	SchemaVersion          int                   `json:"schema_version"`           // 固定为 1。
	ProcessingLimitEnabled bool                  `json:"processing_limit_enabled"` // 共享限制是否启用。
	Processing             *workerProcessingJSON `json:"processing"`               // 未启用时为 null。
}

// workerSnapshotHandler 创建只读查询处理函数。
// worker 必须由 mockasr.New 成功创建。
// 每次请求读取一次快照，查询不占处理名额。
func workerSnapshotHandler(worker *mockasr.Worker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snapshot, enabled := worker.ProcessingSnapshot()

		response := workerSnapshotJSON{
			SchemaVersion:          1,
			ProcessingLimitEnabled: enabled,
		}

		if enabled {
			response.Processing = &workerProcessingJSON{
				Limit:   snapshot.Limit,
				InUse:   snapshot.InUse,
				Waiting: snapshot.Waiting,
			}
		}

		data, err := json.Marshal(response)
		if err != nil {
			slog.Error("marshal worker snapshot", "error", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)

		if _, err := w.Write(data); err != nil {
			slog.Error("write worker snapshot response", "error", err)
		}
	}
}

// routes 创建 Worker 的私有 HTTP 路由，不监听端口。
func routes(worker *mockasr.Worker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/worker", workerSnapshotHandler(worker))
	return mux
}
