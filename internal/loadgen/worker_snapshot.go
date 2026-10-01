package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
)

const workerSnapshotMaxBytes = 4096

// WorkerProcessingState 是共享限制启用时的瞬时名额状态。
// 不代表活跃会话数、CPU 使用率或实测稳定容量。
type WorkerProcessingState struct {
	Limit   int // 实际名额上限，大于零。
	InUse   int // 已登记持有、尚未归还的名额数。
	Waiting int // 尚未完成获取或取消收尾的等待请求数。
}

// WorkerState 是经过协议校验的值快照。
// 只有 FetchWorkerSnapshot 返回 nil 错误时才代表成功观测。
type WorkerState struct {
	ProcessingLimitEnabled bool                  // 共享处理限制是否启用。
	Processing             WorkerProcessingState // 仅启用时有效，否则为零值占位。
}

// workerSnapshotResponse 保留必需字段的缺失、null 和实际值区别。
type workerSnapshotResponse struct {
	SchemaVersion          *int            `json:"schema_version"`
	ProcessingLimitEnabled *bool           `json:"processing_limit_enabled"`
	Processing             json.RawMessage `json:"processing"`
}

// workerProcessingResponse 使用指针区分合法零值与缺失/null。
type workerProcessingResponse struct {
	Limit   *int `json:"limit"`
	InUse   *int `json:"in_use"`
	Waiting *int `json:"waiting"`
}

// decodeWorkerSnapshot 解析并校验 Worker v1 JSON，不执行 I/O。
// data 已由调用方限制大小；失败返回零状态和错误。
func decodeWorkerSnapshot(data []byte) (WorkerState, error) {
	var response workerSnapshotResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return WorkerState{}, fmt.Errorf("decode worker snapshot: %w", err)
	}

	if response.SchemaVersion == nil {
		return WorkerState{}, errors.New("worker snapshot missing schema_version")
	}
	if *response.SchemaVersion != 1 {
		return WorkerState{}, fmt.Errorf("unsupported worker snapshot schema_version: %d", *response.SchemaVersion)
	}

	if response.ProcessingLimitEnabled == nil {
		return WorkerState{}, errors.New("worker snapshot missing processing_limit_enabled")
	}

	if len(response.Processing) == 0 {
		return WorkerState{}, errors.New("worker snapshot missing processing")
	}

	processingData := bytes.TrimSpace(response.Processing)
	processingIsNull := bytes.Equal(processingData, []byte("null"))

	if !*response.ProcessingLimitEnabled {
		if !processingIsNull {
			return WorkerState{}, errors.New("worker snapshot processing must be null when processing limit is disabled")
		}
		return WorkerState{}, nil
	}

	if processingIsNull {
		return WorkerState{}, errors.New("worker snapshot processing must be an object when processing limit is enabled")
	}

	var processing workerProcessingResponse
	if err := json.Unmarshal(processingData, &processing); err != nil {
		return WorkerState{}, fmt.Errorf("decode worker snapshot processing: %w", err)
	}

	if processing.Limit == nil {
		return WorkerState{}, errors.New("worker snapshot processing missing limit")
	}
	if processing.InUse == nil {
		return WorkerState{}, errors.New("worker snapshot processing missing in_use")
	}
	if processing.Waiting == nil {
		return WorkerState{}, errors.New("worker snapshot processing missing waiting")
	}

	if *processing.Limit <= 0 {
		return WorkerState{}, fmt.Errorf("worker snapshot processing limit must be > 0: %d", *processing.Limit)
	}
	if *processing.InUse < 0 {
		return WorkerState{}, fmt.Errorf("worker snapshot processing in_use must be >= 0: %d", *processing.InUse)
	}
	if *processing.InUse > *processing.Limit {
		return WorkerState{}, fmt.Errorf("worker snapshot processing in_use exceeds limit: in_use=%d limit=%d", *processing.InUse, *processing.Limit)
	}
	if *processing.Waiting < 0 {
		return WorkerState{}, fmt.Errorf("worker snapshot processing waiting must be >= 0: %d", *processing.Waiting)
	}

	return WorkerState{
		ProcessingLimitEnabled: true,
		Processing: WorkerProcessingState{
			Limit:   *processing.Limit,
			InUse:   *processing.InUse,
			Waiting: *processing.Waiting,
		},
	}, nil
}

// FetchWorkerSnapshot 执行一次 GET 并校验 Worker HTTP 快照。
// endpoint 是完整 URL；ctx 控制请求及响应读取，调用方应设置期限。
// client 由调用方创建并复用，本函数不修改它或关闭其连接池。
// ctx/client 不能为 nil；失败返回零状态和保留原因的错误。
func FetchWorkerSnapshot(
	ctx context.Context,
	client *http.Client,
	endpoint string,
) (WorkerState, error) {
	if ctx == nil {
		return WorkerState{}, errors.New("worker snapshot context must not be nil")
	}
	if client == nil {
		return WorkerState{}, errors.New("worker snapshot HTTP client must not be nil")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return WorkerState{}, fmt.Errorf("create worker snapshot request: %w", err)
	}

	// 不修改调用者共享的 Client。
	// 禁止自动跟随重定向，让 3xx 在下面作为非 200 响应处理。
	requestClient := *client
	requestClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	response, err := requestClient.Do(req)
	if err != nil {
		return WorkerState{}, fmt.Errorf("fetch worker snapshot: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return WorkerState{}, fmt.Errorf("worker snapshot returned HTTP status %d", response.StatusCode)
	}

	contentType := response.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return WorkerState{}, fmt.Errorf("parse worker snapshot Content-Type %q: %w", contentType, err)
	}
	if mediaType != "application/json" {
		return WorkerState{}, fmt.Errorf("worker snapshot Content-Type must be application/json, got %q", mediaType)
	}

	// 多读一个字节，才能区分：
	//
	//   恰好 4096 字节
	//   实际超过 4096 字节
	//
	// 不能只依赖 Content-Length，因为响应可能使用 chunked encoding，
	// Content-Length 也不是可信的协议边界。
	data, err := io.ReadAll(
		io.LimitReader(response.Body, workerSnapshotMaxBytes+1),
	)
	if err != nil {
		return WorkerState{}, fmt.Errorf("read worker snapshot response: %w", err)
	}

	if len(data) > workerSnapshotMaxBytes {
		return WorkerState{}, fmt.Errorf("worker snapshot response exceeds %d bytes", workerSnapshotMaxBytes)
	}

	state, err := decodeWorkerSnapshot(data)
	if err != nil {
		return WorkerState{}, err
	}

	return state, nil
}
