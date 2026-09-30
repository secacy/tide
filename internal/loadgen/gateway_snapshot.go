package loadgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
)

const gatewaySnapshotMaxBytes = 4096

// GatewayState 是从合法 HTTP 快照响应读取的 Gateway 状态。
// 只有 FetchGatewaySnapshot 返回 nil 错误时，才代表有效观测。
type GatewayState struct {
	ActiveSessions int  // 已接纳但尚未完成清理的会话数
	MaxSessions    int  // 配置的接纳上限，不是实测容量
	Stopping       bool // 表示已永久停止接纳新会话
}

// gatewaySnapshotResponse 对应查询接口 v1。
// 使用指针区分合法的 0/false 与字段缺失或 null。
type gatewaySnapshotResponse struct {
	SchemaVersion  *int  `json:"schema_version"`
	ActiveSessions *int  `json:"active_sessions"`
	MaxSessions    *int  `json:"max_sessions"`
	Stopping       *bool `json:"stopping"`
}

// FetchGatewaySnapshot 执行一次 GET，校验 HTTP 响应与快照 v1 格式。
// endpoint 是完整查询 URL；ctx 控制请求和响应读取的期限与取消。
// client 由调用方创建并复用，本函数不修改它或关闭其连接池。
// 失败返回零 GatewayState 和错误；零返回值不能当作有效样本。
// ctx/client 不能为 nil；调用方应为每次查询设置期限。
func FetchGatewaySnapshot(
	ctx context.Context,
	client *http.Client,
	endpoint string,
) (GatewayState, error) {
	if ctx == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: context is nil")
	}
	if client == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: client is nil")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: create request: %w", err)
	}

	requestClient := *client
	requestClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	resp, err := requestClient.Do(req)
	if err != nil {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: unexpected HTTP status %s", resp.Status)
	}

	contentType := resp.Header.Get("Content-Type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: parse content type %q: %w", contentType, err)
	}
	if mediaType != "application/json" {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: unexpected content type %q", mediaType)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, gatewaySnapshotMaxBytes+1))
	if err != nil {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: read response: %w", err)
	}
	if len(data) > gatewaySnapshotMaxBytes {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: response exceeds %d bytes", gatewaySnapshotMaxBytes)
	}

	var snapshot gatewaySnapshotResponse
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: decode response: %w", err)
	}

	if snapshot.SchemaVersion == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: schema_version is missing or null")
	}
	if snapshot.ActiveSessions == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: active_sessions is missing or null")
	}
	if snapshot.MaxSessions == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: max_sessions is missing or null")
	}
	if snapshot.Stopping == nil {
		return GatewayState{}, errors.New("fetch gateway snapshot: stopping is missing or null")
	}

	if *snapshot.SchemaVersion != 1 {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: unsupported schema_version %d", *snapshot.SchemaVersion)
	}
	if *snapshot.MaxSessions <= 0 {
		return GatewayState{}, fmt.Errorf("fetch gateway snapshot: max_sessions must be greater than zero: %d", *snapshot.MaxSessions)
	}
	if *snapshot.ActiveSessions < 0 || *snapshot.ActiveSessions > *snapshot.MaxSessions {
		return GatewayState{}, fmt.Errorf(
			"fetch gateway snapshot: active_sessions is %d, want range [0, %d]",
			*snapshot.ActiveSessions,
			*snapshot.MaxSessions,
		)
	}

	return GatewayState{
		ActiveSessions: *snapshot.ActiveSessions,
		MaxSessions:    *snapshot.MaxSessions,
		Stopping:       *snapshot.Stopping,
	}, nil
}
