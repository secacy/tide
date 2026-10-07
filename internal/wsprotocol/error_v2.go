package wsprotocol

// V2ErrorCode 表示稳定的入口拒绝类别。
// 客户端依据 Code 判断处理方式，不匹配 Message 文本。
type V2ErrorCode string

const (
	V2ErrorServiceStopping       V2ErrorCode = "service_stopping"        // 当前 Gateway 正在停止服务。
	V2ErrorHandshakeLimit        V2ErrorCode = "handshake_limit"         // 临时握手名额已满。
	V2ErrorSessionLimit          V2ErrorCode = "session_limit"           // 新建所需逻辑会话名额已满。
	V2ErrorInvalidHandshake      V2ErrorCode = "invalid_handshake"       // 首条消息不符合 v2 握手协议或超出上限。
	V2ErrorEntryTimeout          V2ErrorCode = "entry_timeout"           // 本次入口操作未在期限内交接。
	V2ErrorResumeUnavailable     V2ErrorCode = "resume_unavailable"      // 身份无效或恢复资格已终止，不区分认证与查找失败。
	V2ErrorSessionBusy           V2ErrorCode = "session_busy"            // 原连接仍附着或尚未完成实际清理。
	V2ErrorReplayGap             V2ErrorCode = "replay_gap"              // 恢复所需结果早于已释放的确认前缀。
	V2ErrorInvalidResumePosition V2ErrorCode = "invalid_resume_position" // 客户端恢复位置超过服务端结果授权范围。
	V2ErrorWorkerUnavailable     V2ErrorCode = "worker_unavailable"      // 本次新建无法建立 Worker RPC。
	V2ErrorInternalError         V2ErrorCode = "internal_error"          // 未识别的内部失败，公开消息不包含原错误详情。
)

// V2ErrorMessage 表示本次 v2 入口操作未交接成功。
// 恢复请求被拒绝，不代表原逻辑会话已经终止。
// 连接中断时不保证消息送达，缺少消息也不能证明交接结果。
type V2ErrorMessage struct {
	Type    MessageType `json:"type"`    // 固定为 error。
	Code    V2ErrorCode `json:"code"`    // 稳定的公开错误类别。
	Message string      `json:"message"` // 固定提示，不包含内部错误详情。
}
