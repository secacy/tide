package gateway

import (
	"context"
	"errors"

	"github.com/coder/websocket"
)

var (
	errConnectionManagementUnavailable = errors.New("connection management unavailable") // 表示当前 runCoordinator 没有运行真实连接管理模式。
	errConnectionCandidateRequired     = errors.New("connection candidate required")     // 真实连接管理模式禁止只切换 generation 而不安装实际连接。
	errConnectionRetiring              = errors.New("connection retiring")               // 旧连接仍在执行 CloseNow 或等待任务退出。调用方可以在原恢复窗口内重新提交候选；本错误不续期。
	errUnexpectedConnectionStop        = errors.New("unexpected connection task stop")   // reader/writer 在当前 attached且本代 context 仍有效时无故停止。
	errInvalidConnectionEvent          = errors.New("invalid connection event")          // 当前代退出事件的任务标签、退出类型或必要错误非法。
)

// sessionConnections 运行期间仅由会话协调者修改。
// current 包含退出中的连接；其 done 和剩余事件处理完毕后才可清空。
// 协调者返回后，外层运行器接续读取 current 并等待最终清理。
type sessionConnections struct {
	current        *connectionAttachment // 包含正在退出的旧连接。
	outputComplete bool                  // 当前代 writer 正常结束输出；不代表整场确认。
	lastSeq        uint64                // 完成时的末尾序号，不等于已确认位置。
}

// recoverableConnectionFailure 判断传输失败是否允许进入 detached。
// 已知的远端协议/策略关闭不作为普通断线恢复。
func recoverableConnectionFailure(err error) bool {
	if err == nil {
		return false
	}

	status := websocket.CloseStatus(err)

	// 没有 WebSocket CloseStatus 的一般 I/O 错误和写超时，
	// 首版按可恢复传输失败处理。
	if status == -1 {
		return true
	}

	switch status {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return true

	default:
		return false
	}
}

// requestResumeConnection 把候选连接和客户端恢复位置放进同一次协调命令。
// 成功以后候选清理责任归会话；失败仍归调用者。
// appliedSeq 是客户端连续应用位置，必须在已确认/已授权范围内。
// 候选只提交一次；请求交付后等待明确回复，不能因 ctx 取消猜测是否接管。
func (s *resumableSession) requestResumeConnection(
	ctx context.Context,
	candidate *connectionCandidate,
	appliedSeq uint64,
) (uint64, error) {
	if err := validateConnectionCandidate(candidate); err != nil {
		return 0, err
	}

	result := s.submitCommand(ctx, sessionControlCommand{
		kind:      controlResumeConnection,
		resultSeq: appliedSeq,
		candidate: candidate,
	})

	if result.err != nil {
		return 0, result.err
	}

	return result.generation, nil
}
