package gateway

import "context"

// connectionReady 是协调者一次处理命令时取得的值快照。
// 不持有可变状态指针，取得后不会随会话继续运行而变化。
type connectionReady struct {
	sessionID      string // 稳定会话 ID。
	resumeToken    string // 恢复凭据，不得把整个快照记录到普通日志。
	generation     uint64 // 当前连接代次。
	nextOffset     uint64 // 连续音频接纳位置。
	inputEnded     bool   // 是否已接纳 end。
	ackedResultSeq uint64 // 累计结果确认位置。
}

// requestConnectionReady 查询当前 attached 代次的快照。
// ctx 控制本次请求；generation 来自固定写任务。
// 不授权结果、不修改状态；错误时返回零值快照。
func (s *resumableSession) requestConnectionReady(
	ctx context.Context,
	generation uint64,
) (connectionReady, error) {
	result := s.submitCommand(ctx, sessionControlCommand{
		kind:       controlConnectionReady,
		generation: generation,
	})

	if result.err != nil {
		return connectionReady{}, result.err
	}

	return result.ready, nil
}
