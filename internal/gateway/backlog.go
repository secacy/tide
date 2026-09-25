package gateway

import (
	"errors"
	"fmt"
)

// ErrAudioBacklogExceeded 表示当前未确认处理音频量超过配置预算。
// 它是积压超限原因，不表示某次网络操作超时。
var ErrAudioBacklogExceeded = errors.New("audio backlog exceeded")

// checkPendingAudioLimit 检查一个有效计量快照是否超过未确认音频预算。
// maxBytes 的单位是原始音频字节，0 表示关闭限制。
// pendingBytes 等于预算时允许继续；严格大于预算时返回包装 ErrAudioBacklogExceeded 的错误，并包含实际未确认量与预算值。
// 本函数不修改计量、不等待、不取消会话；调用方负责后续控制。
func checkPendingAudioLimit(p audioProgressSnapshot, maxBytes uint64) error {
	if maxBytes == 0 {
		return nil
	}
	if p.pendingBytes <= maxBytes {
		return nil
	}
	return fmt.Errorf("%w (%d > %d)", ErrAudioBacklogExceeded, p.pendingBytes, maxBytes)
}
