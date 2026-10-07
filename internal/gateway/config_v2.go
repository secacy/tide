package gateway

import (
	"fmt"
	"time"
)

// V2Config 描述可恢复实验入口的固定预算；零值使用默认值，负值非法。
// 它不改变 v1 的输入协议和连接生命周期。
type V2Config struct {
	MaxHandshakes          int           // 尚未交接或完成失败清理的临时连接上限。
	EntryTimeout           time.Duration // 整次入口操作的提交预算，不重置各步骤的总期限。
	ResumeWindow           time.Duration // 实际断开后允许接回原会话的窗口。
	ResultRetentionTimeout time.Duration // Worker 正常完成后的固定结果保留期限。
	WorkerStatusTimeout    time.Duration // 上传 EOF 后等待 Recv 真实终态的期限。
	MaxAudioBytes          uint64        // 含在途块的音频存储预算。
	MaxAudioChunks         int           // 含在途块的音频条目预算。
	MaxResultBytes         uint64        // 未确认结果字符串字节预算。
	MaxResults             int           // 未确认结果条目预算。
}

const defaultMaxHandshakes = 64

// normalizeV2Config 复制并归一化配置；nil 表示禁用。
// 返回值不引用调用者的配置，maxMessageBytes 已由 New 填充默认值。
func normalizeV2Config(input *V2Config, maxMessageBytes int64) (*V2Config, error) {
	if input == nil {
		return nil, nil
	}
	c := *input
	if maxMessageBytes < 9 {
		return nil, fmt.Errorf("v2 message limit must be at least 9 bytes")
	}
	for _, n := range []int{c.MaxHandshakes, c.MaxAudioChunks, c.MaxResults} {
		if n < 0 {
			return nil, fmt.Errorf("v2 count limit is invalid")
		}
	}
	for _, d := range []time.Duration{c.EntryTimeout, c.ResumeWindow, c.ResultRetentionTimeout, c.WorkerStatusTimeout} {
		if d < 0 {
			return nil, fmt.Errorf("v2 timeout is invalid")
		}
	}
	if c.MaxHandshakes == 0 {
		c.MaxHandshakes = defaultMaxHandshakes
	}
	if c.EntryTimeout == 0 {
		c.EntryTimeout = 10 * time.Second
	}
	if c.ResumeWindow == 0 {
		c.ResumeWindow = 10 * time.Second
	}
	if c.ResultRetentionTimeout == 0 {
		c.ResultRetentionTimeout = 30 * time.Second
	}
	if c.WorkerStatusTimeout == 0 {
		c.WorkerStatusTimeout = 2 * time.Second
	}
	if c.MaxAudioBytes == 0 {
		c.MaxAudioBytes = 1 << 20
	}
	if c.MaxAudioChunks == 0 {
		c.MaxAudioChunks = 256
	}
	if c.MaxResultBytes == 0 {
		c.MaxResultBytes = 1 << 20
	}
	if c.MaxResults == 0 {
		c.MaxResults = 256
	}
	return &c, nil
}
