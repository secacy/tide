package streamer

import (
	"context"
	"time"
)

// Pacer 用于按照音频本身的时间轴控制发送速度
type Pacer struct {
	start        time.Time     // 整个音频流的基准时间
	audioElapsed time.Duration // 已经发送的 PCM 对应多少音频时长
}

func NewPacer() *Pacer {
	return &Pacer{
		start: time.Now(),
	}
}

// Advance 在成功发送一个 chunk 后推进音频时间轴。
func (p *Pacer) Advance(audioBytes int) {
	p.audioElapsed += AudioDuration(audioBytes)
}

// WaitBeforeSend 等待到下一块音频应该发送的时刻。
func (p *Pacer) WaitBeforeSend(ctx context.Context) error {
	target := p.start.Add(p.audioElapsed)
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil

	case <-ctx.Done():
		// 使用 context 而不是 time.Sleep，
		// 这样客户端取消、RPC 超时或 Ctrl+C 时能够立即退出。
		return ctx.Err()
	}
}
