package audio

import (
	"context"
	"time"
)

// Pacer 用于按照音频本身的时间轴控制发送速度
type Pacer struct {
	start        time.Time     // 整个音频流的基准时间
	audioElapsed time.Duration // 已经发送的 PCM 对应多少音频时长
}

// NewPacer 创建发送节奏控制器。
// 首块音频的计划发送时刻就是创建 Pacer 的时刻，因此首块无需额外等待。
func NewPacer() *Pacer {
	return &Pacer{
		start: time.Now(),
	}
}

// NextSendAt 返回下一块音频的计划发送时刻：起点加已成功发送音频的时长。
// 不等待、不推进时间轴；保留 time.Time 的单调时钟信息。
// 与 WaitBeforeSend、Advance 一样，由同一发送协程顺序调用。
func (p *Pacer) NextSendAt() time.Time {
	return p.start.Add(p.audioElapsed)
}

// Advance 在成功发送一个 chunk 后推进音频时间轴。
// audioBytes 使用本次实际成功写出的字节数，因此也支持不足整块的尾部。
func (p *Pacer) Advance(audioBytes int) {
	p.audioElapsed += DurationFromBytes(audioBytes)
}

// WaitBeforeSend 等待到下一块音频应该发送的时刻。
func (p *Pacer) WaitBeforeSend(ctx context.Context) error {
	target := p.NextSendAt()
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
