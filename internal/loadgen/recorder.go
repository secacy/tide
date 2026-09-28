package loadgen

import (
	"sync"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// SessionObservation 保存单场会话的观测事实，不表示最终成功或失败。
// 未出现的时间保持零值；控制事件 Kind 为空表示尚未观察到。
type SessionObservation struct {
	AudioBytesWritten  int64 // 成功写出的音频字节数。
	AudioChunksWritten int64 // 成功写出的音频块数。
	WriteFailures      int64 // 所有用途的失败 Write 次数。

	MaxAudioWriteDuration time.Duration // 音频 Write 的最大耗时，包含失败尝试。

	FirstAudioStartedAt time.Time // 首次成功音频写入的开始时间。
	LastAudioFinishedAt time.Time // 最后一次成功音频写入的结束时间。

	StartWrite wsclient.WriteEvent // start 写入事件，失败也保存。
	EndWrite   wsclient.WriteEvent // end 写入事件，失败也保存。

	ResultCount      int64 // 收到的有效结果消息数。
	FinalResultCount int64 // IsFinal 消息数，不是完成的会话数。

	FirstResultAt time.Time // 第一条结果的接收时间。
	LastResultAt  time.Time // 最后一条结果的接收时间。
	LastFinalAt   time.Time // 最后一条 final 的接收时间，不保证是整场尾部。
}

// SessionRecorder 汇总单场会话的收发事件。
// 零值可用，首次使用后不得复制；每场会话分别创建实例。
// ObserveWrite、ObserveResult 和 Snapshot 可以并发调用。
type SessionRecorder struct {
	mu          sync.Mutex
	observation SessionObservation
}

// ObserveWrite 汇总客户端的一次实际写入。
// 同一场会话的写入事件由发送协程按顺序提供。
func (r *SessionRecorder) ObserveWrite(event wsclient.WriteEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if event.Err != nil {
		r.observation.WriteFailures++
	}

	switch event.Kind {
	case wsclient.WriteStart:
		r.observation.StartWrite = event

	case wsclient.WriteEnd:
		r.observation.EndWrite = event

	case wsclient.WriteAudio:
		duration := event.FinishedAt.Sub(event.StartedAt)
		if duration > r.observation.MaxAudioWriteDuration {
			r.observation.MaxAudioWriteDuration = duration
		}

		if event.Err == nil {
			if r.observation.AudioChunksWritten == 0 {
				r.observation.FirstAudioStartedAt = event.StartedAt
			}

			r.observation.AudioBytesWritten += int64(event.AudioBytes)
			r.observation.AudioChunksWritten++
			r.observation.LastAudioFinishedAt = event.FinishedAt
		}
	}
}

// ObserveResult 汇总有效结果，保留客户端提供的接收时间。
// 同一场会话的结果由接收协程按顺序提供，不保存正文。
func (r *SessionRecorder) ObserveResult(result wsprotocol.ResultMessage, receivedAt time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.observation.ResultCount == 0 {
		r.observation.FirstResultAt = receivedAt
	}
	r.observation.ResultCount++
	r.observation.LastResultAt = receivedAt
	if result.IsFinal {
		r.observation.FinalResultCount++
		r.observation.LastFinalAt = receivedAt
	}
}

// Snapshot 返回一致的值副本，修改返回值不会改变内部记录。
// 运行中可能尚有事件未到达；Run 返回后才能取得完整观察记录。
func (r *SessionRecorder) Snapshot() SessionObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.observation
}
