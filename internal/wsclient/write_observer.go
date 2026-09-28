package wsclient

import "time"

// WriteKind 表示一次写入的业务用途。
type WriteKind string

const (
	WriteStart WriteKind = "start" // 会话开始控制消息
	WriteAudio WriteKind = "audio" // PCM 音频消息
	WriteEnd   WriteKind = "end"   // 输入结束控制消息
)

// WriteEvent 描述一次已经返回的 conn.Write 调用。
// 写入成功不代表 Gateway 已读取或 Worker 已处理。
type WriteEvent struct {
	Kind WriteKind // 本次写入的业务用途。

	// 本次尝试写入的 PCM 字节数；控制消息为 0。
	// 只有 Err == nil 时，才能计入成功写出的音频量。
	AudioBytes int

	// 紧邻 conn.Write 调用前记录，不包含读取、节奏等待和 JSON 编码。
	StartedAt time.Time

	// conn.Write 返回后立即记录，不包含观察回调的执行时间。
	FinishedAt time.Time

	// conn.Write 返回的原始错误；nil 表示客户端写入成功。
	Err error
}

// WriteHandler 在发送协程内同步观察写入，应快速返回。
// 同一会话内按写入顺序调用，可能与 OnResult 并发执行。
// 多场会话共用回调时也可能并发；调用方负责共享数据的同步。
type WriteHandler func(event WriteEvent)
