package realasr

import "context"

// Engine 拥有一场识别的模型资源，由 RPC 执行流程串行调用。
// 方法同步返回；不承诺正在执行的原生调用可以被强制取消。
type Engine interface {
	// Accept 接收非空的 16kHz 单声道样本。
	// 返回后不持有调用方的切片；必要的前置 padding 由实现负责。
	Accept(samples []float32) error

	// Ready 表示现在是否可以执行一步解码。
	// false 不等于所有原始音频已完成处理。
	Ready() (bool, error)

	// Decode 在 Ready=true 时同步执行一步解码。
	Decode() error

	// Result 返回当前片段的完整文本快照，可以为空。
	Result() (string, error)

	// Finish 正常结束输入，提交模型需要的尾部 padding 和结束标记。
	// 返回后仍需要执行剩余解码。
	Finish() error

	// Close 释放资源；只能在其他调用结束后执行，恰好调用一次。
	// 不补做正常输入刷新；实际返回后 Worker 才能归还名额。
	Close()
}

// EngineFactory 成功时返回一个独立 Engine，并移交所有权。
// 成功时不得返回 nil 或带类型的 nil。
// 失败时自行清理部分资源，返回 nil 和错误。
// ctx 用于协作取消，不保证中断同步原生创建。
type EngineFactory func(ctx context.Context) (Engine, error)
