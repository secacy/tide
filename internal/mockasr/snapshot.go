package mockasr

// ProcessingSnapshot 描述本 Worker 已启用的共享处理名额状态。
// 仅在 Worker.ProcessingSnapshot 返回 enabled=true 时具有观测含义。
// 它是值副本，不引用内部可变状态。
type ProcessingSnapshot struct {
	Limit   int // 实际名额池上限，大于零，不是实测容量。
	InUse   int // 已登记成功获取、尚未有效归还的名额数。
	Waiting int // 因满额登记、尚未完成获取或取消收尾的请求数。
}

// ProcessingSnapshot 返回处理名额快照及共享限制是否启用。
// 未启用时返回零值和 false，零字段不能解释为没有任务。
// 启用时三个字段来自同一次池快照，允许与处理流程并发调用。
// w 必须由 New 成功创建；方法不改变状态，不等待处理结束。
func (w *Worker) ProcessingSnapshot() (
	snapshot ProcessingSnapshot,
	enabled bool,
) {
	if w.slots == nil {
		return ProcessingSnapshot{}, false
	}
	snap := w.slots.snapshot()
	return ProcessingSnapshot{
		Limit:   snap.Limit,
		InUse:   snap.InUse,
		Waiting: snap.Waiting,
	}, true
}
