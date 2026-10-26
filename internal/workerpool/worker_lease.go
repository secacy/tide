package workerpool

// NewUnreservedLease 包装已经选中的有效 Worker，不占用任何本地容量。
// 仅用于旧 Pick 策略的显式适配；Worker 的有效性由选择器契约保证。
// 每次返回新的凭证，Release 可重复调用且无资源归还动作。
func NewUnreservedLease(worker Worker) *WorkerLease {
	return &WorkerLease{worker: worker, release: func() {}}
}
