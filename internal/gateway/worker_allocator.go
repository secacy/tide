package gateway

import (
	"reflect"

	"github.com/secacy/tide-artisan/internal/workerpool"
)

// WorkerAllocator 为新逻辑会话取得一次本地分配，支持并发调用。
// 成功必须返回有效、非 nil 的 Lease 和 nil error；失败返回 nil Lease。
// 不等待容量、不执行网络 I/O；凭证最终由取得者或明确接管者释放。
type WorkerAllocator interface {
	TryAcquire() (*workerpool.WorkerLease, error)
}

// selectorAllocator 适配既有轮询/加权策略；不提供每 Worker 容量限制。
// selector 已由 Gateway.New 校验非 nil，每次申请只调用一次 Pick。
type selectorAllocator struct {
	selector WorkerSelector
}

// TryAcquire 只调用一次 Pick。
// 旧模式下不检查每 Worker 的本地容量。
func (a selectorAllocator) TryAcquire() (*workerpool.WorkerLease, error) {
	return workerpool.NewUnreservedLease(a.selector.Pick()), nil
}

// isNilWorkerAllocator 识别 nil 接口和动态 nil；构造校验不消耗名额。
func isNilWorkerAllocator(allocator WorkerAllocator) bool {
	if allocator == nil {
		return true
	}

	v := reflect.ValueOf(allocator)

	switch v.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Ptr,
		reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
