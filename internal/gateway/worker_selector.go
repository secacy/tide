package gateway

import (
	"reflect"

	"github.com/secacy/tide-artisan/internal/workerpool"
)

// WorkerSelector 为新会话选择后端。
// 同一 Gateway 的会话共享一个选择器，实现必须支持并发调用。
// 返回的 Worker 必须具有有效 ID 和非 nil Client。
// Pick 只执行本地选择，不执行网络操作，也不等待处理名额。
// 后端客户端及连接的生命周期由外部管理。
type WorkerSelector interface {
	// Pick 消耗一次选择并返回后端。
	// 后续建流失败不会撤销本次选择。
	Pick() workerpool.Worker
}

// isNilWorkerSelector 判断接口本身或其中的动态值是否为 nil。
// 仅用于构造时校验，不调用 Pick，不消耗选择位置。
func isNilWorkerSelector(selector WorkerSelector) bool {
	if selector == nil {
		return true
	}

	v := reflect.ValueOf(selector)

	switch v.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
