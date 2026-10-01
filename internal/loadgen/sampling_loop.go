package loadgen

import (
	"context"
	"fmt"
	"time"
)

// samplingAttempt 是一次查询尝试；T 是具体状态的值类型。
// 每次成功查询都持有独立状态，失败尝试也会交付。
type samplingAttempt[T any] struct {
	Index      int           // 本次运行内从 0 递增的序号。
	StartedAt  time.Time     // 查询开始前记录，保留单调时钟。
	FinishedAt time.Time     // 查询返回后立即记录。
	Duration   time.Duration // 查询耗时，不包含交付和间隔。
	State      *T            // 成功时非 nil，失败时 nil。
	Err        error         // 查询原始错误，成功时 nil。
}

// runSamplingLoop 在当前 goroutine 中串行查询、交付和等待，不保存历史。
// ctx/fetch/emit 非 nil，两个时长为正，由公开入口完成前置校验。
// fetch 必须响应查询 context，emit 必须及时返回；不为阻塞回调创建后台任务。
func runSamplingLoop[T any](
	ctx context.Context,
	interval, requestTimeout time.Duration,
	fetch func(context.Context) (T, error),
	emit func(samplingAttempt[T]) error,
) error {
	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context ended: %w", err)
		}
		startedAt := time.Now()
		requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		state, fetchErr := fetch(requestCtx)
		finishedAt := time.Now()
		cancel()

		sample := samplingAttempt[T]{
			Index: index, StartedAt: startedAt, FinishedAt: finishedAt,
			Duration: finishedAt.Sub(startedAt), Err: fetchErr,
		}
		if fetchErr == nil {
			// state 每轮独立；零值也可能是有效状态，不能根据内容判断查询失败。
			sample.State = &state
		}
		if err := emit(sample); err != nil {
			return fmt.Errorf("emit sample %d: %w", index, err)
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context ended after sample %d: %w", index, err)
		}

		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			// timer 不共享也不复用，停止后丢弃即可，无需阻塞排空。
			timer.Stop()
			return fmt.Errorf("wait after sample %d: %w", index, ctx.Err())
		}
	}
}
