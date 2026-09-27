package mockasr

import (
	"context"
	"errors"
	"sync"
)

// processingSlots 限制一个 Worker 同时进行的模拟处理数量；同一 Worker 的所有会话共享它
// 它不保存音频，也不统计活跃会话。必须通过 newProcessingSlots 创建，创建后不得复制。
type processingSlots struct {
	tokens chan struct{} // 容量是名额上限，元素数是当前占用量。
}

// newProcessingSlots 创建固定容量的处理名额池。
// limit 必须大于 0；0 或负数返回错误。
func newProcessingSlots(limit int) (*processingSlots, error) {
	if limit <= 0 {
		return nil, errors.New("limit must be greater than zero")
	}
	return &processingSlots{
		tokens: make(chan struct{}, limit),
	}, nil
}

// acquire 等待取得处理名额，等待期间响应 ctx 取消。
// ctx 必须非 nil。取得名额后再取消 ctx 不会自动释放名额。
// 成功返回归还函数，调用方处理结束后必须调用； 归还函数允许重复或并发调用，但实际只归还一次。
// 失败返回 nil 和 ctx.Err()。
func (s *processingSlots) acquire(ctx context.Context) (release func(), err error) {
	// 检查context是否已经取消
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// 占用一个名额；满了就等待，同时允许 ctx 取消等待
	select {
	case s.tokens <- struct{}{}:
		// 写入成功就表示取得一个名额
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if ctx.Err() != nil {
		// 先归还名额
		<-s.tokens
		return nil, ctx.Err()
	}

	// 成功取得名额后，由调用方负责在处理结束时归还
	var once sync.Once
	release = func() {
		once.Do(func() {
			<-s.tokens
		})
	}

	return release, nil
}
