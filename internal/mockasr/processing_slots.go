package mockasr

import (
	"context"
	"errors"
	"sync"
)

// processingSlotsSnapshot 是处理名额池的值快照。
// 三个字段在同一临界区内读取。
type processingSlotsSnapshot struct {
	Limit int // 配置的名额上限，不是实测容量。

	// 已登记成功获取、尚未有效归还的名额数。
	InUse int

	// 因满额登记、尚未完成获取或取消收尾的请求数。
	Waiting int
}

// processingSlots 保留 channel 名额控制，并维护逻辑状态。
// 必须通过构造函数创建，创建后不得复制。
type processingSlots struct {
	tokens chan struct{} // 物理名额，容量固定。

	mu      sync.Mutex // 保护逻辑计数及状态转换。
	inUse   int        // 已成功登记、尚未归还的名额数。
	waiting int        // 因满额登记、尚未完成获取或取消收尾的请求数。
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	// 再检查一次，处理等待锁期间发生的取消
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	// 在锁内非阻塞尝试取得名额
	select {
	case s.tokens <- struct{}{}:
		// 立即取得名额
		if err := ctx.Err(); err != nil {
			// 尚未登记为 inUse，只撤销刚取得的物理 token
			<-s.tokens
			s.mu.Unlock()
			return nil, err
		}
		s.inUse++
		s.mu.Unlock()
		return s.newRelease(), nil
	default:
		// 当前满额，登记后进入锁外等待
		s.waiting++
		s.mu.Unlock()
		select {
		case s.tokens <- struct{}{}:
			// 取得物理名额，进入成功或取消回滚
			s.mu.Lock()
			s.waiting--
			if err := ctx.Err(); err != nil {
				// 尚未登记为 inUse，只撤销刚取得的物理 token
				<-s.tokens
				s.mu.Unlock()
				return nil, err
			}
			s.inUse++
			s.mu.Unlock()
			return s.newRelease(), nil
		case <-ctx.Done():
			// 没取得名额，只撤销等待登记
			s.mu.Lock()
			s.waiting--
			s.mu.Unlock()
			return nil, ctx.Err()
		}
	}
}

// snapshot 返回同一临界区内的值快照。
// 接收者必须是构造成功的非 nil 名额池。
func (s *processingSlots) snapshot() processingSlotsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return processingSlotsSnapshot{
		Limit:   cap(s.tokens),
		InUse:   s.inUse,
		Waiting: s.waiting,
	}
}

// newRelease 为一次已成功登记的名额创建幂等归还函数。
// 仅在本次 acquire 增加 inUse 后调用一次。
// 返回函数可以重复或并发调用，实际归还一次。
func (s *processingSlots) newRelease() func() {
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.mu.Lock()
			s.inUse--
			<-s.tokens
			s.mu.Unlock()
		})
	}
	return release
}
