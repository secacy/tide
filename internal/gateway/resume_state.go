package gateway

import (
	"errors"
	"math"
	"time"
)

// resumePhase 表示逻辑会话当前的连接附着状态。
type resumePhase uint8

const (
	// resumeAttached 表示当前存在一个有效连接。
	resumeAttached resumePhase = iota

	// resumeDetached 表示当前连接已经断开，但仍处于恢复窗口内。
	resumeDetached

	// resumeClosed 表示恢复资格已经永久结束。
	// 它不表示连接、Worker RPC 或其他会话资源已经清理完成。
	resumeClosed
)

var (
	errResumeAlreadyAttached     = errors.New("resume: connection is already attached")
	errResumeClosed              = errors.New("resume: recovery is closed")
	errResumeExpired             = errors.New("resume: recovery window expired")
	errResumeGenerationExhausted = errors.New("resume: connection generation exhausted")
)

// resumeState 管理连接附着与恢复期限。
// 由会话协调者串行调用，自身不保证并发安全。
// 不持有连接，不启动计时器，也不负责资源清理。
type resumeState struct {
	phase      resumePhase   // 当前连接保留状态。
	generation uint64        // 连接代次，初始为 1，成功恢复后递增。
	window     time.Duration // 有效断开后允许恢复的时间。
	expiresAt  time.Time     // detached 状态的截止时间，其他状态清零。
}

// newResumeState 创建一个已附着、连接代次为 1 的恢复状态。
// window 必须大于零，否则返回错误。
func newResumeState(window time.Duration) (*resumeState, error) {
	if window <= 0 {
		return nil, errors.New("resume: window must be positive")
	}

	return &resumeState{
		phase:      resumeAttached,
		generation: 1,
		window:     window,
	}, nil
}

// detach 报告指定连接代次已经断开。
// 只有 generation 与当前代次一致，并且当前处于 attached 状态时，
// detach 才会成功。成功后状态进入 detached，并将恢复截止时间设置为now+window。
// 旧代次通知、重复断开通知以及 closed 状态均返回 false，不延长窗口。
func (s *resumeState) detach(generation uint64, now time.Time) bool {
	if s.phase != resumeAttached {
		return false
	}

	if generation != s.generation {
		return false
	}

	s.phase = resumeDetached
	s.expiresAt = now.Add(s.window)

	return true
}

// resume 尝试重新附着连接。
// 只有当前处于 detached 状态，且 now 严格早于 expiresAt 时才能成功。
// 成功后进入 attached、清除截止时间，返回递增后的代次。
// 到期则进入 closed 并返回过期错误；attached/closed 返回相应错误。
// 代次溢出必须拒绝且不得回绕为可复用的代次。
func (s *resumeState) resume(now time.Time) (uint64, error) {
	switch s.phase {
	case resumeAttached:
		return 0, errResumeAlreadyAttached

	case resumeClosed:
		return 0, errResumeClosed

	case resumeDetached:
		// 恰好到截止时间也视为已经过期。
		if !now.Before(s.expiresAt) {
			s.phase = resumeClosed
			s.expiresAt = time.Time{}
			return 0, errResumeExpired
		}

		if s.generation == math.MaxUint64 {
			return 0, errResumeGenerationExhausted
		}

		s.generation++
		s.phase = resumeAttached
		s.expiresAt = time.Time{}

		return s.generation, nil

	default:
		panic("gateway: invalid resume phase")
	}
}

// expire 在 detached 且 now 已达到截止时间时进入 closed，返回 true。
// 其他情况返回 false；只改变状态，不承担资源清理。
func (s *resumeState) expire(now time.Time) bool {
	if s.phase != resumeDetached {
		return false
	}

	if now.Before(s.expiresAt) {
		return false
	}

	s.phase = resumeClosed
	s.expiresAt = time.Time{}

	return true
}

// close 永久结束恢复资格。
// 允许重复调用。调用后状态为 closed，并清空恢复截止时间。
// 它不关闭网络连接、不取消 Worker RPC，也不释放会话名额。
func (s *resumeState) close() {
	s.phase = resumeClosed
	s.expiresAt = time.Time{}
}
