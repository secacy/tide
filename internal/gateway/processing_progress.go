package gateway

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	errProcessingTimeout = errors.New("processing_timeout")          // 最早未确认音频已超过处理等待预算。
	errEndTimeout        = errors.New("end_timeout")                 // 合法 End 后未能在预算内完成。
	errProgressCapacity  = errors.New("progress_capacity")           // 未确认音频元数据达到容量上限。
	errInvalidProgress   = errors.New("invalid processing progress") // Worker 确认不满足序号契约。
	errUnconfirmedAudio  = errors.New("worker completed without confirming all audio")
)

// processingProgress 跟踪一个 RPC 的连续处理水位和两个独立期限。
// times 是固定容量环形表，只存到达时间相对 epoch 的单调偏移，不持有音频。
// 队列可以在持有自身锁时获取 mu；此对象不反向获取队列锁，不执行 I/O。
type processingProgress struct {
	mu                              sync.Mutex
	epoch                           time.Time
	times                           []time.Duration
	head, count                     int
	admitted, sending, acknowledged uint64
	processingTimeout, endTimeout   time.Duration
	endAt                           time.Time
	failure                         error         // 已观察到的过期不可被迟到确认清除。
	changed                         chan struct{} // 合并通知；只有协调者消费，不关闭。
}

// newProcessingProgress 只接受经校验的正容量和正期限。
func newProcessingProgress(capacity int, processing, end time.Duration) (*processingProgress, error) {
	if capacity <= 0 || processing <= 0 || end <= 0 {
		return nil, fmt.Errorf("processing capacity and deadlines must be positive")
	}
	return &processingProgress{epoch: time.Now(), times: make([]time.Duration, capacity), processingTimeout: processing, endTimeout: end, changed: make(chan struct{}, 1)}, nil
}

// admit 在成功入队的临界区内调用。失败不增加计数，后续入队操作不得失败。
func (p *processingProgress) admit(now time.Time) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.deadlineLocked(now); err != nil {
		return err
	}
	if p.count == len(p.times) {
		return errProgressCapacity
	}
	if p.admitted == ^uint64(0) {
		return fmt.Errorf("%w: sequence exhausted", errInvalidProgress)
	}
	p.times[(p.head+p.count)%len(p.times)] = now.Sub(p.epoch)
	p.count++
	p.admitted++
	p.notifyLocked()
	return nil
}

// startSend 在调用 Send 前建立水位，允许 Worker 确认先于 Send 返回。
func (p *processingProgress) startSend(seq uint64) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.deadlineLocked(time.Now()); err != nil {
		return err
	}
	if seq != p.sending+1 || seq > p.admitted {
		return fmt.Errorf("%w: send sequence %d", errInvalidProgress, seq)
	}
	p.sending = seq
	return nil
}

// acknowledge 确认连续前缀；重复确认无副作用，迟到确认不能挽救已过期音频。
func (p *processingProgress) acknowledge(seq uint64, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.deadlineLocked(now); err != nil {
		return err
	}
	if seq < p.acknowledged || seq > p.sending {
		return fmt.Errorf("%w: acknowledged=%d received=%d sending=%d", errInvalidProgress, p.acknowledged, seq, p.sending)
	}
	for p.acknowledged < seq {
		p.times[p.head] = 0
		p.head = (p.head + 1) % len(p.times)
		p.count--
		p.acknowledged++
	}
	p.notifyLocked()
	return nil
}

// end 从首次合法 End 开始计时；重复调用不会续期。
func (p *processingProgress) end(now time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.endAt.IsZero() {
		p.endAt = now
		p.notifyLocked()
	}
}

// status 返回最近的截止时刻和已观察到的过期错误；零时刻表示当前没有期限。
func (p *processingProgress) status(now time.Time) (time.Time, error) {
	if p == nil {
		return time.Time{}, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deadlineLocked(now)
}

// deadlineLocked 必须持锁调用；同时过期时选择较早的期限，处理期限同刻优先。
func (p *processingProgress) deadlineLocked(now time.Time) (time.Time, error) {
	if p.failure != nil {
		return time.Time{}, p.failure
	}
	var deadline time.Time
	cause := errProcessingTimeout
	if p.count > 0 {
		deadline = p.epoch.Add(p.times[p.head]).Add(p.processingTimeout)
	}
	if !p.endAt.IsZero() {
		end := p.endAt.Add(p.endTimeout)
		if deadline.IsZero() || end.Before(deadline) {
			deadline = end
			cause = errEndTimeout
		}
	}
	if !deadline.IsZero() && !now.Before(deadline) {
		p.failure = cause
		return deadline, cause
	}
	return deadline, nil
}

// complete 由协调者在发送完成及 Worker EOF 都成立后调用。
func (p *processingProgress) complete(now time.Time) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.deadlineLocked(now); err != nil {
		return err
	}
	if p.count != 0 {
		return errUnconfirmedAudio
	}
	return nil
}

func (p *processingProgress) notifyLocked() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

// processingFailure 将共享进度检查产生的错误映射为明确的会话终态。
func processingFailure(err error) sessionResult {
	kind := resultWorkerFailed
	switch {
	case errors.Is(err, errProcessingTimeout):
		kind = resultProcessingTimedOut
	case errors.Is(err, errEndTimeout):
		kind = resultEndTimedOut
	case errors.Is(err, errProgressCapacity):
		kind = resultOverloaded
	}
	return sessionResult{kind: kind, err: err}
}
