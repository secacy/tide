// Package wsheartbeat 管理独立于业务读写的、有界 Ping/Pong 探测。
package wsheartbeat

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrFailed 标识连接探测失败，不表示已确诊网络或 Worker 故障。
var ErrFailed = errors.New("websocket heartbeat failed")

// Config 由两端各自配置；零值使用两秒周期和三秒单次预算。
type Config struct {
	Interval time.Duration // 两次计划探测之间的间隔；首个探测等待一个间隔。
	Timeout  time.Duration // 写 Ping 和等待匹配 Pong 共享的预算。
	Disabled bool          // 显式停用，用于实验对照或由调用方接管探测。
}

// Normalize 填充默认值并拒绝负值；禁用也不允许隐藏无效参数。
func (c Config) Normalize() (Config, error) {
	if c.Interval < 0 || c.Timeout < 0 {
		return c, errors.New("heartbeat interval and timeout must not be negative")
	}
	if c.Interval == 0 {
		c.Interval = 2 * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = 3 * time.Second
	}
	return c, nil
}

// Monitor 拥有一个探测执行流。Stop 关闭后续入口，Wait 等待在途探测退出。
// mu 只保护探测准入、停止标志和错误；不在持锁时执行网络 I/O 或回调。
type Monitor struct {
	mu      sync.Mutex
	stopped bool
	err     error
	stop    chan struct{}
	done    chan struct{}
}

// Start 启动独立探测。cfg 必须经 Normalize；ping 必须响应自身 Context。
// 调用方须持续 Read WebSocket，并在失败回调中取消业务、关闭连接。
// onFailure 至多调用一次，不得在回调内调用 Wait。
// 父 Context 取消只停止未来探测；在途 Ping 使用独立预算，避免误关健康连接。
func Start(ctx context.Context, cfg Config, ping func(context.Context) error, onFailure func(error)) *Monitor {
	m := &Monitor{stop: make(chan struct{}), done: make(chan struct{})}
	if cfg.Disabled {
		close(m.done)
		return m
	}
	go func() {
		defer close(m.done)
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-m.stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			m.mu.Lock()
			admitted := !m.stopped && ctx.Err() == nil
			m.mu.Unlock()
			if !admitted {
				return
			}
			// 准入后即视为在途；Stop 不撤销它，不持锁等待网络。
			probe, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Timeout)
			err := ping(probe)
			cancel()
			if err != nil {
				m.mu.Lock()
				m.err = fmt.Errorf("%w: %w", ErrFailed, err)
				report := !m.stopped && ctx.Err() == nil
				m.mu.Unlock()
				if report {
					onFailure(m.err)
				}
				return
			}
		}
	}()
	return m
}

// Stop 幂等关闭后续探测入口，不等待，不取消已经准入的 Ping。
func (m *Monitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.stopped {
		m.stopped = true
		close(m.stop)
	}
}

// Wait 等待探测和失败回调完成，返回实际探测错误（包括停止期间的失败）。
// 正常停用需先 Stop；外部取消时由调用方关闭连接解除在途网络 I/O。
func (m *Monitor) Wait() error {
	<-m.done
	return m.err
}
