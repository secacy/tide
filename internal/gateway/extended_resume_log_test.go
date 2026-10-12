package gateway

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var errExtendedEvidence = errors.New("extended resume evidence failed")

// extendedEvent 是不可变的单条证据；时间均来自同一个进程内单调原点。
// Seq 是 Component 内序号；ElapsedNS 是事件发生时间，不是写盘时间。
type extendedEvent struct {
	Version   int             `json:"version"`
	Component string          `json:"component"`
	Seq       uint64          `json:"seq"`
	Kind      string          `json:"kind"`
	ElapsedNS int64           `json:"elapsed_ns"`
	Data      json.RawMessage `json:"data"`
}

// extendedLogLimits 限制编码事件、待写队列与文件缓冲；不是整个夹具的内存上限。
type extendedLogLimits struct {
	Queue  int `json:"queue"`
	Event  int `json:"event_bytes"`
	Buffer int `json:"buffer_bytes"`
}

// extendedLog 非阻塞接纳编码副本，由唯一任务写盘；不可复制。
// submitMu 同步序号、队列关闭和投递；errMu 独立保存首错，不依赖队列。
// Finish 必须在所有事件生产者退出后调用，任意磁盘阻塞受外部进程期限约束。
type extendedLog struct {
	origin   time.Time
	limits   extendedLogLimits
	queue    chan []byte
	done     chan struct{}
	cancel   func()
	submitMu sync.Mutex
	closed   bool
	seq      map[string]uint64 // 只接纳下列固定组件，不能随事件增长。
	errMu    sync.Mutex
	firstErr error
	peak     atomic.Int64
	written  atomic.Uint64 // Write 已接纳字节，不代表 flush/close 已成功。
	events   atomic.Uint64
}

// newExtendedLog 接管 writer 的唯一写入/关闭责任；非法配置不会接管 writer。
func newExtendedLog(writer io.WriteCloser, origin time.Time, cancel func(), limits extendedLogLimits) (*extendedLog, error) {
	if writer == nil || origin.IsZero() || cancel == nil || limits.Queue < 1 || limits.Event < 128 || limits.Buffer < 1 {
		return nil, fmt.Errorf("%w: invalid log configuration", errExtendedEvidence)
	}
	l := &extendedLog{origin: origin, limits: limits, queue: make(chan []byte, limits.Queue), done: make(chan struct{}), cancel: cancel, seq: map[string]uint64{"source": 0, "worker": 0, "proxy": 0, "sample": 0, "run": 0}}
	go l.write(writer)
	return l, nil
}

// fail 首错触发独立取消；调用 cancel 时不持有记录器锁，不输出原始数据。
func (l *extendedLog) fail(reason string) {
	l.errMu.Lock()
	first := l.firstErr == nil
	if first {
		l.firstErr = fmt.Errorf("%w: %s", errExtendedEvidence, reason)
	}
	l.errMu.Unlock()
	if first {
		l.cancel()
	}
}

// err 返回独立保存的首错，失败不能被“队列已经排空”覆盖。
func (l *extendedLog) err() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	return l.firstErr
}

// emit 先同步编码副本再非阻塞投递；调用方可在返回后复用 payload。
// at 必须来自该进程的 time.Now 或同一单调原点推导值。
func (l *extendedLog) emit(component, kind string, at time.Time, payload any) bool {
	l.submitMu.Lock()
	reason := ""
	defer func() {
		l.submitMu.Unlock()
		if reason != "" {
			l.fail(reason)
		}
	}()
	if l.closed {
		reason = "event after log close"
		return false
	}
	seq, known := l.seq[component]
	if !known || kind == "" || at.IsZero() || at.Sub(l.origin) < 0 {
		reason = "invalid event metadata"
		return false
	}
	if l.err() != nil {
		return false
	}
	data, err := json.Marshal(payload)
	if err != nil {
		reason = "event encoding failed"
		return false
	}
	row, err := json.Marshal(extendedEvent{1, component, seq + 1, kind, at.Sub(l.origin).Nanoseconds(), data})
	if err != nil || len(row)+1 > l.limits.Event {
		reason = "event size exceeded"
		return false
	}
	row = append(row, '\n')
	select {
	case l.queue <- row:
		l.seq[component] = seq + 1
		depth := int64(len(l.queue))
		for old := l.peak.Load(); depth > old && !l.peak.CompareAndSwap(old, depth); old = l.peak.Load() {
		}
		return true
	default:
		reason = "event queue full"
		return false
	}
}

// write 只由构造器启动；即使写失败也排空队列，等待所有生产者退出后关闭文件。
func (l *extendedLog) write(writer io.WriteCloser) {
	defer close(l.done)
	buffer := bufio.NewWriterSize(extendedCheckedWriter{writer}, l.limits.Buffer)
	bad := false
	for row := range l.queue {
		if bad {
			continue
		}
		n, err := buffer.Write(row)
		l.written.Add(uint64(n))
		if err != nil || n != len(row) {
			l.fail("event write failed")
			bad = true
			continue
		}
		l.events.Add(1)
	}
	if err := buffer.Flush(); err != nil {
		l.fail("event flush failed")
	}
	if err := writer.Close(); err != nil {
		l.fail("event close failed")
	}
}

// extendedCheckedWriter 检测违反 io.Writer 契约的短写，避免 bufio 对零进展反复重试。
// 不接管 Close；文件关闭仍由 extendedLog.write 唯一负责。
type extendedCheckedWriter struct{ io.Writer }

func (w extendedCheckedWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	if n < 0 || n > len(data) {
		return 0, io.ErrShortWrite
	}
	if n < len(data) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// finish 关闭一次投递入口，等待真实 flush/close；不以取消通知冒充文件关闭。
func (l *extendedLog) finish() error {
	l.submitMu.Lock()
	if !l.closed {
		l.closed = true
		close(l.queue)
	}
	l.submitMu.Unlock()
	<-l.done
	return l.err()
}
