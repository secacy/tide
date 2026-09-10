package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	errAudioQueueFull   = errors.New("audio queue capacity reached") // 入队会超过字节数或条目数上限。
	errAudioQueueClosed = errors.New("audio queue input closed")     // 输入关闭后不能再接纳音频。
	errEmptyAudioChunk  = errors.New("audio chunk is empty")         // 空块不能占用队列条目。
)

// audioQueue 保存等待发送的音频，供一个 Reader 生产、一个 Sender 消费。
// 必须通过 newAudioQueue 创建，使用后不能复制；不负责会话取消或网络 I/O。
type audioQueue struct {
	mu sync.Mutex // 保护以下队列状态；等待通知时必须释放。

	slots    [][]byte // 固定槽位，只保存队列拥有的音频副本。
	head     int      // 下一次出队的位置。
	count    int      // 当前条目数，也是下一次入队位置的偏移量。
	bytes    int      // 当前排队字节数，不包含已经出队的音频。
	maxBytes int      // 排队字节数上限，初始化后不变。
	closed   bool     // 输入永久关闭，已有音频仍可排空。

	// ready 合并“可能有数据或输入已关闭”的通知，仅供一个消费者等待。
	// 它始终保持打开；消费者收到通知后必须重新检查状态。
	ready chan struct{}
}

// newAudioQueue 创建同时受字节数和音频块数量限制的 FIFO。
// 两个上限都必须大于零。
func newAudioQueue(maxBytes, maxChunks int) (*audioQueue, error) {
	if maxBytes <= 0 || maxChunks <= 0 {
		return nil, fmt.Errorf("audio queue limits must be positive: bytes=%d chunks=%d", maxBytes, maxChunks)
	}
	return &audioQueue{
		slots:    make([][]byte, maxChunks),
		maxBytes: maxBytes,
		ready:    make(chan struct{}, 1),
	}, nil
}

// tryPush 尝试立即入队。
// 超过任一容量上限时返回 errAudioQueueFull，不等待空位。
// 输入关闭后返回 errAudioQueueClosed。
// 空块返回 errEmptyAudioChunk。成功时复制 data，返回后调用方可复用原缓冲区。
// 不等待队列空位，但会短暂获取互斥锁；任何失败都不修改队列。
func (q *audioQueue) tryPush(data []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return errAudioQueueClosed
	}
	if len(data) == 0 {
		return errEmptyAudioChunk
	}
	// 用剩余容量比较，避免 bytes + len(data) 的整数溢出。
	if q.count == len(q.slots) || len(data) > q.maxBytes-q.bytes {
		return errAudioQueueFull
	}
	chunk := make([]byte, len(data))
	copy(chunk, data)
	q.slots[(q.head+q.count)%len(q.slots)] = chunk
	q.count++
	q.bytes += len(chunk)
	q.notifyLocked()
	return nil
}

// pop 等待并取出最早入队的音频。
// 输入已关闭且队列为空时返回 io.EOF。
// 等待期间支持 ctx 取消。
// ctx 必须非 nil；已观察到取消时不再取出音频，包括队列尚有积压的情况。
// 返回的音频所有权交给调用方，队列不再持有它。
func (q *audioQueue) pop(ctx context.Context) ([]byte, error) {
	for {
		q.mu.Lock()
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		if q.count > 0 {
			chunk := q.slots[q.head]
			q.slots[q.head] = nil
			q.head = (q.head + 1) % len(q.slots)
			q.count--
			q.bytes -= len(chunk)
			q.mu.Unlock()
			return chunk, nil
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil, io.EOF
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.ready:
			// 通知可能已经过时；重新检查数据和关闭状态。
		}
	}
}

// closeInput 表示不会再有新音频。
// 已入队音频仍可继续取出；允许重复调用。
func (q *audioQueue) closeInput() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.notifyLocked()
	}
}

// notifyLocked 在持有 mu 时合并通知；通知已存在时无需再次发送。
func (q *audioQueue) notifyLocked() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}
