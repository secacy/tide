package wsclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// v2Connection 只提供任务所需的完整消息 I/O；关闭责任仍由 Run 持有。
type v2Connection interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
}

// v2SourceEvent 交付独立块或终态；EOF 可以与非空尾块一起出现。
type v2SourceEvent struct {
	data []byte
	eof  bool
	err  error
}

// v2ReadEvent 按实际读取顺序交付消息及最后错误，避免 EOF 越过 completed。
type v2ReadEvent struct {
	data       []byte
	receivedAt time.Time
	err        error
}

// v2WriteKind 区分唯一写任务的业务用途；零值无任务。
type v2WriteKind uint8

const (
	v2WriteAudio v2WriteKind = iota + 1
	v2WriteEnd
	v2WriteResultAck
	v2WriteCompletedAck
)

// v2WriteJob 是唯一待写/在途任务；data 为独立音频副本。
// offset 是音频起点或输入终点，seq 是两个累计结果确认的位置。
type v2WriteJob struct {
	kind        v2WriteKind
	offset, seq uint64
	data        []byte
}

// v2WriteReceipt 记录实际 Write 结果；不含音频或凭据。
type v2WriteReceipt struct {
	kind v2WriteKind
	seq  uint64
	err  error
}

// collectV2Audio 不依赖网络进度读取，事件交付可取消，阻塞 Read 由 Run 关闭源解除。
// Realtime 按采集时刻及累计音频时长推进；交付迟到不会重置时间轴。
func (c *V2Client) collectV2Audio(ctx context.Context, source io.Reader, events chan<- v2SourceEvent) {
	planned := time.Now()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if c.cfg.Realtime {
			timer := time.NewTimer(time.Until(planned))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		data := make([]byte, c.cfg.ChunkBytes)
		n, err := io.ReadFull(source, data)
		event := v2SourceEvent{data: data[:n]}
		switch {
		case err == nil:
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			event.eof = true
		default:
			event.err = err
		}
		planned = planned.Add(audio.DurationFromBytes(n))
		select {
		case <-ctx.Done():
			return
		case events <- event:
		}
		if event.eof || event.err != nil {
			return
		}
	}
}

// readV2Events 仅接收完整文本消息，成功后立即记时；错误也经同一有界通道。
func readV2Events(ctx context.Context, conn v2Connection, events chan<- v2ReadEvent) {
	for {
		typ, data, err := conn.Read(ctx)
		event := v2ReadEvent{data: data, err: err}
		if err == nil {
			event.receivedAt = time.Now()
			if typ != websocket.MessageText {
				event = v2ReadEvent{err: errV2ClientProtocol}
			}
		} else {
			event.data = nil
		}
		select {
		case <-ctx.Done():
			return
		case events <- event:
		}
		if event.err != nil {
			return
		}
	}
}

// writeV2Jobs 串行写入，每个实际取得的任务必有一个回执；失败后退出。
// 唯一待写任务和容量 1 的回执通道保证取消时也能交付实际结果。
func (c *V2Client) writeV2Jobs(ctx context.Context, conn v2Connection, jobs <-chan v2WriteJob, receipts chan<- v2WriteReceipt) {
	for {
		var job v2WriteJob
		select {
		case <-ctx.Done():
			return
		case job = <-jobs:
		}
		typ, data, err := encodeV2Job(job)
		job.data = nil
		if err == nil {
			writeCtx, cancel := context.WithTimeout(ctx, c.cfg.WriteTimeout)
			err = conn.Write(writeCtx, typ, data)
			cancel()
		}
		data = nil
		receipts <- v2WriteReceipt{kind: job.kind, seq: job.seq, err: err}
		if err != nil {
			return
		}
	}
}

// encodeV2Job 创建帧，音频带大端位置头，控制消息沿用现有线格式。
func encodeV2Job(job v2WriteJob) (websocket.MessageType, []byte, error) {
	if job.kind == v2WriteAudio {
		data := make([]byte, 8+len(job.data))
		binary.BigEndian.PutUint64(data, job.offset)
		copy(data[8:], job.data)
		return websocket.MessageBinary, data, nil
	}
	var message any
	switch job.kind {
	case v2WriteEnd:
		message = wsprotocol.V2EndMessage{Type: wsprotocol.MessageTypeEnd, FinalOffset: strconv.FormatUint(job.offset, 10)}
	case v2WriteResultAck:
		message = wsprotocol.ResultAckMessage{Type: wsprotocol.MessageTypeResultAck, Seq: strconv.FormatUint(job.seq, 10)}
	case v2WriteCompletedAck:
		message = wsprotocol.CompletedAckMessage{Type: wsprotocol.MessageTypeCompletedAck, FinalOffset: job.offset, LastSeq: job.seq}
	default:
		return 0, nil, errV2ClientState
	}
	data, err := json.Marshal(message)
	return websocket.MessageText, data, err
}

// v2ServerMessage 指定唯一有效字段；ready 含凭据，不得整体记录日志。
type v2ServerMessage struct {
	typ        wsprotocol.MessageType
	ready      wsprotocol.ReadyMessage
	audioAck   wsprotocol.AudioAckMessage
	result     wsprotocol.SequencedResultMessage
	completed  wsprotocol.CompletedMessage
	entryError wsprotocol.V2ErrorMessage
}

// decodeV2Message 按 type 解析已有类型，阶段/业务合法性留给协调者。
// 固定错误不回显 JSON 原文；两端受控范围内不另建必填字段校验层。
func decodeV2Message(data []byte) (v2ServerMessage, error) {
	var envelope struct {
		Type wsprotocol.MessageType `json:"type"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return v2ServerMessage{}, errV2ClientProtocol
	}
	message := v2ServerMessage{typ: envelope.Type}
	var target any
	switch message.typ {
	case wsprotocol.MessageTypeReady:
		target = &message.ready
	case wsprotocol.MessageTypeAudioAck:
		target = &message.audioAck
	case wsprotocol.MessageTypeResult:
		target = &message.result
	case wsprotocol.MessageTypeCompleted:
		target = &message.completed
	case wsprotocol.MessageTypeError:
		target = &message.entryError
	default:
		return v2ServerMessage{}, errV2ClientProtocol
	}
	if err := json.Unmarshal(data, target); err != nil {
		return v2ServerMessage{}, errV2ClientProtocol
	}
	if message.typ == wsprotocol.MessageTypeError && message.entryError.Code == "" {
		return v2ServerMessage{}, errV2ClientProtocol
	}
	return message, nil
}
