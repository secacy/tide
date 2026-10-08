package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// V2Config 配置一次新建 v2 会话；数值零值使用默认值，负值非法。
// URL 指向 Gateway 的 /v2/asr；本版本不自动重连。
type V2Config struct {
	URL               string          // 必填的 WebSocket 地址。
	ChunkBytes        int             // PCM 块大小，默认 3200，须按样本字节对齐。
	Realtime          bool            // 按固定采集时间轴读取；false 尽快读取，可能耗尽未确认缓存。
	ReadLimit         int64           // 单条完整服务端消息上限，默认 64 KiB。
	MaxReplayBytes    uint64          // 缓存独占的未确认音频负载上限，默认 10 秒 PCM。
	MaxReplayChunks   int             // 缓存块数上限，默认 128；不是全客户端内存预算。
	HandshakeTimeout  time.Duration   // Dial/start/ready 共用的绝对预算，默认 10s。
	WriteTimeout      time.Duration   // 单次写预算及完成后的最终确认总窗口，默认 5s。
	ApplyTimeout      time.Duration   // 单次业务应用预算，默认 5s，回调须合作取消。
	CompletionTimeout time.Duration   // 首次 end 授权到 completed 的固定预算，默认 30s。
	OnResult          V2ResultHandler // nil 表示有意消费并丢弃结果，仍推进连续应用位置。
}

// V2ResultHandler 按序同步应用结果，nil 返回后才提交并确认。
// 必须及时响应 ctx；失败不自动重试已有外部副作用的应用。
// 多个 Run 共用回调时，调用方负责共享数据的并发安全。
type V2ResultHandler func(ctx context.Context, result wsprotocol.SequencedResultMessage, receivedAt time.Time) error

// V2RunResult 保留已核实的完成事实，即使确认写入或清理随后失败。
type V2RunResult struct {
	Completed            bool   // 已核实本场 completed。
	FinalOffset          uint64 // 仅 Completed=true 时有效，最终音频字节位置。
	AppliedSeq           uint64 // 已连续成功应用的结果位置，允许 0。
	CompletionAckWritten bool   // completed_ack 的实际 Write 成功；不证明对端已处理。
}

// V2Client 保存不可变配置；每个 Run 独占状态、连接和任务。
type V2Client struct{ cfg V2Config }

// V2EntryError 保留入口拒绝码，包括未知的非空码；不回显服务端提示。
type V2EntryError struct{ Code wsprotocol.V2ErrorCode }

// Error 返回固定说明；调用方通过 errors.As 读取 Code。
func (e *V2EntryError) Error() string { return "v2 session entry rejected" }

// ErrV2Incomplete 表示对端正常关闭，但本地尚未核实整场完成。
var ErrV2Incomplete = errors.New("v2 session ended before completion")

// NewV2 填充默认值并校验分块、预算及超时。
func NewV2(cfg V2Config) (*V2Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("websocket URL is required")
	}
	if cfg.ChunkBytes < 0 || cfg.ReadLimit < 0 || cfg.MaxReplayChunks < 0 ||
		cfg.HandshakeTimeout < 0 || cfg.WriteTimeout < 0 || cfg.ApplyTimeout < 0 || cfg.CompletionTimeout < 0 {
		return nil, errors.New("v2 client limits and timeouts must not be negative")
	}
	if cfg.ChunkBytes == 0 {
		cfg.ChunkBytes = audio.ChunkBytesDefault
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 64 * 1024
	}
	if cfg.MaxReplayBytes == 0 {
		cfg.MaxReplayBytes = 10 * audio.BytesPerSecond
	}
	if cfg.MaxReplayChunks == 0 {
		cfg.MaxReplayChunks = 128
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}
	if cfg.CompletionTimeout == 0 {
		cfg.CompletionTimeout = 30 * time.Second
	}
	if cfg.ChunkBytes%audio.BytesDepth != 0 || cfg.ChunkBytes > int(^uint(0)>>1)-8 || uint64(cfg.ChunkBytes) > cfg.MaxReplayBytes {
		return nil, errors.New("v2 chunk must align to PCM samples, fit replay budget and offset header")
	}
	return &V2Client{cfg: cfg}, nil
}

// Run 对 source 新建一次会话；交入的非 nil source 恰好 Close 一次，包括握手失败。
// source.Close 必须可与 Read 并发、及时返回并解除阻塞 Read。调用方不再使用该源。
// 返回前等待全部自建任务退出；回调也必须遵守取消契约。
// nil error 表示完成已核实、确认写成功且本地清理成功。
func (c *V2Client) Run(ctx context.Context, source io.ReadCloser) (result V2RunResult, runErr error) {
	if source == nil {
		return result, errors.New("audio source is nil")
	}
	runCtx, cancel := context.WithCancel(ctx)
	var conn *websocket.Conn
	var tasks sync.WaitGroup
	var receipts chan v2WriteReceipt
	state, err := newV2ClientState(c.cfg.MaxReplayBytes, c.cfg.MaxReplayChunks)
	// 所有路径共用收尾。关闭源/连接解除阻塞，再等待实际退出。
	defer func() {
		cancel()
		if err := source.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close v2 source: %w", err))
		}
		if conn != nil {
			if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
				runErr = errors.Join(runErr, fmt.Errorf("close v2 websocket: %w", err))
			}
		}
		tasks.Wait()
		// 取消与成功回执可能同时就绪；保留实际已发生的确认写成功。
		if receipts != nil {
			select {
			case receipt := <-receipts:
				if receipt.kind == v2WriteCompletedAck && receipt.err == nil {
					result.CompletionAckWritten = true
				}
			default:
			}
		}
		if state != nil {
			result.Completed = state.completed
			result.AppliedSeq = state.appliedSeq
			if state.completed {
				result.FinalOffset = state.audio.nextOffset
			}
			state.detach(state.generation)
		}
	}()
	if err != nil {
		return result, err
	}
	if err := runCtx.Err(); err != nil {
		return result, err
	}
	handshakeCtx, stopHandshake := context.WithTimeout(runCtx, c.cfg.HandshakeTimeout)
	defer stopHandshake()
	var response *http.Response
	conn, response, err = websocket.Dial(handshakeCtx, c.cfg.URL, nil)
	if err != nil {
		if response != nil && response.StatusCode == http.StatusServiceUnavailable && response.Body != nil {
			// coder/websocket 已关闭网络响应体，只保留最多 1024 字节的内存副本。
			data, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
			if message, parseErr := decodeV2Message(data); parseErr == nil && message.typ == wsprotocol.MessageTypeError {
				return result, &V2EntryError{Code: message.entryError.Code}
			}
		}
		return result, fmt.Errorf("dial v2 websocket: %w", err)
	}
	conn.SetReadLimit(c.cfg.ReadLimit)
	start, _ := json.Marshal(wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"})
	if err := conn.Write(handshakeCtx, websocket.MessageText, start); err != nil {
		return result, fmt.Errorf("write v2 start: %w", err)
	}
	typ, data, err := conn.Read(handshakeCtx)
	if err != nil {
		return result, fmt.Errorf("read v2 ready: %w", err)
	}
	if typ != websocket.MessageText {
		return result, errV2ClientProtocol
	}
	message, err := decodeV2Message(data)
	if err != nil {
		return result, err
	}
	if message.typ == wsprotocol.MessageTypeError {
		return result, &V2EntryError{Code: message.entryError.Code}
	}
	if message.typ != wsprotocol.MessageTypeReady {
		return result, errV2ClientProtocol
	}
	if err := state.acceptReady(message.ready); err != nil {
		return result, err
	}
	if err := handshakeCtx.Err(); err != nil {
		return result, fmt.Errorf("complete v2 handshake: %w", err)
	}
	stopHandshake()
	receipts = make(chan v2WriteReceipt, 1)
	result.CompletionAckWritten, err = c.coordinateV2(runCtx, source, conn, state, receipts, &tasks)
	return result, err
}

// coordinateV2 独占状态，I/O 任务只交付事件；关闭/等待由 Run 统一负责。
// connection 接口同时支持真实 WebSocket 与可控阻塞的测试连接。
func (c *V2Client) coordinateV2(ctx context.Context, source io.Reader, conn v2Connection, state *v2ClientState, receipts chan v2WriteReceipt, tasks *sync.WaitGroup) (bool, error) {
	sourceEvents := make(chan v2SourceEvent, 1)
	readEvents := make(chan v2ReadEvent, 1)
	jobs := make(chan v2WriteJob, 1)
	tasks.Go(func() { c.collectV2Audio(ctx, source, sourceEvents) })
	tasks.Go(func() { readV2Events(ctx, conn, readEvents) })
	tasks.Go(func() { c.writeV2Jobs(ctx, conn, jobs, receipts) })
	var sourceInput <-chan v2SourceEvent = sourceEvents
	var deadline time.Time
	var timer *time.Timer
	var timeout <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	setDeadline := func(at time.Time) {
		if timer != nil {
			timer.Stop()
		}
		deadline = at
		timer = time.NewTimer(time.Until(at))
		timeout = timer.C
	}
	var busy bool
	var lastKind v2WriteKind
	var ackWritten uint64
	checkBudget := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			phase := "wait v2 completed"
			if state.completed {
				phase = "write v2 completed ack"
			}
			return fmt.Errorf("%s: %w", phase, context.DeadlineExceeded)
		}
		return nil
	}
	for {
		if err := checkBudget(); err != nil {
			return false, err
		}
		if !busy {
			var job v2WriteJob
			if state.completed {
				job = v2WriteJob{kind: v2WriteCompletedAck, offset: state.audio.nextOffset, seq: state.appliedSeq}
			} else if offset, ok, err := state.authorizeEnd(state.generation); err != nil {
				return false, err
			} else if ok {
				job = v2WriteJob{kind: v2WriteEnd, offset: offset}
				setDeadline(time.Now().Add(c.cfg.CompletionTimeout))
			} else if state.appliedSeq > ackWritten && (lastKind != v2WriteResultAck || state.sendOffset == state.audio.nextOffset) {
				job = v2WriteJob{kind: v2WriteResultAck, seq: state.appliedSeq}
			} else if chunk, ok, err := state.authorizeAudio(state.generation); err != nil {
				return false, err
			} else if ok {
				job = v2WriteJob{kind: v2WriteAudio, offset: chunk.offset, data: chunk.data}
			}
			if job.kind != 0 {
				busy = true
				lastKind = job.kind
				jobs <- job
			}
		}
		// 完成后只等待实际写回执；排队的 EOF 不抢先取消最终确认。
		var reads <-chan v2ReadEvent = readEvents
		inputs := sourceInput
		if state.completed {
			reads = nil
			inputs = nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timeout: // 下一轮按绝对期限检查，事件不会续期。
		case event := <-inputs:
			if err := checkBudget(); err != nil {
				return false, err
			}
			if event.err != nil {
				return false, fmt.Errorf("read v2 audio: %w", event.err)
			}
			if len(event.data)%audio.BytesDepth != 0 {
				return false, errors.New("v2 audio must align to PCM samples")
			}
			if len(event.data) > 0 {
				if _, err := state.appendAudio(event.data); err != nil {
					return false, err
				}
			}
			if event.eof {
				state.sealInput()
				sourceInput = nil
			}
		case receipt := <-receipts:
			if err := checkBudget(); err != nil {
				return receipt.kind == v2WriteCompletedAck && receipt.err == nil, err
			}
			busy = false
			if receipt.err != nil {
				return false, fmt.Errorf("write v2 stream: %w", receipt.err)
			}
			if receipt.kind == v2WriteResultAck {
				ackWritten = receipt.seq
			}
			if receipt.kind == v2WriteCompletedAck {
				return true, nil
			}
		case event := <-reads:
			// 消息与 Timer 同时就绪时，select 不提供期限优先级。
			// 再按绝对时间检查，禁止迟到 completed 绕过完成预算。
			if err := checkBudget(); err != nil {
				return false, err
			}
			if event.err != nil {
				if websocket.CloseStatus(event.err) == websocket.StatusNormalClosure || websocket.CloseStatus(event.err) == websocket.StatusGoingAway {
					return false, ErrV2Incomplete
				}
				return false, fmt.Errorf("read v2 stream: %w", event.err)
			}
			message, err := decodeV2Message(event.data)
			if err != nil {
				return false, err
			}
			switch message.typ {
			case wsprotocol.MessageTypeAudioAck:
				if _, err := state.acceptAudioAck(state.generation, message.audioAck); err != nil {
					return false, err
				}
			case wsprotocol.MessageTypeResult:
				apply, err := state.checkResult(state.generation, message.result)
				if err != nil {
					return false, err
				}
				if !apply {
					continue
				}
				applyDeadline := time.Now().Add(c.cfg.ApplyTimeout)
				if !deadline.IsZero() {
					applyDeadline = minTime(applyDeadline, deadline)
				}
				applyCtx, stop := context.WithDeadline(ctx, applyDeadline)
				if c.cfg.OnResult != nil {
					err = c.cfg.OnResult(applyCtx, message.result, event.receivedAt)
				}
				if cause := applyCtx.Err(); cause != nil {
					err = errors.Join(err, cause)
				}
				stop()
				if err != nil {
					return false, fmt.Errorf("apply v2 result: %w", err)
				}
				if err := state.commitApplied(state.generation, message.result.Seq); err != nil {
					return false, err
				}
			case wsprotocol.MessageTypeCompleted:
				if _, err := state.acceptCompleted(state.generation, message.completed); err != nil {
					return false, err
				}
				setDeadline(time.Now().Add(c.cfg.WriteTimeout))
			default:
				return false, errV2ClientProtocol
			}
		}
	}
}

// minTime 选择已有完成预算与单次应用预算中较早的截止点。
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
