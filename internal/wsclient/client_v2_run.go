package wsclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// v2RunPhase 不代替实际清理完成；所有阶段均由整场协调者独占。
type v2RunPhase uint8

const (
	v2Handshaking v2RunPhase = iota // 初始 start 或后续 resume。
	v2Streaming                     // 当前连接可处理业务。
	v2Closing                       // 旧连接退出中；继续采集，不开启新 Dial。
	v2Backoff                       // 有限等待期间继续采集。
	v2Finishing                     // 已核实完成，仅等待最终确认。
)

// v2RecoveryState 由协调者独占；成功 ready 不重置整场统计或近期记录。
type v2RecoveryState struct {
	deadline     time.Time   // 当前断线固定截止点，成功接纳 ready 后清零。
	attempts     int         // 整场实际启动的恢复尝试数，成功后不清零。
	attemptTimes []time.Time // 仅滑动模式使用；按启动顺序保存，有效记录数至多 MaxAttempts。
	failures     int         // 当前一轮失败尝试数，决定下一次退避。
	lastErr      error       // 最近故障原因，预算耗尽时一并返回。
}

// resumeAttemptAllowed 检查次数额度但不消耗；调用前须已确认 Resume 非 nil。
// now 保留单调时钟；滑动模式仅保留 (now-AttemptWindow, now] 内的记录。
// 仅在可恢复故障和准备启动握手时检查，不能因为额度用完而结束正常传输。
func (r *v2ClientRun) resumeAttemptAllowed(now time.Time) bool {
	cfg := r.client.cfg.Resume
	if cfg.AttemptWindow == 0 {
		return r.recovery.attempts < cfg.MaxAttempts
	}
	times := r.recovery.attemptTimes
	expired := 0
	for expired < len(times) && now.Sub(times[expired]) >= cfg.AttemptWindow {
		expired++
	}
	if expired > 0 {
		remaining := copy(times, times[expired:])
		clear(times[remaining:]) // 清除尾部 time.Time 所含引用，复用已分配存储。
		r.recovery.attemptTimes = times[:remaining]
	}
	return len(r.recovery.attemptTimes) < cfg.MaxAttempts
}

// recordResumeAttempt 在协调者确认额度后、启动恢复握手任务前调用一次。
// 两种模式都累计整场统计；滑动模式另记 now，首次 start 不调用此方法。
func (r *v2ClientRun) recordResumeAttempt(now time.Time) {
	r.recovery.attempts++
	if r.client.cfg.Resume.AttemptWindow > 0 {
		r.recovery.attemptTimes = append(r.recovery.attemptTimes, now)
	}
}

// v2ClientRun 拥有整场资源；只由调用 Run 的 goroutine 读写。
// 子任务只读取固定 I/O 依赖，通过有界事件或 done 发布结果，不访问运行状态。
type v2ClientRun struct {
	client             *V2Client
	ctx                context.Context
	cancel             context.CancelFunc
	source             io.ReadCloser
	state              *v2ClientState
	phase              v2RunPhase
	dial               v2Dialer
	attempt            *v2HandshakeAttempt // 至多一个候选，交接前始终有可追踪的拥有者。
	current            *v2ClientConnection // 包含尚未退出的旧连接。
	sourceTasks        sync.WaitGroup
	sourceStarted      bool
	sourceInput        <-chan v2SourceEvent // EOF 后置 nil，不重新启动采集。
	recovery           v2RecoveryState
	backoffUntil       time.Time
	backoffRand        *rand.Rand // 仅 jitter 启用时创建；本 Run 协调者独占，恢复成功不重新播种。
	completionDeadline time.Time  // 第一次 end 授权设置，跨连接固定。
	finalDeadline      time.Time  // Completed 后最终确认的固定总窗口。
	resumed            int
	ackWritten         bool // 实际 completed_ack Write 成功，不证明远端已处理。
}

// newV2Run 创建独占状态；调用方从此负责 cleanup，包含握手失败。
func (c *V2Client) newV2Run(ctx context.Context, source io.ReadCloser) (*v2ClientRun, error) {
	state, err := newV2ClientState(c.cfg.MaxReplayBytes, c.cfg.MaxReplayChunks)
	if err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	var backoffRand *rand.Rand
	if cfg := c.cfg.Resume; cfg != nil && cfg.Jitter {
		seed := cfg.JitterSeed
		if seed == 0 {
			seed = rand.Uint64()
		}
		// 固定种子只复现调度序列，不用于会话身份或恢复凭据。
		backoffRand = rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	}
	return &v2ClientRun{client: c, ctx: runCtx, cancel: cancel, source: source, state: state, backoffRand: backoffRand, dial: func(ctx context.Context, url string) (v2ManagedConnection, *http.Response, error) {
		conn, response, err := websocket.Dial(ctx, url, nil)
		if conn == nil {
			return nil, response, err
		}
		return conn, response, err
	}}, nil
}

// startCapture 在第一次合法 ready 后调用一次；重连不更改源或采集时间轴。
func (r *v2ClientRun) startCapture() {
	if r.sourceStarted {
		return
	}
	events := make(chan v2SourceEvent, 1)
	r.sourceStarted = true
	r.sourceInput = events
	r.sourceTasks.Go(func() { r.client.collectV2Audio(r.ctx, r.source, events) })
}

// startHandshake 构造独立身份快照；resume 的预算包含此前清理与退避。
func (r *v2ClientRun) startHandshake(resume bool) {
	now := time.Now()
	var request any = wsprotocol.StartMessage{Type: wsprotocol.MessageTypeStart, Version: "v2"}
	if resume {
		r.recordResumeAttempt(now)
		request = wsprotocol.ResumeMessage{Type: wsprotocol.MessageTypeResume, Version: "v2", SessionID: r.state.sessionID, ResumeToken: r.state.resumeToken, AppliedSeq: r.state.appliedSeq}
	}
	deadline := earlierV2(now.Add(r.client.cfg.HandshakeTimeout), r.recovery.deadline, r.completionDeadline)
	r.attempt = r.client.startV2Handshake(r.ctx, request, deadline, r.dial)
	r.phase = v2Handshaking
	r.backoffUntil = time.Time{}
}

// checkBudget 在 select 前及事件提交前复核绝对期限，拒绝迟到 ready/completed。
func (r *v2ClientRun) checkBudget() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if !r.finalDeadline.IsZero() && !now.Before(r.finalDeadline) {
		return fmt.Errorf("write v2 completed ack: %w", context.DeadlineExceeded)
	}
	if !r.state.completed && !r.completionDeadline.IsZero() && !now.Before(r.completionDeadline) {
		return fmt.Errorf("wait v2 completed: %w", context.DeadlineExceeded)
	}
	if !r.recovery.deadline.IsZero() && !now.Before(r.recovery.deadline) {
		return errors.Join(ErrV2RecoveryExhausted, context.DeadlineExceeded, r.recovery.lastErr)
	}
	return nil
}

// recover 在明确的连接/握手失败处调用；源、回调及状态错误直接终止。
func (r *v2ClientRun) recover(failure v2AttemptFailure) error {
	if err := r.checkBudget(); err != nil {
		return err
	}
	cfg := r.client.cfg.Resume
	if cfg == nil || r.state.generation == 0 || r.state.completed || !retryableV2Failure(failure) {
		return failure.err
	}
	r.recovery.lastErr = failure.err
	if !r.resumeAttemptAllowed(time.Now()) {
		return errors.Join(ErrV2RecoveryExhausted, failure.err)
	}
	if r.recovery.deadline.IsZero() {
		r.recovery.deadline = time.Now().Add(cfg.Window)
		r.recovery.failures = 0
	}
	if r.current != nil {
		r.state.detach(r.current.generation)
		r.current.beginClose()
		r.phase = v2Closing
	} else {
		r.recovery.failures++
		r.backoffUntil = time.Now().Add(r.resumeBackoffDelay(r.recovery.failures))
		r.phase = v2Backoff
	}
	return nil
}

// dispatch 只在活动连接空闲时产生一个授权/副本，保持 ACK 与音频交替。
func (r *v2ClientRun) dispatch() error {
	a, s := r.current, r.state
	if a == nil || a.writerBusy || (r.phase != v2Streaming && r.phase != v2Finishing) {
		return nil
	}
	var job v2WriteJob
	if s.completed {
		job = v2WriteJob{kind: v2WriteCompletedAck, offset: s.audio.nextOffset, seq: s.appliedSeq}
	} else if offset, ok, err := s.authorizeEnd(a.generation); err != nil {
		return err
	} else if ok {
		job = v2WriteJob{kind: v2WriteEnd, offset: offset}
		if r.completionDeadline.IsZero() {
			r.completionDeadline = time.Now().Add(r.client.cfg.CompletionTimeout)
		}
	} else if s.appliedSeq > a.lastResultAckWritten && (a.lastWriteKind != v2WriteResultAck || s.sendOffset == s.audio.nextOffset) {
		job = v2WriteJob{kind: v2WriteResultAck, seq: s.appliedSeq}
	} else if chunk, ok, err := s.authorizeAudio(a.generation); err != nil {
		return err
	} else if ok {
		job = v2WriteJob{kind: v2WriteAudio, offset: chunk.offset, data: chunk.data}
	}
	if job.kind != 0 {
		a.writerBusy = true
		a.lastWriteKind = job.kind
		a.writeJobs <- job
	}
	return nil
}

// acceptSource 全阶段持续接管新音频；预算满明确失败，EOF 只封口。
func (r *v2ClientRun) acceptSource(event v2SourceEvent) error {
	if event.err != nil {
		return fmt.Errorf("read v2 audio: %w", event.err)
	}
	if len(event.data)%audio.BytesDepth != 0 {
		return errors.New("v2 audio must align to PCM samples")
	}
	if len(event.data) > 0 {
		if _, err := r.state.appendAudio(event.data); err != nil {
			return err
		}
	}
	if event.eof {
		r.state.sealInput()
		r.sourceInput = nil
	}
	return nil
}

// acceptRead 绑定当前连接，成功应用后才提交；回调错误不进入恢复。
func (r *v2ClientRun) acceptRead(event v2ReadEvent) error {
	if event.err != nil {
		err := event.err
		if status := websocket.CloseStatus(err); status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
			err = errors.Join(ErrV2Incomplete, err)
		}
		return r.recover(v2AttemptFailure{kind: v2AttemptTransport, err: fmt.Errorf("read v2 stream: %w", err)})
	}
	msg, err := decodeV2Message(event.data)
	if err != nil {
		return err
	}
	generation := r.current.generation
	switch msg.typ {
	case wsprotocol.MessageTypeAudioAck:
		_, err = r.state.acceptAudioAck(generation, msg.audioAck)
		return err
	case wsprotocol.MessageTypeResult:
		apply, err := r.state.checkResult(generation, msg.result)
		if err != nil || !apply {
			return err
		}
		deadline := earlierV2(time.Now().Add(r.client.cfg.ApplyTimeout), r.completionDeadline)
		applyCtx, cancel := context.WithDeadline(r.ctx, deadline)
		if r.client.cfg.OnResult != nil {
			err = r.client.cfg.OnResult(applyCtx, msg.result, event.receivedAt)
		}
		if cause := applyCtx.Err(); cause != nil {
			err = errors.Join(err, cause)
		}
		cancel()
		if err != nil {
			return fmt.Errorf("apply v2 result: %w", err)
		}
		return r.state.commitApplied(generation, msg.result.Seq)
	case wsprotocol.MessageTypeCompleted:
		if _, err := r.state.acceptCompleted(generation, msg.completed); err != nil {
			return err
		}
		r.finalDeadline = time.Now().Add(r.client.cfg.WriteTimeout)
		r.phase = v2Finishing
		return nil
	default:
		return errV2ClientProtocol
	}
}

// coordinate 始终留在 Run 调用者 goroutine；握手和清理等待不阻塞源事件处理。
func (r *v2ClientRun) coordinate() error {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		if err := r.checkBudget(); err != nil {
			return err
		}
		if r.phase == v2Backoff && !time.Now().Before(r.backoffUntil) {
			if !r.resumeAttemptAllowed(time.Now()) {
				return errors.Join(ErrV2RecoveryExhausted, r.recovery.lastErr)
			}
			r.startHandshake(true)
		}
		if err := r.dispatch(); err != nil {
			return err
		}
		var reads <-chan v2ReadEvent
		var receipts <-chan v2WriteReceipt
		var closed, handshakeDone <-chan struct{}
		inputs := r.sourceInput
		if r.phase == v2Streaming {
			reads = r.current.readEvents
			receipts = r.current.writeReceipts
		}
		if r.phase == v2Finishing {
			receipts = r.current.writeReceipts
			inputs = nil
		}
		if r.phase == v2Closing {
			closed = r.current.done
		}
		if r.attempt != nil {
			handshakeDone = r.attempt.done
		}
		completion := r.completionDeadline
		if r.state.completed {
			completion = time.Time{}
		}
		next := earlierV2(completion, r.finalDeadline, r.recovery.deadline, r.backoffUntil)
		var wake <-chan time.Time
		timer.Stop()
		if !next.IsZero() {
			timer.Reset(time.Until(next))
			wake = timer.C
		}
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-wake:
		case event := <-inputs:
			if err := r.checkBudget(); err != nil {
				return err
			}
			if err := r.acceptSource(event); err != nil {
				return err
			}
		case event := <-reads:
			if err := r.checkBudget(); err != nil {
				return err
			}
			if err := r.acceptRead(event); err != nil {
				return err
			}
		case receipt := <-receipts:
			if receipt.kind == v2WriteCompletedAck && receipt.err == nil {
				r.ackWritten = true
			}
			if err := r.checkBudget(); err != nil {
				return err
			}
			r.current.writerBusy = false
			if receipt.err != nil {
				if err := r.recover(v2AttemptFailure{kind: v2AttemptTransport, err: fmt.Errorf("write v2 stream: %w", receipt.err)}); err != nil {
					return err
				}
				continue
			}
			if receipt.kind == v2WriteResultAck {
				r.current.lastResultAckWritten = receipt.seq
			}
			if receipt.kind == v2WriteCompletedAck {
				return nil
			}
		case <-closed:
			if err := r.checkBudget(); err != nil {
				return err
			}
			closedConnection := r.current
			r.current = nil
			if closedConnection.closeErr != nil {
				return closedConnection.closeErr
			}
			r.phase = v2Backoff
			r.backoffUntil = time.Now() // 首次恢复无需退避。
		case <-handshakeDone:
			if err := r.checkBudget(); err != nil {
				return err
			} // attempt 仍持有候选，最终 cleanup 不会遗失。
			attempt := r.attempt
			if attempt.result.failure.err != nil {
				r.attempt = nil
				if err := r.recover(attempt.result.failure); err != nil {
					return err
				}
				continue
			}
			ready := attempt.result.ready
			resumed := r.state.generation != 0
			if err := r.state.acceptReady(ready); err != nil {
				return err
			}
			r.current = r.client.startV2Connection(r.ctx, attempt.result.conn, ready)
			attempt.result.conn = nil
			r.attempt = nil // socket 所有权转入 current。
			if resumed {
				r.resumed++
			}
			r.recovery.deadline = time.Time{}
			r.recovery.failures = 0
			r.recovery.lastErr = nil
			r.phase = v2Streaming
			r.startCapture()
		}
	}
}

// cleanup 只在整场退出调用一次；取消、关闭并等待全部拥有的资源。
func (r *v2ClientRun) cleanup() error {
	r.cancel()
	if r.attempt != nil {
		r.attempt.cancel()
	}
	if r.current != nil {
		r.current.beginClose()
	}
	var cleanupErr error
	if err := r.source.Close(); err != nil {
		cleanupErr = fmt.Errorf("close v2 source: %w", err)
	}
	if r.attempt != nil {
		<-r.attempt.done
		if r.attempt.result.conn != nil {
			cleanupErr = errors.Join(cleanupErr, closeV2Socket(r.attempt.result.conn))
		}
		// 失败清理中的错误同样不能因预算先到期而被丢弃。
		if r.attempt.result.failure.kind == v2AttemptTerminal {
			cleanupErr = errors.Join(cleanupErr, r.attempt.result.failure.err)
		}
	}
	if r.current != nil {
		<-r.current.done
		cleanupErr = errors.Join(cleanupErr, r.current.closeErr)
		select {
		case receipt := <-r.current.writeReceipts:
			if receipt.kind == v2WriteCompletedAck && receipt.err == nil {
				r.ackWritten = true
			}
		default:
		}
	}
	r.sourceTasks.Wait()
	r.state.detach(r.state.generation)
	return cleanupErr
}

// result 仅在协调及清理完成后读取，保留已知业务完成和实际写出事实。
func (r *v2ClientRun) result() V2RunResult {
	result := V2RunResult{Completed: r.state.completed, AppliedSeq: r.state.appliedSeq, CompletionAckWritten: r.ackWritten, ResumeAttempts: r.recovery.attempts, ResumedConnections: r.resumed}
	if result.Completed {
		result.FinalOffset = r.state.audio.nextOffset
	}
	return result
}

// earlierV2 选出非零时间中的最早截止点，全部为零时返回零。
func earlierV2(times ...time.Time) time.Time {
	var result time.Time
	for _, at := range times {
		if !at.IsZero() && (result.IsZero() || at.Before(result)) {
			result = at
		}
	}
	return result
}
