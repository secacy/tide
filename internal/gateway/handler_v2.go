package gateway

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/workerpool"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

var (
	errResumeUnavailable   = errors.New("resume unavailable")    // 缺失会话或认证失败，不暴露凭据。
	errV2WorkerUnavailable = errors.New("v2 worker unavailable") // 建流失败的入口分类，保留内部原因。
)

// forwardEntryCancellation 临时将 source 的取消原因转发给 target。
// 返回的停止函数可重复调用，等待已开始的短回调真正返回。
// 停止必须在提交锁外执行；解除转发不代表 source 仍有效。
func forwardEntryCancellation(source context.Context, target context.CancelCauseFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(source, func() {
		target(context.Cause(source))
		close(done)
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}

// ServeV2HTTP 管理一次临时连接的握手及所有权交接。
// 成功交接后逻辑运行器拥有 socket，handler 不再读写或主动关闭。
func (g *Gateway) ServeV2HTTP(w http.ResponseWriter, r *http.Request) {
	if g.v2 == nil {
		http.NotFound(w, r)
		return
	}
	if err := g.gate.tryEnterHandshake(); err != nil {
		writeV2EntryHTTPError(w, err)
		return
	}
	defer g.gate.leaveHandshake()

	base, cancelBase := context.WithCancelCause(r.Context())
	stopGatewayCancellation := forwardEntryCancellation(g.ctx, cancelBase)
	entryCtx, cancelEntry := context.WithTimeout(base, g.v2.EntryTimeout)
	defer func() {
		stopGatewayCancellation()
		cancelEntry()
		cancelBase(nil)
	}()
	if g.ctx.Err() != nil {
		writeV2EntryHTTPError(w, errGatewayStopping)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if cause := context.Cause(entryCtx); cause != nil {
		writeV2EntryHTTPError(w, cause)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	owned := true
	defer func() {
		if owned {
			_ = conn.CloseNow()
		}
	}()

	handshake, err := readV2Handshake(entryCtx, conn, g.cfg.StartTimeout, g.cfg.MaxMessageBytes)
	if err == nil {
		var candidate *connectionCandidate
		candidate, err = newConnectionCandidate(conn, g.cfg.ResultWriteTimeout, g.cfg.MaxMessageBytes)
		if err == nil {
			switch handshake.Kind {
			case wsprotocol.V2HandshakeStart:
				err = g.startV2(entryCtx, candidate)
			case wsprotocol.V2HandshakeResume:
				err = g.resumeV2(entryCtx, candidate, handshake)
			default:
				panic("gateway: invalid decoded v2 handshake")
			}
		}
	}
	if err != nil {
		// 仅未交接的入口写失败提示；期限已到仍有独立的有界收尾预算。
		if g.ctx.Err() == nil && r.Context().Err() == nil && websocket.CloseStatus(err) == -1 {
			writeCtx, cancel := context.WithTimeout(g.ctx, g.cfg.ResultWriteTimeout)
			data, _ := json.Marshal(v2EntryError(err))
			_ = conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
		}
		return
	}
	owned = false
}

// startV2 申请逻辑名额，准备资源，在短提交中登记，再无条件启动运行器。
// nil 表示 candidate、Worker 与逻辑名额已交接；失败不接管候选。
func (g *Gateway) startV2(entryCtx context.Context, candidate *connectionCandidate) (err error) {
	if err := g.gate.tryEnterSession(); err != nil {
		return err
	}
	committed := false
	var worker *sessionWorker
	defer func() {
		if !committed {
			if worker != nil {
				worker.stopIO(err)
				worker.releaseBuffers()
				worker.releaseLease()
			}
			g.tracker.leave()
		}
	}()
	s, err := newResumableSession(rand.Reader, g.v2.ResumeWindow)
	if err != nil {
		return err
	}
	s.entryGate = g.gate
	worker, err = g.prepareV2Worker(entryCtx)
	if err != nil {
		return err
	}
	err = g.gate.withCommit(entryCtx, func() error {
		if cause := context.Cause(g.ctx); cause != nil {
			return cause
		}
		if cause := context.Cause(worker.config.rpcCtx); cause != nil {
			return cause
		}
		return g.registry.add(s)
	})
	if err != nil {
		return err
	}
	// 登记是交接点；随后取消仍由运行器统一退出，不能再报告未交接。
	committed = true
	go g.runV2Session(s, worker, candidate)
	return nil
}

// prepareV2Worker 取得一次 Lease 并建立原 RPC，构造但不启动任务。
// RPC 继承 g.ctx；返回前解除入口取消转发，并等待已开始的回调。
// 失败先取消 RPC 再归还 Lease；成功将两者交给调用者。
func (g *Gateway) prepareV2Worker(entryCtx context.Context) (prepared *sessionWorker, err error) {
	if cause := context.Cause(entryCtx); cause != nil {
		return nil, cause
	}
	rpcCtx, cancelRPC := context.WithCancelCause(g.ctx)
	stopForwarding := forwardEntryCancellation(entryCtx, cancelRPC)
	var lease *workerpool.WorkerLease
	transferred := false
	defer func() {
		stopForwarding()
		if !transferred {
			cancelRPC(err)
			if lease != nil {
				lease.Release()
			}
		}
	}()
	lease, acquireErr := g.allocator.TryAcquire()
	// 申请过程中发生的入口/服务取消仍优先于本地容量错误。
	if cause := context.Cause(entryCtx); cause != nil {
		return nil, cause
	}
	if cause := context.Cause(rpcCtx); cause != nil {
		return nil, cause
	}
	if acquireErr != nil {
		return nil, fmt.Errorf("%w: %w", errV2WorkerUnavailable, acquireErr)
	}
	selected := lease.Worker()
	stream, openErr := selected.Client.StreamingRecognize(rpcCtx)
	stopForwarding()
	if cause := context.Cause(entryCtx); cause != nil {
		return nil, cause
	}
	if cause := context.Cause(rpcCtx); cause != nil {
		return nil, cause
	}
	if openErr != nil {
		return nil, fmt.Errorf("%w: %w", errV2WorkerUnavailable, openErr)
	}
	worker, err := newSessionWorker(sessionWorkerConfig{
		rpcCtx: rpcCtx, cancelRPC: cancelRPC, stream: stream,
		sendTimeout: g.cfg.WorkerSendTimeout, tailTimeout: g.cfg.TailTimeout,
		inputProgressTimeout: g.v2.InputProgressTimeout,
		statusTimeout:        g.v2.WorkerStatusTimeout, resultRetentionTimeout: g.v2.ResultRetentionTimeout,
		maxAudioBytes: g.v2.MaxAudioBytes, maxAudioChunks: g.v2.MaxAudioChunks,
		maxResultBytes: g.v2.MaxResultBytes, maxResults: g.v2.MaxResults,
		maxPendingAudioBytes: uint64(g.cfg.MaxPendingAudioBytes),
	})
	if err != nil {
		return nil, err
	}
	worker.lease = lease
	transferred = true
	return worker, nil
}

// resumeV2 只定位、认证并提交一次恢复命令，不申请新逻辑名额或重建 RPC。
// 等待明确回复才转移 candidate 所有权；失败不回退新建。
func (g *Gateway) resumeV2(entryCtx context.Context, candidate *connectionCandidate, handshake wsprotocol.V2Handshake) error {
	s, ok := g.registry.lookup(handshake.SessionID)
	if !ok || !s.identity.matchesResumeToken(handshake.ResumeToken) {
		return errResumeUnavailable
	}
	_, err := s.requestResumeConnection(entryCtx, candidate, handshake.AppliedSeq)
	return err
}

// runV2Session 在新建提交后恰好启动一次；本地清理后撤销注册、归还 Lease 及总名额。
// 本地任务退出不确认远端模型释放，Worker 仍负责最终模型准入。
func (g *Gateway) runV2Session(s *resumableSession, worker *sessionWorker, initial *connectionCandidate) {
	_ = s.runWithConnection(g.ctx, time.Now, worker, initial)
	g.registry.remove(s.identity.id, s)
	worker.releaseLease()
	g.tracker.leave()
}
