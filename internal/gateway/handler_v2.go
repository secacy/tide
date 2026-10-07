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
		http.Error(w, v2EntryErrorText(err), http.StatusServiceUnavailable)
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
	if entryCtx.Err() != nil || g.ctx.Err() != nil {
		http.Error(w, "service is stopping", http.StatusServiceUnavailable)
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
			data, _ := json.Marshal(wsprotocol.ErrorMessage{Type: wsprotocol.MessageTypeError, Message: v2EntryErrorText(err)})
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

// prepareV2Worker 只选择/建立一次原 RPC，构造但不启动任务。
// RPC 继承 g.ctx；返回前解除入口取消转发，并等待已开始的回调。
// 失败取消 RPC；成功后的取消或交接责任属于调用者。
func (g *Gateway) prepareV2Worker(entryCtx context.Context) (*sessionWorker, error) {
	if cause := context.Cause(entryCtx); cause != nil {
		return nil, cause
	}
	rpcCtx, cancelRPC := context.WithCancelCause(g.ctx)
	stopForwarding := forwardEntryCancellation(entryCtx, cancelRPC)
	defer stopForwarding()
	selected := g.pool.Pick()
	stream, openErr := selected.Client.StreamingRecognize(rpcCtx)
	stopForwarding()
	if cause := context.Cause(entryCtx); cause != nil {
		cancelRPC(cause)
		return nil, cause
	}
	if cause := context.Cause(rpcCtx); cause != nil {
		cancelRPC(cause)
		return nil, cause
	}
	if openErr != nil {
		cancelRPC(openErr)
		return nil, fmt.Errorf("%w: %w", errV2WorkerUnavailable, openErr)
	}
	worker, err := newSessionWorker(sessionWorkerConfig{
		rpcCtx: rpcCtx, cancelRPC: cancelRPC, stream: stream,
		sendTimeout: g.cfg.WorkerSendTimeout, tailTimeout: g.cfg.TailTimeout,
		statusTimeout: g.v2.WorkerStatusTimeout, resultRetentionTimeout: g.v2.ResultRetentionTimeout,
		maxAudioBytes: g.v2.MaxAudioBytes, maxAudioChunks: g.v2.MaxAudioChunks,
		maxResultBytes: g.v2.MaxResultBytes, maxResults: g.v2.MaxResults,
		maxPendingAudioBytes: uint64(g.cfg.MaxPendingAudioBytes),
	})
	if err != nil {
		cancelRPC(err)
		return nil, err
	}
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

// runV2Session 在新建提交后恰好启动一次；完成实际清理后才撤销注册、归还名额。
func (g *Gateway) runV2Session(s *resumableSession, worker *sessionWorker, initial *connectionCandidate) {
	_ = s.runWithConnection(g.ctx, time.Now, worker, initial)
	g.registry.remove(s.identity.id, s)
	g.tracker.leave()
}

// v2EntryErrorText 将内部原因映射为固定公开文本，不包含报文、凭据或后端错误。
// 这是实验入口分类，后续客户端重试协议再收敛为机器可读错误码。
func v2EntryErrorText(err error) string {
	switch {
	case errors.Is(err, errGatewayStopping):
		return "service is stopping"
	case errors.Is(err, errHandshakeLimit):
		return "handshake limit exceeded"
	case errors.Is(err, errSessionLimit):
		return "session limit exceeded"
	case errors.Is(err, ErrHandshakeTimeout), errors.Is(err, context.DeadlineExceeded):
		return "entry timeout"
	case errors.Is(err, wsprotocol.ErrInvalidV2Handshake), errors.Is(err, websocket.ErrMessageTooBig):
		return "invalid handshake"
	case errors.Is(err, errResumeUnavailable), errors.Is(err, errResumeClosed), errors.Is(err, errResumeExpired), errors.Is(err, errResumeGenerationExhausted):
		return "resume unavailable"
	case errors.Is(err, errResumeAlreadyAttached), errors.Is(err, errConnectionRetiring):
		return "session busy"
	case errors.Is(err, errResultReplayGap), errors.Is(err, errResultAckAhead):
		return "invalid resume position"
	case errors.Is(err, errV2WorkerUnavailable):
		return "worker unavailable"
	default:
		return "internal error"
	}
}
