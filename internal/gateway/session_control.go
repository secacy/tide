package gateway

import (
	"context"
	"errors"
	"net"
)

// attachTransport 绑定升级接管的底层连接。
//
// conn 必须非 nil，每个 Session 只允许调用一次。
// 如果此前已经请求强制停止，则在绑定后立即关闭该连接。
// 网络关闭必须在释放 controlMu 后执行。
func (s *session) attachTransport(conn net.Conn) {
	s.controlMu.Lock()
	s.transport = conn
	aborted := s.aborted
	s.controlMu.Unlock()
	if aborted {
		_ = conn.Close()
	}
}

// abort 请求强制停止本会话，允许并发和重复调用。
//
// 取消识别工作，并关闭已绑定的底层连接。
// 尚未绑定连接时，保留停止标记，由后续绑定流程关闭连接。
// 此方法不注销会话，也不等待 run 和它创建的 goroutine 退出。
func (s *session) abort() {
	s.cancel(errSessionAborted)
	s.controlMu.Lock()
	if s.aborted {
		s.controlMu.Unlock()
		return
	}
	s.aborted = true
	transport := s.transport
	s.controlMu.Unlock()
	if transport != nil {
		_ = transport.Close()
	}
}

// cancellationResult 将已经发生的取消映射为会话结果，未取消时返回 false。
// 保留首次取消的原因；显式强制停止和父 Context 取消分别归类。
func cancellationResult(ctx context.Context) (sessionResult, bool) {
	if ctx.Err() == nil {
		return sessionResult{}, false
	}
	cause := context.Cause(ctx)
	kind := resultServerStopping
	if errors.Is(cause, errSessionAborted) {
		kind = resultAborted
	}
	return sessionResult{kind: kind, err: cause}, true
}
