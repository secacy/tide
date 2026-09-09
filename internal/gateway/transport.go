package gateway

import (
	"bufio"
	"net"
	"net/http"
)

// sessionResponseWriter 在连接被接管时，将底层连接登记到 Session。
// 普通 HTTP 响应操作仍委托给原 ResponseWriter。
type sessionResponseWriter struct {
	http.ResponseWriter               // 原始响应写入器，保留普通 HTTP 响应行为。
	hijacker            http.Hijacker // 从原写入器或其 Unwrap 链中取得的接管能力。
	session             *session      // 接收底层连接的会话。
}

// Hijack 委托底层写入器接管连接，并在返回前绑定到 Session。
func (w *sessionResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, b, err := w.hijacker.Hijack()
	if err != nil {
		return conn, b, err
	}
	w.session.attachTransport(conn)
	return conn, b, nil
}

// wrapSessionResponseWriter 为支持连接接管的响应写入器增加会话绑定能力。
//
// 从 w 或其 Unwrap 链中查找 http.Hijacker。
// 找到后，返回保留原始 w 响应行为的 sessionResponseWriter。
// 不支持接管时返回原始 w，让 websocket.Accept 按原有方式拒绝升级。
func wrapSessionResponseWriter(w http.ResponseWriter, s *session) http.ResponseWriter {
	for current := w; current != nil; {
		if hijacker, ok := current.(http.Hijacker); ok {
			return &sessionResponseWriter{
				ResponseWriter: w,
				hijacker:       hijacker,
				session:        s,
			}
		}
		unwrapper, ok := current.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		current = unwrapper.Unwrap()
	}
	return w
}

// Unwrap 保留 ResponseController 沿包装链查找其他响应控制能力的路径。
func (w *sessionResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
