package wsclient

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// attemptEvent serializes transport observations for the logical Session owner.
// done is last: the connection, heartbeat and Sender have already been joined.
type attemptEvent struct {
	id      string
	message *wsprotocol.RecoveryMessage
	done    bool
	err     error
	fatal   bool
}

// recognitionAttempt owns one transport lifetime. The coordinator alone changes
// its offsets and flags; joined closes after all I/O and heartbeat work exits.
type recognitionAttempt struct {
	id                     string
	from, sent             uint64
	input                  chan capturedFrame
	cancel                 context.CancelFunc
	joined                 chan struct{}
	ready, ending, invalid bool
}

func (c *Client) attemptIO(parent context.Context, start wsprotocol.StartMessage, input <-chan capturedFrame, emit func(attemptEvent)) (error, bool) {
	conn, response, err := websocket.Dial(parent, c.cfg.URL, nil)
	if err != nil {
		fatal := response != nil && response.StatusCode >= 400 && response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests
		if response != nil {
			_ = response.Body.Close()
		}
		return err, fatal
	}
	defer conn.CloseNow()
	conn.SetReadLimit(c.cfg.ReadLimit)
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	hb := wsheartbeat.Start(ctx, c.cfg.Heartbeat, conn.Ping, func(err error) { cancel(err); _ = conn.CloseNow() })
	defer func() { hb.Stop(); _ = conn.CloseNow(); _ = hb.Wait() }()
	if err := c.writeJSON(ctx, conn, start); err != nil {
		return err, false
	}
	var sendErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case f := <-input:
				if f.end {
					sendErr = c.writeJSON(ctx, conn, wsprotocol.EndMessage{Type: wsprotocol.MessageTypeEnd, ThroughSample: f.from})
				} else {
					data := make([]byte, 8+len(f.data))
					binary.BigEndian.PutUint64(data, f.from)
					copy(data[8:], f.data)
					sendErr = conn.Write(ctx, websocket.MessageBinary, data)
				}
				if sendErr != nil {
					cancel(sendErr)
					_ = conn.CloseNow()
					return
				}
				if f.end {
					return
				}
			}
		}
	}()
	defer func() { hb.Stop(); cancel(nil); _ = conn.CloseNow(); wg.Wait() }()
	for {
		typ, data, err := conn.Read(parent)
		if err != nil {
			hb.Stop()
			cancel(nil)
			_ = conn.CloseNow()
			wg.Wait()
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure && sendErr == nil {
				return nil, false
			}
			fatal := websocket.CloseStatus(err) == websocket.StatusPolicyViolation || websocket.CloseStatus(err) == websocket.StatusProtocolError
			if errors.Is(context.Cause(ctx), wsheartbeat.ErrFailed) {
				err = errors.Join(err, context.Cause(ctx))
			}
			return err, fatal
		}
		var msg wsprotocol.RecoveryMessage
		if typ != websocket.MessageText || json.Unmarshal(data, &msg) != nil || (msg.Type != wsprotocol.MessageTypeReady && msg.Type != wsprotocol.MessageTypeCheckpoint) {
			return fmt.Errorf("%w: unexpected message", ErrRecoveryProtocol), true
		}
		if msg.SessionID != start.SessionID {
			return fmt.Errorf("%w: attempt identity mismatch", ErrRecoveryProtocol), true
		}
		if msg.AttemptID != start.AttemptID {
			continue
		} // Fence delayed old-attempt results.
		emit(attemptEvent{id: start.AttemptID, message: &msg})
	}
}
