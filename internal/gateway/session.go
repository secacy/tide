package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/coder/websocket"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"golang.org/x/sync/errgroup"
)

// workerStream 描述 Gateway 真正依赖的 gRPC stream 能力。
//
// 使用一个本地小接口，可以减少 Gateway 对 protoc 生成类型名称的依赖， 同时方便后续写单元测试。
type workerStream interface {
	Send(request *asrv1.StreamingRecognizeRequest) error
	Recv() (*asrv1.StreamingRecognizeResponse, error)
	CloseSend() error
}

// session 表示一个 WebSocket Connection 与一个 gRPC stream 之间的一对一桥接关系。
type session struct {
	ws     *websocket.Conn
	worker workerStream
}

var errWorkerCompleted = errors.New("worker stream completed")

// run 同时运行两个单向转发循环。 任意一个方向异常时，errgroup 会取消另外一个方向。
func (s *session) run(ctx context.Context) error {
	group, groupCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return s.forwardRequests(groupCtx)
	})

	group.Go(func() error {
		return s.forwardResponses(groupCtx)
	})

	err := group.Wait()
	if errors.Is(err, errWorkerCompleted) {
		return nil
	}

	return err
}

// forwardRequests 负责 WebSocket -> gRPC 的单向转发。
func (s *session) forwardRequests(ctx context.Context) error {
	for {
		messageType, data, err := s.ws.Read(ctx)
		if err != nil {
			return fmt.Errorf("read websocket message: %w", err)
		}

		switch messageType {
		case websocket.MessageBinary:
			err := s.forwardAudio(data)
			switch {
			case err == nil:
				continue
			case errors.Is(err, io.EOF):
				// Worker 可能已经以 ResourceExhausted、InvalidArgument 等状态终止 RPC。真正的 status 将由 Recv() 获取。
				return nil
			default:
				// 此时应该终止整个 session。
				return fmt.Errorf("send PCM to worker: %w", err)
			}

		case websocket.MessageText:
			end, err := isEndMessage(data)
			if err != nil {
				return err
			}
			if !end {
				return fmt.Errorf("unexpected websocket text message")
			}
			if err := s.worker.CloseSend(); err != nil {
				return fmt.Errorf("close worker send side: %w", err)
			}
			return nil

		default:
			return fmt.Errorf("unsupported websocket message type: %v", messageType)
		}
	}
}

// forwardAudio 立即把一个 WebSocket PCM Message 转发给 Worker。
func (s *session) forwardAudio(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty PCM message")
	}
	req := &asrv1.StreamingRecognizeRequest{
		Data: data,
	}
	return s.worker.Send(req)
}

func isEndMessage(data []byte) (bool, error) {
	var message struct {
		Type wsprotocol.MessageType `json:"type"`
	}

	if err := json.Unmarshal(data, &message); err != nil {
		return false, fmt.Errorf("decode websocket control message: %w", err)
	}

	switch message.Type {
	case wsprotocol.MessageTypeEnd:
		return true, nil

	case wsprotocol.MessageTypeStart:
		return false, fmt.Errorf("duplicate start message")

	default:
		return false, fmt.Errorf("unsupported control message type %q", message.Type)
	}
}
