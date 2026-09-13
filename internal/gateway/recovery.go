package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/secacy/tide-artisan/internal/wsprotocol"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInvalidRecovery = errors.New("invalid recovery protocol")

// recoveryState has separate Reader and download-owned offsets. sent is shared
// with Sender, published before Send so an immediate checkpoint is not rejected.
type recoveryState struct {
	start     wsprotocol.StartMessage
	received  uint64
	sent      atomic.Uint64
	committed uint64
	ready     bool
}

func newRecoveryState(start wsprotocol.StartMessage) *recoveryState {
	r := &recoveryState{start: start, received: start.FromSample, committed: start.FromSample}
	r.sent.Store(start.FromSample)
	return r
}
func (r *recoveryState) decodeAudio(data []byte) ([]byte, error) {
	if len(data) <= 8 || (len(data)-8)%2 != 0 {
		return nil, fmt.Errorf("invalid v2 PCM frame")
	}
	from := binary.BigEndian.Uint64(data[:8])
	n := uint64((len(data) - 8) / 2)
	if from != r.received || from+n < from {
		return nil, fmt.Errorf("noncontiguous v2 audio")
	}
	r.received += n
	return data[8:], nil
}
func (s *session) parseEndMessage(data []byte) error {
	if err := parseEnd(data); err != nil {
		return err
	}
	if s.recovery != nil {
		var end wsprotocol.EndMessage
		if err := json.Unmarshal(data, &end); err != nil {
			return err
		}
		if end.ThroughSample != s.recovery.received {
			return fmt.Errorf("End offset does not match audio")
		}
	}
	return nil
}
func (s *session) openStream(ctx context.Context) (workerStream, error) {
	if s.recovery == nil {
		return s.worker.StreamingRecognize(ctx)
	}
	stream, err := s.worker.RecoverableRecognize(ctx)
	if err != nil {
		return nil, err
	}
	start := s.recovery.start
	if err := stream.Send(&asrv1.StreamingRecognizeRequest{RecoveryStart: &asrv1.RecoveryStart{SessionId: start.SessionID, AttemptId: start.AttemptID, FromSample: start.FromSample}}); err != nil {
		// Send can report EOF before Recv exposes Unimplemented from an old Worker.
		if errors.Is(err, io.EOF) {
			if _, recvErr := stream.Recv(); recvErr != nil {
				err = recvErr
			}
		}
		return nil, err
	}
	return &recoveryStream{workerStream: stream, state: s.recovery}, nil
}

// recoveryStream adds absolute offsets without changing the v1 FIFO or local seq.
type recoveryStream struct {
	workerStream
	state *recoveryState
}

func (r *recoveryStream) Send(req *asrv1.StreamingRecognizeRequest) error {
	req.StartSample = r.state.sent.Load()
	next := req.StartSample + uint64(len(req.Data)/2)
	if next < req.StartSample {
		return fmt.Errorf("sample offset overflow")
	}
	r.state.sent.Store(next)
	return r.workerStream.Send(req)
}
func (r *recoveryState) response(resp *asrv1.StreamingRecognizeResponse) (wsprotocol.RecoveryMessage, bool, error) {
	msg := wsprotocol.RecoveryMessage{SessionID: r.start.SessionID, AttemptID: r.start.AttemptID}
	ordinary := resp.Text != "" || resp.SegmentId != "" || resp.IsFinal
	if resp.Ready {
		if r.ready || resp.Progress != nil || resp.Checkpoint != nil || ordinary {
			return msg, true, fmt.Errorf("invalid Worker ready")
		}
		r.ready = true
		msg.Type = wsprotocol.MessageTypeReady
		msg.FromSample = r.start.FromSample
		msg.ThroughSample = r.start.FromSample
		return msg, true, nil
	}
	if !r.ready {
		return msg, true, fmt.Errorf("Worker response before ready")
	}
	if cp := resp.Checkpoint; cp != nil {
		if ordinary || resp.Progress != nil || cp.FromSample != r.committed || cp.ThroughSample <= cp.FromSample || cp.ThroughSample > r.sent.Load() {
			return msg, true, fmt.Errorf("invalid Worker recovery checkpoint")
		}
		r.committed = cp.ThroughSample
		msg.Type = wsprotocol.MessageTypeCheckpoint
		msg.FromSample = cp.FromSample
		msg.ThroughSample = cp.ThroughSample
		msg.Text = cp.Text
		return msg, true, nil
	}
	if ordinary || resp.Progress == nil {
		return msg, true, fmt.Errorf("v2 requires checkpoint or progress")
	}
	return msg, false, nil
}
func (s *session) writeRecovery(ctx context.Context, msg wsprotocol.RecoveryMessage, timeout time.Duration) sessionResult {
	writeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := writeJSON(writeCtx, s.ws, msg)
	if err == nil {
		return sessionResult{kind: resultCompleted}
	}
	if errors.Is(writeCtx.Err(), context.DeadlineExceeded) {
		return sessionResult{kind: resultWriteTimedOut, err: fmt.Errorf("%w: %w", errResultWriteTimeout, err)}
	}
	return clientDisconnected(err)
}
func isRecoveryUnsupported(err error) bool { return status.Code(err) == codes.Unimplemented }
