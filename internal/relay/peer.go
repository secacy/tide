package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	peerv1 "github.com/secacy/tide/gen/tide/peer/v1"
	"github.com/secacy/tide/internal/session"
	"google.golang.org/grpc"
)

type PeerServer struct {
	peerv1.UnimplementedGatewayPeerServer
	NodeID  string
	Store   session.Store
	Manager *Manager
}

func (s *PeerServer) Relay(stream grpc.BidiStreamingServer[peerv1.EdgeToOwner, peerv1.OwnerToEdge]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	open := first.GetOpen()
	if open == nil {
		return fmt.Errorf("first relay message must be open")
	}
	current, err := s.Store.Get(stream.Context(), open.StreamId)
	if err != nil {
		return err
	}
	if current.TenantID != open.TenantId || current.Generation != open.Generation {
		return session.ErrForbidden
	}
	if current.OwnerID != s.NodeID || current.Epoch != open.Epoch {
		return session.ErrOwnerConflict
	}
	bridge, err := s.Manager.Attach(current, open.Generation)
	if err != nil {
		return err
	}
	defer bridge.Close()
	if err := stream.Send(&peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Ready{Ready: &peerv1.Ready{
		Epoch: current.Epoch, NextSampleOffset: current.NextOffset,
	}}}); err != nil {
		return err
	}
	receiveErr := make(chan error, 1)
	go func() {
		for {
			message, err := stream.Recv()
			if err != nil {
				receiveErr <- err
				return
			}
			switch payload := message.Payload.(type) {
			case *peerv1.EdgeToOwner_Audio:
				err = bridge.SendAudio(AudioFrame{
					SampleOffset: payload.Audio.SampleOffset,
					PCM:          append([]byte(nil), payload.Audio.Pcm...),
					ReceivedAt:   time.Unix(0, payload.Audio.ReceivedUnixNano),
				})
			case *peerv1.EdgeToOwner_End:
				err = bridge.End(payload.End.Reason)
			default:
				err = errors.New("unexpected relay message")
			}
			if err != nil {
				receiveErr <- err
				return
			}
		}
	}()
	for {
		select {
		case event, ok := <-bridge.Events():
			if !ok {
				return nil
			}
			if err := stream.Send(eventToProto(event)); err != nil {
				return err
			}
		case err := <-receiveErr:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

type PeerDialer struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	options []grpc.DialOption
}

func NewPeerDialer(options ...grpc.DialOption) *PeerDialer {
	return &PeerDialer{conns: make(map[string]*grpc.ClientConn), options: options}
}

func (d *PeerDialer) Attach(ctx context.Context, ownerAddr string, stream session.Session) (Bridge, error) {
	conn, err := d.connection(ownerAddr)
	if err != nil {
		return nil, err
	}
	relayCtx, cancel := context.WithCancel(ctx)
	rpc, err := peerv1.NewGatewayPeerClient(conn).Relay(relayCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open peer relay: %w", err)
	}
	if err := rpc.Send(&peerv1.EdgeToOwner{Payload: &peerv1.EdgeToOwner_Open{Open: &peerv1.Open{
		StreamId: stream.ID, TenantId: stream.TenantID, Generation: stream.Generation,
		Epoch: stream.Epoch, LanguageCode: stream.LanguageCode,
	}}}); err != nil {
		cancel()
		return nil, fmt.Errorf("send peer open: %w", err)
	}
	firstResult := make(chan struct {
		message *peerv1.OwnerToEdge
		err     error
	}, 1)
	go func() {
		message, receiveErr := rpc.Recv()
		firstResult <- struct {
			message *peerv1.OwnerToEdge
			err     error
		}{message: message, err: receiveErr}
	}()
	var first *peerv1.OwnerToEdge
	select {
	case result := <-firstResult:
		if result.err != nil {
			cancel()
			return nil, fmt.Errorf("receive peer ready: %w", result.err)
		}
		first = result.message
	case <-time.After(3 * time.Second):
		cancel()
		return nil, errors.New("timed out waiting for peer ready")
	case <-ctx.Done():
		cancel()
		return nil, ctx.Err()
	}
	if first.GetReady() == nil {
		cancel()
		return nil, errors.New("peer did not send ready")
	}
	remote := &remoteBridge{rpc: rpc, events: make(chan Event, eventQueueSize), cancel: cancel}
	go remote.receive()
	return remote, nil
}

func (d *PeerDialer) connection(addr string) (*grpc.ClientConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if conn := d.conns[addr]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(addr, d.options...)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", addr, err)
	}
	d.conns[addr] = conn
	return conn, nil
}

func (d *PeerDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.conns = make(map[string]*grpc.ClientConn)
}

type remoteBridge struct {
	rpc    peerv1.GatewayPeer_RelayClient
	events chan Event
	mu     sync.Mutex
	closed bool
	once   sync.Once
	cancel context.CancelFunc
}

func (r *remoteBridge) Events() <-chan Event { return r.events }

func (r *remoteBridge) SendAudio(frame AudioFrame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	return r.rpc.Send(&peerv1.EdgeToOwner{Payload: &peerv1.EdgeToOwner_Audio{Audio: &peerv1.Audio{
		SampleOffset: frame.SampleOffset, Pcm: frame.PCM,
		ReceivedUnixNano: frame.ReceivedAt.UnixNano(),
	}}})
}

func (r *remoteBridge) End(reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrClosed
	}
	return r.rpc.Send(&peerv1.EdgeToOwner{Payload: &peerv1.EdgeToOwner_End{End: &peerv1.End{Reason: reason}}})
}

func (r *remoteBridge) Close() {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		_ = r.rpc.CloseSend()
		r.cancel()
		r.mu.Unlock()
	})
}

func (r *remoteBridge) receive() {
	defer close(r.events)
	for {
		message, err := r.rpc.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				r.events <- Event{Type: EventError, Code: "owner_unavailable", Message: "owner connection lost", Retryable: true}
			}
			return
		}
		r.events <- protoToEvent(message)
	}
}

func eventToProto(event Event) *peerv1.OwnerToEdge {
	switch event.Type {
	case EventAck:
		return &peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Ack{Ack: &peerv1.Ack{NextSampleOffset: event.NextSampleOffset}}}
	case EventTranscript:
		return &peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Transcript{Transcript: &peerv1.Transcript{
			Epoch: event.Epoch, SegmentId: event.SegmentID, Revision: event.Revision,
			Text: event.Text, IsFinal: event.IsFinal, StartMs: event.StartMS, EndMs: event.EndMS,
			ReceivedUnixNano: event.ReceivedAt.UnixNano(),
		}}}
	case EventDiscontinuity:
		return &peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Discontinuity{Discontinuity: &peerv1.Discontinuity{
			PreviousEpoch: event.PreviousEpoch, Epoch: event.Epoch, Reason: event.Reason,
		}}}
	case EventEnded:
		return &peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Ended{Ended: &peerv1.Ended{Reason: event.Reason}}}
	default:
		return &peerv1.OwnerToEdge{Payload: &peerv1.OwnerToEdge_Error{Error: &peerv1.RelayError{
			Code: event.Code, Message: event.Message, Retryable: event.Retryable,
		}}}
	}
}

func protoToEvent(message *peerv1.OwnerToEdge) Event {
	switch payload := message.Payload.(type) {
	case *peerv1.OwnerToEdge_Ack:
		return Event{Type: EventAck, NextSampleOffset: payload.Ack.NextSampleOffset}
	case *peerv1.OwnerToEdge_Transcript:
		value := payload.Transcript
		return Event{
			Type: EventTranscript, Epoch: value.Epoch, SegmentID: value.SegmentId,
			Revision: value.Revision, Text: value.Text, IsFinal: value.IsFinal,
			StartMS: value.StartMs, EndMS: value.EndMs,
			ReceivedAt: time.Unix(0, value.ReceivedUnixNano),
		}
	case *peerv1.OwnerToEdge_Discontinuity:
		return Event{Type: EventDiscontinuity, PreviousEpoch: payload.Discontinuity.PreviousEpoch, Epoch: payload.Discontinuity.Epoch, Reason: payload.Discontinuity.Reason}
	case *peerv1.OwnerToEdge_Ended:
		return Event{Type: EventEnded, Reason: payload.Ended.Reason}
	case *peerv1.OwnerToEdge_Error:
		return Event{Type: EventError, Code: payload.Error.Code, Message: payload.Error.Message, Retryable: payload.Error.Retryable}
	default:
		return Event{Type: EventError, Code: "invalid_owner_message", Message: "owner sent an invalid message"}
	}
}
