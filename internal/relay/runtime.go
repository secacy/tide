package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	asrv1 "github.com/secacy/tide/gen/tide/asr/v1"
	"github.com/secacy/tide/internal/metrics"
	"github.com/secacy/tide/internal/session"
)

const (
	audioQueueFrames = 50
	eventQueueSize   = 100
	eventHistorySize = 96
)

type Manager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	runtimes     map[string]*runtime
	creating     map[string]*runtimeCreation
	asr          asrv1.ASRClient
	store        session.Store
	nodeID       string
	ownerLease   time.Duration
	ownerRenew   time.Duration
	readyTimeout time.Duration
	detachWindow time.Duration
	retention    time.Duration
	metrics      *metrics.Metrics
	logger       *slog.Logger
}

type runtimeCreation struct {
	done chan struct{}
	err  error
}

func NewManager(
	asr asrv1.ASRClient,
	store session.Store,
	nodeID string,
	readyTimeout time.Duration,
	ownerLease, ownerRenew, detachWindow, retention time.Duration,
	metrics *metrics.Metrics,
	logger *slog.Logger,
) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ctx: ctx, cancel: cancel, runtimes: make(map[string]*runtime),
		creating: make(map[string]*runtimeCreation),
		asr:      asr, store: store, nodeID: nodeID,
		ownerLease: ownerLease, ownerRenew: ownerRenew, readyTimeout: readyTimeout,
		detachWindow: detachWindow, retention: retention,
		metrics: metrics, logger: logger,
	}
}

func (m *Manager) Attach(stream session.Session, generation, lastEventID uint64) (Bridge, error) {
	for {
		m.mu.Lock()
		if r := m.runtimes[stream.ID]; r != nil {
			m.mu.Unlock()
			return r.attach(generation, lastEventID)
		}
		if pending := m.creating[stream.ID]; pending != nil {
			m.mu.Unlock()
			select {
			case <-pending.done:
				if pending.err != nil {
					return nil, pending.err
				}
				continue
			case <-m.ctx.Done():
				return nil, m.ctx.Err()
			}
		}
		pending := &runtimeCreation{done: make(chan struct{})}
		m.creating[stream.ID] = pending
		m.mu.Unlock()

		r, err := newRuntime(m, stream)
		m.mu.Lock()
		delete(m.creating, stream.ID)
		pending.err = err
		if err == nil {
			m.runtimes[stream.ID] = r
			m.metrics.ActiveOwnedStreams.Inc()
		}
		close(pending.done)
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return r.attach(generation, lastEventID)
	}
}

func (m *Manager) Stop() {
	m.cancel()
	m.mu.Lock()
	runtimes := make([]*runtime, 0, len(m.runtimes))
	for _, r := range m.runtimes {
		runtimes = append(runtimes, r)
	}
	m.mu.Unlock()
	for _, r := range runtimes {
		r.stop("gateway_shutdown")
	}
}

// Drain asks clients to reconnect, expires every local owner lease, and keeps
// logical sessions resumable on another node.
func (m *Manager) Drain() {
	m.mu.Lock()
	runtimes := make([]*runtime, 0, len(m.runtimes))
	for _, r := range m.runtimes {
		runtimes = append(runtimes, r)
	}
	m.mu.Unlock()
	for _, r := range runtimes {
		r.handoff()
	}
}

func (m *Manager) remove(id string, target *runtime) {
	m.mu.Lock()
	if m.runtimes[id] == target {
		delete(m.runtimes, id)
		m.metrics.ActiveOwnedStreams.Dec()
	}
	m.mu.Unlock()
}

type runtime struct {
	manager   *Manager
	stream    session.Session
	ctx       context.Context
	cancel    context.CancelFunc
	asr       asrv1.ASR_TranscribeClient
	audio     chan AudioFrame
	asrEvents chan *asrv1.ASRToGateway
	asrErr    chan error
	end       chan string
	done      chan struct{}

	mu               sync.Mutex
	generation       uint64
	acceptedOffset   uint64
	committedOffset  uint64
	events           chan Event
	detachTimer      *time.Timer
	closed           bool
	preserveSession  bool
	nextEventID      uint64
	historyFloor     uint64
	transcriptEvents []Event
}

type attachment struct {
	runtime    *runtime
	generation uint64
	events     <-chan Event
	once       sync.Once
}

func newRuntime(manager *Manager, stream session.Session) (*runtime, error) {
	ctx, cancel := context.WithDeadline(manager.ctx, stream.ExpiresAt)
	asrStream, err := manager.asr.Transcribe(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open ASR stream: %w", err)
	}
	start := &asrv1.GatewayToASR{Payload: &asrv1.GatewayToASR_Start{Start: &asrv1.Start{
		StreamId: stream.ID, Epoch: stream.Epoch, LanguageCode: stream.LanguageCode,
		InitialSampleOffset: stream.AcceptedOffset,
		Audio:               &asrv1.AudioConfig{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1},
	}}}
	if err := asrStream.Send(start); err != nil {
		cancel()
		return nil, fmt.Errorf("start ASR stream: %w", err)
	}
	r := &runtime{
		manager: manager, stream: stream, ctx: ctx, cancel: cancel, asr: asrStream,
		audio:     make(chan AudioFrame, audioQueueFrames),
		asrEvents: make(chan *asrv1.ASRToGateway, 16), asrErr: make(chan error, 1),
		end: make(chan string, 1), done: make(chan struct{}),
		acceptedOffset: stream.AcceptedOffset, committedOffset: stream.CommittedOffset,
	}
	go r.receiveASR()
	timer := time.NewTimer(manager.readyTimeout)
	defer timer.Stop()
	select {
	case message := <-r.asrEvents:
		ready := message.GetReady()
		if ready == nil || ready.Epoch != stream.Epoch {
			cancel()
			return nil, errors.New("ASR did not confirm the stream epoch")
		}
	case err := <-r.asrErr:
		cancel()
		return nil, fmt.Errorf("wait for ASR ready: %w", err)
	case <-timer.C:
		cancel()
		return nil, errors.New("timed out waiting for ASR ready")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	go r.run()
	return r, nil
}

func (r *runtime) attach(generation, lastEventID uint64) (Bridge, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if generation < r.generation {
		return nil, ErrReplaced
	}
	if r.detachTimer != nil {
		r.detachTimer.Stop()
		r.detachTimer = nil
	}
	if r.events != nil {
		select {
		case r.events <- Event{Type: EventError, Code: "connection_replaced", Message: "replaced by a newer connection"}:
		default:
		}
		close(r.events)
	}
	events := make(chan Event, eventQueueSize)
	if lastEventID > r.nextEventID || lastEventID < r.historyFloor {
		events <- Event{
			Type: EventError, Code: "event_gap",
			Message: "transcript replay window is no longer available", Retryable: false,
		}
	} else {
		for _, event := range r.transcriptEvents {
			if event.EventID > lastEventID {
				event.ReceivedAt = time.Time{}
				events <- event
			}
		}
	}
	r.events = events
	r.generation = generation
	return &attachment{runtime: r, generation: generation, events: events}, nil
}

func (a *attachment) Events() <-chan Event { return a.events }

func (a *attachment) Ready() Ready {
	r := a.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return Ready{
		Epoch: r.stream.Epoch, AcceptedOffset: r.acceptedOffset,
		CommittedOffset: r.committedOffset,
	}
}

func (a *attachment) SendAudio(frame AudioFrame) error {
	r := a.runtime
	r.mu.Lock()
	if r.closed || r.generation != a.generation || r.events == nil {
		r.mu.Unlock()
		return ErrReplaced
	}
	samples := uint64(len(frame.PCM) / 2)
	if frame.SampleOffset < r.acceptedOffset {
		next := r.acceptedOffset
		events := r.events
		r.mu.Unlock()
		select {
		case events <- Event{Type: EventAck, NextSampleOffset: next}:
		default:
		}
		return nil
	}
	if frame.SampleOffset != r.acceptedOffset {
		r.mu.Unlock()
		return ErrInvalidOffset
	}
	select {
	case r.audio <- frame:
		r.acceptedOffset += samples
		r.mu.Unlock()
		return nil
	default:
		r.mu.Unlock()
		return ErrBackpressure
	}
}

func (a *attachment) End(reason string) error {
	select {
	case a.runtime.end <- reason:
		return nil
	case <-a.runtime.done:
		return ErrClosed
	}
}

func (a *attachment) Close() {
	a.once.Do(func() { a.runtime.detach(a.generation) })
}

func (r *runtime) detach(generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.generation != generation {
		return
	}
	if r.events != nil {
		close(r.events)
		r.events = nil
	}
	if r.detachTimer != nil {
		r.detachTimer.Stop()
	}
	r.detachTimer = time.AfterFunc(r.manager.detachWindow, func() {
		current, err := r.manager.store.Get(context.Background(), r.stream.ID)
		if err == nil && current.Generation == generation && current.State == session.StateDetached {
			r.stop("detach_timeout")
		}
	})
}

func (r *runtime) receiveASR() {
	for {
		message, err := r.asr.Recv()
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			select {
			case r.asrErr <- err:
			default:
			}
			return
		}
		select {
		case r.asrEvents <- message:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *runtime) run() {
	renew := time.NewTicker(r.manager.ownerRenew)
	var processedFrames uint64
	defer func() {
		renew.Stop()
		r.finish()
	}()
	for {
		select {
		case frame := <-r.audio:
			message := &asrv1.GatewayToASR{Payload: &asrv1.GatewayToASR_Audio{Audio: &asrv1.Audio{
				SampleOffset: frame.SampleOffset, Pcm: frame.PCM,
				ReceivedUnixNano: frame.ReceivedAt.UnixNano(),
			}}}
			if err := r.asr.Send(message); err != nil {
				r.publish(Event{Type: EventError, Code: "asr_unavailable", Message: "ASR stream unavailable", Retryable: true})
				return
			}
			r.manager.metrics.AudioRelaySeconds.Observe(time.Since(frame.ReceivedAt).Seconds())
			processedFrames++
			if processedFrames%10 == 0 {
				r.mu.Lock()
				offset := r.acceptedOffset
				r.mu.Unlock()
				_ = r.manager.store.UpdateAcceptedOffset(r.ctx, r.stream.ID, r.manager.nodeID, offset)
			}
		case message := <-r.asrEvents:
			r.handleASR(message)
		case err := <-r.asrErr:
			r.manager.logger.Warn("ASR stream ended", "stream_id", r.stream.ID, "error", err)
			r.publish(Event{Type: EventError, Code: "asr_unavailable", Message: "ASR stream unavailable", Retryable: true})
			return
		case reason := <-r.end:
			_ = r.asr.Send(&asrv1.GatewayToASR{Payload: &asrv1.GatewayToASR_End{End: &asrv1.End{Reason: reason}}})
			_ = r.asr.CloseSend()
			r.publish(Event{Type: EventEnded, Reason: reason})
			return
		case now := <-renew.C:
			if err := r.manager.store.RenewOwner(r.ctx, r.stream.ID, r.manager.nodeID, now, r.manager.ownerLease); err != nil {
				r.publish(Event{Type: EventError, Code: "owner_lost", Message: "stream ownership changed", Retryable: true})
				return
			}
		case <-r.ctx.Done():
			r.publish(Event{Type: EventEnded, Reason: "session_expired"})
			return
		}
	}
}

func (r *runtime) handleASR(message *asrv1.ASRToGateway) {
	now := time.Now()
	switch payload := message.Payload.(type) {
	case *asrv1.ASRToGateway_Ack:
		r.mu.Lock()
		next := payload.Ack.NextSampleOffset
		if next < r.committedOffset || next > r.acceptedOffset {
			r.mu.Unlock()
			r.publish(Event{Type: EventError, Code: "invalid_asr_ack", Message: "ASR returned an invalid ACK", Retryable: true})
			return
		}
		if next == r.committedOffset {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		if err := r.manager.store.UpdateCommittedOffset(r.ctx, r.stream.ID, r.manager.nodeID, next); err != nil {
			r.publish(Event{Type: EventError, Code: "store_unavailable", Message: "could not commit audio progress", Retryable: true})
			return
		}
		r.mu.Lock()
		if next > r.committedOffset {
			r.committedOffset = next
		}
		r.mu.Unlock()
		r.publish(Event{Type: EventAck, NextSampleOffset: next, ReceivedAt: now})
	case *asrv1.ASRToGateway_Transcript:
		value := payload.Transcript
		r.publish(Event{
			Type: EventTranscript, Epoch: value.Epoch, SegmentID: value.SegmentId,
			Revision: value.Revision, Text: value.Text, IsFinal: value.IsFinal,
			StartMS: value.StartMs, EndMS: value.EndMs, ReceivedAt: now,
		})
	case *asrv1.ASRToGateway_Error:
		r.publish(Event{
			Type: EventError, Code: payload.Error.Code,
			Message: payload.Error.Message, Retryable: payload.Error.Retryable,
			ReceivedAt: now,
		})
	}
}

func (r *runtime) publish(event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if event.Type == EventTranscript {
		r.nextEventID++
		event.EventID = r.nextEventID
		r.transcriptEvents = append(r.transcriptEvents, event)
		cutoff := time.Now().Add(-r.manager.detachWindow)
		for len(r.transcriptEvents) > 0 &&
			(len(r.transcriptEvents) > eventHistorySize ||
				(!r.transcriptEvents[0].ReceivedAt.IsZero() && r.transcriptEvents[0].ReceivedAt.Before(cutoff))) {
			r.historyFloor = r.transcriptEvents[0].EventID
			r.transcriptEvents = r.transcriptEvents[1:]
		}
	}
	if r.events == nil {
		return
	}
	queued := len(r.events)
	capacity := cap(r.events)
	if (event.Type == EventAck || (event.Type == EventTranscript && !event.IsFinal)) && queued >= capacity-2 {
		return
	}
	if event.Type == EventTranscript && event.IsFinal && queued >= capacity-1 {
		r.manager.metrics.Errors.WithLabelValues("slow_client").Inc()
		r.events <- Event{
			Type: EventError, Code: "slow_client",
			Message: "client cannot consume final results fast enough", Retryable: true,
		}
		close(r.events)
		r.events = nil
		return
	}
	select {
	case r.events <- event:
	default:
		r.manager.metrics.Errors.WithLabelValues("slow_client").Inc()
		close(r.events)
		r.events = nil
	}
}

func (r *runtime) stop(reason string) {
	select {
	case r.end <- reason:
	default:
		r.cancel()
	}
}

func (r *runtime) handoff() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.preserveSession = true
	if r.events != nil {
		select {
		case r.events <- Event{
			Type: EventError, Code: "owner_draining",
			Message: "owner is draining; reconnect to continue", Retryable: true,
		}:
		default:
		}
	}
	r.mu.Unlock()
	_ = r.manager.store.ReleaseOwner(context.Background(), r.stream.ID, r.manager.nodeID)
	r.stop("owner_handoff")
}

func (r *runtime) finish() {
	r.cancel()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	generation, accepted, committed, preserve := r.generation, r.acceptedOffset, r.committedOffset, r.preserveSession
	if r.detachTimer != nil {
		r.detachTimer.Stop()
	}
	if r.events != nil {
		close(r.events)
		r.events = nil
	}
	r.mu.Unlock()
	_ = r.manager.store.UpdateAcceptedOffset(context.Background(), r.stream.ID, r.manager.nodeID, accepted)
	_ = r.manager.store.UpdateCommittedOffset(context.Background(), r.stream.ID, r.manager.nodeID, committed)
	if preserve {
		_ = r.manager.store.ReleaseOwner(context.Background(), r.stream.ID, r.manager.nodeID)
		_ = r.manager.store.MarkDetached(
			context.Background(), r.stream.ID, generation,
			time.Now().Add(r.manager.detachWindow),
		)
	} else {
		_ = r.manager.store.End(context.Background(), r.stream.ID, "", "runtime_ended", r.manager.retention)
	}
	close(r.done)
	r.manager.remove(r.stream.ID, r)
}
