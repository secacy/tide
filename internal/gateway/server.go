package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/secacy/tide/internal/auth"
	"github.com/secacy/tide/internal/config"
	"github.com/secacy/tide/internal/id"
	"github.com/secacy/tide/internal/metrics"
	"github.com/secacy/tide/internal/protocol"
	"github.com/secacy/tide/internal/relay"
	"github.com/secacy/tide/internal/session"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	streamTokenTTL = 2 * time.Minute
	maxRequestBody = 4096
	maxWSMessage   = 4096
)

type Server struct {
	config      config.Config
	ownerID     string
	store       session.Store
	auth        *auth.Verifier
	tokens      *auth.StreamTokenService
	manager     *relay.Manager
	peers       *relay.PeerDialer
	metrics     *metrics.Metrics
	logger      *slog.Logger
	active      atomic.Int64
	open        atomic.Int64
	draining    atomic.Bool
	rateMu      sync.Mutex
	rateWindows map[string]*rateWindow
	example     http.Handler
}

type rateWindow struct {
	start time.Time
	count int
}

func NewServer(
	cfg config.Config,
	ownerID string,
	store session.Store,
	verifier *auth.Verifier,
	tokens *auth.StreamTokenService,
	manager *relay.Manager,
	peers *relay.PeerDialer,
	metrics *metrics.Metrics,
	logger *slog.Logger,
	example http.Handler,
) *Server {
	return &Server{
		config: cfg, ownerID: ownerID, store: store, auth: verifier, tokens: tokens,
		manager: manager, peers: peers, metrics: metrics, logger: logger,
		rateWindows: make(map[string]*rateWindow), example: example,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/streams", s.createStream)
	mux.HandleFunc("POST /v1/streams/{stream_id}/resume-token", s.rotateResumeToken)
	mux.HandleFunc("DELETE /v1/streams/{stream_id}", s.deleteStream)
	mux.HandleFunc("GET /v1/streams/{stream_id}/ws", s.connectStream)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := s.store.Health(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "session_store_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	if s.config.Environment != "production" {
		mux.HandleFunc("POST /dev/token", s.developmentToken)
	}
	if s.example != nil {
		mux.Handle("/", s.example)
	}
	return recoverMiddleware(s.logger, otelhttp.NewHandler(mux, "tide-http"))
}

func (s *Server) SetDraining() { s.draining.Store(true) }

func (s *Server) WaitForNoConnections(ctx context.Context) bool {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.open.Load() == 0 {
			return true
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return false
		}
	}
}

type createRequest struct {
	LanguageCode string `json:"language_code"`
}

type createResponse struct {
	StreamID    string    `json:"stream_id"`
	WebSocket   string    `json:"websocket_url"`
	AttachToken string    `json:"attach_token"`
	ExpiresAt   time.Time `json:"expires_at"`
	Audio       audioSpec `json:"audio"`
}

type audioSpec struct {
	Encoding     string `json:"encoding"`
	SampleRateHz int    `json:"sample_rate_hz"`
	Channels     int    `json:"channels"`
	FrameMS      int    `json:"frame_ms"`
}

type rotateTokenResponse struct {
	ResumeToken string    `json:"resume_token"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func (s *Server) createStream(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "draining", "gateway is draining")
		return
	}
	identity, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
		return
	}
	if !s.allowRate("create:"+identity.TenantID, s.config.CreateRatePerMin) {
		w.Header().Set("Retry-After", "60")
		writeError(w, http.StatusTooManyRequests, "rate_limited", "stream creation rate exceeded")
		return
	}
	var request createRequest
	body := http.MaxBytesReader(w, r.Body, maxRequestBody)
	if err := json.NewDecoder(body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", "invalid request body")
		return
	}
	if len(request.LanguageCode) > 35 {
		writeError(w, http.StatusBadRequest, "invalid_language", "language_code is too long")
		return
	}
	streamID, err := id.New()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not create stream")
		return
	}
	token, tokenHash, tokenExpiry, err := s.tokens.Issue(
		streamID, identity.TenantID, auth.TokenAttach, 0, 0, streamTokenTTL,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not create stream")
		return
	}
	now := time.Now()
	stream := session.Session{
		ID: streamID, TenantID: identity.TenantID,
		LanguageCode: request.LanguageCode, State: session.StateCreated,
		TokenHash: tokenHash, CreatedAt: now, ExpiresAt: now.Add(s.config.MaxSessionAge),
	}
	if err := s.store.Create(r.Context(), stream, s.config.TenantMaxStreams); err != nil {
		if errors.Is(err, session.ErrQuotaExceeded) {
			writeError(w, http.StatusTooManyRequests, "quota_exceeded", "tenant stream quota exceeded")
			return
		}
		s.logger.Error("create stream", "error", err)
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "session store unavailable")
		return
	}
	s.metrics.StreamsCreated.Inc()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, createResponse{
		StreamID: streamID, WebSocket: s.webSocketURL(r, streamID),
		AttachToken: token, ExpiresAt: tokenExpiry,
		Audio: audioSpec{Encoding: "pcm_s16le", SampleRateHz: 16000, Channels: 1, FrameMS: 40},
	})
}

func (s *Server) rotateResumeToken(w http.ResponseWriter, r *http.Request) {
	identity, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
		return
	}
	if !s.allowRate("resume-token:"+identity.TenantID, s.config.ConnectRatePerMin) {
		w.Header().Set("Retry-After", "60")
		writeError(w, http.StatusTooManyRequests, "rate_limited", "resume token rate exceeded")
		return
	}
	jti, tokenHash, err := s.tokens.NewTokenID()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not create resume token")
		return
	}
	stream, err := s.store.RotateToken(
		r.Context(), r.PathValue("stream_id"), identity.TenantID, tokenHash,
		time.Now(), s.config.DetachWindow,
	)
	if err != nil {
		switch {
		case errors.Is(err, session.ErrNotFound):
			writeError(w, http.StatusNotFound, "stream_not_found", "stream not found")
		case errors.Is(err, session.ErrForbidden):
			writeError(w, http.StatusForbidden, "forbidden", "stream belongs to another tenant")
		case errors.Is(err, session.ErrResumeExpired):
			writeError(w, http.StatusGone, "resume_window_expired", "stream resume window has expired")
		case errors.Is(err, session.ErrEnded), errors.Is(err, session.ErrExpired):
			writeError(w, http.StatusGone, "stream_ended", "stream has ended")
		default:
			s.logger.Error("rotate resume token", "stream_id", r.PathValue("stream_id"), "error", err)
			writeError(w, http.StatusServiceUnavailable, "store_unavailable", "session store unavailable")
		}
		return
	}
	kind := auth.TokenResume
	if stream.State == session.StateCreated && stream.Generation == 0 {
		kind = auth.TokenAttach
	}
	token, expiresAt, err := s.tokens.IssueWithID(
		stream.ID, stream.TenantID, kind, jti,
		stream.Generation, stream.Epoch, streamTokenTTL,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not create resume token")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, rotateTokenResponse{ResumeToken: token, ExpiresAt: expiresAt})
}

func (s *Server) deleteStream(w http.ResponseWriter, r *http.Request) {
	identity, err := s.authenticate(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
		return
	}
	if err := s.store.End(r.Context(), r.PathValue("stream_id"), identity.TenantID, "api_request", s.config.EndedRetention); err != nil {
		if errors.Is(err, session.ErrForbidden) {
			writeError(w, http.StatusForbidden, "forbidden", "stream belongs to another tenant")
			return
		}
		s.logger.Error("end stream", "error", err)
		writeError(w, http.StatusServiceUnavailable, "store_unavailable", "session store unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type helloMessage struct {
	Type  string `json:"type"`
	Token string `json:"token"`
}

type wireEvent struct {
	Type             string    `json:"type"`
	StreamID         string    `json:"stream_id,omitempty"`
	Epoch            uint64    `json:"epoch,omitempty"`
	PreviousEpoch    uint64    `json:"previous_epoch,omitempty"`
	NextSampleOffset uint64    `json:"next_sample_offset,omitempty"`
	ResumeToken      string    `json:"resume_token,omitempty"`
	SegmentID        string    `json:"segment_id,omitempty"`
	Revision         uint64    `json:"revision,omitempty"`
	Text             string    `json:"text,omitempty"`
	IsFinal          bool      `json:"is_final,omitempty"`
	StartMS          uint64    `json:"start_ms,omitempty"`
	EndMS            uint64    `json:"end_ms,omitempty"`
	Code             string    `json:"code,omitempty"`
	Message          string    `json:"message,omitempty"`
	Retryable        bool      `json:"retryable,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	ExpiresAt        time.Time `json:"expires_at,omitempty"`
}

func (s *Server) connectStream(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() || !s.reserveConnection() {
		w.Header().Set("Retry-After", "5")
		http.Error(w, "gateway unavailable", http.StatusServiceUnavailable)
		return
	}
	defer s.open.Add(-1)
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  s.config.AllowedOrigins,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(maxWSMessage)
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	authCtx, authCancel := context.WithTimeout(ctx, s.config.AttachTimeout)
	messageType, raw, err := conn.Read(authCtx)
	authCancel()
	if err != nil || messageType != websocket.MessageText {
		closeWS(conn, websocket.StatusPolicyViolation, "hello required")
		return
	}
	var hello helloMessage
	if json.Unmarshal(raw, &hello) != nil || hello.Type != "hello" {
		closeWS(conn, websocket.StatusPolicyViolation, "invalid hello")
		return
	}
	claims, err := s.tokens.Verify(hello.Token)
	if err != nil || claims.StreamID != r.PathValue("stream_id") {
		closeWS(conn, 4401, "invalid stream token")
		return
	}
	if !s.allowRate("connect:"+claims.TenantID, s.config.ConnectRatePerMin) {
		closeWS(conn, 4429, "connection rate exceeded")
		return
	}
	nextJTI, nextHash, err := s.tokens.NewTokenID()
	if err != nil {
		closeWS(conn, websocket.StatusInternalError, "internal error")
		return
	}
	now := time.Now()
	stream, err := s.store.Attach(
		ctx, claims.StreamID, claims.TenantID, claims.Generation,
		auth.HashTokenID(claims.ID), nextHash, now, s.config.DetachWindow,
	)
	if err != nil {
		status := websocket.StatusPolicyViolation
		if errors.Is(err, session.ErrTokenConsumed) {
			status = 4409
		} else if errors.Is(err, session.ErrResumeExpired) {
			status = 4410
		}
		closeWS(conn, status, "stream cannot be attached")
		return
	}
	stream, acquired, _, err := s.store.AcquireOwner(
		ctx, stream.ID, s.ownerID, s.config.PeerAdvertiseAddr,
		time.Now(), s.config.OwnerLease,
	)
	if err != nil {
		closeWS(conn, websocket.StatusInternalError, "owner unavailable")
		return
	}
	resume, tokenExpiry, err := s.tokens.IssueWithID(
		stream.ID, stream.TenantID, auth.TokenResume, nextJTI,
		stream.Generation, stream.Epoch, streamTokenTTL,
	)
	if err != nil {
		closeWS(conn, websocket.StatusInternalError, "internal error")
		return
	}
	if err := writeWS(ctx, conn, wireEvent{
		Type: "ready", StreamID: stream.ID, Epoch: stream.Epoch,
		NextSampleOffset: stream.NextOffset, ResumeToken: resume, ExpiresAt: tokenExpiry,
	}); err != nil {
		return
	}
	if claims.Kind == auth.TokenResume && stream.Epoch > claims.Epoch {
		if err := writeWS(ctx, conn, wireEvent{
			Type: "discontinuity", PreviousEpoch: claims.Epoch,
			Epoch: stream.Epoch, Reason: "owner_changed",
		}); err != nil {
			return
		}
	}
	var bridge relay.Bridge
	if acquired {
		bridge, err = s.manager.Attach(stream, stream.Generation)
	} else {
		bridge, err = s.peers.Attach(ctx, stream.OwnerAddr, stream)
	}
	if err != nil {
		s.logger.Warn("attach relay", "stream_id", stream.ID, "error", err)
		_ = writeWS(ctx, conn, wireEvent{
			Type: "error", Code: "relay_unavailable",
			Message: "relay unavailable; reconnect with the rotated token", Retryable: true,
		})
		return
	}
	defer bridge.Close()
	defer func() {
		_ = s.store.MarkDetached(
			context.Background(), stream.ID, stream.Generation,
			time.Now().Add(s.config.DetachWindow),
		)
	}()
	s.active.Add(1)
	s.metrics.ActiveConnections.Inc()
	defer s.active.Add(-1)
	defer s.metrics.ActiveConnections.Dec()
	s.serveAttached(ctx, conn, bridge)
}

func (s *Server) serveAttached(ctx context.Context, conn *websocket.Conn, bridge relay.Bridge) {
	readDone := make(chan error, 1)
	go func() { readDone <- s.readAudio(ctx, conn, bridge) }()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	var endTimer <-chan time.Time
	for {
		select {
		case event, ok := <-bridge.Events():
			if !ok {
				return
			}
			wire := toWire(event)
			if err := writeWS(ctx, conn, wire); err != nil {
				return
			}
			if event.ReceivedAt.UnixNano() > 0 {
				s.metrics.ResultRelaySeconds.Observe(time.Since(event.ReceivedAt).Seconds())
			}
			if event.Type == relay.EventError || event.Type == relay.EventEnded {
				return
			}
		case err := <-readDone:
			if err != nil {
				code := "protocol_error"
				if errors.Is(err, relay.ErrBackpressure) {
					code = "backpressure"
				} else if errors.Is(err, relay.ErrInvalidOffset) {
					code = "invalid_offset"
				}
				s.metrics.Errors.WithLabelValues(code).Inc()
				_ = writeWS(ctx, conn, wireEvent{Type: "error", Code: code, Message: safeRelayError(err), Retryable: true})
				return
			}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			endTimer = timer.C
			readDone = nil
		case <-endTimer:
			return
		case <-heartbeat.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) readAudio(ctx context.Context, conn *websocket.Conn, bridge relay.Bridge) error {
	for {
		messageType, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		switch messageType {
		case websocket.MessageBinary:
			frame, err := protocol.ParseAudioFrame(data)
			if err != nil {
				return err
			}
			frame.ReceivedAt = time.Now()
			if err := bridge.SendAudio(frame); err != nil {
				return err
			}
			s.metrics.AudioBytes.Add(float64(len(frame.PCM)))
		case websocket.MessageText:
			var control struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(data, &control) != nil || control.Type != "end" {
				return errors.New("unexpected control message")
			}
			if err := bridge.End("client_request"); err != nil {
				return err
			}
			return nil
		default:
			return errors.New("unsupported WebSocket message")
		}
	}
}

func (s *Server) authenticate(r *http.Request) (auth.Identity, error) {
	return s.auth.Verify(r.Context(), auth.Bearer(r.Header.Get("Authorization")))
}

func (s *Server) developmentToken(w http.ResponseWriter, r *http.Request) {
	var request struct {
		TenantID string `json:"tenant_id"`
		Subject  string `json:"subject"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody)).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "invalid request body")
		return
	}
	if request.TenantID == "" {
		request.TenantID = "demo-clinic"
	}
	if request.Subject == "" {
		request.Subject = "demo-doctor"
	}
	token, err := s.auth.SignDevelopment(auth.Identity{TenantID: request.TenantID, Subject: request.Subject}, 15*time.Minute)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not issue token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"access_token": token})
}

func (s *Server) allowRate(key string, limit int) bool {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	now := time.Now()
	if len(s.rateWindows) > 10_000 {
		for key, candidate := range s.rateWindows {
			if now.Sub(candidate.start) >= 2*time.Minute {
				delete(s.rateWindows, key)
			}
		}
	}
	window := s.rateWindows[key]
	if window == nil || now.Sub(window.start) >= time.Minute {
		s.rateWindows[key] = &rateWindow{start: now, count: 1}
		return true
	}
	if window.count >= limit {
		return false
	}
	window.count++
	return true
}

func (s *Server) reserveConnection() bool {
	for {
		current := s.open.Load()
		if current >= s.config.MaxConnections {
			return false
		}
		if s.open.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *Server) webSocketURL(r *http.Request, streamID string) string {
	if s.config.PublicWSBaseURL != "" {
		return strings.TrimRight(s.config.PublicWSBaseURL, "/") + "/v1/streams/" + streamID + "/ws"
	}
	scheme := "ws"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "wss"
	}
	return (&url.URL{Scheme: scheme, Host: r.Host, Path: "/v1/streams/" + streamID + "/ws"}).String()
}

func toWire(event relay.Event) wireEvent {
	return wireEvent{
		Type: string(event.Type), Epoch: event.Epoch, PreviousEpoch: event.PreviousEpoch,
		NextSampleOffset: event.NextSampleOffset, SegmentID: event.SegmentID,
		Revision: event.Revision, Text: event.Text, IsFinal: event.IsFinal,
		StartMS: event.StartMS, EndMS: event.EndMS, Code: event.Code,
		Message: event.Message, Retryable: event.Retryable, Reason: event.Reason,
	}
}

func writeWS(ctx context.Context, conn *websocket.Conn, value wireEvent) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, data)
}

func closeWS(conn *websocket.Conn, status websocket.StatusCode, reason string) {
	_ = conn.Close(status, reason)
}

func safeRelayError(err error) string {
	switch {
	case errors.Is(err, relay.ErrBackpressure):
		return "audio queue is full"
	case errors.Is(err, relay.ErrInvalidOffset):
		return "audio sample offset is not contiguous"
	case errors.Is(err, relay.ErrReplaced):
		return "connection was replaced"
	default:
		return "connection ended"
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func recoverMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				logger.Error("request panic", "error", fmt.Sprint(recovered))
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
