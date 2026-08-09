package gateway_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	asrv1 "github.com/secacy/tide/gen/tide/asr/v1"
	"github.com/secacy/tide/internal/auth"
	"github.com/secacy/tide/internal/config"
	"github.com/secacy/tide/internal/gateway"
	"github.com/secacy/tide/internal/metrics"
	"github.com/secacy/tide/internal/relay"
	"github.com/secacy/tide/internal/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testASR struct {
	asrv1.UnimplementedASRServer
	readyGate <-chan struct{}
}

func (s testASR) Transcribe(stream grpc.BidiStreamingServer[asrv1.GatewayToASR, asrv1.ASRToGateway]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return io.ErrUnexpectedEOF
	}
	if s.readyGate != nil {
		select {
		case <-s.readyGate:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Ready{
		Ready: &asrv1.Ready{Epoch: start.Epoch},
	}}); err != nil {
		return err
	}
	for {
		message, err := stream.Recv()
		if err != nil {
			return err
		}
		if message.GetEnd() != nil {
			return nil
		}
		audio := message.GetAudio()
		next := audio.SampleOffset + uint64(len(audio.Pcm)/2)
		if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Ack{
			Ack: &asrv1.Ack{NextSampleOffset: next},
		}}); err != nil {
			return err
		}
		if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Transcript{
			Transcript: &asrv1.Transcript{
				Epoch: start.Epoch, SegmentId: "test-segment", Revision: 1,
				Text: "mock transcript", IsFinal: true, EndMs: next * 1000 / 16000,
			},
		}}); err != nil {
			return err
		}
	}
}

func TestWebSocketToASRFlow(t *testing.T) {
	asrListener := bufconn.Listen(1024 * 1024)
	asrServer := grpc.NewServer()
	readyGate := make(chan struct{})
	asrv1.RegisterASRServer(asrServer, testASR{readyGate: readyGate})
	go func() { _ = asrServer.Serve(asrListener) }()
	t.Cleanup(asrServer.Stop)
	asrConn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return asrListener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asrConn.Close() })

	cfg := config.Config{
		Environment: "test", NodeID: "node-a", PeerAdvertiseAddr: "node-a:9090",
		TenantMaxStreams: 10, MaxConnections: 100, CreateRatePerMin: 100, ConnectRatePerMin: 100,
		AttachTimeout: time.Second, DetachWindow: time.Second,
		MaxSessionAge: time.Hour, EndedRetention: time.Minute,
		OwnerLease: 10 * time.Second, OwnerRenew: time.Second,
	}
	store := session.NewMemoryStore()
	appMetrics := metrics.New(prometheus.NewRegistry())
	ownerID := cfg.NodeID + "/test-incarnation"
	manager := relay.NewManager(
		asrv1.NewASRClient(asrConn), store, ownerID,
		cfg.AttachTimeout,
		cfg.OwnerLease, cfg.OwnerRenew, cfg.DetachWindow, cfg.EndedRetention,
		appMetrics, testLogger(),
	)
	t.Cleanup(manager.Stop)
	peerDialer := relay.NewPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(peerDialer.Close)
	accessVerifier := auth.NewVerifier(
		"01234567890123456789012345678901", "", "test-issuer", "test-audience",
	)
	accessToken, err := accessVerifier.SignDevelopment(
		auth.Identity{TenantID: "clinic", Subject: "doctor"}, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	api := gateway.NewServer(
		cfg, ownerID, store, accessVerifier,
		auth.NewStreamTokenService("abcdefghijklmnopqrstuvwxyz123456"),
		manager, peerDialer, appMetrics, testLogger(), nil,
	)
	httpServer := httptest.NewServer(api.Handler())
	t.Cleanup(httpServer.Close)

	request, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/v1/streams", bytes.NewBufferString(`{"language_code":"zh-CN"}`))
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create status %d: %s", response.StatusCode, body)
	}
	var created struct {
		StreamID    string `json:"stream_id"`
		WebSocket   string `json:"websocket_url"`
		AttachToken string `json:"attach_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.WebSocket, "ws://") {
		t.Fatalf("unexpected WebSocket URL %q", created.WebSocket)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, created.WebSocket, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if err := writeJSONWS(ctx, conn, map[string]string{"type": "hello", "token": created.AttachToken}); err != nil {
		t.Fatal(err)
	}
	attached := readWire(t, ctx, conn)
	if attached.Type != "attached" || attached.ResumeToken == "" || attached.Epoch != 1 {
		t.Fatalf("unexpected attached: %+v", attached)
	}
	close(readyGate)
	ready := readWire(t, ctx, conn)
	if ready.Type != "ready" || ready.Epoch != 1 || ready.CommittedOffset != 0 {
		t.Fatalf("unexpected ready: %+v", ready)
	}
	if err := writeAudioWS(ctx, conn, ready.NextSampleOffset); err != nil {
		t.Fatal(err)
	}
	gotAck, gotTranscript := false, false
	for !gotAck || !gotTranscript {
		event := readWire(t, ctx, conn)
		switch event.Type {
		case "ack":
			gotAck = event.NextSampleOffset == 640
		case "transcript":
			gotTranscript = event.Text == "mock transcript" && event.IsFinal
		case "error":
			t.Fatalf("gateway error: %+v", event)
		}
	}
	stored, err := store.Get(t.Context(), created.StreamID)
	if err != nil || stored.AcceptedOffset != 640 || stored.CommittedOffset != 640 {
		t.Fatalf("committed store progress: stream=%+v err=%v", stored, err)
	}
	rotateRequest, _ := http.NewRequest(
		http.MethodPost,
		httpServer.URL+"/v1/streams/"+created.StreamID+"/resume-token",
		nil,
	)
	rotateRequest.Header.Set("Authorization", "Bearer "+accessToken)
	rotateResponse, err := http.DefaultClient.Do(rotateRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer rotateResponse.Body.Close()
	if rotateResponse.StatusCode != http.StatusOK || rotateResponse.Header.Get("Cache-Control") != "no-store" {
		body, _ := io.ReadAll(rotateResponse.Body)
		t.Fatalf("rotate token status=%d cache=%q body=%s", rotateResponse.StatusCode, rotateResponse.Header.Get("Cache-Control"), body)
	}
	var rotated struct {
		ResumeToken string `json:"resume_token"`
	}
	if err := json.NewDecoder(rotateResponse.Body).Decode(&rotated); err != nil || rotated.ResumeToken == "" {
		t.Fatalf("decode rotated token: token=%q err=%v", rotated.ResumeToken, err)
	}
	if err := conn.Close(websocket.StatusNormalClosure, "test reconnect"); err != nil {
		t.Fatal(err)
	}

	reconnected, _, err := websocket.Dial(ctx, created.WebSocket, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reconnected.CloseNow()
	if err := writeJSONWS(ctx, reconnected, map[string]string{"type": "hello", "token": rotated.ResumeToken}); err != nil {
		t.Fatal(err)
	}
	reattached := readWire(t, ctx, reconnected)
	if reattached.Type != "attached" || reattached.ResumeToken == "" || reattached.Epoch != ready.Epoch {
		t.Fatalf("unexpected reattached: %+v", reattached)
	}
	resumed := readWire(t, ctx, reconnected)
	if resumed.Type != "ready" || resumed.Epoch != ready.Epoch || resumed.CommittedOffset != 640 {
		t.Fatalf("unexpected resumed ready: %+v", resumed)
	}

	replay, _, err := websocket.Dial(ctx, created.WebSocket, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSONWS(ctx, replay, map[string]string{"type": "hello", "token": created.AttachToken}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replay.Read(ctx); websocket.CloseStatus(err) != 4409 {
		t.Fatalf("replayed attach token close status=%d err=%v", websocket.CloseStatus(err), err)
	}
	_ = replay.CloseNow()

	if err := writeAudioWS(ctx, reconnected, resumed.NextSampleOffset); err != nil {
		t.Fatal(err)
	}
	for {
		event := readWire(t, ctx, reconnected)
		if event.Type == "ack" {
			break
		}
	}
	if err := writeJSONWS(ctx, reconnected, map[string]string{"type": "end"}); err != nil {
		t.Fatal(err)
	}
	for {
		event := readWire(t, ctx, reconnected)
		if event.Type == "ended" {
			break
		}
	}
}

type testWireEvent struct {
	Type             string `json:"type"`
	Epoch            uint64 `json:"epoch"`
	NextSampleOffset uint64 `json:"next_sample_offset"`
	CommittedOffset  uint64 `json:"committed_sample_offset"`
	ResumeToken      string `json:"resume_token"`
	Text             string `json:"text"`
	IsFinal          bool   `json:"is_final"`
	Code             string `json:"code"`
}

func readWire(t *testing.T, ctx context.Context, conn *websocket.Conn) testWireEvent {
	t.Helper()
	kind, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.MessageText {
		t.Fatalf("unexpected message type %v", kind)
	}
	var event testWireEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	return event
}

func writeJSONWS(ctx context.Context, conn *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func writeAudioWS(ctx context.Context, conn *websocket.Conn, offset uint64) error {
	frame := make([]byte, 8+1280)
	binary.BigEndian.PutUint64(frame, offset)
	return conn.Write(ctx, websocket.MessageBinary, frame)
}
