package relay

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	asrv1 "github.com/secacy/tide/gen/tide/asr/v1"
	peerv1 "github.com/secacy/tide/gen/tide/peer/v1"
	appmetrics "github.com/secacy/tide/internal/metrics"
	"github.com/secacy/tide/internal/session"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type peerTestASR struct {
	asrv1.UnimplementedASRServer
}

func (peerTestASR) Transcribe(stream grpc.BidiStreamingServer[asrv1.GatewayToASR, asrv1.ASRToGateway]) error {
	if _, err := stream.Recv(); err != nil {
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
		if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Ack{
			Ack: &asrv1.Ack{NextSampleOffset: audio.SampleOffset + uint64(len(audio.Pcm)/2)},
		}}); err != nil {
			return err
		}
	}
}

func TestPeerRelayForwardsAudioToOwner(t *testing.T) {
	asrListener := bufconn.Listen(1024 * 1024)
	asrServer := grpc.NewServer()
	asrv1.RegisterASRServer(asrServer, peerTestASR{})
	go func() { _ = asrServer.Serve(asrListener) }()
	t.Cleanup(asrServer.Stop)
	asrConn, err := grpc.NewClient(
		"passthrough:///asr",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return asrListener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asrConn.Close() })

	store := session.NewMemoryStore()
	now := time.Now()
	stream := session.Session{
		ID: "stream-one", TenantID: "clinic",
		State: session.StateCreated, TokenHash: "first",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	if err := store.Create(t.Context(), stream, 10); err != nil {
		t.Fatal(err)
	}
	stream, err = store.Attach(t.Context(), stream.ID, stream.TenantID, 0, "first", "second")
	if err != nil {
		t.Fatal(err)
	}
	stream, acquired, _, err := store.AcquireOwner(
		t.Context(), stream.ID, "owner-a", "passthrough:///owner-a",
		time.Now(), 10*time.Second,
	)
	if err != nil || !acquired {
		t.Fatalf("acquire owner: acquired=%v err=%v", acquired, err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := NewManager(
		asrv1.NewASRClient(asrConn), store, "owner-a",
		10*time.Second, time.Second, time.Second, time.Minute,
		appmetrics.New(prometheus.NewRegistry()), logger,
	)
	t.Cleanup(manager.Stop)

	peerListener := bufconn.Listen(1024 * 1024)
	peerServer := grpc.NewServer()
	peerv1.RegisterGatewayPeerServer(peerServer, &PeerServer{
		NodeID: "owner-a", Store: store, Manager: manager,
	})
	go func() { _ = peerServer.Serve(peerListener) }()
	t.Cleanup(peerServer.Stop)
	dialer := NewPeerDialer(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return peerListener.Dial()
		}),
	)
	t.Cleanup(dialer.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	bridge, err := dialer.Attach(ctx, stream.OwnerAddr, stream)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()
	if err := bridge.SendAudio(AudioFrame{
		SampleOffset: 0, PCM: make([]byte, 1280), ReceivedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-bridge.Events():
		if event.Type != EventAck || event.NextSampleOffset != 640 {
			t.Fatalf("unexpected owner event: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := bridge.End("test_complete"); err != nil {
		t.Fatal(err)
	}
}
