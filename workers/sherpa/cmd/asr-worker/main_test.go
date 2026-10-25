package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"github.com/secacy/tide-artisan/workers/sherpa/internal/native"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestParseConfig(t *testing.T) {
	var output bytes.Buffer
	if _, err := parseConfig([]string{"-h"}, &output); !errors.Is(err, flag.ErrHelp) || !strings.Contains(output.String(), "model-dir") {
		t.Fatalf("help=%v output=%s", err, output.String())
	}
	for _, args := range [][]string{nil, {"-model-dir=x", "-threads=0"}, {"-model-dir=x", "-threads=17"}, {"-model-dir=x", "-listen="}, {"-model-dir=x", "extra"}, {"-unknown"}} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("accepted invalid args %v", args)
		}
	}
	cfg, err := parseConfig([]string{"-model-dir=relative/model"}, io.Discard)
	if err != nil || cfg.listen != "127.0.0.1:50051" || cfg.model.Threads != 2 || cfg.model.ModelDir != "relative/model" {
		t.Fatalf("parsed=%+v err=%v", cfg, err)
	}
}

func TestRunChecksAssetsBeforeListen(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg := config{listen: "invalid address", model: native.Config{ModelDir: t.TempDir(), Threads: 2}}
	if err := run(context.Background(), cfg, logger); err == nil || !strings.Contains(err.Error(), "configure native factory") {
		t.Fatalf("did not validate assets before listening: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, config{}, logger); err != nil {
		t.Fatalf("pre-start shutdown=%v", err)
	}
}

// shutdownTestService 模拟无法立即返回的同步推理；释放通道代表安全调用边界。
type shutdownTestService struct {
	asrv1.UnimplementedASRServiceServer
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
}

func (s *shutdownTestService) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error {
	close(s.started)
	<-stream.Context().Done()
	close(s.canceled)
	<-s.release
	return stream.Context().Err()
}

func TestServeForcedStopWaitsForHandler(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s := &shutdownTestService{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(s.release) })
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	asrv1.RegisterASRServiceServer(server, s)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, server, l, 20*time.Millisecond) }()
	conn, err := grpc.NewClient("passthrough:///"+l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := asrv1.NewASRServiceClient(conn).StreamingRecognize(rpcCtx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&asrv1.StreamingRecognizeRequest{Data: []byte{0, 0}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.started:
	case <-rpcCtx.Done():
		t.Fatal("handler did not start")
	}
	stop()
	select {
	case <-s.canceled:
	case <-rpcCtx.Done():
		t.Fatal("forced stop did not cancel RPC")
	}
	select {
	case err := <-done:
		t.Fatalf("server returned before handler cleanup: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(s.release) })
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "graceful shutdown expired") {
			t.Fatalf("forced stop lost failure: %v", err)
		}
	case <-rpcCtx.Done():
		t.Fatal("serve did not return after handler cleanup")
	}
}

func TestServeIdleShutdownAndUnexpectedListenerClose(t *testing.T) {
	for _, unexpected := range []bool{false, true} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := grpc.NewServer(grpc.WaitForHandlers(true))
		ctx, cancel := context.WithCancel(context.Background())
		if unexpected {
			_ = l.Close()
		} else {
			cancel()
		}
		err = serve(ctx, server, l, time.Second)
		cancel()
		_ = l.Close()
		if unexpected != (err != nil) {
			t.Fatalf("unexpected=%v, err=%v", unexpected, err)
		}
	}
}
