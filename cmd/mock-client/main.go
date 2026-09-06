package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/secacy/tide-artisan/internal/streamer"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// SIGINT/SIGTERM 到来时取消整个 gRPC stream
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

// run 负责应用级资源组装。
func run(ctx context.Context) error {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := asrv1.NewASRServiceClient(conn)

	audioStreamer, err := streamer.New(
		client,
		streamer.Config{
			ChunkBytes: 3200,
		},
	)
	if err != nil {
		return err
	}

	audioFile, err := os.Open("data/pcm/test6s.pcm")
	if err != nil {
		return err
	}
	defer audioFile.Close()

	return audioStreamer.Stream(ctx, audioFile)
}
