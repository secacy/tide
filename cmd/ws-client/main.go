package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
)

func main() {
	// Ctrl+C 或 SIGTERM 时取消整个 WebSocket session。
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

func run(ctx context.Context) error {
	heartbeat, err := wsheartbeat.FromEnvironment(os.Getenv)
	if err != nil {
		return err
	}
	client, err := wsclient.New(
		wsclient.Config{
			Heartbeat:  heartbeat,
			URL:        "ws://localhost:8080/v1/asr",
			ChunkBytes: 3200, // 默认模拟 100ms 左右的音频块
			Realtime:   true,
		},
	)
	if err != nil {
		return err
	}
	f, err := os.Open("testdata/pcm/test6s.pcm")
	if err != nil {
		return err
	}
	defer f.Close()

	return client.Run(ctx, f)
}
