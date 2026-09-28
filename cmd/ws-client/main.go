package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
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
	client, err := wsclient.New(
		wsclient.Config{
			URL:        "ws://localhost:8080/v1/asr",
			ChunkBytes: 3200, // 默认模拟 100ms 左右的音频块
			Realtime:   true,
			OnResult: func(result wsprotocol.ResultMessage, _ time.Time) {
				fmt.Printf("result text=%q final=%v\n", result.Text, result.IsFinal)
			},
		},
	)
	if err != nil {
		return err
	}
	f, err := os.Open("data/pcm/test6s.pcm")
	if err != nil {
		return err
	}
	defer f.Close()

	return client.Run(ctx, f)
}
