package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/secacy/tide-artisan/internal/wsclient"
	"github.com/secacy/tide-artisan/internal/wsheartbeat"
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
	recoverable := false
	if value := os.Getenv("TIDE_RECOVERY"); value != "" {
		var err error
		recoverable, err = strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid TIDE_RECOVERY: %w", err)
		}
	}
	heartbeat, err := wsheartbeat.FromEnvironment(os.Getenv)
	if err != nil {
		return err
	}
	client, err := wsclient.New(
		wsclient.Config{
			Recovery: wsclient.RecoveryConfig{Observe: func(msg wsprotocol.RecoveryMessage) {
				fmt.Printf("[%d,%d) %s\n", msg.FromSample, msg.ThroughSample, msg.Text)
			}},
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

	if recoverable {
		report, runErr := client.RunRecoverable(ctx, wsclient.ReaderSource{Reader: f})
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			return fmt.Errorf("write recovery report: %w (session: %v)", err, runErr)
		}
		return runErr
	}
	return client.Run(ctx, f)
}
