package main

import (
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		slog.Error("mock ASR worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	// 允许本地启动多个独立进程；这不改变 Mock 的处理能力模型。
	address := os.Getenv("TIDE_MOCK_ASR_ADDR")
	if address == "" {
		address = ":50051"
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	worker := mockasr.New(
		mockasr.Config{
			PartialEvery:  500 * time.Millisecond, // 每收到约 500ms 音频返回一次 partial result
			ResponseDelay: 50 * time.Millisecond,
			PartialTexts: []string{
				"今",
				"今天",
				"今天天气",
				"今天天气不错",
			},
			FinalText: "今天天气不错",
		},
	)

	// 把 worker 注册成为 ASRService 的服务实现
	asrv1.RegisterASRServiceServer(grpcServer, worker)

	slog.Info("mock ASR worker started", "address", address)
	return grpcServer.Serve(listener)
}
