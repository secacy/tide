package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/secacy/tide-artisan/internal/mockasr"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := parseWorkerConfig(os.Args[1:])
	if err != nil {
		// 用户主动查看帮助，属于正常退出。
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		slog.Error("failed to parse worker config", "error", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		slog.Error("mock ASR worker exited", "error", err)
		os.Exit(1)
	}
}

// run 使用已解析的启动配置创建并运行 Mock Worker。
// 本函数不读取命令行、不退出进程；Worker 配置校验在监听端口之前完成。
func run(cfg workerConfig) error {
	worker, err := mockasr.New(cfg.Mock)
	if err != nil {
		return fmt.Errorf("create mock ASR worker: %w", err)
	}

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}

	grpcServer := grpc.NewServer()

	// 把 worker 注册成为 ASRService 的服务实现
	asrv1.RegisterASRServiceServer(grpcServer, worker)

	slog.Info("mock ASR worker started",
		"address", listener.Addr().String(),
		"processing_concurrency", cfg.Mock.ProcessingConcurrency,
		"processing_delay", cfg.Mock.ProcessingDelay,
		"response_delay", cfg.Mock.ResponseDelay,
	)
	return grpcServer.Serve(listener)
}
