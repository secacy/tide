package gateway

import (
	"net"
	"testing"
	"time"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// baselineMS 保留毫秒小数，不提前截断短写入的测量值。
func baselineMS(duration time.Duration) float64 { return float64(duration) / float64(time.Millisecond) }

// newBaselineTCPWorkerClient 使用本机 TCP，避免把 bufconn 的固定缓冲行为当成真实网络现象。
// Client、Gateway、Worker 仍在同一进程，结果只代表本次本机实验环境。
func newBaselineTCPWorkerClient(t *testing.T, worker asrv1.ASRServiceServer) asrv1.ASRServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	asrv1.RegisterASRServiceServer(server, worker)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("gRPC server did not stop")
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return asrv1.NewASRServiceClient(conn)
}
