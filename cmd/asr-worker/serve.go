package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// workerShutdownTimeout 是收到停止通知后的共享收尾窗口。
const workerShutdownTimeout = 5 * time.Second

// serveWorker 协调 gRPC、可选 HTTP 和清理任务。
// gRPC 参数必须有效；两个 debug 参数同时为 nil，或同时有效。
// 服务为尚未运行的新实例，监听器已成功创建。
func serveWorker(
	ctx context.Context,
	grpcServer *grpc.Server,
	grpcListener net.Listener,
	debugServer *http.Server,
	debugListener net.Listener,
) error {
	if (debugServer == nil) != (debugListener == nil) {
		return errors.New("debug server and listener must both be nil or both be non-nil")
	}

	group, groupCtx := errgroup.WithContext(ctx)

	// cleanupErr 只由清理 goroutine 写入。
	// group.Wait 返回后所有 goroutine 已结束，再读取不会产生数据竞争。
	var cleanupErr error

	group.Go(func() error {
		err := grpcServer.Serve(grpcListener)

		// 已经进入整体停止流程时，正常的 Server 停止结果不算错误。
		if groupCtx.Err() != nil {
			if err == nil || errors.Is(err, grpc.ErrServerStopped) {
				return nil
			}

			// 即使正在停止，也不能吞掉真正的 Serve 错误。
			return fmt.Errorf("serve gRPC server: %w", err)
		}

		// 没有停止通知，服务却自己结束，属于异常退出。
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return errors.New("gRPC server stopped unexpectedly")
		}

		return fmt.Errorf("serve gRPC server: %w", err)
	})

	if debugServer != nil {
		group.Go(func() error {
			err := debugServer.Serve(debugListener)

			if groupCtx.Err() != nil {
				if err == nil || errors.Is(err, http.ErrServerClosed) {
					return nil
				}

				return fmt.Errorf("serve debug HTTP server: %w", err)
			}

			if err == nil || errors.Is(err, http.ErrServerClosed) {
				return errors.New("debug HTTP server stopped unexpectedly")
			}

			return fmt.Errorf("serve debug HTTP server: %w", err)
		})
	}

	// context 只是停止通知，本身不会关闭 gRPC 或 HTTP Server。
	// 无论是父 context 取消，还是某个 Serve 返回错误，都在这里统一清理。
	group.Go(func() error {
		<-groupCtx.Done()

		cleanupErr = shutdownWorkerServers(
			grpcServer,
			debugServer,
			workerShutdownTimeout,
		)

		// 清理错误由外层在 group.Wait 后合并。
		// 这里返回 nil，避免它覆盖真正触发停止的 Serve 错误。
		return nil
	})

	serveErr := group.Wait()
	return errors.Join(serveErr, cleanupErr)
}

// shutdownWorkerServers 并行收尾两个服务，共享 timeout 窗口。
// grpcServer 非 nil；debugServer 为 nil 表示关闭 HTTP。
// timeout 必须大于零；返回前等待已启动的关闭任务结束。
func shutdownWorkerServers(
	grpcServer *grpc.Server,
	debugServer *http.Server,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		return errors.New("shutdown timeout must be greater than zero")
	}

	// 不能从已经取消的 group context 派生，否则收尾窗口会立即失效。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// GracefulStop 会一直阻塞到已有 RPC 完成，因此放到独立 goroutine。
	grpcDone := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcDone)
	}()

	// HTTP 与 gRPC 同时开始 graceful shutdown，共享同一个 deadline。
	var httpDone <-chan error
	if debugServer != nil {
		done := make(chan error, 1)
		httpDone = done

		go func() {
			err := debugServer.Shutdown(shutdownCtx)
			if err == nil {
				done <- nil
				return
			}

			shutdownErr := fmt.Errorf("shutdown debug HTTP server: %w", err)

			// Shutdown 失败后强制关闭剩余连接。
			if closeErr := debugServer.Close(); closeErr != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("close debug HTTP server: %w", closeErr))
			}

			done <- shutdownErr
		}()
	}

	var grpcErr error

	select {
	case <-grpcDone:
		// 自然收尾完成。

	case <-shutdownCtx.Done():
		// deadline 与 grpcDone 可能几乎同时就绪。
		// 再检查一次，已经完成就不要误判为超时。
		select {
		case <-grpcDone:
			// 已经正常完成。

		default:
			grpcErr = fmt.Errorf("graceful stop gRPC server: %w", shutdownCtx.Err())

			// 超时后强制取消现有 RPC 和连接。
			grpcServer.Stop()

			// 不提前返回，确保 GracefulStop goroutine 真正结束。
			<-grpcDone
		}
	}

	var httpErr error
	if httpDone != nil {
		httpErr = <-httpDone
	}

	return errors.Join(grpcErr, httpErr)
}
