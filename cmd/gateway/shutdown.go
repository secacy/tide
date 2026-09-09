package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// shutdownConfig 配置自然排空与强制停止后的清理等待时间。
// 两个值都必须大于零。
type shutdownConfig struct {
	// DrainTimeout 是 HTTP 关闭与会话自然排空共享的时间预算。
	DrainTimeout time.Duration

	// CleanupTimeout 是进入强制停止阶段后的清理等待预算。
	CleanupTimeout time.Duration
}

// validate 在服务启动或直接执行关闭编排之前检查时间预算。
func (cfg shutdownConfig) validate() error {
	if cfg.DrainTimeout <= 0 {
		return fmt.Errorf("drain timeout must be positive")
	}
	if cfg.CleanupTimeout <= 0 {
		return fmt.Errorf("cleanup timeout must be positive")
	}
	return nil
}

// gatewayLifecycle 是关闭编排对会话服务的最小依赖。
type gatewayLifecycle interface {
	// StopAccepting 停止接纳新会话，保留已有会话。
	StopAccepting()

	// Abort 请求强制停止所有剩余会话。
	Abort()

	// Wait 等待会话完成清理和注销。
	Wait(context.Context) error
}

// shutdownServer 停止接入，并执行自然排空和必要的强制清理。
//
// server、sessions、logger 必须非 nil，cfg 中的时间必须大于零。
// 自然排空超时后，如果强制清理成功，则返回 nil。
// 实际服务关闭错误或最终清理失败时返回错误。
// 此函数不负责关闭共享 gRPC ClientConn。
// Context 限制等待时间，不能抢占 Abort 和 Close 内部的同步调用。
func shutdownServer(server *http.Server, sessions gatewayLifecycle, logger *slog.Logger, cfg shutdownConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	// 先停止接纳新的业务会话
	sessions.StopAccepting()

	// 自然排空阶段
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	shutdownErr := server.Shutdown(drainCtx)
	waitErr := sessions.Wait(drainCtx)
	// 在主动 cancel 前记录实际超时，避免吞掉并非本阶段期限引起的错误。
	drainExpired := errors.Is(drainCtx.Err(), context.DeadlineExceeded)
	cancelDrain()

	if shutdownErr == nil && waitErr == nil {
		logger.Info("shutdown cleanup completed", "mode", "graceful")
		return nil
	}

	logger.Warn("graceful shutdown incomplete; starting forced cleanup", "http_error", shutdownErr, "session_error", waitErr)

	// 自然排空期限耗尽只触发升级；其他 HTTP 或会话错误必须保留。
	var errs []error
	if shutdownErr != nil && !(drainExpired && errors.Is(shutdownErr, context.DeadlineExceeded)) {
		errs = append(errs, fmt.Errorf("graceful HTTP shutdown: %w", shutdownErr))
	}
	if waitErr != nil && !(drainExpired && errors.Is(waitErr, context.DeadlineExceeded)) {
		errs = append(errs, fmt.Errorf("wait for session drain: %w", waitErr))
	}

	// 强制清理阶段
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
	defer cancelCleanup()
	sessions.Abort()
	if err := server.Close(); err != nil {
		errs = append(errs, fmt.Errorf("force close HTTP server: %w", err))
	}
	if err := sessions.Wait(cleanupCtx); err != nil {
		errs = append(errs, fmt.Errorf("wait for session cleanup: %w", err))
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	logger.Info("shutdown cleanup completed", "mode", "forced")
	return nil
}
