package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	asrv1 "github.com/secacy/tide/gen/tide/asr/v1"
	peerv1 "github.com/secacy/tide/gen/tide/peer/v1"
	"github.com/secacy/tide/internal/auth"
	"github.com/secacy/tide/internal/config"
	"github.com/secacy/tide/internal/gateway"
	"github.com/secacy/tide/internal/metrics"
	"github.com/secacy/tide/internal/relay"
	"github.com/secacy/tide/internal/session"
	"github.com/secacy/tide/internal/telemetry"
	"github.com/secacy/tide/internal/tlsutil"
	"github.com/secacy/tide/internal/webui"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := telemetry.Setup(ctx, "tide-gateway", cfg.OTLPEndpoint)
	if err != nil {
		logger.Error("initialize telemetry", "error", err)
		os.Exit(2)
	}
	defer shutdownTelemetry(context.Background())

	var store session.Store
	if cfg.RedisAddr == "" {
		logger.Warn("using in-memory session store; cross-node resume is disabled")
		store = session.NewMemoryStore()
	} else {
		redisStore := session.NewRedisStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = redisStore.Ping(pingCtx)
		cancel()
		if err != nil {
			logger.Error("connect Redis", "error", err)
			os.Exit(2)
		}
		store = redisStore
	}
	defer store.Close()

	asrTransport, err := tlsutil.ClientOption(cfg.ASRTLSCA, cfg.ASRTLSCert, cfg.ASRTLSKey)
	if err != nil {
		logger.Error("configure ASR TLS", "error", err)
		os.Exit(2)
	}
	asrConn, err := grpc.NewClient(cfg.ASRAddr, asrTransport, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		logger.Error("create ASR client", "error", err)
		os.Exit(2)
	}
	defer asrConn.Close()

	registry := prometheus.NewRegistry()
	appMetrics := metrics.New(registry)
	manager := relay.NewManager(
		asrv1.NewASRClient(asrConn), store, cfg.NodeID,
		cfg.OwnerLease, cfg.OwnerRenew, cfg.DetachWindow, cfg.EndedRetention,
		appMetrics, logger,
	)
	defer manager.Stop()

	peerTransport, err := tlsutil.ClientOption(cfg.PeerTLSCA, cfg.PeerTLSCert, cfg.PeerTLSKey)
	if err != nil {
		logger.Error("configure peer client TLS", "error", err)
		os.Exit(2)
	}
	peerDialer := relay.NewPeerDialer(peerTransport, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	defer peerDialer.Close()
	peerServerOption, err := tlsutil.ServerOption(cfg.PeerTLSCA, cfg.PeerTLSCert, cfg.PeerTLSKey)
	if err != nil {
		logger.Error("configure peer server TLS", "error", err)
		os.Exit(2)
	}
	grpcServer := grpc.NewServer(peerServerOption, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	peerv1.RegisterGatewayPeerServer(grpcServer, &relay.PeerServer{
		NodeID: cfg.NodeID, Store: store, Manager: manager,
	})
	peerListener, err := net.Listen("tcp", cfg.PeerAddr)
	if err != nil {
		logger.Error("listen peer gRPC", "error", err)
		os.Exit(2)
	}
	go func() {
		logger.Info("peer gRPC listening", "addr", cfg.PeerAddr, "advertise_addr", cfg.PeerAdvertiseAddr)
		if err := grpcServer.Serve(peerListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("serve peer gRPC", "error", err)
			stop()
		}
	}()

	api := gateway.NewServer(
		cfg, store,
		auth.NewVerifier(cfg.JWTSecret, cfg.JWKSURL, cfg.JWTIssuer, cfg.JWTAudience),
		auth.NewStreamTokenService(cfg.TokenSecret),
		manager, peerDialer, appMetrics, logger, webui.Handler(),
	)
	httpServer := &http.Server{
		Addr: cfg.HTTPAddr, Handler: api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       75 * time.Second,
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	metricsServer := &http.Server{
		Addr: cfg.MetricsAddr, Handler: metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("HTTP gateway listening", "addr", cfg.HTTPAddr, "node_id", cfg.NodeID)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve HTTP", "error", err)
			stop()
		}
	}()
	go func() {
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve metrics", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("gateway draining")
	api.SetDraining()
	manager.Drain()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if !api.WaitForNoConnections(drainCtx) {
		logger.Warn("connection drain deadline reached")
	}
	drainCancel()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
	manager.Stop()
	grpcServer.GracefulStop()
}
