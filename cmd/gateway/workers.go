package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/secacy/tide-artisan/internal/gateway"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// workerEndpoint 是启动时固定的后端地址及本 Gateway 对它的会话配额。
type workerEndpoint struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Capacity int    `json:"capacity"`
}

// workerSettings 不热更新；多 Worker 必须显式选择候选策略。
type workerSettings struct {
	MaxSessions int                           `json:"max_sessions"`
	Policy      gateway.WorkerSelectionPolicy `json:"worker_policy"`
	Workers     []workerEndpoint              `json:"workers"`
}

// readWorkerSettings 未提供文件时保留原单 Worker 启动方式；文件配置必须完整且合法。
func readWorkerSettings(path string) (workerSettings, error) {
	if path == "" {
		return workerSettings{MaxSessions: 100, Policy: gateway.RoundRobin, Workers: []workerEndpoint{{ID: "default", Address: workerAddr, Capacity: 100}}}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return workerSettings{}, err
	}
	defer f.Close()
	var cfg workerSettings
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("decode gateway config: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("gateway config must contain exactly one JSON object")
	}
	if cfg.MaxSessions <= 0 || len(cfg.Workers) == 0 {
		return cfg, fmt.Errorf("positive max_sessions and nonempty workers required")
	}
	if cfg.Policy == "" && len(cfg.Workers) == 1 {
		cfg.Policy = gateway.RoundRobin
	}
	if cfg.Policy != gateway.RoundRobin && cfg.Policy != gateway.LeastReservedRatio {
		return cfg, fmt.Errorf("explicit worker_policy must be round_robin or least_reserved_ratio")
	}
	ids, addresses := map[string]bool{}, map[string]bool{}
	for _, w := range cfg.Workers {
		if strings.TrimSpace(w.ID) == "" || strings.TrimSpace(w.Address) == "" || w.Capacity <= 0 {
			return cfg, fmt.Errorf("invalid worker endpoint: ID and address must be nonempty, capacity positive")
		}
		if ids[w.ID] || addresses[w.Address] {
			return cfg, fmt.Errorf("duplicate worker ID or address for %q", w.ID)
		}
		ids[w.ID], addresses[w.Address] = true, true
	}
	return cfg, nil
}

// openWorkerPool 创建惰性 gRPC 客户端，成功不代表 Worker 可达或健康。
// cleanup 在初始化失败时关闭已创建连接，正常运行时由应用在关闭编排后调用。
func openWorkerPool(cfg workerSettings) (*gateway.WorkerPool, func(), error) {
	var connections []*grpc.ClientConn
	cleanup := func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}
	var workers []gateway.WorkerConfig
	for _, w := range cfg.Workers {
		conn, err := grpc.NewClient(w.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("create Worker %q: %w", w.ID, err)
		}
		connections = append(connections, conn)
		workers = append(workers, gateway.WorkerConfig{ID: w.ID, Client: asrv1.NewASRServiceClient(conn), Capacity: w.Capacity})
	}
	pool, err := gateway.NewWorkerPool(workers, cfg.Policy)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return pool, cleanup, nil
}
