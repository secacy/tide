package main

import (
	"reflect"
	"testing"

	"github.com/secacy/tide-artisan/internal/gateway"
)

// TestWorkerAddressesValues 在辅助函数和完整命令行入口验证相同的地址语义。
// 保留顺序与原始表示，不做网络解析；显式检查启动配置确实使用了校验结果。
func TestWorkerAddressesValues(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      []string
	}{
		{"single", "localhost:50051", []string{"localhost:50051"}},
		{"ordered", "localhost:50052,localhost:50051", []string{"localhost:50052", "localhost:50051"}},
		{"trimmed", " \tlocalhost:50051 , 127.0.0.1:50052\n", []string{"localhost:50051", "127.0.0.1:50052"}},
		{"ipv6", "[::1]:50051,[2001:db8::1]:50052", []string{"[::1]:50051", "[2001:db8::1]:50052"}},
		{"port_bounds", "worker-a:1,worker-b:65535", []string{"worker-a:1", "worker-b:65535"}},
		{"no_alias_normalization", "localhost:050051,127.0.0.1:50051", []string{"localhost:050051", "127.0.0.1:50051"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("helper", func(t *testing.T) {
				got, err := parseWorkerAddresses(tc.raw)
				if err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("addresses=%q err=%v, want %q", got, err, tc.want)
				}
			})
			t.Run("cli", func(t *testing.T) {
				cfg, err := parseGatewayConfig([]string{"-workers", tc.raw, "-max-pending-audio-bytes=32000"})
				if err != nil {
					t.Fatal(err)
				}
				want := gatewayConfig{WorkerAddrs: tc.want, WorkerStrategy: workerStrategyRoundRobin, Gateway: gateway.Config{MaxMessageBytes: 1024 * 1024, MaxPendingAudioBytes: 32000}}
				if !reflect.DeepEqual(cfg, want) {
					t.Fatalf("config=%+v, want %+v", cfg, want)
				}
			})
		})
	}
}

// TestWorkerAddressesRejected 防止部分列表被静默采用，也防止 CLI 绕过辅助函数校验。
func TestWorkerAddressesRejected(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"empty", ""},
		{"blank", " \t\n"},
		{"leading_empty", ",localhost:50051"},
		{"middle_empty", "localhost:50051,,localhost:50052"},
		{"trailing_empty", "localhost:50051,"},
		{"duplicate", "localhost:50051,localhost:50051"},
		{"duplicate_after_trim", "localhost:50051, localhost:50051 "},
		{"missing_port", "localhost"},
		{"empty_host", ":50051"},
		{"empty_port", "localhost:"},
		{"named_port", "localhost:http"},
		{"zero_port", "localhost:0"},
		{"port_too_large", "localhost:65536"},
		{"negative_port", "localhost:-1"},
		{"signed_port", "localhost:+50051"},
		{"port_overflow", "localhost:999999999999999999999"},
		{"host_whitespace", "local host:50051"},
		{"host_slash", "host/name:50051"},
		{"host_backslash", `host\name:50051`},
		{"resolver_uri", "dns:///localhost:50051"},
		{"unbracketed_ipv6", "::1:50051"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("helper", func(t *testing.T) {
				got, err := parseWorkerAddresses(tc.raw)
				if err == nil || got != nil {
					t.Fatalf("invalid address list accepted: addresses=%q err=%v", got, err)
				}
			})
			t.Run("cli", func(t *testing.T) {
				got, err := parseGatewayConfig([]string{"-workers=" + tc.raw})
				if err == nil || !reflect.DeepEqual(got, gatewayConfig{}) {
					t.Fatalf("invalid CLI returned config=%+v err=%v", got, err)
				}
			})
		})
	}
}

// TestGatewayWorkerFlags 检查 flag 的覆盖语义、缺失参数和重复解析隔离。
func TestGatewayWorkerFlags(t *testing.T) {
	t.Run("last_value_wins", func(t *testing.T) {
		cfg, err := parseGatewayConfig([]string{"-workers=localhost:50051", "-workers=localhost:50052,localhost:50053"})
		if err != nil || !reflect.DeepEqual(cfg.WorkerAddrs, []string{"localhost:50052", "localhost:50053"}) {
			t.Fatalf("config=%+v err=%v", cfg, err)
		}
	})
	t.Run("missing_value", func(t *testing.T) {
		cfg, err := parseGatewayConfig([]string{"-workers"})
		if err == nil || !reflect.DeepEqual(cfg, gatewayConfig{}) {
			t.Fatalf("config=%+v err=%v", cfg, err)
		}
	})
	t.Run("independent_calls", func(t *testing.T) {
		first, err := parseGatewayConfig([]string{"-workers=localhost:50052"})
		if err != nil {
			t.Fatal(err)
		}
		first.WorkerAddrs[0] = "mutated"
		second, err := parseGatewayConfig(nil)
		if err != nil || !reflect.DeepEqual(second.WorkerAddrs, []string{"localhost:50051"}) {
			t.Fatalf("default changed: config=%+v err=%v", second, err)
		}
	})
}
