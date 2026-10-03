package main

import (
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
)

// TestWorkerWeightsValues 验证十进制、逐项空白和总量边界，不依赖选择算法的内部状态。
func TestWorkerWeightsValues(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		want      []int64
	}{
		{"single", "1", []int64{1}},
		{"ordered", "2,1", []int64{2, 1}},
		{"trimmed", " \t2 , 1\n", []int64{2, 1}},
		{"decimal_leading_zero", "002,010", []int64{2, 10}},
		{"positive_sign", "+2,+1", []int64{2, 1}},
		{"single_at_limit", "1000000", []int64{workerpool.MaxTotalWeight}},
		{"sum_at_limit", "999999,1", []int64{999999, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWorkerWeights(tc.raw, len(tc.want))
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("weights=%v err=%v want=%v", got, err, tc.want)
			}
		})
	}
}

// TestWorkerWeightsRejected 任何局部失败都不得泄露可用权重，包括整数溢出和累加超限。
func TestWorkerWeightsRejected(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		count     int
		hint      string
	}{
		{"empty", "", 1, "index 0"},
		{"blank", " \t", 1, "index 0"},
		{"leading_empty", ",1", 2, "index 0"},
		{"middle_empty", "2,,1", 3, "index 1"},
		{"trailing_empty", "2,", 2, "index 1"},
		{"too_few", "2", 2, "does not match"},
		{"too_many", "2,1,1", 2, "does not match"},
		{"zero_workers", "1", 0, "does not match"},
		{"negative_workers", "1", -1, "does not match"},
		{"zero", "2,0", 2, "index 1"},
		{"negative", "2,-1", 2, "index 1"},
		{"word", "2,one", 2, "index 1"},
		{"fraction", "2,1.5", 2, "index 1"},
		{"exponent", "2,1e2", 2, "index 1"},
		{"hexadecimal", "2,0x1", 2, "index 1"},
		{"underscore", "2,1_0", 2, "index 1"},
		{"internal_space", "2,1 0", 2, "index 1"},
		{"positive_overflow", "2,9223372036854775808", 2, "index 1"},
		{"negative_overflow", "2,-9223372036854775809", 2, "index 1"},
		{"max_int_first", "9223372036854775807", 1, "exceeds maximum"},
		{"max_int_later", "1,9223372036854775807", 2, "exceeds maximum"},
		{"min_int", "-9223372036854775808", 1, "must be positive"},
		{"single_over_limit", "1000001", 1, "exceeds maximum"},
		{"sum_over_limit", "500000,500001", 2, "exceeds maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWorkerWeights(tc.raw, tc.count)
			if err == nil || got != nil || !strings.Contains(err.Error(), tc.hint) {
				t.Fatalf("weights=%v err=%v expected nil weights and error containing %q", got, err, tc.hint)
			}
		})
	}
}

// TestGatewayWorkerStrategyValues 验证完整 CLI 配置及地址顺序、权重和业务配置共同保留。
func TestGatewayWorkerStrategyValues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		addrs    []string
		strategy string
		weights  []int64
		budget   int64
	}{
		{"default", nil, []string{"localhost:50051"}, workerStrategyRoundRobin, nil, 0},
		{"explicit_round_robin", []string{"-worker-strategy=round_robin"}, []string{"localhost:50051"}, workerStrategyRoundRobin, nil, 0},
		{"weighted_default_address", []string{"-worker-strategy=weighted_round_robin", "-worker-weights=2"}, []string{"localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{2}, 0},
		{"weighted_equals", []string{"-workers=localhost:50052,localhost:50051", "-worker-strategy=weighted_round_robin", "-worker-weights=2,1", "-max-pending-audio-bytes=32000"}, []string{"localhost:50052", "localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{2, 1}, 32000},
		{"weighted_separate", []string{"-worker-weights", " 2 , 1 ", "-worker-strategy", "weighted_round_robin", "-workers", " localhost:50052 , localhost:50051 "}, []string{"localhost:50052", "localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{2, 1}, 0},
		{"weighted_at_limit", []string{"-worker-strategy=weighted_round_robin", "-worker-weights=1000000"}, []string{"localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{workerpool.MaxTotalWeight}, 0},
		{"last_strategy_wins", []string{"-worker-strategy=bad", "-worker-strategy=round_robin"}, []string{"localhost:50051"}, workerStrategyRoundRobin, nil, 0},
		{"last_weights_win", []string{"-worker-strategy=weighted_round_robin", "-worker-weights=bad", "-worker-weights=3"}, []string{"localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{3}, 0},
		{"last_addresses_match_weights", []string{"-workers=localhost:50053", "-workers=localhost:50052,localhost:50051", "-worker-strategy=weighted_round_robin", "-worker-weights=2,1"}, []string{"localhost:50052", "localhost:50051"}, workerStrategyWeightedRoundRobin, []int64{2, 1}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGatewayConfig(tc.args)
			want := gatewayConfig{WorkerAddrs: tc.addrs, WorkerStrategy: tc.strategy, WorkerWeights: tc.weights, Gateway: gateway.Config{MaxMessageBytes: 1024 * 1024, MaxPendingAudioBytes: tc.budget}}
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("config=%+v err=%v want=%+v", got, err, want)
			}
		})
	}
}

// TestGatewayWorkerStrategyRejected 检查显式空权重、组合规则和完整入口，防止仅辅助函数正确。
func TestGatewayWorkerStrategyRejected(t *testing.T) {
	weighted := []string{"-workers=localhost:50051,localhost:50052", "-worker-strategy=weighted_round_robin"}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"unknown_strategy", []string{"-worker-strategy=least_loaded"}},
		{"empty_strategy", []string{"-worker-strategy="}},
		{"case_changed", []string{"-worker-strategy=ROUND_ROBIN"}},
		{"strategy_whitespace", []string{"-worker-strategy= round_robin "}},
		{"round_robin_weights", []string{"-worker-weights=1"}},
		{"round_robin_explicit_empty", []string{"-worker-weights="}},
		{"round_robin_explicit_blank", []string{"-worker-strategy=round_robin", "-worker-weights= "}},
		{"round_robin_after_weighted", []string{"-worker-strategy=weighted_round_robin", "-worker-weights=1", "-worker-strategy=round_robin"}},
		{"weighted_missing_weights", weighted},
		{"weighted_empty", appendCopy(weighted, "-worker-weights=")},
		{"weighted_blank", appendCopy(weighted, "-worker-weights= ")},
		{"weighted_zero", appendCopy(weighted, "-worker-weights=2,0")},
		{"weighted_negative", appendCopy(weighted, "-worker-weights=2,-1")},
		{"weighted_empty_element", appendCopy(weighted, "-worker-weights=2,")},
		{"weighted_too_few", appendCopy(weighted, "-worker-weights=2")},
		{"weighted_too_many", appendCopy(weighted, "-worker-weights=2,1,1")},
		{"weighted_overflow", appendCopy(weighted, "-worker-weights=2,9223372036854775808")},
		{"weighted_sum_over_limit", appendCopy(weighted, "-worker-weights=500000,500001")},
		{"weighted_hexadecimal", appendCopy(weighted, "-worker-weights=2,0x1")},
		{"weighted_duplicate_addresses", []string{"-workers=localhost:50051,localhost:50051", "-worker-strategy=weighted_round_robin", "-worker-weights=2,1"}},
		{"weighted_invalid_address", []string{"-workers=localhost", "-worker-strategy=weighted_round_robin", "-worker-weights=2"}},
		{"missing_strategy_value", []string{"-worker-strategy"}},
		{"missing_weights_value", []string{"-worker-strategy=weighted_round_robin", "-worker-weights"}},
		{"last_weights_empty", appendCopy(weighted, "-worker-weights=2,1", "-worker-weights=")},
		{"last_strategy_invalid", []string{"-worker-strategy=round_robin", "-worker-strategy=bad"}},
		{"positional", appendCopy(weighted, "-worker-weights=2,1", "extra")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGatewayConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) || !reflect.DeepEqual(got, gatewayConfig{}) {
				t.Fatalf("invalid config accepted or leaked: config=%+v err=%v", got, err)
			}
		})
	}
}

// appendCopy 避免 table case 共享参数切片的底层数组。
func appendCopy(args []string, extra ...string) []string {
	return append(append([]string(nil), args...), extra...)
}

// TestGatewayWorkerStrategyIsolation 多次解析和调用方修改返回配置不得改变后续默认值或权重。
func TestGatewayWorkerStrategyIsolation(t *testing.T) {
	args := []string{"-workers=localhost:50052,localhost:50051", "-worker-strategy=weighted_round_robin", "-worker-weights=2,1"}
	first, err := parseGatewayConfig(args)
	if err != nil {
		t.Fatal(err)
	}
	first.WorkerAddrs[0] = "mutated"
	first.WorkerWeights[0] = 999999
	first.WorkerStrategy = "mutated"
	second, err := parseGatewayConfig(args)
	if err != nil || second.WorkerAddrs[0] != "localhost:50052" || !reflect.DeepEqual(second.WorkerWeights, []int64{2, 1}) || second.WorkerStrategy != workerStrategyWeightedRoundRobin {
		t.Fatalf("parse calls share config: %+v err=%v", second, err)
	}
	defaults, err := parseGatewayConfig(nil)
	if err != nil || defaults.WorkerStrategy != workerStrategyRoundRobin || defaults.WorkerWeights != nil || !reflect.DeepEqual(defaults.WorkerAddrs, []string{"localhost:50051"}) {
		t.Fatalf("weighted parse polluted defaults: %+v err=%v", defaults, err)
	}
	if _, err = parseGatewayConfig([]string{"-worker-weights="}); err == nil {
		t.Fatal("expected invalid combination")
	}
	afterError, err := parseGatewayConfig(nil)
	if err != nil || !reflect.DeepEqual(afterError, defaults) {
		t.Fatalf("failed parse polluted defaults: %+v err=%v", afterError, err)
	}
}
