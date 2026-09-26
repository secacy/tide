package main

import (
	"context"
	"errors"
	"flag"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/secacy/tide-artisan/internal/gateway"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
)

// configOnlyWorker 仅供构造时校验配置；不允许测试依赖 Worker 网络连接。
// Gateway.New 不调用 RPC，因此无需提供可运行的 stream。
type configOnlyWorker struct{ asrv1.ASRServiceClient }

// TestParseGatewayConfigValues 验证两种赋值形式、开关、int64 边界和原消息上限。
// 多次解析使用独立 FlagSet，前一次显式配置不能污染后一次默认值。
func TestParseGatewayConfigValues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		budget int64
	}{
		{"enabled_equals", []string{"-max-pending-audio-bytes=32000"}, 32000},
		{"default_after_enabled", nil, 0},
		{"explicit_disabled", []string{"-max-pending-audio-bytes=0"}, 0},
		{"enabled_separate", []string{"-max-pending-audio-bytes", "64000"}, 64000},
		{"small_positive", []string{"-max-pending-audio-bytes=1"}, 1},
		{"maximum_int64", []string{"-max-pending-audio-bytes=9223372036854775807"}, math.MaxInt64},
		{"negative_deferred_to_gateway", []string{"-max-pending-audio-bytes=-1"}, -1},
		{"minimum_int64", []string{"-max-pending-audio-bytes=-9223372036854775808"}, math.MinInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseGatewayConfig(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			want := gateway.Config{MaxMessageBytes: 1024 * 1024, MaxPendingAudioBytes: tc.budget}
			if !reflect.DeepEqual(cfg, want) {
				t.Fatalf("config=%+v, want %+v", cfg, want)
			}
			// 解析成功只表示语法合法，负数仍由构造边界拒绝。
			g, err := gateway.New(context.Background(), &configOnlyWorker{}, cfg)
			if tc.budget < 0 {
				if g != nil || err == nil || !strings.Contains(err.Error(), "max pending audio bytes") {
					t.Fatalf("negative budget not rejected by Gateway: gateway=%v err=%v", g, err)
				}
			} else if g == nil || err != nil {
				t.Fatalf("valid configuration rejected: %v", err)
			}
		})
	}
}

// TestParseGatewayConfigInvalidArguments 防止拼错参数、溢出和额外位置参数被静默忽略。
func TestParseGatewayConfigInvalidArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"not_integer", []string{"-max-pending-audio-bytes=one"}},
		{"empty_value", []string{"-max-pending-audio-bytes="}},
		{"missing_value", []string{"-max-pending-audio-bytes"}},
		{"positive_overflow", []string{"-max-pending-audio-bytes=9223372036854775808"}},
		{"negative_overflow", []string{"-max-pending-audio-bytes=-9223372036854775809"}},
		{"unknown_flag", []string{"-max-pending-audio-byte=32000"}},
		{"positional", []string{"32000"}},
		{"positional_after_option", []string{"-max-pending-audio-bytes=32000", "extra"}},
		{"positional_before_option", []string{"extra", "-max-pending-audio-bytes=32000"}},
		{"positional_after_terminator", []string{"--", "extra"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseGatewayConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected argument error, got %v", err)
			}
			if cfg != (gateway.Config{}) {
				t.Fatalf("invalid arguments returned usable partial config: %+v", cfg)
			}
		})
	}
}

// TestParseGatewayConfigHelp 保留帮助信号，供 main 将帮助作为正常退出处理。
func TestParseGatewayConfigHelp(t *testing.T) {
	for _, arg := range []string{"-h", "-help"} {
		t.Run(arg, func(t *testing.T) {
			_, err := parseGatewayConfig([]string{arg})
			if !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("help error=%v, want flag.ErrHelp", err)
			}
		})
	}
}
