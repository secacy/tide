package main

import (
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/mockasr"
)

// expectedWorkerConfig 固定迁移前入口的默认行为，避免配置抽取时意外改变响应节奏和文本。
func expectedWorkerConfig() workerConfig {
	return workerConfig{ListenAddr: ":50051", Mock: mockasr.Config{
		PartialEvery: 500 * time.Millisecond, ResponseDelay: 50 * time.Millisecond,
		PartialTexts: []string{"今", "今天", "今天天气", "今天天气不错"}, FinalText: "今天天气不错",
	}}
}

// TestParseWorkerConfigValues 检查有效配置及校验分层；负并行度由 Worker 构造拒绝，
// 非空监听地址的格式和可绑定性留给后续 net.Listen，本测试不创建网络资源。
func TestParseWorkerConfigValues(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		args                      []string
		address                   string
		concurrency               int
		processing, timeToRespond time.Duration
	}{
		{"defaults", nil, ":50051", 0, 0, 50 * time.Millisecond},
		{"explicit_equals", []string{"-listen=127.0.0.1:50052", "-processing-concurrency=2", "-processing-delay=25ms", "-response-delay=0s"}, "127.0.0.1:50052", 2, 25 * time.Millisecond, 0},
		{"explicit_separate", []string{"-listen", "[::1]:50053", "-processing-concurrency", "3", "-processing-delay", "1.5s", "-response-delay", "5ms"}, "[::1]:50053", 3, 1500 * time.Millisecond, 5 * time.Millisecond},
		{"explicit_disabled", []string{"-processing-concurrency=0", "-processing-delay=0s"}, ":50051", 0, 0, 50 * time.Millisecond},
		{"negative_concurrency_deferred", []string{"-processing-concurrency=-1"}, ":50051", -1, 0, 50 * time.Millisecond},
		{"address_validation_deferred", []string{"-listen=not-a-tcp-address"}, "not-a-tcp-address", 0, 0, 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseWorkerConfig(tc.args)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			want := expectedWorkerConfig()
			want.ListenAddr = tc.address
			want.Mock.ProcessingConcurrency = tc.concurrency
			want.Mock.ProcessingDelay = tc.processing
			want.Mock.ResponseDelay = tc.timeToRespond
			if !reflect.DeepEqual(cfg, want) {
				t.Fatalf("config=%+v, want %+v", cfg, want)
			}
			w, err := mockasr.New(cfg.Mock)
			if tc.concurrency < 0 {
				if err == nil || w != nil {
					t.Fatalf("negative concurrency not rejected: worker=%v err=%v", w, err)
				}
			} else if err != nil || w == nil {
				t.Fatalf("valid Worker config rejected: %v", err)
			}
		})
	}
}

// TestParseWorkerConfigInvalidArguments 检查错误对应目标参数，避免因无关默认值错误而假通过。
func TestParseWorkerConfigInvalidArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		hint string
	}{
		{"unknown", []string{"-unknown=1"}, "flag provided but not defined"},
		{"missing_value", []string{"-listen"}, "flag needs an argument"},
		{"not_integer", []string{"-processing-concurrency=two"}, "invalid value"},
		{"integer_overflow", []string{"-processing-concurrency=999999999999999999999999"}, "invalid value"},
		{"duration_without_unit", []string{"-processing-delay=20"}, "invalid value"},
		{"invalid_response_duration", []string{"-response-delay=fast"}, "invalid value"},
		{"duration_overflow", []string{"-processing-delay=999999999999999999999h"}, "invalid value"},
		{"positional", []string{"extra"}, "positional"},
		{"positional_after_flags", []string{"-listen=:50052", "extra"}, "positional"},
		{"positional_after_terminator", []string{"--", "extra"}, "positional"},
		{"empty_address", []string{"-listen="}, "listen"},
		{"blank_address", []string{"-listen= \t "}, "listen"},
		{"negative_processing", []string{"-processing-delay=-1ms"}, "processing-delay"},
		{"negative_response", []string{"-response-delay=-1ms"}, "response-delay"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseWorkerConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) {
				t.Fatalf("expected argument error, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.hint) {
				t.Errorf("error=%v, want mention of %q", err, tc.hint)
			}
			// 合法参数却未绑定不能伪装成语义校验成功。
			if tc.name == "empty_address" || tc.name == "blank_address" || tc.name == "negative_processing" || tc.name == "negative_response" {
				if strings.Contains(err.Error(), "flag provided but not defined") {
					t.Errorf("expected value validation, got unknown flag: %v", err)
				}
			}
			if !reflect.DeepEqual(cfg, workerConfig{}) {
				t.Errorf("failed parse returned partial config: %+v", cfg)
			}
		})
	}
}

// TestParseWorkerConfigHelp 保留帮助信号，供后续 main 正常退出。
func TestParseWorkerConfigHelp(t *testing.T) {
	for _, arg := range []string{"-h", "-help"} {
		t.Run(arg, func(t *testing.T) {
			_, err := parseWorkerConfig([]string{arg})
			if !errors.Is(err, flag.ErrHelp) {
				t.Fatalf("got %v, want flag.ErrHelp", err)
			}
		})
	}
}

// TestParseWorkerConfigIndependentCalls 检查重复解析不保留前次参数或共享可变的默认文本切片。
func TestParseWorkerConfigIndependentCalls(t *testing.T) {
	first, err := parseWorkerConfig([]string{"-processing-concurrency=2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Mock.PartialTexts) == 0 {
		t.Fatal("missing default partial texts")
	}
	first.Mock.PartialTexts[0] = "changed by caller"
	second, err := parseWorkerConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, expectedWorkerConfig()) {
		t.Fatalf("defaults contaminated: %+v", second)
	}
}
