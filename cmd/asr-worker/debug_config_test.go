package main

import (
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestWorkerDebugListenValues 固定关闭语义、地址原值和旧配置默认值。
// 非空地址的格式及可绑定性由运行层判断，解析阶段不修剪或试探端口。
func TestWorkerDebugListenValues(t *testing.T) {
	for _, tc := range []struct {
		name        string
		args        []string
		debug, grpc string
	}{
		{"default_disabled", nil, "", ":50051"},
		{"empty_equals", []string{"-debug-listen="}, "", ":50051"},
		{"empty_separate", []string{"-debug-listen", ""}, "", ":50051"},
		{"explicit_equals", []string{"-debug-listen=127.0.0.1:50081"}, "127.0.0.1:50081", ":50051"},
		{"explicit_separate", []string{"-debug-listen", "127.0.0.1:50082"}, "127.0.0.1:50082", ":50051"},
		{"ipv6", []string{"-debug-listen=[::1]:50081"}, "[::1]:50081", ":50051"},
		{"wildcard", []string{"-debug-listen=:50081"}, ":50081", ":50051"},
		{"system_port", []string{"-debug-listen=127.0.0.1:0"}, "127.0.0.1:0", ":50051"},
		{"format_deferred", []string{"-debug-listen=not-a-tcp-address"}, "not-a-tcp-address", ":50051"},
		{"preserve_spaces", []string{"-debug-listen= 127.0.0.1:50081 "}, " 127.0.0.1:50081 ", ":50051"},
		{"same_zero_addresses", []string{"-listen=127.0.0.1:0", "-debug-listen=127.0.0.1:0"}, "127.0.0.1:0", "127.0.0.1:0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWorkerConfig(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			want := expectedWorkerConfig()
			want.DebugListenAddr, want.ListenAddr = tc.debug, tc.grpc
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("config=%+v want=%+v", got, want)
			}
		})
	}
}

// TestWorkerDebugListenInvalid 确认纯空白拒绝并返回零配置，不以未知参数错误冒充校验。
func TestWorkerDebugListenInvalid(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		hint string
	}{
		{"spaces", []string{"-debug-listen=   "}, "debug-listen"},
		{"tabs_newlines", []string{"-debug-listen=\t\r\n"}, "debug-listen"},
		{"unicode_spaces", []string{"-debug-listen=\u00a0\u3000"}, "debug-listen"},
		{"missing_value", []string{"-debug-listen"}, "flag needs an argument"},
		{"positional", []string{"-debug-listen=127.0.0.1:50081", "extra"}, "positional"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWorkerConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) || !strings.Contains(err.Error(), tc.hint) || strings.Contains(err.Error(), "flag provided but not defined") {
				t.Fatalf("error=%v want=%q", err, tc.hint)
			}
			if !reflect.DeepEqual(got, workerConfig{}) {
				t.Fatalf("partial config on error: %+v", got)
			}
		})
	}
}

// TestWorkerDebugListenIndependentOfProcessing 查询开关与共享处理限制分别配置，不能互相推导。
func TestWorkerDebugListenIndependentOfProcessing(t *testing.T) {
	for _, limit := range []int{0, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			for _, address := range []string{"", "127.0.0.1:50081"} {
				got, err := parseWorkerConfig([]string{fmt.Sprintf("-processing-concurrency=%d", limit), "-debug-listen=" + address})
				if err != nil {
					t.Fatal(err)
				}
				want := expectedWorkerConfig()
				want.DebugListenAddr, want.Mock.ProcessingConcurrency = address, limit
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("config=%+v want=%+v", got, want)
				}
			}
		})
	}
}

func TestWorkerDebugListenIndependentParses(t *testing.T) {
	first, err := parseWorkerConfig([]string{"-debug-listen=127.0.0.1:50081"})
	if err != nil || first.DebugListenAddr != "127.0.0.1:50081" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := parseWorkerConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second, expectedWorkerConfig()) {
		t.Fatalf("defaults retained previous flags: %+v", second)
	}
}
