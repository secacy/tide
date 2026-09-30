package main

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/loadgen"
)

// TestParseSamplerConfigValues 验证默认参数、覆盖值和时间参数相互独立；不从实现反推期望。
func TestParseSamplerConfigValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		edit func(*samplerConfig)
	}{
		{"defaults", nil, func(*samplerConfig) {}},
		{"all_overrides", []string{"-url=https://gateway.invalid:8443/debug/gateway?run=1", "-interval=250ms", "-request-timeout=2s", "-duration=1m", "-output-dir= run with spaces "}, func(c *samplerConfig) {
			c.Sampling = loadgen.GatewaySamplingConfig{Endpoint: "https://gateway.invalid:8443/debug/gateway?run=1", Interval: 250 * time.Millisecond, RequestTimeout: 2 * time.Second}
			c.Duration = time.Minute
			c.OutputDir = " run with spaces "
		}},
		{"ipv6", []string{"-url", "http://[::1]:8080/debug/gateway"}, func(c *samplerConfig) { c.Sampling.Endpoint = "http://[::1]:8080/debug/gateway" }},
		{"short_run", []string{"-duration=1ns"}, func(c *samplerConfig) { c.Duration = time.Nanosecond }},
		{"long_request", []string{"-request-timeout=1m"}, func(c *samplerConfig) { c.Sampling.RequestTimeout = time.Minute }},
		{"long_interval", []string{"-interval=1m"}, func(c *samplerConfig) { c.Sampling.Interval = time.Minute }},
		{"short_interval", []string{"-interval=1ns"}, func(c *samplerConfig) { c.Sampling.Interval = time.Nanosecond }},
		{"short_request", []string{"-request-timeout=1ns"}, func(c *samplerConfig) { c.Sampling.RequestTimeout = time.Nanosecond }},
		{"space_wrapped_dash", []string{"-output-dir= - "}, func(c *samplerConfig) { c.OutputDir = " - " }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-output-dir", "run"}, tc.args...)
			before := append([]string(nil), args...)
			want := samplerConfig{
				Sampling: loadgen.GatewaySamplingConfig{Endpoint: "http://localhost:8080/debug/gateway", Interval: 100 * time.Millisecond, RequestTimeout: time.Second},
				Duration: 30 * time.Second, OutputDir: "run",
			}
			tc.edit(&want)
			got, err := parseSamplerConfig(args)
			if err != nil || got != want {
				t.Fatalf("config=%+v err=%v want=%+v", got, err, want)
			}
			if !reflect.DeepEqual(args, before) {
				t.Fatal("changed caller arguments")
			}
		})
	}
}

// TestParseSamplerConfigRejects 验证参数解析与校验错误均返回零配置，并指出错误所属参数。
func TestParseSamplerConfigRejects(t *testing.T) {
	tests := []struct {
		name string
		args []string
		hint string
	}{
		{"unknown", []string{"-unknown=1"}, "flag provided but not defined"},
		{"missing_value", []string{"-url"}, "flag needs an argument"},
		{"positional", []string{"unexpected"}, "positional"},
		{"after_terminator", []string{"--", "unexpected"}, "positional"},
		{"empty_url", []string{"-url="}, "scheme"},
		{"wrong_scheme", []string{"-url=ws://gateway.invalid/debug/gateway"}, "scheme"},
		{"missing_scheme", []string{"-url=//gateway.invalid/debug/gateway"}, "scheme"},
		{"empty_host", []string{"-url=http:///debug/gateway"}, "hostname"},
		{"port_without_host", []string{"-url=http://:8080/debug/gateway"}, "hostname"},
		{"malformed_url", []string{"-url=http://[::1"}, "endpoint"},
		{"empty_output", []string{"-output-dir="}, "output directory"},
		{"blank_output", []string{"-output-dir= \t\n"}, "output directory"},
		{"stdout_output", []string{"-output-dir=-"}, "output directory"},
	}
	for _, parameter := range []string{"interval", "request-timeout", "duration"} {
		for _, value := range []string{"0s", "-1s", "abc", "10", "999999999999999999h"} {
			hint := "invalid value"
			if value == "0s" || value == "-1s" {
				hint = parameter
				if parameter == "request-timeout" {
					hint = "timeout"
				}
			}
			tests = append(tests, struct {
				name string
				args []string
				hint string
			}{parameter + "_" + value, []string{"-" + parameter + "=" + value}, hint})
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseSamplerConfig(append([]string{"-output-dir=run"}, tc.args...))
			if err == nil || errors.Is(err, flag.ErrHelp) || !strings.Contains(err.Error(), tc.hint) || cfg != (samplerConfig{}) {
				t.Fatalf("config=%+v err=%v want zero config and error containing %q", cfg, err, tc.hint)
			}
		})
	}
}

// TestParseSamplerConfigRequiredAndHelp 验证必填目录和无需提供目录即可请求帮助的行为。
func TestParseSamplerConfigRequiredAndHelp(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		help bool
	}{
		{"no_args", nil, false},
		{"missing_output", []string{"-duration=1s"}, false},
		{"short_help", []string{"-h"}, true},
		{"long_help", []string{"-help"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseSamplerConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) != tc.help || cfg != (samplerConfig{}) {
				t.Fatalf("config=%+v err=%v want help=%v", cfg, err, tc.help)
			}
		})
	}
}

// TestParseSamplerConfigDoesNotTouchOutput 验证路径可用性留给运行层，解析不创建或改写文件。
func TestParseSamplerConfigDoesNotTouchOutput(t *testing.T) {
	for _, mode := range []string{"new_directory", "missing_parent", "existing_directory", "existing_file"} {
		t.Run(mode, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, " run ")
			protected := ""
			switch mode {
			case "missing_parent":
				dir = filepath.Join(parent, "missing", "run")
			case "existing_directory":
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				protected = filepath.Join(dir, "manifest.json")
			case "existing_file":
				protected = dir
			}
			if protected != "" {
				if err := os.WriteFile(protected, []byte("keep old evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := parseSamplerConfig([]string{"-output-dir", dir, "-url=http://unresolvable.invalid/debug/gateway"})
			if err != nil || cfg.OutputDir != dir {
				t.Fatalf("config=%+v err=%v", cfg, err)
			}
			if protected != "" {
				if data, err := os.ReadFile(protected); err != nil || string(data) != "keep old evidence" {
					t.Fatalf("old evidence changed: %q %v", data, err)
				}
			}
			if mode == "existing_directory" {
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
					t.Fatalf("directory changed: %v %v", entries, err)
				}
			}
			if mode == "new_directory" || mode == "missing_parent" {
				if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
					t.Fatalf("created artifacts: %v %v", entries, err)
				}
			}
		})
	}
}
