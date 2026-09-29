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

// defaultLoadConfig 固定公开的默认参数，不从解析实现反推期望值。
func defaultLoadConfig() loadConfig {
	return loadConfig{Batch: loadgen.BatchConfig{Sessions: 1, Session: loadgen.SessionConfig{
		URL: "ws://localhost:8080/v1/asr", AudioBytes: 1920000, ChunkBytes: 3200,
		Realtime: true, Timeout: 90 * time.Second, ExpectedFinalText: "expected tail",
	}}, OutputPath: "report.json"}
}

// TestParseLoadConfigValues 检查实际绑定值；短超时允许用于故意失败的实验。
func TestParseLoadConfigValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		edit func(*loadConfig)
	}{
		{"defaults", nil, func(*loadConfig) {}},
		{"all_overrides", []string{
			"-url", "wss://gateway.invalid:8443/v1/asr?test=1", "-sessions", "3",
			"-audio-bytes", "16002", "-chunk-bytes", "2000", "-realtime=false",
			"-session-timeout", "1.5s", "-expected-final-text", " 尾部\n", "-output", " path with spaces.json ",
		}, func(c *loadConfig) {
			c.Batch = loadgen.BatchConfig{Sessions: 3, Session: loadgen.SessionConfig{
				URL: "wss://gateway.invalid:8443/v1/asr?test=1", AudioBytes: 16002, ChunkBytes: 2000,
				Realtime: false, Timeout: 1500 * time.Millisecond, ExpectedFinalText: " 尾部\n",
			}}
			c.OutputPath = " path with spaces.json "
		}},
		{"ipv6", []string{"-url=ws://[::1]:8080/v1/asr"}, func(c *loadConfig) { c.Batch.Session.URL = "ws://[::1]:8080/v1/asr" }},
		{"short_timeout", []string{"-session-timeout=1ns"}, func(c *loadConfig) { c.Batch.Session.Timeout = time.Nanosecond }},
		{"whitespace_tail", []string{"-expected-final-text= \t"}, func(c *loadConfig) { c.Batch.Session.ExpectedFinalText = " \t" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-expected-final-text=expected tail", "-output=report.json"}, tc.args...)
			before := append([]string(nil), args...)
			got, err := parseLoadConfig(args)
			if err != nil {
				t.Fatal(err)
			}
			want := defaultLoadConfig()
			tc.edit(&want)
			if got != want {
				t.Fatalf("config = %+v, want %+v", got, want)
			}
			if !reflect.DeepEqual(args, before) {
				t.Fatal("parser changed caller arguments")
			}
		})
	}
}

// TestParseLoadConfigRejects 检查错误归属及零配置返回，避免未知参数被误当成值校验。
func TestParseLoadConfigRejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		hint string
	}{
		{"unknown", []string{"-unknown=1"}, "flag provided but not defined"},
		{"missing_value", []string{"-url"}, "flag needs an argument"},
		{"positional", []string{"unexpected"}, "positional"},
		{"after_terminator", []string{"--", "unexpected"}, "positional"},
		{"separated_bool_value", []string{"-realtime", "false"}, "positional"},
		{"invalid_integer", []string{"-sessions=many"}, "invalid value"},
		{"integer_overflow", []string{"-sessions=9999999999999999999999"}, "invalid value"},
		{"int64_overflow", []string{"-audio-bytes=9223372036854775808"}, "invalid value"},
		{"duration_unit_missing", []string{"-session-timeout=10"}, "invalid value"},
		{"duration_overflow", []string{"-session-timeout=999999999999999999h"}, "invalid value"},
		{"invalid_bool", []string{"-realtime=sometimes"}, "invalid boolean value"},
		{"zero_sessions", []string{"-sessions=0"}, "sessions"},
		{"negative_sessions", []string{"-sessions=-1"}, "sessions"},
		{"zero_audio", []string{"-audio-bytes=0"}, "audio bytes"},
		{"negative_audio", []string{"-audio-bytes=-2"}, "audio bytes"},
		{"unaligned_audio", []string{"-audio-bytes=3"}, "align"},
		{"zero_chunk", []string{"-chunk-bytes=0"}, "chunk bytes"},
		{"negative_chunk", []string{"-chunk-bytes=-2"}, "chunk bytes"},
		{"unaligned_chunk", []string{"-chunk-bytes=3"}, "align"},
		{"zero_timeout", []string{"-session-timeout=0s"}, "timeout"},
		{"negative_timeout", []string{"-session-timeout=-1s"}, "timeout"},
		{"planned_total_overflow", []string{"-sessions=2", "-audio-bytes=4611686018427387904"}, "overflow"},
		{"empty_tail", []string{"-expected-final-text="}, "final text"},
		{"empty_url", []string{"-url="}, "URL"},
		{"wrong_scheme", []string{"-url=https://gateway.invalid/v1/asr"}, "scheme"},
		{"missing_scheme", []string{"-url=//gateway.invalid/v1/asr"}, "scheme"},
		{"empty_host", []string{"-url=ws:///v1/asr"}, "hostname"},
		{"port_without_host", []string{"-url=ws://:8080/v1/asr"}, "hostname"},
		{"malformed_url", []string{"-url=ws://[::1"}, "invalid url"},
		{"empty_path", []string{"-output="}, "output path"},
		{"blank_path", []string{"-output= \t"}, "output path"},
		{"stdout_not_supported", []string{"-output=-"}, "output path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-expected-final-text=expected tail", "-output=report.json"}, tc.args...)
			cfg, err := parseLoadConfig(args)
			if err == nil || errors.Is(err, flag.ErrHelp) || !strings.Contains(err.Error(), tc.hint) {
				t.Fatalf("error = %v, want argument error mentioning %q", err, tc.hint)
			}
			if cfg != (loadConfig{}) {
				t.Fatalf("invalid input returned partial config: %+v", cfg)
			}
		})
	}
}

func TestParseLoadConfigRequiredAndHelp(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		help bool
	}{
		{"no_args", nil, false},
		{"missing_tail", []string{"-output=report.json"}, false},
		{"missing_output", []string{"-expected-final-text=tail"}, false},
		{"short_help", []string{"-h"}, true},
		{"long_help", []string{"-help"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseLoadConfig(tc.args)
			if err == nil || errors.Is(err, flag.ErrHelp) != tc.help || cfg != (loadConfig{}) {
				t.Fatalf("config = %+v, err = %v, want zero config, help=%v", cfg, err, tc.help)
			}
		})
	}
}

// TestParseLoadConfigDoesNotTouchOutput 解析只保存路径，不提前打开、覆盖或检查父目录。
func TestParseLoadConfigDoesNotTouchOutput(t *testing.T) {
	dir := t.TempDir()
	for _, existing := range []bool{false, true} {
		name := "missing_parent"
		path := filepath.Join(dir, "missing", "result.json")
		if existing {
			name = "existing_file"
			path = filepath.Join(dir, "existing.json")
			if err := os.WriteFile(path, []byte("keep this result"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		t.Run(name, func(t *testing.T) {
			cfg, err := parseLoadConfig([]string{"-expected-final-text=tail", "-output", path, "-url=ws://unresolvable.invalid/asr"})
			if err != nil || cfg.OutputPath != path {
				t.Fatalf("config=%+v err=%v", cfg, err)
			}
			data, err := os.ReadFile(path)
			if existing {
				if err != nil || string(data) != "keep this result" {
					t.Fatalf("existing file changed: %q, %v", data, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected output created: %v", err)
			}
		})
	}
}
