package gateway

import (
	"errors"
	"os"
	"runtime"
	"testing"
)

// extendedCollectedConfig 只解析独立采集入口，不能缩短正式条件或隐式运行十分钟。
func extendedCollectedConfig(mode, name, enabled string) (extendedConfig, error) {
	if mode != "probe" && mode != "formal" {
		return extendedConfig{}, errors.New("invalid collection mode")
	}
	if mode == "formal" && enabled != "1" {
		return extendedConfig{}, errors.New("formal experiment must be explicitly enabled")
	}
	for _, cfg := range extendedCases(mode == "probe") {
		if cfg.Case == name {
			return cfg, nil
		}
	}
	return extendedConfig{}, errors.New("unknown collected case")
}

// TestExtendedResumeCollectedCase 默认跳过；外部工具逐场独立启动同一个非 race 二进制。
// 输出目录必须尚不存在，原始事件及终态不通过大条 test2json 报告传递。
func TestExtendedResumeCollectedCase(t *testing.T) {
	mode := os.Getenv("TIDE_EXTENDED_MODE")
	if mode == "" {
		t.Skip("explicit extended collection only")
	}
	cfg, err := extendedCollectedConfig(mode, os.Getenv("TIDE_EXTENDED_CASE"), os.Getenv("TIDE_RUN_EXTENDED_FORMAL"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("TIDE_EXTENDED_CASE_OUTPUT")
	if dir == "" {
		t.Fatal("missing case output")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Logf("environment go=%s os=%s arch=%s GOMAXPROCS=%d mode=%s case=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), mode, cfg.Case)
	summary := runExtendedProbe(t, cfg, dir)
	if summary.LogError || summary.Forced || !summary.NaturalIdle {
		t.Error("collection did not yield complete natural-cleanup evidence")
	}
}

func TestExtendedResumeCollectedConfig(t *testing.T) {
	for _, mode := range []string{"probe", "formal"} {
		for _, cfg := range extendedCases(mode == "probe") {
			actual, err := extendedCollectedConfig(mode, cfg.Case, "1")
			if err != nil || actual.Chunks != cfg.Chunks {
				t.Fatal("fixed collection profile differs")
			}
		}
	}
	for _, tc := range [][3]string{{"formal", "A_normal", "0"}, {"probe", "unknown", "0"}, {"unknown", "A_normal", "1"}} {
		if _, err := extendedCollectedConfig(tc[0], tc[1], tc[2]); err == nil {
			t.Fatal("invalid collection entry accepted")
		}
	}
}
