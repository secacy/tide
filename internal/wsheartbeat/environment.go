package wsheartbeat

import (
	"fmt"
	"time"
)

// FromEnvironment 供命令入口读取启动参数；库组件本身不隐式读取进程环境。
// 支持 Go duration，例如 2s、500ms；未设置或 0 使用默认值，不支持热更新。
func FromEnvironment(getenv func(string) string) (Config, error) {
	var cfg Config
	for _, item := range []struct {
		key   string
		value *time.Duration
	}{
		{"TIDE_HEARTBEAT_INTERVAL", &cfg.Interval},
		{"TIDE_HEARTBEAT_TIMEOUT", &cfg.Timeout},
	} {
		if raw := getenv(item.key); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil {
				return Config{}, fmt.Errorf("%s: %w", item.key, err)
			}
			*item.value = value
		}
	}
	return cfg.Normalize()
}
