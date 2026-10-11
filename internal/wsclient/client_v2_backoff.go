package wsclient

import "time"

// v2BackoffDelay 按当前轮失败次数增长基础间隔并封顶；failures 从 1 开始。
// 先比较再乘二避免 duration 溢出，下一档不依赖上一次随机结果。
func v2BackoffDelay(cfg V2ResumeConfig, failures int) time.Duration {
	delay := cfg.InitialBackoff
	for i := 1; i < failures && delay < cfg.MaxBackoff; i++ {
		if delay > cfg.MaxBackoff/2 {
			return cfg.MaxBackoff
		}
		delay *= 2
	}
	return min(delay, cfg.MaxBackoff)
}

// v2JitterDelay 对正基础间隔 base 做整数纳秒的 ±50% 均匀抽样。
// 调用前已校验 base+base/2 不溢出；sample(n) 必须返回 [0,n) 的整数。
// 只计算时长，不睡眠、不更改预算；不再截到基础上限，以保留均值 base。
func v2JitterDelay(base time.Duration, sample func(int64) int64) time.Duration {
	half := base / 2
	return base - half + time.Duration(sample(int64(2*half+1)))
}

// resumeBackoffDelay 计算当前轮 failures 次失败后的下一次等待；failures 从 1 开始。
// 关闭 jitter 时返回旧基础值，开启时由 Run 独占随机源抽样一次。
func (r *v2ClientRun) resumeBackoffDelay(failures int) time.Duration {
	cfg := *r.client.cfg.Resume
	base := v2BackoffDelay(cfg, failures)
	if !cfg.Jitter {
		return base
	}
	return v2JitterDelay(base, r.backoffRand.Int64N)
}
