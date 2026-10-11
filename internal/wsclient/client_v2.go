package wsclient

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/wsprotocol"
)

// V2Config 配置一次新建 v2 会话；数值零值使用默认值，负值非法。
// URL 指向 Gateway 的 /v2/asr；Resume 非 nil 时启用有限原场恢复。
type V2Config struct {
	URL               string          // 必填的 WebSocket 地址。
	ChunkBytes        int             // PCM 块大小，默认 3200，须按样本字节对齐。
	Realtime          bool            // 按固定采集时间轴读取；false 尽快读取，可能耗尽未确认缓存。
	ReadLimit         int64           // 单条完整服务端消息上限，默认 64 KiB。
	MaxReplayBytes    uint64          // 缓存独占的未确认音频负载上限，默认 10 秒 PCM。
	MaxReplayChunks   int             // 缓存块数上限，默认 128；不是全客户端内存预算。
	HandshakeTimeout  time.Duration   // Dial/start/ready 共用的绝对预算，默认 10s。
	WriteTimeout      time.Duration   // 单次写预算及完成后的最终确认总窗口，默认 5s。
	ApplyTimeout      time.Duration   // 单次业务应用预算，默认 5s，回调须合作取消。
	CompletionTimeout time.Duration   // 首次 end 授权到 completed 的固定预算，默认 30s。
	Resume            *V2ResumeConfig // nil 保持单连接行为；NewV2 保存独立配置副本。
	OnResult          V2ResultHandler // nil 表示有意消费并丢弃结果，仍推进连续应用位置。
}

// V2ResumeConfig 限制原会话的恢复；非 nil 显式启用，零字段使用默认值。
// jitter 零值不写入 JSON，保持旧确定性实验的配置格式；开启后显式标记策略。
type V2ResumeConfig struct {
	MaxAttempts    int           // 默认 5；整场累计 resume Dial 次数，首次 start 不计。
	Window         time.Duration // 默认 5s；每轮断线含清理、退避、握手的固定总预算。
	InitialBackoff time.Duration // 默认 100ms；第一次恢复失败后的基础等待，随机化前。
	MaxBackoff     time.Duration // 默认 1s；基础指数退避上限，须 >= InitialBackoff；jitter 后最多增加 50%。
	Jitter         bool          `json:"Jitter,omitempty"`     // 默认关闭；开启后在基础间隔上下各 50% 范围内均匀抽样。
	JitterSeed     uint64        `json:"JitterSeed,omitempty"` // 仅 Jitter=true 有效；0 为每 Run 自动选种子，非零用于复现实验。
}

// V2ResultHandler 按序同步应用结果，nil 返回后才提交并确认。
// 必须及时响应 ctx；失败不自动重试已有外部副作用的应用。
// 多个 Run 共用回调时，调用方负责共享数据的并发安全。
type V2ResultHandler func(ctx context.Context, result wsprotocol.SequencedResultMessage, receivedAt time.Time) error

// V2RunResult 保留已核实的完成事实，即使确认写入或清理随后失败。
type V2RunResult struct {
	Completed            bool   // 已核实本场 completed。
	FinalOffset          uint64 // 仅 Completed=true 时有效，最终音频字节位置。
	AppliedSeq           uint64 // 已连续成功应用的结果位置，允许 0。
	ResumeAttempts       int    // 实际启动的恢复拨号数，包含失败尝试。
	ResumedConnections   int    // 本地接纳的恢复 ready 数，不等于断网次数。
	CompletionAckWritten bool   // completed_ack 的实际 Write 成功；不证明对端已处理。
}

// V2Client 保存不可变配置；每个 Run 独占状态、连接和任务。
type V2Client struct{ cfg V2Config }

// V2EntryError 保留入口拒绝码，包括未知的非空码；不回显服务端提示。
type V2EntryError struct{ Code wsprotocol.V2ErrorCode }

// Error 返回固定说明；调用方通过 errors.As 读取 Code。
func (e *V2EntryError) Error() string { return "v2 session entry rejected" }

// ErrV2Incomplete 表示对端正常关闭，但本地尚未核实整场完成。
var ErrV2Incomplete = errors.New("v2 session ended before completion")

// ErrV2RecoveryExhausted 表示恢复窗口或整场尝试次数耗尽；同时保留最近原因。
var ErrV2RecoveryExhausted = errors.New("v2 recovery budget exhausted")

// NewV2 填充默认值并校验分块、预算及超时。
func NewV2(cfg V2Config) (*V2Client, error) {
	if cfg.URL == "" {
		return nil, errors.New("websocket URL is required")
	}
	if cfg.ChunkBytes < 0 || cfg.ReadLimit < 0 || cfg.MaxReplayChunks < 0 ||
		cfg.HandshakeTimeout < 0 || cfg.WriteTimeout < 0 || cfg.ApplyTimeout < 0 || cfg.CompletionTimeout < 0 {
		return nil, errors.New("v2 client limits and timeouts must not be negative")
	}
	if cfg.ChunkBytes == 0 {
		cfg.ChunkBytes = audio.ChunkBytesDefault
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 64 * 1024
	}
	if cfg.MaxReplayBytes == 0 {
		cfg.MaxReplayBytes = 10 * audio.BytesPerSecond
	}
	if cfg.MaxReplayChunks == 0 {
		cfg.MaxReplayChunks = 128
	}
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = 5 * time.Second
	}
	if cfg.CompletionTimeout == 0 {
		cfg.CompletionTimeout = 30 * time.Second
	}
	if cfg.ChunkBytes%audio.BytesDepth != 0 || cfg.ChunkBytes > int(^uint(0)>>1)-8 || uint64(cfg.ChunkBytes) > cfg.MaxReplayBytes {
		return nil, errors.New("v2 chunk must align to PCM samples, fit replay budget and offset header")
	}
	if cfg.Resume != nil {
		resume := *cfg.Resume
		if resume.MaxAttempts < 0 || resume.Window < 0 || resume.InitialBackoff < 0 || resume.MaxBackoff < 0 {
			return nil, errors.New("v2 resume limits must not be negative")
		}
		if resume.MaxAttempts == 0 {
			resume.MaxAttempts = 5
		}
		if resume.Window == 0 {
			resume.Window = 5 * time.Second
		}
		if resume.InitialBackoff == 0 {
			resume.InitialBackoff = 100 * time.Millisecond
		}
		if resume.MaxBackoff == 0 {
			resume.MaxBackoff = time.Second
		}
		if resume.MaxBackoff < resume.InitialBackoff {
			return nil, errors.New("v2 max backoff must be at least initial backoff")
		}
		if !resume.Jitter && resume.JitterSeed != 0 {
			return nil, errors.New("v2 jitter seed requires jitter")
		}
		const maxDuration = time.Duration(1<<63 - 1)
		if resume.Jitter && resume.MaxBackoff > maxDuration-resume.MaxBackoff/2 {
			return nil, errors.New("v2 jitter backoff overflows duration")
		}
		cfg.Resume = &resume
	}
	return &V2Client{cfg: cfg}, nil
}

// Run 新建一次逻辑会话，启用 Resume 时在预算内接回已有身份的原场。
// 非 nil source 交入后恰好 Close 一次，Close 须并发安全并解除阻塞 Read。
// 回调也须响应取消；返回前等待所有自建任务和连接实际清理完成。
// nil error 表示完成已核实、最终确认已写成功且清理成功。
func (c *V2Client) Run(ctx context.Context, source io.ReadCloser) (result V2RunResult, runErr error) {
	if source == nil {
		return result, errors.New("audio source is nil")
	}
	run, err := c.newV2Run(ctx, source)
	if err != nil {
		return result, errors.Join(err, source.Close())
	}
	defer func() {
		runErr = errors.Join(runErr, run.cleanup())
		result = run.result()
	}()
	if err := run.ctx.Err(); err != nil {
		return result, err
	}
	run.startHandshake(false)
	return result, run.coordinate()
}
