# Tide

面向长时临床对话的实时语音流式处理系统，使用 Go 构建。

Tide 的业务背景是 AI 医生助理：实时转录医患对话，在问诊结束后辅助生成病历草稿。本仓库聚焦其中的 **ASR 流式后端**，负责音频接入、会话管理、流量控制、Worker 调度与结果返回。

一次问诊可能持续数十分钟，多个会话同时运行时，还会遇到下游处理变慢、连接中断和资源回收等问题。项目围绕这些问题逐步实现，并通过可控负载与故障实验验证设计。

## 整体架构

```mermaid
flowchart LR
    C[客户端 / 负载工具] <-->|WebSocket：音频与结果| G[Go Gateway]
    G <-->|gRPC 双向流| W[ASR Worker 池]
```

Gateway 管理连接与会话，每场会话固定绑定一个 Worker 流；模型计算通过统一接口接入。当前使用可控制处理延迟与并发名额的 Mock Worker，输出模拟文本；真实 ASR 尚未接入，AIGC 病历生成不在本仓库范围内。

## 核心能力与进度

| 能力 | 当前实现 |
| --- | --- |
| 会话生命周期 | 正常结束保留尾部结果；异常、超时及服务停止时统一取消和清理 |
| 背压与容量控制 | 会话准入、未处理音频预算、操作超时；慢 Worker 与过载保护已验证 |
| 多 Worker 调度 | 普通轮询与静态平滑加权轮询，已有同资源对照实验 |
| 负载与观测 | 并发负载工具、Gateway/Worker 状态采样、逐场报告与实验分析 |
| 异常恢复（开发中） | 实验 v2 服务端支持接回原会话、累计确认与结果重放；客户端音频缓存已验收，自动重连待接入 |

当前恢复范围是原 Gateway 与 Worker 存活时的短暂断连。真实 ASR、跨实例恢复及临床长时稳定容量仍待验证。详细进度见 [Milestones](docs/MILESTONES.md)。

## 快速开始

需要 Go 1.26 或更新版本，并能获取 Go 依赖。从仓库根目录操作，确保端口 50051 和 8080 可用。

终端一，启动 Mock Worker：

```sh
go run ./cmd/asr-worker -listen=127.0.0.1:50051
```

终端二，启动 Gateway：

```sh
go run ./cmd/gateway -workers=127.0.0.1:50051
```

终端三，模拟两场各 2 秒的语音输入，无需音频文件：

```sh
report_dir=$(mktemp -d)
go run ./cmd/loadgen \
  -url=ws://127.0.0.1:8080/v1/asr \
  -sessions=2 -audio-bytes=64000 -chunk-bytes=3200 \
  -realtime=true -session-timeout=10s \
  -expected-final-text='今天天气不错' \
  -output="$report_dir/smoke.json"
cat "$report_dir/smoke.json"
```

成功时客户端退出码为 0，报告中 `summary.completed` 为 `2`。输入是 16kHz、单声道、16-bit 静音 PCM；尾部文本来自 Mock。每次使用新目录，因为报告不会覆盖已有文件。上述命令运行 v1 链路；实验 v2 入口需显式开启。

默认测试入口：`make test`；并发检查：`make test-race`。更多参数、采样与实验命令见[运行说明](docs/command.md)和[测试说明](docs/testing.md)。

## 验证结果

- **调度对照：** 在同一台 Apple M2 Pro 上，固定两个单处理名额的 Mock Worker（每块 10ms / 20ms），12 并发、每场 20 秒、每种策略三批。仅将轮询改为权重 2:1 的加权轮询，完整完成数由 **30/36 提升至 36/36**，完成率提高 16.7 个百分点。[实验条件与原始依据](docs/experiments/strategy-comparison.md)
- **资源清理：** 正常结束、客户端断开和 Worker 报错混合运行，共 **600 个正式会话**；每轮活跃会话和未关闭连接均回到 0，每次运行结束时 goroutine 回到预热基线。[验证记录](docs/experiments/session-lifecycle-cleanup.md)

这些结果限定于对应 Mock、负载和测试条件，不代表真实识别质量或生产容量；全链路严格资源上界仍有待验证。

## 文档导航

- [阶段进度与验收](docs/MILESTONES.md)
- [架构决策与方案取舍](docs/adr/README.md)
- [运行命令](docs/command.md) · [测试入口](docs/testing.md)
- [实验报告与数据](docs/experiments/) · [文档索引](docs/README.md)
