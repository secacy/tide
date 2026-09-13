# EXP-014: 有限恢复实现的故障与完整性验证

Date: 2026-09-13
Commit: `469947dfb0833320fd8fbfd4c650601bfbd33e46`
Related: [ADR-009](../adr/ADR-009-bounded-recovery.md), [EXP-013](EXP-013-recovery-checkpoint.md)

## Question / Hypothesis

EXP-013 的候选语义接入真实客户端、WebSocket 和 Gateway 后，能否在故障下补齐可保留音频，同时保证持续失败最终停止、缺口不被最后一次正常关闭掩盖？依据 ADR-009，预期重放从客户端已提交断点开始；缓存不足、预算耗尽或协议错误不会被报告为完整问诊。

## Setup / Metrics

使用生产 `Client.RunRecoverable` → loopback HTTP/WebSocket Gateway → gRPC/bufconn 可控 Worker。每组一个 Gateway、一个 Worker，Gateway 配额 1；其他处理、队列及 End 期限保留默认，心跳关闭以隔离恢复协议。普通回归另覆盖心跳与正常关闭竞争。

每块为 320 字节 / 10 ms 的带编号 PCM，Worker 检查位置、块序号和音频内容，并生成独立、不可变的人工片段。有限尾部场景不按实时速度发送；缓存组按实时速度采集。缓存组缩短到 40 ms / 1280 字节，失败组使用 150 / 180 ms 总恢复预算，其余使用 1 秒；重试间隔 10 ms。没有模拟真实模型的上下文或推理质量。

故障包括：Worker 处理后、checkpoint 发出前失败；checkpoint 发出后失败；WebSocket 中继收到第二个完整 checkpoint 后不向客户端转发并切断上下游连接；End 后关闭丢失；持续无 checkpoint、持续尾部失败以及不支持/非法 Worker 契约。中继是应用消息丢弃和真实连接中断，不是随机 TCP 丢包。

记录完整性、结果区间、显式缺口、采集末尾、连接尝试数、每个 RPC 重启位置、PCM 缓存峰值和最终清理。`run_elapsed_ms` 从 Run 调用到返回，包含首次接入、输入及本地清理，**不是故障到恢复延迟**；`attempts` 含未成功进入 Worker RPC 的接入尝试。

环境：Go 1.26.5，darwin/arm64，12 个逻辑 CPU，race 开启。详细环境和输入源码 SHA-256 见元数据；没有采集内存 RSS、CPU 利用率或并发容量。

## Procedure / Evidence

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  python3 scripts/run_production_recovery_experiment.py \
  --output /private/tmp/EXP-014-reproduction.jsonl --count 3 --race
python3 scripts/summarize_production_recovery_experiment.py \
  /private/tmp/EXP-014-reproduction.jsonl
```

原始数据：[JSONL](results/EXP-014-production-recovery.jsonl)、[元数据](results/EXP-014-production-recovery.jsonl.meta.json)、[汇总](results/EXP-014-production-recovery-summary.md)。采集版本如上，相关运行源码与该提交一致；工作区其他未提交文件不参与本实验。采集后修正汇总器，将 `go test -json` 拆开的长日志按测试重组再解析；原始数据未改动或重跑，汇总独立记录分析脚本 SHA-256。验证了汇总可重建、原始文件校验失败及测试 fail 事件会被拒绝。

## Results

十个场景各三轮，共三十个样本，全部通过预设语义和清理检查，无排除。其中十五个完整、十五个按预期不完整；后者保留原始错误和缺口，不当作成功恢复。

| 场景 | 三轮观测 |
| --- | --- |
| 正常 | 一次尝试完整覆盖 1600 采样点 |
| Worker 在 checkpoint 前 / 后失败 | 均两次尝试完成；分别从 320 / 480 采样点重启，无区间重叠或遗漏 |
| WebSocket checkpoint 交付丢失 | 第二个 checkpoint 被截断在客户端应用之前；两次尝试完成，重放从客户端已提交的 160 开始 |
| End 后关闭丢失 | 已提交全部 1600 采样点，从 1600 发起空尾部 RPC，正常完成后才报告完整 |
| 无断点导致缓存耗尽，随后恢复 | 两次尝试；保留 `[0,640)` 缺口，后续覆盖到 5600，最终仍为不完整；缓存峰值 1280 字节 |
| End 尾部持续失败 | 150 ms 预算触发停止；调用耗时约 154.9–157.1 ms、10–11 次接入尝试；RPC 始终从 0 重放，返回 `[0,800)` 缺口 |
| 持续无断点并反复改从新片段开始 | 六次尝试，180 ms 共享预算耗尽；从首次采集算调用约 229.2–229.6 ms，未 End，保留历史缺口和未确认尾部；没有每次重新开启预算 |
| 不支持恢复 / 非法 checkpoint | 均一次尝试后明确失败，无自动重试 |

所有样本的“已提交结果 + 显式缺口”连续覆盖返回报告中的采集范围；没有用最终 1000 消除之前的缺口。所有 Gateway 排空，受控 Worker 活动 RPC 计数归零。该计数仅验证这些遵守取消的 Worker，不证明任意外部模型都已释放计算资源。

## Conclusion / Follow-up

已实现链路在所测故障下符合 ADR-009 的有限恢复语义。另有普通回归覆盖真实 Mock、HTTP 503 共享预算、Ready 停滞、旧尝试/重复事件、非法偏移、EOF 缺少检查点、输入错误、Source 取消和正常关闭期间的在途 Ping；全项目及实验标签的 race 与 vet 通过。

本实验不能证明默认十五秒缓存、十秒预算下能追上持续实时输入，也没有覆盖 Gateway/Worker 进程退出后重新路由、WAN 弱网或并发恢复。下一步在持续输入及可控处理速率下联合测量检测、重新接入、重放和安全断点追赶，随后补实例故障；真实模型的安全断点契约与质量留给 M7。M6 尚未整体完成。
