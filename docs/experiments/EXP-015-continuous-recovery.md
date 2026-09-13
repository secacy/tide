# EXP-015: 默认预算下的持续实时输入追赶

Date: 2026-09-13
Commit: `986565dabc98c557a124bf6b78178c93e358869f`
Related: [ADR-009](../adr/ADR-009-bounded-recovery.md), [EXP-014](EXP-014-production-recovery.md)

## Question / Hypothesis

重新接入并恢复出字，是否意味着已恢复实时性？持续采集时，默认十五秒缓存、十秒总恢复预算能否支撑不同处理余量的 Worker？预期处理明显快于实时的 Worker 能消化积压；仅略快于实时或等速的 Worker 可能持续返回 checkpoint，却仍在预算到期时中断。沿用 ADR-009 的已确认语义，不改变实现策略。

## Setup

生产 `RunRecoverable` → loopback WebSocket Gateway → gRPC/bufconn 可控 Worker，单并发、单 Worker，Gateway 配额 1。连续十六秒带编号 PCM，每 100 ms 采集一块 3200 字节；每累计 500 ms 音频产生独立、不可变的人工 checkpoint。客户端采集按实时速度，重放按当前实现尽快发送，Worker 验证音频内容、全局位置和 RPC 局部序号。

客户端恢复配置全部使用默认值：十五秒 / 480,000 字节缓存、4096 块、十秒总预算、100 ms 重试间隔、三秒 Ready 期限。双端心跳保持默认两秒 / 三秒；Gateway 队列、三秒处理期限和五秒 End 期限也保留默认值。

| 场景 | 初始 RPC | 故障及替代 RPC |
| --- | --- | --- |
| healthy_40ms | 每块处理 40 ms | 无故障，对照组持续运行至 End |
| recover_40ms | 每块处理 20 ms，第 20 块处理后、checkpoint 前返回 Unavailable | 新接入在两秒内由 HTTP 门控返回 503；后续每块处理 40 ms |
| recover_80ms | 同上 | 同上，后续每块处理 80 ms |
| recover_100ms | 同上 | 同上，后续每块处理 100 ms |

503 门控模拟接入不可用，不代表本实验测得了 Worker 容量不足；健康组不触发门控。故障是明确的 RPC 错误，不是静默断网，心跳在本次负载中正常响应。接入约束仍经过生产 Gateway，但此处没有故意持有旧 Worker 名额。

环境：Go 1.26.5，darwin/arm64，12 个逻辑 CPU，race 开启；详细环境及源码哈希见元数据。固定处理等待不是共享计算资源或真实 ASR 推理，不能推导生产容量。

## Metrics / Procedure

通过 Go `-overlay` 在客户端实际状态转换后加入只读时间观测，不修改分支、期限或策略。记录：故障注入、恢复预算启动、每次连接开始及退出、Worker RPC 开始及退出、Ready、实际 checkpoint 应用、协调者按原谓词判定追上，以及调用返回。所有事件使用同一单调时间基准；观测锁和日志保存有开销，结论限定在该实验环境。

- Fault→budget：明确 RPC 故障到客户端启动恢复预算，不是静默失联检测时间。
- Budget→Ready：包含重试间隔、两秒 503 门控、接入及 Worker Ready。
- Ready→caught up：从替代 RPC 就绪到生产协调者确认 checkpoint 追上当时采集末尾，包含重放处理及 checkpoint 发布。
- Recovery / stop：预算启动到追上，或未追上时的调用返回；正常十六秒采集的总时长不作为恢复延迟。
- 另记录 PCM 缓存峰值、结果/缺口覆盖、尝试数、拒绝数和清理。一个调用中的接入尝试数包含 HTTP 503，不等于 Worker RPC 数。

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  python3 scripts/run_catchup_experiment.py \
  --output /private/tmp/EXP-015-reproduction.jsonl --count 2 --race
python3 scripts/summarize_catchup_experiment.py \
  /private/tmp/EXP-015-reproduction.jsonl
```

正式证据：[原始数据](results/EXP-015-catchup.jsonl)、[元数据](results/EXP-015-catchup.jsonl.meta.json)、[逐样本汇总](results/EXP-015-catchup-summary.md)。共四个场景各两轮，八个正式样本，无排除。运行输入与上述提交匹配，overlay 由已提交脚本重建并独立记录 SHA-256。

另保留提交前的四个[预跑样本](results/EXP-015-pilot/catchup.jsonl)、[元数据](results/EXP-015-pilot/catchup.jsonl.meta.json)及[汇总](results/EXP-015-pilot/summary.md)，与正式统计分开。预跑元数据记录当时 HEAD 及新增未提交实验文件的哈希；这些输入与最终实验代码一致。预跑也保留两种未追上的结果，不删掉失败恢复来提高成功率。汇总器验证原始哈希、完整样本数、区间覆盖、同轮预算未刷新及清理，并验证过损坏文件和 fail 事件会被拒绝。

## Results

| 场景 | 两轮结果 | 预算启动→Ready | Ready→追上 | 预算启动→追上或返回 | PCM 峰值 | 最终缺口 |
| --- | --- | --- | --- | --- | --- | --- |
| healthy_40ms | 完整结束 | — | — | 无恢复 | 16,000 B | 无 |
| recover_40ms | 完整结束 | 2.067–2.071 s | 1.950 s | 4.016–4.021 s | 89,600 B | 无 |
| recover_80ms | 预算耗尽，输入尚未 End | 2.070–2.076 s | 未追上 | 10.002–10.003 s | 96,000 B | 16,000 采样点，1 s 未确认转录 |
| recover_100ms | 预算耗尽，输入尚未 End | 2.062–2.072 s | 未追上 | 10.002–10.003 s | 99,200–102,400 B | 48,000 采样点，3 s 未确认转录 |

所有故障样本均有 21 次接入尝试，其中 19 次为注入的 HTTP 503，实际使用两个 Worker RPC。明确 RPC 失败到预算启动为约 1.3–3.7 ms，不能将这个数当作网络失联检测能力。

八个正式样本均满足预设覆盖、预算与清理检查：四个完整、四个因未追上而明确中断。全部结果和显式缺口覆盖报告中的采集范围，所有 Gateway 排空，受控 Worker RPC 归零。本轮没有观察到 M4 队列过载或处理期限先结束恢复，也没有触及十五秒客户端缓存上限。失败尾部的缺口表示缺少可靠转录，不声称对应 PCM 全部物理丢失。

## Conclusion / Follow-up

**恢复出字不等于恢复实时性。** 在本组故障、接入等待和 checkpoint 周期下，40 ms 档能在预算内追上；80 ms 档虽继续提交结果且名义处理速率快于实时，仍不能及时补齐历史积压；100 ms 档没有足够的处理余量缩小积压。

用音频秒表示积压 B、用每墙钟秒处理的音频秒数表示速率 v，持续输入为 1 时，理想追赶时间约为 `B / (v - 1)`，再加接入等待与 checkpoint 发布等成本。40 ms 档名义 v=2.5，80 ms 档 v=1.25；后者每秒仅有约 0.25 秒音频的追赶余量。该估算解释观测趋势，不代替实际测量或构成容量公式。十秒预算还包含已经消耗的约两秒接入等待。

此次证据支持保留“追上才结束本轮恢复，持续未追上则按原预算停止”的可见行为，不据此自动延长预算或增加缓存。还不能判断真实 Worker 的安全余量，也不能宣布 M6 完成。下一步将故障换成静默链路阻断，把默认心跳发现失联前新增的音频一并计入，重点检查更大重放积压是否先触发 M4 队列/处理期限；随后补实例退出与并发恢复，涉及策略调整时再讨论最终决策。
