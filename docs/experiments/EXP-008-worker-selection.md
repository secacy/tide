# EXP-008: 固定 Worker 预留与两种分配策略

Date: 2026-09-12
Commit: `e92c1288afad46f0f6da05b3375d851a80a94709`
Related: [ADR-005](../adr/ADR-005-worker-reservation-lifecycle.md), [EXP-006](EXP-006-shared-worker-capacity.md), [EXP-007](EXP-007-admission-protection.md)

## Question / Hypothesis

升级前原子预留、收尾后归还是已确认的生命周期决策。本实验比较两种候选选择算法：轮询跳过满员/停止接入者（RR），最小 `reserved/capacity`、同比例轮换（LRR）。

假设：同配额时差异有限；配额与处理能力不同且能表达相对容量时，LRR 可减少小 Worker 先受压的情况。两种策略都不会增加总名额；LRR 需要扫描候选，其并发成本可能随 Worker 数量增长。算法的最终决策另见 [ADR-006](../adr/ADR-006-least-reserved-ratio-selection.md)。

## Setup / Metrics

- **逻辑重放**：直接调用生产 `WorkerPool`，200 个虚拟 tick；初始四次接入，tick 20 后每三个 tick 接入一次，60/120 各额外突发十次。租期为 10–49 tick，PCG 固定种子 17/29，对两种策略使用相同到达、租期序列，先释放再接入。配额分别为 `[4,4]`、`[2,6]`。记录每次分配/拒绝、初始分布和每 tick 最大与最小预留比例之差的均值。虚拟 tick 不是实际时间。
- **流式负载**：真实 Gateway WebSocket 与两个独立 gRPC 实验 Worker，均通过 bufconn；四个客户端依次接入后，同时各输入 6 秒 PCM，640 B/20 ms，共 1,200 块/轮。每块在 Worker 独立共享槽位中模拟 12 ms，再返回累计处理进度及回显。相同容量组配额 `[4,4]`、槽位 `[2,2]`；异构组配额 `[2,6]`、槽位 `[1,3]`。Gateway 总上限 8，其余使用生产默认期限和队列容量。
- **微基准**：生产 `TryAcquire + Release`，Worker 数 2/8/32，单线程及 `4×GOMAXPROCS=48` 个并发调用者，每个子项 100,000 次、重复两轮。配额足够，不含拒绝分支、网络或长会话。`ns/op` 是批次时间摊销，不是并发请求延迟分位数。
- 环境：Go 1.26.5，macOS 26.6.2 arm64，12 逻辑 CPU，无 race 的正式采样。机器总内存未采集，本次不报告内存容量。每个逻辑与流式场景重复两轮；逻辑重复用于核对确定性，不是独立随机样本。

异构组有意使小 Worker 同时承担两条流时超过处理速度：输入约 100 块/秒，单槽位模拟处理上限约 83 块/秒，实际还含调度和协议开销。大 Worker 此时仍有余量。这用于暴露按会话数量均分的问题；2/6 不是已标定的安全配额，也不应直接部署。两种候选使用完全相同的能力和配额，实验只改变选择策略。

回显延迟从客户端实际发送时间戳到读取对应结果统计，另记客户端计划发送迟到、应用队列和 Worker 处理计数。固定使用 RR 后 LRR 的运行顺序，重复次数有限，不报告统计显著性。

## Procedure / Evidence

```bash
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/run_streaming_load.py \
  --experiment selection --count 2 \
  --output docs/experiments/results/EXP-008-worker-selection.jsonl

PYTHONDONTWRITEBYTECODE=1 python3 scripts/summarize_worker_selection.py \
  docs/experiments/results/EXP-008-worker-selection.jsonl \
  --output docs/experiments/results/EXP-008-summary.json
```

重跑需另选输出路径，工具不会覆盖已有样本。[原始 Go JSON](results/EXP-008-worker-selection.jsonl)、[版本/源码及 overlay 哈希/环境/命令](results/EXP-008-worker-selection.jsonl.meta.json)、[汇总与逐样本质量记录](results/EXP-008-summary.json) 均保留。

源码哈希全部与上述提交一致。16 个场景样本及 24 个微基准样本通过记账、清理及时间连续性检查，无排除。沿用 wall-clock 与 Go elapsed 差值绝对值超过 1 秒的排除规则；微基准单独比较输出时间跨度与 `N × ns/op`，不从测试总耗时推导业务延迟。400 ms 的 smoke/race 检查只验证工具和并发正确性，未混入正式结果。

## Results

逻辑重放每一轮均为 84 次尝试、50 次接纳、34 次拒绝；满额判断和总接纳量相同。

| 配额 | 初始分布 RR / LRR | 平均预留比例差 RR | LRR |
| --- | --- | ---: | ---: |
| 4 / 4 | 2:2 / 2:2 | 0.1125 | 0.1125 |
| 2 / 6 | 2:2 / 1:3 | 0.1917 | 0.1217 |

流式数据以下为两轮范围，各策略/容量组合均为 8/8 个会话正常完成：

| 配额与槽位 | 策略 | Worker 会话分布 | 回显 P95 | 最大回显延迟 |
| --- | --- | --- | ---: | ---: |
| 4/4；2/2 | RR | 2:2 | 15 ms | 15.98–21.99 ms |
| 4/4；2/2 | LRR | 2:2 | 14–15 ms | 16.40–28.36 ms |
| 2/6；1/3 | RR | 2:2 | 1,487–1,490 ms | 1,660.74–1,663.12 ms |
| 2/6；1/3 | LRR | 1:3 | 15–16 ms | 20.65–22.14 ms |

总计 32 个流式会话、9,600 块音频及对应结果，无丢失或串流，全部正常关闭。客户端发送迟到 P99 为 2–4 ms，单会话应用队列峰值均为 640 B。各轮 Gateway 登记、Worker 预留、活动 RPC、处理/等待槽位及队列观察引用最终全部归零。RR 异构组虽在六秒输入后完整排空，回显已显著落后；这再次表明队列很短和最终成功都不能证明实时性。

微基准每次预留及释放均为 32 B、1 次分配，下面是两轮的摊销时间：

| Worker 数 | 调用方式 | RR ns/op | LRR ns/op |
| --- | --- | ---: | ---: |
| 2 | 串行 | 84.13–99.05 | 45.70–47.91 |
| 8 | 串行 | 49.66–49.79 | 45.76–47.83 |
| 32 | 串行 | 48.02–48.06 | 91.89–91.99 |
| 2 | 48 个并发调用者 | 226.5–282.8 | 254.5–259.0 |
| 8 | 48 个并发调用者 | 232.2–240.0 | 275.1–275.7 |
| 32 | 48 个并发调用者 | 244.3–247.8 | 367.4–378.2 |

小规模串行结果存在排序/预热等噪声，不能据此声称某策略总是更快。32 Worker 时 LRR 的扫描成本可见；目前没有由此引入复杂调度结构的证据。此项仅覆盖短临界区的竞争，不证明每秒能接纳相同数量的完整会话。

## Conclusion / Follow-up

当前证据支持在固定、异构配额下进一步考虑 LRR：它使用已有的容量信息，使本负载避免了小 Worker 受压、大 Worker 留有余量的分配。它没有增加可接纳总数，也不自动获知 CPU/GPU 负载、当前推理速度或各会话实际音频速率。

该收益依赖配额能表达相对能力；配置错误、Worker 临时变慢或每会话消耗差异较大时，预留比例不是准确负载估计。两个策略都不会迁移已接入会话，配额满时都会拒绝新会话，不能保证动态故障下的延迟。

本轮完成了生命周期机制和候选比较；后续 [ADR-006](../adr/ADR-006-least-reserved-ratio-selection.md) 基于这些证据确认最小预留比例为当前推荐策略。长期容量仍需延续 EXP-006/007 的持续负载、接入保护和抖动余量验证；六秒合成回显、逻辑重放与预留微基准不能替代真实 ASR、TCP 部署或临床时长容量验证。
