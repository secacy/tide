# ADR-006: 固定 Worker 使用最小预留比例分配

Status: Accepted
Date: 2026-09-12
Related: [ADR-005](ADR-005-worker-reservation-lifecycle.md), [EXP-008](../experiments/EXP-008-worker-selection.md)

## Problem

固定 Worker 的会话配额可能不同。轮询按接入次数分配，即使没有超过配额，也可能使小 Worker 持续落后，而大 Worker 仍有处理余量。EXP-008 的异构组出现了这一情况，需要确定当前场景的推荐策略。

## Requirements

- 适用于单 Gateway、固定 Worker 列表和静态正整数配额，利用已有的相对容量信息。
- 延续 ADR-005 的原子预留、满额立即拒绝、会话固定 Worker 和清理后归还语义。
- 选择过程不依赖网络探测或额外动态负载状态；开销适合当前小规模 Worker 池。

## Options

| 方案 | 收益 | 代价 |
| --- | --- | --- |
| RoundRobin：轮询并跳过满员/停止接入者 | 实现简单，空闲时通常很快找到候选 | 不利用配额比例，异构场景可能先压满小 Worker |
| LeastReservedRatio：选择最小预留比例 | 利用已有配额表达相对容量，EXP-008 异构组分配与回显表现更好 | 每次选择扫描 Worker；配额失准或会话消耗差异会降低负载估计的准确性 |

## Decision

将 **LeastReservedRatio（最小预留比例）** 作为当前固定 Worker 场景的推荐策略，配置值为 `least_reserved_ratio`。

仅在允许接入且 `reserved < capacity` 的 Worker 中，选择 `reserved/capacity` 最小者；同比例时从上一次选中位置之后轮换。比较比例、选择及增加预留数在同一个 Pool 临界区内完成。保留现有精确比例比较与线性扫描实现。

保留 RoundRobin 用于显式配置和实验对照。多 Worker 仍要求显式指定策略，缺省配置不会自动切换算法；单 Worker 兼容入口沿用原行为。更新推荐配置示例即可使用已经实现并验证的 LRR 路径。

## Invariants

- 不给满员或停止接入的 Worker 新增预留；没有可选 Worker 时立即拒绝，不等待、不抢占。
- 预留数覆盖准备、运行和清理阶段，保持 `0 <= reserved <= capacity`。
- 已接纳会话不因比例变化迁移；释放顺序及重复释放语义遵循 ADR-005。
- 比例反映本地会话配额占用，不表示远端健康或实时 CPU/GPU 利用率。

## Trade-offs

利用容量差异改善新会话分布，代价是每次选择 O(N) 扫描及共享锁竞争。EXP-008 的 32 Worker、48 个并发调用者微基准中，LRR 为 367.4–378.2 ns/op，RR 为 244.3–247.8 ns/op；这是预留加释放的摊销开销，不是请求延迟或完整会话吞吐。当前证据不足以支持引入更复杂的调度结构。

收益依赖配额合理且单会话资源消耗相近。配额错误、临时性能下降或音频速率差异不会被该策略自动修正，满额时也不能增加总容量。若持续负载暴露比例与实际处理压力脱节，或 Worker 数量增长使选择成为瓶颈，再以实验重新评估容量标定、负载信号或选择算法；本次不引入自动健康检测、动态配额或迁移。

## Validation

[EXP-008](../experiments/EXP-008-worker-selection.md) 在相同输入下比较两种已实现策略：异构配额 2/6、处理槽位 1/3 时，RR 分配 2:2，回显 P95 为 1,487–1,490 ms；LRR 分配 1:3，P95 为 15–16 ms。同容量组均约 14–15 ms。32 个流式会话全部正常结束并完成清理；逻辑重放中两者每轮均接纳 50 次、拒绝 34 次，异构组 LRR 的平均预留比例差更小。

[Pool 测试](../../internal/gateway/worker_pool_test.go) 与[接入测试](../../internal/gateway/worker_admission_test.go) 覆盖分配、并发容量、释放、停止接入和固定路由；相关 race 检查及 vet 已在实现阶段通过。本次确认推荐并记录决策，不改变算法执行路径。

实验配额 2/6 尚未标定为安全容量。六秒 Mock 回显、逻辑重放和微基准支持本次策略选择，不能证明长期稳定容量、真实 ASR 延迟或生产就绪；持续负载及容量余量仍属于 M5 后续验证。
