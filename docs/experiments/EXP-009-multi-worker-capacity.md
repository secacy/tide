# EXP-009: 多 Worker 持续容量与故障余量

Date: 2026-09-12
Commit: `d9f051bb6b27a806ba41b046c1a24af59c23ae67`
Related: [ADR-005](../adr/ADR-005-worker-reservation-lifecycle.md), [ADR-006](../adr/ADR-006-least-reserved-ratio-selection.md), [EXP-006](EXP-006-shared-worker-capacity.md), [EXP-008](EXP-008-worker-selection.md)

## Question / Hypothesis

确定使用最小预留比例后，怎样选择能够持续运行且保有抖动余量的配额？预期接近算术处理上限的配额会持续积压；减少一个名额可能改善暂停后的恢复，但静态配额不能保证持续降速时已有会话仍全部完成。

## Setup

- 单 Gateway，LRR 固定分配；A/B 分别为 1/3 个共享处理槽位，每块模拟 12 ms。每会话输入 640 B/20 ms，即 50 块/秒，客户端持续读取回显。槽位按块借用，不是会话配额。理论槽位处理速率不含调度/协议开销，不能直接换算安全配额。
- Gateway 总上限 64，高于各组主负载及十次突发接入之和，确保突发请求由 Worker 配额拒绝。处理期限 3 秒、End 期限 5 秒，其余生产默认参数保持不变。
- 先将配额 1/3、1/4、1/5 各接满并输入 60 秒，各两轮；以通过正常负载的 1/4 做突发、暂停和降速，各两轮。1/4 暂停后未全部达到恢复观测标准，补测 1/3 周期暂停两轮、突发/降速各一轮，再将 1/3 正常输入延长至 300 秒一轮。
- 突发：第 5 秒并发尝试十次新接入；暂停：B 从第 10 秒起每十秒暂停 500 ms；降速：B 从第 20 秒起将每块处理时间增至 24 ms。故障注入不修改调度或自动停止接入，A 保持原处理速度。
- 暂停只延长窗口内取得槽位的块，已在处理的块不被追溯暂停。时序在尾部排空期继续生效，少量尾部块可能命中第 60 秒暂停；恢复统计针对输入期间第 10/20/30/40/50 秒的五个窗口，命中块数不能直接当作暂停次数。
- WS、gRPC 均经 bufconn，同进程运行客户端、Gateway、两个实验 Worker。Go 1.26.5、macOS 26.6.2 arm64、12 逻辑 CPU；正式采样关闭 race，负载顺序执行。源码及 overlay 摘要保存在各批元数据；既有命令行客户端及 interview 改动不参与实验。

每轮主负载完全清理后重新接满两个 Worker，各发送五块并验证完整结果，探针单独计数。降速仍会作用于探针；短探针成功只证明名额和链路可复用，不证明处理能力恢复。

## Metrics / Acceptance

- 正常组要求全部已接纳会话完整结束，各 Worker 每十秒窗口的回显 P95 ≤ 1 秒。窗口按结果到达时间划分，包含尾部窗口；没有结果不伪造零延迟，另报每个失败会话的时刻、原因和尾部数量。
- 每 200 ms 采样各 Worker 的成功发送/结果数、处理计数、活动 RPC、预留以及 Gateway 登记、队列、堆和 goroutine。成功发送减结果数用于观察全链路未回显音频，包含在途、协议缓冲和处理等待，不等于 Gateway 已接纳但未确认的音频；计数是近同时采样的估计；失败后还包含不会再返回结果的尾部，不能将全部差值归因于存活会话积压。
- 暂停后恢复的操作性定义在汇总工具采样前固定：B 的未回显数降至每会话至多两块，并在连续采样中保持至少一秒，且原有 RPC 全部仍在。报告首次达标时刻相对暂停结束的间隔，以及随后确认时刻；分辨率约 200 ms，须在下一次暂停前确认。此阈值是实验观测口径，不是新增生产期限。
- 突发组要求十次 HTTP 503、不增加 RPC，原有会话完整结束。持续降速组报告实际失败及对 A 的影响，不要求 B 全部完成。所有组检查登记、预留、队列引用、活动 RPC 和处理/等待槽位归零，并验证复用。
- CPU 为整个测试进程的 CPU 时间差除以主负载窗口；堆峰值为定时采样。客户端、观测器和 Mock 均计入，不能归因于 Gateway 或真实 ASR。失败后降载会影响资源均值。

## Procedure / Evidence

从仓库根目录运行，依赖已缓存；重跑需另选输出路径，工具拒绝覆盖既有样本：

```bash
export GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off PYTHONDONTWRITEBYTECODE=1
python3 scripts/run_streaming_load.py --experiment multi --case 'normal_1_(3|4|5)' --count 2 --output docs/experiments/results/EXP-009-capacity.jsonl
python3 scripts/run_streaming_load.py --experiment multi --case '(burst|jitter|slow)_1_4' --worker-b-capacity 4 --count 2 --output docs/experiments/results/EXP-009-faults.jsonl
python3 scripts/run_streaming_load.py --experiment multi --case jitter_1_3 --worker-b-capacity 3 --count 2 --output docs/experiments/results/EXP-009-jitter-margin.jsonl
python3 scripts/run_streaming_load.py --experiment multi --case '(burst|slow)_1_3' --worker-b-capacity 3 --output docs/experiments/results/EXP-009-conservative-faults.jsonl
python3 scripts/run_streaming_load.py --experiment multi --case normal_1_3 --duration-ms 300000 --output docs/experiments/results/EXP-009-sustained.jsonl
python3 scripts/summarize_multi_capacity.py \
  docs/experiments/results/EXP-009-capacity.jsonl \
  docs/experiments/results/EXP-009-faults.jsonl \
  docs/experiments/results/EXP-009-jitter-margin.jsonl \
  docs/experiments/results/EXP-009-conservative-faults.jsonl \
  docs/experiments/results/EXP-009-sustained.jsonl \
  --output docs/experiments/results/EXP-009-summary.json
```

驱动：[multi_capacity_experiment_test.go](../../internal/gateway/multi_capacity_experiment_test.go)；[汇总与校验工具](../../scripts/summarize_multi_capacity.py)。保留[容量组](results/EXP-009-capacity.jsonl)、[1/4 故障组](results/EXP-009-faults.jsonl)、[1/3 暂停组](results/EXP-009-jitter-margin.jsonl)、[1/3 突发/降速组](results/EXP-009-conservative-faults.jsonl)、[五分钟组](results/EXP-009-sustained.jsonl)及同名 `.meta.json`。最终[汇总](results/EXP-009-summary.json)含逐样本来源、哈希、质量检查和排除清单。

## Results

一分钟正常组各两轮：

| 配额 A/B | 正常完成 | A 最高窗口 P95 | B 最高窗口 P95 | B 失败 |
| --- | --- | ---: | ---: | --- |
| 1/3 | 4/4、4/4 | 14–15 ms | 15 ms | 无 |
| 1/4 | 5/5、5/5 | 14–15 ms | 27 ms | 无 |
| 1/5 | 5/6、5/6 | 15–16 ms | 2,852–2,875 ms | 各一次 processing_timeout |

1/5 两轮分别约在第 49.321、43.141 秒失败，各有 151 块已成功发送但未收到对应结果。B 的未回显量明显增长，失败后降载；不能将之后四条 B 会话的完成解释成五条稳定。1/4 的 B 在第 1–11 秒与末十秒的平均未回显量分别约 2.8→3.28、3.24→3.0 块，没有观察到同类增长。

| 负载与配额 | 正常完成 | B 最高窗口 P95 | 补充观察 |
| --- | --- | ---: | --- |
| 突发，1/4，两轮 | 5/5、5/5 | 26–27 ms | 每轮十次拒绝，主负载仍五个 RPC |
| 周期暂停，1/4，两轮 | 5/5、5/5 | 633–634 ms | 十次暂停中七次确认恢复，三次未确认 |
| 持续降速，1/4，两轮 | 1/5、1/5 | 2,920–2,923 ms | B 八个会话全部超时，A 均完成 |
| 周期暂停，1/3，两轮 | 4/4、4/4 | 331–333 ms | 十次暂停全部确认恢复 |
| 突发，1/3，一轮 | 4/4 | 15 ms | 十次拒绝，主负载仍四个 RPC |
| 持续降速，1/3，一轮 | 1/4 | 2,950 ms | B 三个会话全部超时，A 完成 |

1/4 暂停组的最大回显延迟为 633.36–736.31 ms；已确认的恢复间隔为暂停结束后 2.5–5.9 秒，另加至少一秒采样确认。未确认出现在第一轮第 50 秒、第二轮第 10/50 秒。第一轮最后一次暂停后，后段未回显量约在 7–11 块波动，未连续满足八块阈值。这不证明积压无限增长，也不否定该组完成率和 P95 已合格；它限制了本轮对恢复余量的结论，不能事后放宽阈值将样本改记为通过。

1/3 暂停组约在暂停结束后 0.900–0.901 秒首次进入阈值，随后经至少一秒采样确认，均早于下一次暂停；最大回显约 514 ms。持续降速组则说明即使降低配额，原有会话也可能失败；接入保护不等于处理能力降级保护。

五分钟正常组 1/3 四个会话全部完成，共发送并收到 60,000 块回显。A/B 全程 P95 均为 14 ms、最高十秒窗口 P95 均为 15 ms，最大回显分别为 35.09/35.37 ms；客户端发送迟到 P99 均为 2 ms。B 第 1–11 秒与末十秒的平均未回显量约 2.12→1.37 块，采样峰值三块，未观察到持续积压。

资源方面，一分钟组测试进程平均 CPU 约 0.057–0.092 核，采样堆峰值最高 27.31 MB，主负载后 GC 堆约 11.37–11.96 MB；五分钟组平均 CPU 约 0.066 核，堆由初始 9.97 MB 到采样峰值 26.90 MB，主负载后 GC 为 14.51 MB。五分钟组在第 10–290 秒采样的 goroutine 一直为 54，结束后为 22。初始值在首次 RPC 建立前采集，共享 gRPC 及 HTTP 空闲连接随后仍存活；不能要求进程 goroutine 回到零。五分钟比一分钟保留了更多时序样本及窗口摘要，GC 后堆差不能直接当作泄漏量。这里的瓶颈是注入的共享处理能力，低 CPU 不表示真实模型也有同样余量。

合计 17 轮、80 个主负载会话：67 个正常完成、13 个处理超时；另有 30 次升级前 HTTP 503，耗时 0.327–2.731 ms，仅代表本机内存传输。主负载共成功发送 270,246 块、收到 268,283 个对应结果，相差 1,963 块，保留为失败尾部；这些块不保证全部已被 Gateway 接纳。另计 80 个复用探针会话，均正常完成，不混入主负载延迟。每会话队列峰值为 640–1,280 B，全部最终登记、预留、活动 RPC、队列引用和处理等待归零。

五批源码摘要与所记提交一致，17 个正式样本通过记账、清理和时间连续性检查，未排除任何样本，包括超时组及未确认恢复的窗口。沿用子测试墙钟跨度与 Go elapsed 相差超过一秒则不采纳的规则。Go 测试通过表示驱动及协议/清理检查通过，不表示该组容量满足验收。全量 race 回归、六场景六秒 race、故障时序测试、旧共享容量工具回归及带实验标签的 vet 通过；短时 race 压缩故障间隔以覆盖代码路径，没有作为容量样本。

## Conclusion / Boundaries

正常负载下 1/4 比 1/3 多接纳一个会话，两轮一分钟均达标；1/5 不满足验收。1/3 在本轮周期暂停的恢复观测上更稳定，并通过一次五分钟正常负载，代价是总名额从五降至四。因此建议在这些模拟条件下以 1/3 作为更保守的实验配额；1/4 保留为正常负载下的较高利用率候选。该建议尚待确认，没有修改生产配置或形成新的容量 ADR，也没有证明最大稳定容量。

静态配额和 LRR 不感知持续降速，也不迁移已有会话。失败发生在 B 时，A 在本组实验中均正常完成；这是本机双 Worker 实验的观测，不能推广成任意共享 CPU/GPU 或网络故障下的隔离保证。

恢复指标包含传输和结果路径，不能直接定位 Worker 内部队列；采样相位与在途数据也会影响低水位判断。资源采样包含实验观测开销、按时间保留的样本及共享空闲连接，不用于证明无内存泄漏或生产单会话资源上界。真实模型、独立进程 TCP、临床时长与更广故障仍需后续验证。
