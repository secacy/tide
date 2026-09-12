# EXP-007: 突发接入下的已有会话保护

Date: 2026-09-12
Commit: 919daab23de1cadf2a69ef47b8be2c1001f704a2
Related: [ADR-004](../adr/ADR-004-admission-protection.md), [EXP-006](EXP-006-shared-worker-capacity.md)

## Question

已有六条实时会话时，再突发十次接入，按候选配额立即拒绝是否能保护原有问诊？拒绝是否发生在 Worker RPC 创建之前？短暂处理暂停和会话释放后的再接入是否仍能正确工作？

## Hypothesis

接纳全部新增请求会超过本实验 Worker 的共享能力，可能使原有问诊也处理超时。配额为六时，新请求立即被拒绝，原有问诊可维持正常处理；500 ms 短暂停顿后有机会追赶，而不要求无限增加缓冲。

## Setup

- Client：起点并发接入六条会话，各计划输入 60 秒；第 5 秒再发起十次并发接入，成功接纳的新增会话计划输入 55 秒。每条流每 20 ms 发送 640 字节，持续读取结果；新请求被拒绝后不自动重试，也不发送音频。分别标识 `existing` 和 `incoming`，检查结果序号及归属。
- Worker：复用 EXP-006 的单 gRPC Worker、四个共享槽位、每块 10 ms 模拟处理。槽位按块借用，结果和处理确认发送前释放，等待与处理均可取消。
- 暂停：pause 组在第 10–10.5 秒增加一次共享处理暂停。块取得槽位后若进入该时间窗，把剩余暂停时间加到模拟等待，持有槽位直到恢复；不抢占已经开始的块。记录实际命中次数，不能只配置了暂停就声称故障发生。
- Gateway：两组分别配置 `MaxSessions=16` 和 `6`；除此以外参数一致，沿用 64,000 字节/128 块队列、2 秒结果写入期限、3 秒处理期限和 5 秒 End 期限。拒绝由现有生产升级前登记逻辑执行，没有实现实验专用接入过滤器。
- 传输与环境：Go 1.26.5、darwin/arm64，运行时 12 个逻辑 CPU、GOMAXPROCS=12，CPU 型号与物理内存未取得；同进程、WS/gRPC 均经 64 KiB bufconn，gRPC 流/连接窗口固定为 64 KiB/1 MiB。无真实模型、TCP 或跨机器时间戳。

每批运行四个场景，各一次：`limit16`、`limit6`、`limit16_pause`、`limit6_pause`，顺序执行且关闭 race。共享观测器只记录队列和 Worker 指标；两个客户端群体分别记录结果延迟，避免拒绝样本或幸存流掩盖原有会话的表现。

每轮在全部主负载 handler 退出、注册表归零后，保持同一个 Gateway 接入开放，新增一条五块音频的探针，确认完成且 RPC 创建数只增加一次，然后停止接入并等待排空。探针不计入主负载的音频、延迟与吞吐。

## Metrics

- 原有/新增请求数、HTTP 101 接纳数、HTTP 503 拒绝数、WebSocket 1000 正常完成数和接纳后的失败原因。
- `dial_ms` 从调用 Dial 前到返回，包含本地连接、握手或错误响应；只对拒绝请求汇总拒绝耗时，不宣称这是服务内部判定耗时或线上 SLO。
- 每群体回显 P95、最大值和调度迟到 P99。回显从该客户端发送前的单调时间算到收到结果；仅包含已返回结果，不等价于真实出字。拒绝群体没有音频结果，汇总为 null，而不是零延迟。
- 每请求开始/结束时刻、计划/发送/收到结果块数。失败尾部用“成功发送但未收到结果”的差值报告，不冒充已接纳未确认数量。
- Worker RPC 创建尝试数须等于接纳数；开始突发前还要求恰好六个 RPC 已创建且六个会话仍登记。拒绝不能启动额外 RPC。
- 队列峰值、处理槽位占用和每 200 ms 的会话/处理计数曲线；最终队列、等待者、处理槽位、注册表归零并检查复用探针。

本轮未测 CPU/堆曲线或拒绝风暴的入口资源消耗；EXP-006 的资源数字不能作为本轮测量值。容量控制实验不替代入口连接/请求速率防护。

## Procedure

检出上述 commit，从仓库根目录运行，依赖需已缓存：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
python3 scripts/run_streaming_load.py --experiment admission --count 1 \
  --output /tmp/tide-exp007.jsonl

PYTHONDONTWRITEBYTECODE=1 python3 scripts/summarize_admission_experiment.py \
  /tmp/tide-exp007.jsonl --output /tmp/tide-exp007-summary.json
```

短时正确性检查可用 `--duration-ms 6000 --race`，它会把突发/暂停时刻缩放为第 0.5/1 秒，不能与正式一分钟样本混合。队列观测使用临时 Go overlay，生产源文件不改写；元数据记录源码与 overlay 摘要、版本、命令、环境和退出码，输出不覆盖旧样本。

驱动：[admission_experiment_test.go](../../internal/gateway/admission_experiment_test.go)、[共享能力控制](../../internal/gateway/capacity_experiment_test.go)；[运行器](../../scripts/run_streaming_load.py)、[汇总器](../../scripts/summarize_admission_experiment.py)。

## Results

共执行两批、八次子测试。四次未通过既有的时间连续性规则，保留原始数据并排除耗时统计；其余四次全部采用，各场景恰好一个合格样本，没有按结果好坏筛选。首批 `limit6`、`limit16_pause`、`limit6_pause` 的墙钟跨度与报告耗时相差约 1.853、928.287、9141.654 秒；重跑 `limit16` 相差约 66.756 秒。原因未定位，不能把受影响时段当作连续负载证据。

最终选用首批 `limit16`，以及重跑的 `limit6`、`limit16_pause`、`limit6_pause`。选中与排除清单见质量核验文件。默认汇总仍严格拒绝包含时间缺口的文件；本次增加显式 `--exclude-timing-gaps`，仅按同一 1 秒规则逐个子测试排除，原始文件不改写。复现本次筛选：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/summarize_admission_experiment.py \
  docs/experiments/results/EXP-007-admission-protection.jsonl \
  docs/experiments/results/EXP-007-admission-protection-rerun.jsonl \
  --exclude-timing-gaps --output /tmp/tide-exp007-selected-summary.json
```

| 场景 | 原有会话正常完成 | 原有回显 P95 / 最大 ms | 新请求拒绝 | 新会话接纳后正常完成 | 主负载 RPC 创建数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 上限 16 | 4/6 | 2086 / 2996.736 | 0/10 | 0/10 | 16 |
| 上限 6 | 6/6 | 22 / 37.760 | 10/10 | 无接纳 | 6 |
| 上限 16＋暂停 | 0/6 | 2275 / 2700.087 | 0/10 | 0/10 | 16 |
| 上限 6＋暂停 | 6/6 | 22 / 518.923 | 10/10 | 无接纳 | 6 |

接纳后失败均为 1013 / `processing_timeout`。上限 16 的暂停组中全部会话提前失败，实际主负载只持续约 10.5 秒；不能说它保持了 16 并发一分钟。上限 6 的两组则均完成六条一分钟输入，暂停组确有四个块命中暂停窗口，最大回显延迟约 519 ms；只看全程 P95 22 ms 会漏掉这次短暂停顿。

合格样本共 64 次主负载接入尝试：44 次接纳、20 次 HTTP 503 拒绝；接纳后 16 次正常完成、28 次处理超时。两个保护组的 20 个拒绝耗时为 0.074792–1.025042 ms，仅表示本机内存传输的客户端 Dial 耗时。它们没有音频样本，主负载 RPC 数始终为六，未启动额外识别流。

正常上限 16 组的原有/新增会话分别有 302/1510 块成功发送但未收到结果；暂停组为 905/1507 块。两个上限 6 组原有会话该差值为零。四组每会话队列峰值均仅 640 字节，客户端发送调度迟到 P99 均为 2 ms，再次说明应用队列短不代表下游没有积压。

四个合格样本及四个排除样本的计数、最终清理和独立复用探针均通过，但排除样本不支持性能结论。合格样本的四个探针均正常完成，另计四次接入，不混入主负载的上述 64 次统计。两批均校验了 40 个源码摘要与两个 overlay 摘要；先行全量 race、vet、四场景六秒 race，以及 EXP-006 共享工具回归通过。

数据：[首批原始记录](results/EXP-007-admission-protection.jsonl)、[首批元数据](results/EXP-007-admission-protection.jsonl.meta.json)、[重跑原始记录](results/EXP-007-admission-protection-rerun.jsonl)、[重跑元数据](results/EXP-007-admission-protection-rerun.jsonl.meta.json)、[汇总](results/EXP-007-summary.json)、[质量核验与排除清单](results/EXP-007-data-quality.json)。

## Conclusion

在本轮负载与候选配额下，接入保护使原有六条问诊全部完成，并容忍一次 500 ms 共享处理暂停；代价是十次新增接入全部拒绝。接纳全部新增流的对照出现了原有会话超时，支持 ADR-004 优先保护已接入问诊的选择。既有生产 `MaxSessions` 能完成此单 Gateway、单 Worker 验证，无需新增接入等待或修改转发结构。

这不是“并发提高到 16 仍全部成功”，也不是动态容量管理。固定配额不自动感知 Worker 降速；本轮只有各一个合格样本，不能估计失败概率或证明六是最优配额。运行环境时间中断进一步限制可推广性；没有证据支持真实 ASR、临床时长、异构 Worker 或多 Gateway 的容量结论。

## Follow-up

依据对照结果评估候选配额的适用范围，再进入多 Worker 的名额预留与分配设计。六仅是本实验下的候选参数；异构 Worker、持续降速、多 Gateway 共享同一 Worker，以及更长问诊都需要独立容量证据与协调策略。
