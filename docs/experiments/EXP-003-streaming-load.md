# EXP-003: 持续音频与多会话负载

Date: 2026-09-11
Commit: 6c5e798c1596733b75a723e5a35d936a2d3e32cd
Related: [ADR-002](../adr/ADR-002-streaming-io-backpressure.md), [EXP-001](EXP-001-send-stall-disconnect.md), [EXP-002](EXP-002-slow-websocket-write.md)

## Question

持续输入及并发会话下，当前队列是否积压、进程堆如何变化、结果延迟如何变化？Worker 短暂抖动与持续处理不足，是否表现为不同的积压和退出行为？

## Hypothesis

- Worker 持续处理快于输入时，队列不会持续增长，会话能正常完成。
- 短暂抖动能被链路缓冲吸收，但缓冲可能位于 gRPC 内部；应用队列较短不代表端到端延迟较低。
- Worker 持续处理慢于输入时，最终会触发有界队列过载；触发之前结果延迟可能已经显著增加。

## Setup

Client：每会话每 20 ms 发送 640 字节，即 32,000 字节/秒。使用合成 PCM，其中编码客户端标识、序号和单调时间戳，Worker 回显用于检查归属、顺序和计算延迟。客户端持续读取结果。各客户端按同一时间基准启动，迟到时会补发，另记录调度迟到量。

| 场景 | 会话数 | 输入时长 | 每块模拟处理时间 | 重复次数 |
| --- | ---: | ---: | --- | ---: |
| normal_1 | 1 | 8 秒 | 5 ms | 3 |
| normal_8 | 8 | 8 秒 | 5 ms | 3 |
| normal_32 | 32 | 8 秒 | 5 ms | 3 |
| jitter_8 | 8 | 12 秒 | 通常 5 ms，每 100 块中的第 51 块等待 500 ms | 3 |
| slow_8 | 8 | 最多 20 秒，过载可提前结束 | 50 ms | 3 |
| sustained_8 | 8 | 60 秒 | 5 ms | 1 |

Gateway：生产逻辑和默认参数保持不变，音频队列 64,000 字节 / 128 块，结果写入期限 2 秒，接入上限等于场景会话数。

Worker：一个真实 gRPC 服务，通过单个共享 ClientConn 接入；各 stream 独立等待模拟耗时并逐块返回结果，没有共享模型计算池或 CPU/GPU 争用模型。流接收窗口固定为 64 KiB，连接窗口固定为 1 MiB；上下行均显式配置，避免动态调窗改变条件。

Environment：Go 1.26.5，darwin/arm64；运行时报告 12 个逻辑 CPU，GOMAXPROCS=12。具体 CPU 型号和物理内存未取得。客户端、Gateway、Worker 位于同一测试进程，WebSocket 和 gRPC 均使用 64 KiB bufconn 内存传输。正式采样关闭 race detector，各组命令顺序运行。

观测方式：脚本利用 Go `-overlay` 在临时构建副本中注入入队、出队和释放 hook，原生产文件不改写。hook 使用原队列锁及实验指标锁，额外记录槽位时间；会话结束后移除队列引用。直方图固定内存，避免逐条保存延迟样本。观测有 CPU、锁和内存开销，本轮没有测量其零开销对照。

## Metrics

- `chunk_result_latency`：客户端准备某块数据、调用 WebSocket Write 前，到读到对应结果的时间，包含 gRPC 缓冲及模拟处理；不是实际音频采集到真实 ASR 出字的延迟。仅统计已返回结果，失败后未返回的尾部不进入直方图。
- `queue_wait`：成功入队至 Sender 取出的等待，按所有会话已出队音频聚合；异常退出时残留音频单独计数。
- `grpc_send_wait`：真实 gRPC Send 调用耗时，包括因取消而失败的调用；不是模型处理时间。
- `client_schedule_lateness`：实际准备音频相对于计划时间的迟到量，用于检查负载发生器是否跟上节奏。
- 队列峰值：hook 在每次变化时记录单会话和全会话峰值，不由低频采样推算；包含瞬时入队的一个块。
- 进程堆及 goroutine：建立空闲连接后 GC 作为 idle 基线，每 200 ms 采样，会话及 Worker handler 退出后再次 GC。包括客户端、gRPC、实验观测器与测试框架；不是 RSS、Gateway 独占内存或泄漏证明。后置采样时共享服务与连接设施尚未销毁。
- 正常完成数、关闭码、已发送/入队/Worker 接收/结果返回数量及最终登记数。

每轮百分位按该轮所有已观测块汇总，使用 1 ms 向上取整的固定直方图，最大值保留原始精度；超过 60 秒另计 overflow。不同轮次的百分位报告范围，不把它们平均成合并百分位。

数据连续性复核发现：最初 normal_1、normal_8 的第二轮，日志墙钟跨度分别比测试报告耗时多约 967.443 秒、898.859 秒；最初 sustained_8 多约 5.337 秒。原因可能为运行环境暂停或时钟变化，未定位。为避免将这些记录当作连续运行证据，本轮采用“子测试墙钟跨度与报告耗时相差超过 1 秒则标记”的质量检查，保留原组记录并整组补跑。最终对照使用补跑组，未按延迟高低筛选单个样本；[质量报告](results/EXP-003-data-quality.json) 列出选择依据。

## Procedure

检出上述 commit。驱动：[streaming_load_experiment_test.go](../../internal/gateway/streaming_load_experiment_test.go)；运行器：[run_streaming_load.py](../../scripts/run_streaming_load.py)。例如重跑 32 会话组：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
python3 scripts/run_streaming_load.py --case normal_32 --count 3 \
  --output /tmp/tide-exp003-normal32.jsonl
```

其余场景替换 `--case`；sustained_8 使用 `--count 1`。依赖需已缓存。脚本拒绝覆盖已有输出；相邻 `.meta.json` 保存源码摘要、overlay 摘要、版本、命令和环境。Go JSON 会拆分长日志行，使用 [summarize_streaming_load.py](../../scripts/summarize_streaming_load.py) 重组后提取记录。

汇总器的 `--strict-timing` 检查时间连续性；它能识别上述原组问题，全部最终选用的组通过检查。汇总器以本实验文档所在版本为准；被测生产代码与观测驱动均为上述 commit。

驱动检查：全场景 200 ms 输入的 race smoke，以及完整 slow_8 race 检查通过；常规全量 race 和 vet 通过。上述检查不进入正式样本。

## Results

本轮验证当前实现，没有参数优化后的 After。最终使用 16 轮样本，共 179 个会话；155 个正常完成，24 个因持续慢 Worker 过载退出。所有轮次最终登记数和观测器保留队列数均归零，无结果归属或顺序校验错误。

| 场景 | 正常完成 / 会话数 | 结果延迟 P95 ms，轮次范围 | 结果最大延迟 ms，所有轮次最大值 | 单会话队列峰值字节，轮次范围 |
| --- | ---: | ---: | ---: | ---: |
| normal_1 | 3/3 | 8 | 13.174 | 640 |
| normal_8 | 24/24 | 8 | 11.545 | 640 |
| normal_32 | 96/96 | 7 | 9.881 | 640 |
| jitter_8 | 24/24 | 432–434 | 502.354 | 640 |
| slow_8 | 0/24，均返回 1013 | 5358–5380 | 5639.541 | 64,000 |
| sustained_8 | 8/8 | 8 | 58.859 | 2560 |

正常组只在有限负载下观察到接近的延迟，不能由 1 ms 差异推导更高并发反而更快。sustained_8 保留了最大 58.859 ms 的结果延迟和最大 61.805 ms 的客户端调度迟到；其时间连续性检查通过。

慢 Worker 每轮约 9.201–9.202 秒触发退出，单会话队列达到 100 块 / 64,000 字节，先触及字节上限。队列等待 P95 为 918–930 ms，最大约 1287.141 ms；gRPC Send 最大阻塞约 1307.198 ms，但 Send P99 仍为 1 ms，说明大部分发送快速返回，少数长等待不能仅由 P99 看出。

每轮慢 Worker 组中，客户端成功 Write 了 3688 块，Gateway 入队 3680 块、取出 2880 块，Worker 接收 1440 块，返回并被客户端观察到 1428–1432 个结果；退出时队列残留 800 块。这些计数明确反映了失败尾部，延迟直方图未覆盖未返回结果。取出数量与 Worker 接收数量的差异包含正在发送及传输缓冲中的音频，不能直接换算为 gRPC 实际堆内存。

| 场景 | 进程 idle 堆 MiB，轮次范围 | 采样峰值 MiB，轮次范围 | 会话结束后 GC 堆 MiB，轮次范围 |
| --- | ---: | ---: | ---: |
| normal_1 | 2.459–2.773 | 4.492–5.048 | 2.843–2.953 |
| normal_8 | 3.543–3.885 | 7.509–8.115 | 3.711–4.049 |
| normal_32 | 7.225–7.735 | 16.173–16.802 | 7.450–7.657 |
| jitter_8 | 3.554–3.809 | 8.183–8.291 | 3.990–4.055 |
| slow_8 | 3.543–3.874 | 9.984–10.149 | 3.924–4.008 |
| sustained_8 | 3.542 | 7.949 | 4.031 |

sustained_8 的六个连续 10 秒窗口，活跃会话期间采样堆范围依次为 3.557–7.887、4.272–7.594、4.259–7.804、4.276–7.949、4.368–7.757、4.312–7.839 MiB；本次一分钟内没有观察到持续单向增长。该轮 goroutine 从 idle 的 16 个到采样峰值 77 个，会话结束后为 13 个。共享服务设施仍在运行，这些结果不能作为长期无泄漏证明。

原始数据与复现元数据：

| 场景 | 数据 | 元数据 |
| --- | --- | --- |
| normal_1 | [补跑数据](results/EXP-003-normal-1-rerun.jsonl) | [metadata](results/EXP-003-normal-1-rerun.jsonl.meta.json) |
| normal_8 | [补跑数据](results/EXP-003-normal-8-rerun.jsonl) | [metadata](results/EXP-003-normal-8-rerun.jsonl.meta.json) |
| normal_32 | [数据](results/EXP-003-normal-32.jsonl) | [metadata](results/EXP-003-normal-32.jsonl.meta.json) |
| jitter_8 | [数据](results/EXP-003-jitter-8.jsonl) | [metadata](results/EXP-003-jitter-8.jsonl.meta.json) |
| slow_8 | [数据](results/EXP-003-slow-8.jsonl) | [metadata](results/EXP-003-slow-8.jsonl.meta.json) |
| sustained_8 | [补跑数据](results/EXP-003-sustained-8-rerun.jsonl) | [metadata](results/EXP-003-sustained-8-rerun.jsonl.meta.json) |

[机器可读汇总](results/EXP-003-summary.json)。存在时间连续性问题的原组数据仍保留：[normal_1](results/EXP-003-normal-1.jsonl)、[normal_8](results/EXP-003-normal-8.jsonl)、[sustained_8](results/EXP-003-sustained-8.jsonl)，各自的相邻 metadata 同样保留。

## Conclusion

三项假设在当前观测范围内得到支持。正常输入和短暂抖动均正常结束；持续慢 Worker 最终触发有界过载失败。进程堆在一分钟组内呈区间波动，清理后登记和观测队列归零。

最重要的限制是：应用队列上界不等于整条链路延迟上界。jitter_8 的应用队列仅一个块，结果 P95 已超过 400 ms；slow_8 在过载前已返回结果的最大延迟超过 5 秒。gRPC 缓冲和 Worker 处理时间都在队列指标之外。

实验支持当前实现的有界积压及退出行为，不能证明默认参数最优、32 会话是稳定容量，或能支撑数十分钟临床问诊。未包含真实网络慢读、真实 ASR 推理或共享计算池瓶颈。

## Follow-up

- 先明确可接受的实时结果延迟与失败尾部语义，再比较等待时间限制、发送期限或容量调整；不根据队列峰值单独调参。
- 用有共享处理能力上限的 Worker、真实 TCP 慢读和临床量级会话时长，继续验证稳定容量与资源趋势。
- 本轮保持队列容量、写入期限和过载语义不变；新的设计选择先在对话中讨论确认，再形成 ADR。
