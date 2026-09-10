# EXP-001: Send 停滞时的客户端断开感知

Date: 2026-09-10
Baseline commit: 7c47f95a6d82e62a097c547bc25a083252718ca2 + [相关工作区补丁](results/EXP-001-worktree.patch)；含实验驱动和已有 protobuf 生成文件头变更，见 [基线 SHA-256](results/EXP-001-source.sha256)。
After commit: cb22071224a5a04f69eecfe1032e866f880474bb；[源文件 SHA-256](results/ADR-002-after-source.sha256)。
Related: [ADR-002](../adr/ADR-002-streaming-io-backpressure.md), [EXP-002](EXP-002-slow-websocket-write.md)
Status: Baseline and After measured

## Question

Worker Send 持续阻塞、Recv 不返回结果时，客户端断开能否在没有显式 Abort 的情况下取消 RPC 并释放会话？

## Hypothesis

Baseline 的串行 upload 无法越过阻塞的 Send 去读取断开。拆分 Reader/Sender 后，断开应能取消 RPC；队列满时应明确失败而不是阻塞 Reader。实验分别验证这两种退出来源。

## Setup

Client:
- concurrency: 1
- chunk rate: 不模拟持续速率；合法 Start 后发送一个 2 字节 PCM 块
- session duration: 由事件控制；确认 Send 已进入阻塞后使用 CloseNow 关闭客户端

Worker:
- count: 1 个进程内可控 client/stream 测试替身
- processing latency / mode: Send 与 Recv 等待 RPC Context 取消；不使用真实推理或真实 gRPC 流控窗口

Gateway:
- relevant config: MaxSessions=1，真实 HTTP/WebSocket，经 64 KiB bufconn 内存传输。Baseline 无队列或阶段超时；After 使用 64,000 字节 / 128 块队列和 2 秒结果写入期限
- 满队列扩展：字节限制配置为 2 字节 / 10 块；条目限制配置为 100 字节 / 1 块。第一块停在 Send，第二块排队，第三块触发过载，每种配置重复 5 次

Environment:
- CPU: arm64；具体型号和核心数因当前执行环境限制未取得
- Memory: 因当前执行环境限制未取得
- Go version: go1.26.5 darwin/arm64
- 未启用 race detector 的本地单会话诊断；本实验不报告吞吐或容量
- After 的 EXP-001 与 EXP-002 命令同时启动，共享本机资源；不将微秒级样本差异解释为稳定性能差异

## Metrics

- 客户端 CloseNow 返回后，观察设定为 100 ms 的窗口：是否收到 handler 完成通知、Recv 是否已经观察到 RPC 取消、采样时登记数。若 handler 提前完成则提前采样；调度可能使实际窗口稍长。
- 观察窗实际耗时，仅描述本次采样；100 ms 是观察窗口，不是业务 SLO。
- 最后 Abort 到完成清理的耗时，用于确认实验没有留下会话，不作为系统延迟 benchmark。
- After 新增从调用客户端 CloseNow 前到 Recv 观察取消、测试观察到 handler 完成的时间。两者包含调度耗时；Baseline 没有这两个字段，不能直接计算同比延迟倍数。
- 满队列扩展从第三块发送前计时，记录 RPC 取消、handler 完成、关闭码，以及客户端读取关闭帧前和清理后的登记数。

## Procedure

驱动：[streaming_experiment_test.go](../../internal/gateway/streaming_experiment_test.go)。每个版本各重复 5 次；原负载均先等待 Send 进入，再关闭客户端，观察结束后 Abort 兜底。满队列扩展不主动断开或 Abort，观察过载关闭及清理。默认测试跳过诊断实验。

在仓库根目录运行（重跑输出另存，保留原始基线）：

```sh
TIDE_RUN_EXPERIMENTS=1 GOCACHE=/private/tmp/tide-review-gocache \
GOPROXY=off GOSUMDB=off \
go test -mod=readonly ./internal/gateway \
  -run '^TestExperimentSendStallDisconnect$' -count=5 -timeout=15s -json \
  > /tmp/tide-exp-001-rerun.jsonl
```

离线命令要求依赖已在本地缓存。恢复基线需在独立 checkout 检出 Baseline commit 并应用补丁。After 检出上述 After commit，运行：

```sh
TIDE_RUN_EXPERIMENTS=1 GOCACHE=/private/tmp/tide-review-gocache \
GOPROXY=off GOSUMDB=off \
go test -mod=readonly ./internal/gateway \
  -run '^TestExperiment(SendStallDisconnect|AudioQueueOverload)$' \
  -count=5 -timeout=30s -json > /tmp/tide-exp-001-after-rerun.jsonl
```

## Results

| Metric | Baseline | After |
| --- | --- | --- |
| 观察到 handler 完成的采样次数 | 0/5 | 5/5 |
| Recv 已观察到 RPC 取消的采样次数 | 0/5 | 5/5 |
| 采样时登记数 | 每次 1 | 每次 0 |

| 轮次 | 实际观察时间 ms | 最后 Abort 到清理完成 ms |
| --- | ---: | ---: |
| 1 | 103.478 | 0.575 |
| 2 | 101.047 | 0.123 |
| 3 | 101.062 | 0.135 |
| 4 | 101.050 | 0.158 |
| 5 | 100.134 | 0.119 |

原始记录：[EXP-001-baseline.jsonl](results/EXP-001-baseline.jsonl)，执行时间 2026-09-10 17:17:50–17:17:51 +08:00。上述清理时间只描述这 5 次样本，不推导 p99、稳定延迟或全系统退出上界。测试 pass 仅表示实验执行和最终清理完成，不表示断开感知要求已经满足。

After 原始记录：[EXP-001-after.jsonl](results/EXP-001-after.jsonl)，执行时间 2026-09-10 20:48:28–20:48:29 +08:00。

| After 情境，各 5 次 | RPC 取消观测 ms，min–max | handler 完成观测 ms，min–max | 退出与登记 |
| --- | ---: | ---: | --- |
| Send 停滞后客户端断开 | 0.008708–0.039875 | 0.017208–0.156334 | 5/5 在 Abort 前注销 |
| 音频字节上限触发过载 | 0.009667–0.045959 | 0.027792–0.063625 | 5/5 关闭码 1013；关闭确认前登记 1，清理后 0 |
| 音频条目上限触发过载 | 0.009500–0.018958 | 0.025292–0.038125 | 5/5 关闭码 1013；关闭确认前登记 1，清理后 0 |

后两项是新增负载，没有同配置 Baseline。没有直接采样队列峰值或 heap，不能将配置上限当作内存测量结果。

## Conclusion

Baseline 复现了 Send 与 Recv 同时等待时断开未推动清理；After 在同一断开负载下均完成取消和注销，支持上行解耦的效果。新增满队列负载均明确返回 1013，并在关闭清理完成后归还名额，支持立即过载失败语义。

这是局部故障实验，不能推导真实网络断开的检测上界、静默断网行为、吞吐或稳定容量；100 ms 仍不是业务 SLO。实验也不证明需要下行结果队列。

## Follow-up

- 以真实 gRPC、持续音频和多会话负载测量队列峰值、堆内存与端到端延迟，评估默认容量与过载敏感度。
