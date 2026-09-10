# EXP-001: Send 停滞时的客户端断开感知

Date: 2026-09-10
Commit: 7c47f95a6d82e62a097c547bc25a083252718ca2 + [相关工作区补丁](results/EXP-001-worktree.patch)；含新实验驱动和已有 protobuf 生成文件头变更。Gateway 生产实现未修改，相关源文件见 [SHA-256 清单](results/EXP-001-source.sha256)。
Related: [ADR-002](../adr/ADR-002-streaming-io-backpressure.md), [EXP-002](EXP-002-slow-websocket-write.md)
Status: Baseline measured; After not measured

## Question

Worker Send 持续阻塞、Recv 不返回结果时，客户端断开能否在没有显式 Abort 的情况下取消 RPC 并释放会话？

## Hypothesis

当前 upload 无法越过阻塞的 Send 去读取断开；download 也没有结果可写，因此客户端关闭后，观察窗口内会话可能仍在登记，RPC 仍未取消。此处是待验证假设，不是生产环境发生率或无限等待的证明。

## Setup

Client:
- concurrency: 1
- chunk rate: 不模拟持续速率；合法 Start 后发送一个 2 字节 PCM 块
- session duration: 由事件控制；确认 Send 已进入阻塞后使用 CloseNow 关闭客户端

Worker:
- count: 1 个进程内可控 client/stream 测试替身
- processing latency / mode: Send 与 Recv 等待 RPC Context 取消；不使用真实推理或真实 gRPC 流控窗口

Gateway:
- relevant config: MaxSessions=1，真实 HTTP/WebSocket，经 64 KiB bufconn 内存传输；无新的队列或阶段超时

Environment:
- CPU: arm64；具体型号和核心数因当前执行环境限制未取得
- Memory: 因当前执行环境限制未取得
- Go version: go1.26.5 darwin/arm64
- 未启用 race detector 的本地单会话诊断；本实验不报告吞吐或容量

## Metrics

- 客户端 CloseNow 返回后，观察设定为 100 ms 的窗口：是否收到 handler 完成通知、Recv 是否已经观察到 RPC 取消、采样时登记数。若 handler 提前完成则提前采样；调度可能使实际窗口稍长。
- 观察窗实际耗时，仅描述本次采样；100 ms 是观察窗口，不是业务 SLO。
- 最后 Abort 到完成清理的耗时，用于确认实验没有留下会话，不作为系统延迟 benchmark。

## Procedure

驱动：[streaming_experiment_test.go](../../internal/gateway/streaming_experiment_test.go)。重复 5 次，不修改生产实现；每轮先等待 Send 确实进入，再关闭客户端。观察结束后显式 Abort 并等待清理。默认测试跳过此诊断实验；它记录行为，不断言缺陷必须出现，后续实现仍可使用同一负载比较。

在仓库根目录运行（重跑输出另存，保留原始基线）：

```sh
TIDE_RUN_EXPERIMENTS=1 GOCACHE=/private/tmp/tide-review-gocache \
GOPROXY=off GOSUMDB=off \
go test -mod=readonly ./internal/gateway \
  -run '^TestExperimentSendStallDisconnect$' -count=5 -timeout=15s -json \
  > /tmp/tide-exp-001-rerun.jsonl
```

离线命令要求依赖已在本地缓存。若需恢复基线，在独立 checkout 检出上述 Commit 并应用工作区补丁；本次没有提交或覆盖用户的其他工作。

## Results

| Metric | Baseline | After |
| --- | --- | --- |
| 观察到 handler 完成的采样次数 | 0/5 | 未运行 |
| Recv 已观察到 RPC 取消的采样次数 | 0/5 | 未运行 |
| 采样时登记数 | 每次 1 | 未运行 |

| 轮次 | 实际观察时间 ms | 最后 Abort 到清理完成 ms |
| --- | ---: | ---: |
| 1 | 103.478 | 0.575 |
| 2 | 101.047 | 0.123 |
| 3 | 101.062 | 0.135 |
| 4 | 101.050 | 0.158 |
| 5 | 100.134 | 0.119 |

原始记录：[EXP-001-baseline.jsonl](results/EXP-001-baseline.jsonl)，执行时间 2026-09-10 17:17:50–17:17:51 +08:00。上述清理时间只描述这 5 次样本，不推导 p99、稳定延迟或全系统退出上界。测试 pass 仅表示实验执行和最终清理完成，不表示断开感知要求已经满足。

## Conclusion

假设在本实验的观察范围内得到支持：Send 与 Recv 持续等待时，客户端关闭后没有在窗口内触发 handler 完成，登记名额仍被占用；显式 Abort 后可以完成清理。结合 upload 的串行执行顺序，根因解释是读取客户端断开的路径被 Send 阻塞，而下行也没有新的写入去观察连接错误。

这是一项局部 failure case 复现，不能证明“永远不退出”、真实 gRPC 流控的发生率、吞吐、稳定容量或某个候选方案一定更优；100 ms 也不是已接受的业务 SLO。没有 After 数据，更没有证明必须采用四个 I/O loop。

## Follow-up

- 结合 EXP-002 评估 I/O 解耦方案；候选实现完成后用相同负载补充 After，覆盖音频队列满的情境。新增 cancellation 时间指标与会话注销时间分开记录。
- 如需性能结论，另设计真实 gRPC、持续音频及多会话 workload。
