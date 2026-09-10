# EXP-002: WebSocket 写入停滞时的取消与清理

Date: 2026-09-10
Commit: 7c47f95a6d82e62a097c547bc25a083252718ca2 + [相关工作区补丁](results/EXP-002-worktree.patch)，包括实验驱动和已有 protobuf 文件头变更；[源文件 SHA-256](results/EXP-002-source.sha256)。生产 Gateway 未修改。
Related: [ADR-002](../adr/ADR-002-streaming-io-backpressure.md), [EXP-001](EXP-001-send-stall-disconnect.md)
Status: Baseline measured; After not measured

## Question

当前下行串行 Recv → ws.Write 在写入阻塞时，是否阻止客户端断开或显式 Abort 引起的取消与清理？后端终止状态是否仍能被读取？这些是不同的触发情境，分别观测。

## Hypothesis

- 客户端关闭或显式 Abort 仍可能通过独立 upload/控制路径取消 RPC 并中断 ws.Write。
- 写入持续阻塞时不会开始下一次 Recv；仅在下一次 Recv 可获得的 Worker 错误可能暂时无法被协调者观察。
- 第二项即使复现，也不自动说明需要结果队列：是否要求在慢客户端仍在线时及时观察 Worker 故障、可接受写入等待多久，需要结合项目目标评估。

## Setup

Client:
- concurrency: 1，每种情境重复 5 次
- chunk rate: 合法 Start 后发送一个 2 字节 PCM 块，无持续速率
- session duration: 事件驱动；仅读取结果帧的前 2 字节，停止消费剩余内容

Worker:
- count: 1 个可控 client/stream 测试替身
- processing latency / mode: Send 正常返回；首次 Recv 返回 8192 字符的 partial；后续 Recv 等待终止事件或 RPC 取消

Gateway:
- relevant config: MaxSessions=1，现有生产实现，无新增队列或写入期限
- 运行真实 Gateway.ServeHTTP / Session / registry，通过可控 Hijacker 接管无缓冲 net.Pipe；缓冲读写器和 WebSocket 帧处理仍真实执行

Environment:
- CPU: arm64，具体型号/核心数未取得
- Memory: 未取得
- Go version: go1.26.5 darwin/arm64
- 非真实 TCP 拥塞或 gRPC 流控 benchmark；net.Pipe 用于确定性制造底层写入阻塞

## Metrics

- 触发事件到测试观察者发现 RPC Context Done 的时间，包含观察 goroutine 调度耗时，不等同于调用 cancel 的精确时刻。
- 触发事件到 ServeHTTP 返回（其清理 defer 已完成）的时间，与 cancellation 分开。
- 采样时登记数、Recv 调用次数；100 ms 观察窗只是实验窗口，不是已选定的 T。

## Procedure

确认结果帧进入实际传输且剩余写入阻塞后，分别触发：客户端关闭、Gateway.Abort、Worker 终止错误准备就绪。最后一种保持客户端连接且不读结果，错误只能由下一次 Recv 取得，不等价于外部取消 RPC。

观测满 100 ms 或同时观察到取消与 handler 完成后记录数据；随后 Abort 兜底清理。驱动：[slow_write_experiment_test.go](../../internal/gateway/slow_write_experiment_test.go)。每种情境 5 次，性能采样未启用 race detector；测试通过只表示驱动和兜底清理完成，不表示某个业务目标已达成。

仓库根目录重跑（保留原始基线，另存输出）：

```sh
TIDE_RUN_EXPERIMENTS=1 GOCACHE=/private/tmp/tide-review-gocache \
GOPROXY=off GOSUMDB=off \
go test -mod=readonly ./internal/gateway \
  -run '^TestExperimentSlowWebSocketWrite$' -count=5 -timeout=20s -json \
  > /tmp/tide-exp-002-rerun.jsonl
```

依赖需已缓存。恢复基线时，在独立 checkout 检出上述 Commit 并应用补丁；已有 EXP-001 驱动也包含在快照中，但本命令只执行 EXP-002。

## Results

| Metric | Baseline | After |
| --- | --- | --- |
| 客户端关闭后的取消与清理 | 5/5 观察到 RPC 取消和 handler 完成，登记数归零 | 未运行 |
| 显式 Abort 后的取消与清理 | 5/5 观察到 RPC 取消和 handler 完成，登记数归零 | 未运行 |
| Worker 终止错误的读取与清理 | 5/5 在观察窗内未观察到取消/handler 完成，登记数为 1，Recv 调用数仍为 1 | 未运行 |

| 情境 | RPC 取消观测时间 ms，样本 min–max | handler 完成时间 ms，样本 min–max | 实际观察时间 ms |
| --- | ---: | ---: | ---: |
| client_close | 0.022667–0.043417 | 0.044000–0.335500 | 0.048417–0.355042 |
| gateway_abort | 0.008583–0.016208 | 0.025583–0.069500 | 0.031000–0.083125 |
| worker_error_ready | 未观察到 | 未观察到 | 100.162917–100.825208 |

原始输出：[EXP-002-baseline.jsonl](results/EXP-002-baseline.jsonl)。每种情境只有 5 次局部样本，上述范围不是 p99 或 SLA。worker_error_ready 的 null 时间表示观察窗内未取得事件，不能解释为零耗时；观测后的 Abort 均完成最终清理。

辅助正确性检查：`go vet -mod=readonly ./...` 通过。启用实验的全量 `go test -mod=readonly -race ./... -count=1 -timeout=30s` 首次在已有 `TestGatewayWaitCancellationPreservesStreamingSession/canceled` 的首次结果读取处出现 EOF；该测试单独重复 20 次及全量重跑均通过，原因尚未定位，不视为已修复。race 运行不纳入上述性能样本。

## Conclusion

两项假设均在该负载的观测范围内得到支持：下行写入阻塞没有阻止客户端关闭/显式 Abort 路径的取消与清理；但下一次 Recv 的推进受写入阻塞影响，已经可由 Recv 返回的 Worker 错误未被读取。

前者支持“上行 reader 可推进时，不必仅为对端关闭/Abort 就预设结果队列”；后者说明还需评估“客户端保持慢读时，后端终止状态必须多久被观察并释放资源”。因为该情境没有客户端断开，不能直接宣称它违反客户端断开目标 T。

这是极端停止消费的 net.Pipe 局部故障实验，Worker 是测试替身；不涵盖真实 gRPC 内部状态传播、持续慢速 TCP 读取、多会话内存趋势或生产环境的故障发生率。也未验证候选 B/C 的 After，不能据此直接接受任何架构。下一次 Recv 可读取错误与真实 Worker 服务端已经停止，是不同测量边界。

## Follow-up

- 对 ADR-002 的实现补同负载 After，新增写入期限耗尽情境，并按配置期限调整观察窗口；100 ms 不是业务 T。
- 另测持续结果、真实 TCP 慢读、并发与资源趋势，评估参数是否需要调整。
