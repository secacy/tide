# EXP-004-09：结果写回期限验收

Date: 2026-09-22
Status: 本步实现和验收完成；第四阶段尚未完成。
Related: [慢客户端基线](slow-reader-baseline.md)、[方案与取舍](../worklog/designs/2026-09-22-result-write-timeout.md)

## Situation / Task

客户端不读取结果时，真实 TCP 缓冲耗尽后会阻塞网关写回。改造前三次实验均观察到同一次底层 Write 持续等待约 804–808ms、会话仍占着唯一名额，最后需要外部取消；随后 handler 还等待约 5 秒才返回。

本步为单条结果的 WebSocket Write 设置独立期限，让实际写入退出、Worker 被取消和会话名额被释放，同时避免把写回超时误记为客户端主动断开。

## Action / Trade-off

1. 增加 ResultWriteTimeout，0 使用开发默认 2 秒，负值无效，显式传入 session。JSON 编码在期限开始前完成；同步 Write 由其子 context 限制。
2. 写入前保存 context；写入结束后保留可查询的状态。即使 upload 的断开事件先到，协调者也能恢复写回超时原因；已有服务停止、发送超时和输入空闲超时优先级保留。
3. 超时取消 RPC 和 WebSocket I/O，再用 CloseNow 兜底。此时连接可能已经关闭，客户端又不读取，故不等待正常关闭握手，也不承诺它收到特定业务关闭码。
4. 没有增加结果队列或独立写入 goroutine。代价是写回过慢时结束当前问诊连接，不保证所有结果完整送达；单次 Write 成功也不代表客户端已显示结果。

## 条件与测量

沿用基线的 slowReaderWorker：最多 64 条结果，每条 256KiB ASCII 文本，无额外间隔；客户端握手后发送 start 和一条 2 字节音频，不发送 end，不调用 Read。该有上限的加速负载不代表实际临床转录。

单实例 MaxSessions=1，InputIdleTimeout=30s。改造后测试设置 ResultWriteTimeout=200ms；默认 2 秒通过配置测试验证。WebSocket 与 gRPC 使用本机 TCP，同进程，未改 TCP 缓冲大小，默认不压缩。Go 1.26.5 / macOS 26.6.2，硬件参照同日准入实验。正式三次未启用 race，在全量回归结束后执行。

基线等待底层 Write 持续至少 300ms，再观察 500ms；改造后预算较短，改为等待至少 50ms 的同一次底层 Write，随后观测自主退出。负载相同，观察阈值不同。退出后核对该次 Write 已以错误返回、没有发生后续主连接写入。

计时起点是该次 **net.Conn.Write** 开始，不是整个 WebSocket Write 开始，也不是客户端停止读取的时刻。一次 WebSocket Write 可以包含多次底层 Write，不能从这些数字计算精确的期限误差或结果条数。

## Result

| 指标 | 改造前 | 改造后（200ms 测试预算） |
| --- | --- | --- |
| 重复次数 | 3 | 3 |
| 无外部取消的自主清理 | 观察窗口内 0/3 | 3/3 |
| 需要外部取消兜底 | 3/3 | 0/3 |
| 主会话清理后 active | 兜底后三次均为 0 | 自主清理后三次均为 0 |
| 清理后重新握手接入成功 | 未测 | 3/3 |
| 新连接关闭后 active | 未测 | 三次均为 0 |

改造后，从观测到阻塞的那次底层 Write 开始计时：

- handler 返回为 **201.18–201.41ms**。
- Worker 方法返回为 **201.41–201.87ms**，三次错误码均为 Canceled。

handler 返回表示网关本地会话操作和清理已经完成；远端 Worker 方法返回是另一个事件，分别记录，不能假设两者固定先后。新连接只验证名额复用，不发送 start，不是第二次完整 ASR。

基线“外部取消到 handler 返回约 5 秒”和本次“阻塞 Write 到 handler 返回约 201ms”的起点不同，不据此计算加速倍数或降低百分比。改造后的结果同时包含新增写回期限和超时分支直接清理策略的效果。

## 原因分类与回归证据

真实 Gateway 实验记录自主清理、底层 I/O、Worker 退出及名额计数；现有 ServeHTTP 不暴露 session.run 返回值，不能假装实验直接采集到了内部错误类型。

错误分类由两层测试验证：

- TestResultWriteSessionTimeout 通过受控底层写入运行完整 session.run，直接断言返回 ErrResultWriteTimeout；服务 context 未被外部取消。
- TestResultWriteTimeoutPriority 固定写入期限状态和 upload 先到的断开事件，检查仍得到写回超时，并保留先前约定的其他原因优先级。

配置、辅助写入、上述分类、真实 TCP 自主清理及正常尾部测试连续三轮通过 race 检查。全量 `go test -race ./...` 通过（Gateway 实际执行，Mock 使用已有缓存）。测试构造调用已同步新增参数。

## Think / 限制

本步证明在受测慢客户端负载下，可以由写回期限自主结束会话，外部兜底从 3/3 降为 0/3，释放的名额可以再次使用。它不证明默认 2 秒是最优策略，也不证明所有网络条件下的清理硬上限。

尚未解决输入过程中的识别进度落后、应用层缓冲和单会话总资源边界；不宣称提升了真实 ASR 准确率、吞吐量或稳定并发容量。

代码检查另发现 ServeHTTP 在构造后再次将相同 ResultWriteTimeout 赋给 session；这是可删除的重复赋值，不影响本次验收。业务代码由开发者维护，助手未修改该行。

## 复现与证据

```sh
TIDE_RUN_RESULT_WRITE_TIMEOUT_EXPERIMENT=1 go test ./internal/gateway \
  -run '^TestResultWriteTimeoutExperiment$' -count=3 -timeout=30s -v
```

- [真实 TCP 实验](../../internal/gateway/result_write_experiment_test.go)、[集成和优先级测试](../../internal/gateway/result_write_integration_test.go)、[辅助方法测试](../../internal/gateway/result_write_test.go)
- [原始日志](results/result-write-timeout-2026-09-22.log)、[汇总](results/result-write-timeout-2026-09-22-summary.json)
- [环境、命令、源码摘要和计量限制](results/result-write-timeout-2026-09-22-manifest.json)、[业务源码修改参照](results/result-write-timeout-2026-09-22-source.patch)
