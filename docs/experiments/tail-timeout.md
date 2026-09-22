# EXP-004-07：尾部期限接入与清理验收

Date: 2026-09-22
Status: 本步实现及验收完成；第四阶段仍在进行。
Related: [改造前基线](tail-stall-baseline.md)、[方案与取舍](../worklog/designs/2026-09-22-tail-timeout.md)

## Situation / Task

输入结束后，Worker 不结束响应流时，发送期限和输入空闲期限都不能使会话退出。改造前三次实验在约 500ms 观察窗口末均占着唯一名额，新客户端均被容量限制拒绝，最后需要外部取消清理。

本步目标是在合法 end 后设置一次性完成等待预算，超时明确失败并清理资源，同时保留预算内的完整尾部结果，不限制 end 前问诊时长。

## Action / Trade-off

- Gateway.New 校验 TailTimeout；0 使用开发默认值 15 秒，负值报错，正值传入 session。
- 协调者观察到 end 通知后启动 timer，返回超时或其他决定性事件。已有服务停止、发送超时和输入空闲超时优先级保留；Worker 正常完成仍要求合法 end。
- 超时通过 finish 取消 RPC、尝试关闭 WebSocket（1011 / tail timeout）、取消 WebSocket I/O；run 等待内部操作退出，handler 最后归还名额。
- 选择一次性预算而非整条 RPC 期限或随结果刷新期限，避免限制长时问诊，也避免零星输出无限延长占位。代价是尾部过慢时明确失败，不能保证获得完整尾部文本。
- timer 在退出时停止，end 接收入口处理后置 nil；不增加专门的监控 goroutine。

## 环境与负载

Go 1.26.5、macOS 26.6.2，同日准入实验机器（Apple M2 Pro / 32GiB / 12 个逻辑 CPU）。客户端、Gateway、Worker 同进程，通过本机 TCP 使用 WebSocket 和 gRPC。正式三次运行不启用 race，开始前全量 race 回归已经结束。

沿用改造前的 normalEndWorker：接收两条测试音频请求、返回片段结果，读取请求 EOF 后不放行尾部，只响应 RPC 取消。输入是测试字符串，实验验证协议与资源清理，不是真实 PCM 识别性能。

MaxSessions=1，InputIdleTimeout=100ms，WorkerSendTimeout=100ms；改造后的 TailTimeout 测试预算为 200ms。客户端持续读取。每次等待 handler 返回并确认 active=0 后，再次握手接入；新连接不发送 start，关闭后确认再次归零。因此“重新接入成功”不代表执行了第二次完整 ASR。

## Result

| 指标 | 改造前，未启用尾部期限 | 改造后，测试预算 200ms |
| --- | --- | --- |
| 重复次数 | 3 | 3 |
| 自主按尾部超时退出 | 500ms 观察窗口内 0/3 | 3/3 |
| 需要外部服务取消兜底 | 3/3 | 0/3 |
| 退出后 active | 外部取消后三次均为 0 | 自主清理后三次均为 0 |
| 超时关闭原因 | 不适用；兜底后为服务退出 1001 | 三次均为 1011 / tail timeout |
| 清理后额外接入成功 | 本项未测 | 3/3 |

改造后三次耗时范围（最小值至最大值）：

| 事件 | 相对测试观察到 Worker EOF 通知的耗时 |
| --- | --- |
| Worker 方法返回 | 201.32–202.38ms |
| 客户端观察到超时关闭 | 201.39–202.57ms |
| handler 返回，即本次会话清理完成 | 201.45–202.65ms |

Worker 三次均因 RPC 取消退出，客户端与服务 context 相互独立；成功样本未调用外部 stop 来促成退出。这些会话是明确的超时失败，不是识别成功。

**时间口径限制：** 上述起点是测试读取到 Worker EOF 通知，不是协调者创建 timer 的精确时刻。不能据此声称超时误差只有几毫秒。关闭握手、调度和清理也不计入 TailTimeout 的等待预算，本次结果不代表所有网络条件下都能在该时间释放资源。

## 正常路径与回归

新增真实链路测试分别验证：

1. TailTimeout=200ms，在输入阶段等待 300ms，随后继续输入并在 end 后等待 50ms 尾部，完整结果正常返回。此场景给输入空闲单独配置 2 秒，避免混淆原因。
2. TailTimeout=1s，end 后等待 500ms，尾部结果完整有序返回、正常关闭；发送期限和输入空闲期限仍为 100ms。
3. TailTimeout=200ms，尾部永久等待时自主超时、Worker 取消、handler 返回、active=0，随后成功复用名额。

三种真实链路场景随定向测试各运行三轮并通过 race 检查。新增协调者测试覆盖超时错误分类、Worker 提前 EOF 不算正常完成、已有退出优先级；全量 `go test -race ./...` 通过（Gateway 实际执行，Mock 使用已有测试缓存）。测试中的旧构造调用已同步增加尾部参数。

## Think

会话上限和尾部期限解决不同问题：前者限制同时接纳的数量，后者防止已结束输入的会话长期占住名额。本步可量化收益是外部兜底从 3/3 降为 0/3，并验证清理后的名额可复用；没有测量吞吐、内存下降或真实模型识别质量的改善。

默认 15 秒只是初始策略。预算过小会拒绝本可完成的慢尾部，预算过大则占位更久。尾部预算涵盖处理、传输与写回，不能将全部超时都归因于 Worker 推理。输入阶段的识别落后、慢客户端写入和应用层缓冲边界仍需继续处理。

## 复现与证据

```sh
TIDE_RUN_TAIL_TIMEOUT_EXPERIMENT=1 go test ./internal/gateway \
  -run '^TestTailTimeoutExperiment$' -count=3 -v
```

- [完整链路测试与实验](../../internal/gateway/tail_integration_test.go)、[等待及协调者测试](../../internal/gateway/tail_test.go)、[配置测试](../../internal/gateway/tail_config_test.go)
- [原始日志](results/tail-timeout-2026-09-22.log)、[结果汇总](results/tail-timeout-2026-09-22-summary.json)
- [环境、源码摘要和命令](results/tail-timeout-2026-09-22-manifest.json)、[相对基准提交的业务修改](results/tail-timeout-2026-09-22-source.patch)
