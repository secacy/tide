# EXP-004-03：单次发送期限接入后的自主清理

Date: 2026-09-21
Status: 实现与验证完成；仅代表单次 Send 等待保护，不代表第四阶段整体完成。
Related: [改造前停读实验](stalled-worker.md)、[方案取舍](../worklog/designs/2026-09-21-worker-send-timeout.md)、[Milestones](../MILESTONES.md)

## 问题与改动

改造前，Worker 停止读取后会使 Gateway Send 阻塞。客户端断开后观察约两秒，三次会话均未自主退出，需要测试主动取消服务 context 才能清理。

本次增加 `WorkerSendTimeout`：零值使用开发默认值 2 秒，负值拒绝。每次发送启动独立计时，到期以 `ErrWorkerSendTimeout` 为原因取消该会话 RPC。协调者按服务停止、发送超时、输入空闲超时、原始事件的顺序分类，再执行现有连接与 goroutine 收尾。

目标是自动限制发送等待。配置期限不是识别处理期限，也不保证客户端断开立即被发现。

## 负载与测量

- 延续 EXP-004-02：单会话、3200 字节/块；Worker 处理 5 块后停读；两个处理延迟均为 0，500ms 音频产生首个 partial。
- 前 5 块确认双向流动，然后加速发送。上限仍为 5000 块、故障观察最多 15 秒。
- Gateway Send 与客户端 Write 都持续等待超过一秒后，进入相应场景。
- `timeout_connected`：客户端保持连接并读取结果，不发出服务取消。
- `client_disconnect`：客户端 CloseNow 后最多观察两秒。保留失败时的兜底清理，但只要依赖兜底，就判自主清理验证失败。
- 两组各运行三次，设置默认的 WorkerSendTimeout=2 秒。每次验证 RPC 取消原因为 `worker send timeout`、服务父 context 未取消、Worker 仅处理 5 块、最终会话与服务端连接数为 0。
- Gateway 使用默认 StartTimeout=10 秒、InputIdleTimeout=30 秒、MaxMessageBytes=1 MiB。
- 同一台 Apple M2 Pro / 32 GiB / 12 逻辑 CPU，macOS 26.6.2，Go 1.26.5 darwin/arm64，GOMAXPROCS=12。同进程 Client/Gateway/Worker，本机 TCP。性能采样不启用 race。

主要时间从 **实际阻塞的 gRPC Send 调用开始** 计算，记录其返回、Worker 退出与 handler 返回。另通过测试侧 context.AfterFunc 记录“观察到 RPC 取消”的时刻；该回调存在调度延迟，不等同于计时器精确触发时刻。

handler 返回前已经完成 session.run 的 goroutine 等待、WebSocket 兜底关闭与会话退出计数。成功验证不依赖测试 cleanup 中的服务取消或连接关闭。

## 结果

范围为三次运行的最小值至最大值，不是置信区间，也不是生产 SLA。

| 指标 | 客户端保持连接 | 客户端在阻塞后断开 |
| --- | --- | --- |
| 配置发送期限 | 2 秒 | 2 秒 |
| Send 开始至返回 | 2.0006–2.0012 秒 | 2.0010–2.0028 秒 |
| Send 开始至 Worker 退出 | 2.0008–2.0030 秒 | 2.0013–2.0032 秒 |
| Send 开始至 handler 返回 | 2.0050–2.0065 秒 | 2.0061–2.0101 秒 |
| 实验观察到取消至 handler 返回 | 4.391–5.028ms | 4.890–7.037ms |
| 客户端断开完成至观察到 handler 返回 | 不适用 | 995.685–998.614ms |
| 无兜底自主清理 | 3/3 | 3/3 |
| RPC 取消原因 | 均为 worker send timeout | 均为 worker send timeout |
| 最终活跃会话 / 服务端未关闭连接数 | 均为 0 | 均为 0 |

客户端保持连接时均收到 1011，关闭原因为 `worker send timeout`。客户端已断开时不要求收到关闭通知。两组 Worker 均仅处理 5 块（16000 字节），随后取消退出；这属于失败会话清理，不是完整识别成功。

### 与改造前对比

| 同一客户端断开场景 | 改造前 EXP-004-02 | 改造后 |
| --- | --- | --- |
| 断开后的两秒观察期 | 3/3 仍保留会话，Send 等待未解除 | 3/3 自主清理，在约 1 秒内观察到退出 |
| 测试兜底取消次数 | 3/3 | 0/3 |
| 推动退出的原因 | 测试发出的服务取消 | 单次 Send 期限到达后取消 RPC |

改造后断开至退出约一秒，是因为客户端在 Send 已等待约一秒时断开，而发送期限为两秒。不能由此声称“断开检测耗时为一秒”。该机制同样会结束保持连接但发送持续受阻的会话。

旧实验没有在无外部取消时观察到最终退出，因此不计算“清理速度提升百分比”。目前可支持的结果是：在相同故障类型和负载下，从依赖测试兜底变为由配置期限自动触发清理，且保留明确的业务失败原因。

## 验证依据与复现

- [接入测试](../../internal/gateway/worker_send_timeout_test.go)：配置、upload 分类、download 先报错、服务停止优先、真实 WebSocket 1011 及清理。
- [辅助函数测试](../../internal/gateway/send_test.go)：计时竞争、取消原因、旧计时器及底层 Send 退出。
- [尾部结果回归](../../internal/gateway/normal_end_test.go)：发送期限 100ms、尾部延迟 500ms，结果仍完整且正常结束，证明没有将发送期限误用为整条 RPC 期限。
- 全量 `go test -race ./... -count=1 -timeout=90s` 通过。之后新增的尾部期限条件与真实 TCP 实验也单独使用 race 验证通过；race 数据不进入结果表。
- [实验入口](../../internal/gateway/stalled_worker_experiment_test.go)：`TestWorkerSendTimeoutExperiment`。复用原停读实验装置，并新增自动取消原因和无需兜底的断言。

```sh
TIDE_RUN_WORKER_SEND_TIMEOUT_EXPERIMENT=1 \
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test ./internal/gateway -run '^TestWorkerSendTimeoutExperiment$' \
  -v -count=3 -timeout=90s
```

未显式设置环境变量时，这项真实时间实验跳过，不拖慢普通回归。

原始记录：[六次输出](results/worker-send-timeout-2026-09-21.log)、[统计汇总](results/worker-send-timeout-2026-09-21-summary.json)、[环境/命令/源码 SHA-256](results/worker-send-timeout-2026-09-21-manifest.json)、[Gateway 修改与新增辅助函数](results/worker-send-timeout-2026-09-21-gateway.patch)。基准 HEAD 为 `b2b520047be2e1bd1748918f2ed41a323f82ad6c`，加未提交业务修改与测试，不把基准 HEAD 当作完整实验版本。

## 限制与后续

- 两秒是当前开发默认值。尚未用真实 ASR 和负载分布校准，可能终止本可恢复的短暂慢会话。
- 保护只作用于单次 Send，无法约束 Send 快速返回后的识别落后、尾部等待或客户端写回阻塞。
- 客户端不回应关闭握手时仍可能增加收尾时间；本次没有验证所有网络故障下的硬性清理上限。
- 没有新增有界音频队列或会话准入控制，没有测量内存上界、稳定容量或计时器的性能开销。
- 下一步应依据短暂抖动与持续过慢的需求比较缓冲策略和满载行为，并明确实际业务允许的等待与失败方式；不因本次取消验证通过就预设必须加入队列。
