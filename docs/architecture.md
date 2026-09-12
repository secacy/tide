# Tide 当前架构

Updated: 2026-09-12
Source: e92c1288afad46f0f6da05b3375d851a80a94709
Related: [ADR-002](adr/ADR-002-streaming-io-backpressure.md), [ADR-003](adr/ADR-003-processing-progress-deadline.md), [ADR-004](adr/ADR-004-admission-protection.md), [ADR-005](adr/ADR-005-worker-reservation-lifecycle.md), [ADR-006](adr/ADR-006-least-reserved-ratio-selection.md)

## 系统边界

当前实现为 WebSocket Client → 单 Go Gateway → 固定列表中的 gRPC ASR Worker。Mock Worker 支持返回 partial/final；Gateway 负责实时转发、会话登记、总接入上限、各 Worker 名额预留及生命周期收尾。一个 WebSocket 会话固定一个 Worker、对应一次 RPC，不包含重连恢复、跨 Worker 重试或病历生成。

音频约定为 16 kHz、单声道、16-bit PCM，即每秒 32,000 字节；客户端默认每块 3,200 字节。

## Worker 选择与预留

应用为各 Worker 创建共享 gRPC 客户端，配置唯一 ID 和正整数会话配额；Gateway 使用独立的本地 `WorkerPool`。`TryAcquire` 在同一互斥锁内检查是否允许接入、选择 Worker 并增加预留数；锁内不做网络操作，不与注册表/Session 锁嵌套。

```text
登记 Session（占 Gateway 名额）
  → TryAcquire（占 Worker 名额，无空位则 HTTP 503 并注销）
  → WebSocket 升级 → Start → 固定 Worker RPC → 会话运行
  → 连接与 I/O 执行流清理 → Lease.Release → 注销 Session
```

准备和收尾都占名额，取消 RPC 不会立即释放。Lease 是没有 TTL/续租的本地预留凭证，重复或并发释放只生效一次；不负责取消或关闭客户端连接。`Gateway.Wait` 排空后，该 Gateway 接入路径持有的 Worker 预留已归还。远端处理是否已停止仍受 RPC 取消契约约束，本地账本不能证明远端资源已释放。

`RoundRobin` 从上次选择之后轮询，跳过已满或停止接入的 Worker；`LeastReservedRatio` 比较 `reserved/capacity`，同值轮换。当前推荐 `LeastReservedRatio`，保留轮询供显式配置和对照；多 Worker 配置仍必须提供策略，不隐式切换。`StopAccepting(workerID)` 只停止之后的新预留，已预留会话继续；它没有管理 HTTP 接口，也不包含自动健康检测。列表、容量在进程运行期间固定，不协调多个 Gateway。

实现：[worker_pool.go](../internal/gateway/worker_pool.go)、[接入流程](../internal/gateway/gateway.go)。单 Worker `New` 入口与旧调用兼容，其 Worker 配额等于 `MaxSessions`。多 Worker 使用 `NewWithPool`，启动配置见 [运行命令](command.md)。

## 会话执行结构

完成 Start 校验和 Worker 建流后，Session 启动三个 I/O goroutine：

```text
WebSocket Reader → 有界音频 FIFO → Worker Sender → gRPC Send

gRPC Recv → download → 带期限的 WebSocket Write

三个执行流 → Session 协调者 → 取消 / 关闭 / 等待退出
```

| 角色 | 职责与等待边界 |
| --- | --- |
| Reader | 独占 WebSocket 读取。立即尝试入队，满时报告过载；合法 End 关闭队列输入，随后继续监测断开和非法后续数据 |
| 音频队列 | 同时限制字节数和条目数，复制成功入队的音频，FIFO 出队时移除引用；只关闭输入时保留积压供 Sender 排空 |
| Sender | 独占 Send/CloseSend；发送前建立音频序号水位，空队列等待使用 RPC Context；排空后半关闭请求方向 |
| download | 校验并消费 Worker 处理进度，不转发为文字；结果继续串行写入，每条有独立期限，写入期间不继续 Recv |
| 协调者 | 处理失败及发送完成、Worker EOF 事件，复用一个计时器等待最近的音频/End 截止时刻；执行清理并等待三个执行流退出 |

实现入口：[session.go](../internal/gateway/session.go)、[upload.go](../internal/gateway/upload.go)、[audio_queue.go](../internal/gateway/audio_queue.go)、[download.go](../internal/gateway/download.go)。

[processing_progress.go](../internal/gateway/processing_progress.go) 独立跟踪全部已接纳但未确认处理的音频，包含已出队和正在发送的部分。固定环形表只存到达时间的单调偏移，确认后释放槽位；不保存音频副本，不为每块音频创建计时器或 goroutine。队列可获取进度锁，反向获取禁止，所有网络操作都在锁外。

## Worker 进度协议

每个 RPC 的请求 `audio_seq` 从 1 连续递增。独立 `ProcessingProgress.processed_through_seq` 确认此前所有音频均已处理，包含静音；允许累计和重复确认，拒绝倒退、越过已开始发送的水位或与文字结果混装。开始发送水位在 Send 调用前建立，确认可以先于 Send 返回。

进度表示处理完成，不能用接收确认代替。批量周期和返回延迟计入等待预算。协议新增字段需要 Gateway 与 Worker 配套升级：旧 Worker 的缺失确认不会被视作成功。WebSocket Start/result 格式保持原样，客户端通过最终关闭状态判断整次转录是否完整，分段 `isFinal` 不代表会话完成。

## 配置与资源边界

| 配置 | 默认值 | 含义 |
| --- | ---: | --- |
| MaxMessageBytes | 1 MiB | 单条入站 WebSocket 消息上限 |
| AudioQueueMaxBytes | 64,000 | 等待发送的音频字节上限，约 2 秒音频 |
| AudioQueueMaxChunks | 128 | 等待发送的音频块数量上限 |
| ResultWriteTimeout | 2 秒 | 单次结果写入预算，不是 Worker 结果间隔或整段问诊时长限制 |
| ProcessingTimeout | 3 秒 | 最早未确认处理音频自成功入队起的等待预算 |
| EndTimeout | 5 秒 | 自首次合法 End 起，等待尾部结果写入、发送完成与 Worker EOF 的预算 |
| MaxUnprocessedChunks | 4096 | 未确认音频时间记录的数量上限，包含队列外的音频 |
| MaxSessions | 必须显式配置正数 | 已登记且尚未清理完成的会话上限 |

队列、进度记录容量和各等待期限的零值使用默认配置，负数被拒绝。默认值是工程初值；音频时长不等于实际排队时间。队列上限不包含正在读取、发送的音频和协议缓冲，也不是单会话堆内存测量值。默认进度时间数组约 32 KiB/会话，另计结构开销；这不是单会话总内存测量值。

## 完成与失败语义

正常结束依次经历：Reader 接收 End → Sender 排空队列 → 开始并完成 CloseSend → 收齐结果及 Worker EOF → WebSocket 正常关闭 → 等待执行流退出 → 归还 Worker 名额 → 注销。CloseSend 返回和 Worker EOF 的观测顺序可以交错，协调者需要同时确认两者。Worker 在开始半关闭前就返回 EOF，按失败处理。

正常完成还要求全部已接纳音频均获处理确认。已到期的失败不能被迟到确认或 End 重复通知清除；已有音频全部确认后，音频期限撤销，End 期限仍独立存在。期限约束协调者等待业务完成的过程，之后的关闭握手和清理仍须单独观测，不构成无条件的总退出时间上界。

Send EOF 仅表示发送停止，不能当作 RPC 成功。Send EOF 或 CloseSend 错误之后，Recv 仍负责给出最终接收状态。已知发送失败不会与 Worker EOF 合并成正常完成。

音频满队列立即取消 RPC，尝试返回 WebSocket 1013；空音频块或非法控制消息按协议错误处理。写入超时取消 RPC 并清理连接；它可能先使 Reader 观察到断开，收尾后会补充写入超时原因。异常结束允许存在未处理音频或未送达结果，但不能报告正常完成。

处理等待超时尝试返回 1013 / `processing_timeout`；End 超时返回 1011 / `end_timeout`；进度记录满返回 1013 / `progress_capacity`。这些路径均先取消 RPC，再尽力通知客户端并完成清理。非法进度或缺失尾部确认按 Worker 失败 1011 处理。

容量不足时优先保护已接纳问诊，升级前立即返回 HTTP 503，拒绝请求不创建 Worker RPC，不提供接入等待或抢占已有会话。当前分别检查 Gateway 的 `MaxSessions` 与选定 Worker 的配额；这些仍是静态配置，不会自动感知 Worker 剩余处理能力，实验候选值 6 未设为生产默认容量。

会话在升级前登记，所有执行流与连接清理完成后才注销。StopAccepting 拒绝新会话，Wait 仅等待注销；Abort 取消会话并关闭原始传输。服务关闭先共享 5 秒自然排空预算，必要时进入 2 秒强制清理等待预算；同步关闭调用不能由等待预算抢占。

## 已验证范围

[EXP-001](experiments/EXP-001-send-stall-disconnect.md) 验证了 Send 停滞时的断开退出及两种容量上限触发的过载失败。[EXP-002](experiments/EXP-002-slow-websocket-write.md) 验证了慢写期间的客户端关闭、Abort 和写入期限退出。回归测试覆盖顺序、结束协调和并发取消。

[EXP-003](experiments/EXP-003-streaming-load.md) 完成了 1/8/32 会话短负载和 8 会话一分钟连续输入的初测，并验证 Worker 抖动与持续变慢时的表现。应用队列较短时，gRPC 内部仍可能积压，当前队列上限不构成端到端延迟保证。

上述 EXP-001～003 记录各自版本的历史验证，EXP-003 的堆数据不包含本次新增进度表，不能直接作为当前版本的资源测量值。

[EXP-004](experiments/EXP-004-processing-progress-deadline.md) 的生产对照覆盖正常、抖动、模拟静音、批量进度、持续慢处理、单块停滞和 End 后无 EOF。14 次运行中 8 次正常完成、6 次期限退出，全部清理后登记数归零。协议边界、记录表复用、迟到确认和并发进度由回归测试覆盖。

[EXP-005](experiments/EXP-005-tcp-slow-client-cleanup.md) 补充 18 次真实 loopback TCP 慢读、RST、Abort 和恢复读取实验，全部最终注销。默认 2 秒写入期限下能结束慢写；将写入期限设为 8 秒后，RPC 约在处理/End 的 3/5 秒期限取消，会话却接近第 8 秒才注销。当前没有独立的异常收尾预算，尽力发送关闭通知可能延迟资源释放。已确认暂时保留这一取舍，不承诺整个收尾在 5 秒内完成，也不保证关闭原因送达；后续需要延长写入期限或发现清理占用影响接入时再评估。

[EXP-008](experiments/EXP-008-worker-selection.md) 验证固定 Worker 预留和两种候选分配的行为：异构组中最小预留比例改善本轮短负载回显延迟，同容量组表现接近；32 个流式会话全部正常完成并归还名额。逻辑重放、短回显和微基准不代表长期或真实模型容量，[ADR-006](adr/ADR-006-least-reserved-ratio-selection.md) 据此确认当前推荐最小预留比例策略。

[EXP-009](experiments/EXP-009-multi-worker-capacity.md) 延长多 Worker 容量验证：配额 1/3 在本机 Mock 下完成四会话五分钟正常输入，并在两轮周期暂停中确认十次恢复；1/4 正常负载达标但有三次暂停恢复未确认，1/5 出现处理超时。持续降速时 B 仍失败，A 在本实验中正常完成。全部名额与活动流最终清理；这些事实形成实验容量建议，不改变当前生产默认配置，也不代表最大稳定容量。

当前仍没有 Start/建流专用期限、静默断网心跳发现、结果持久化或恢复协议。无未确认音频且未 End 的连接不会仅因无文字输出而超时。WAN/TLS 慢读、临床量级会话时长及共享模型资源下的稳定容量尚未验证；正常转录可见延迟 P95 目标 1 秒仍需真实模型验证。
