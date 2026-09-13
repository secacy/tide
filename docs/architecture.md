# Tide 当前架构

Updated: 2026-09-13
Source: 469947dfb0833320fd8fbfd4c650601bfbd33e46
Related: [ADR-002](adr/ADR-002-streaming-io-backpressure.md), [ADR-003](adr/ADR-003-processing-progress-deadline.md), [ADR-004](adr/ADR-004-admission-protection.md), [ADR-005](adr/ADR-005-worker-reservation-lifecycle.md), [ADR-006](adr/ADR-006-least-reserved-ratio-selection.md), [ADR-007](adr/ADR-007-experimental-capacity-margin.md), [ADR-008](adr/ADR-008-websocket-heartbeat-lifecycle.md)

## 系统边界

当前实现为 WebSocket Client → 单 Go Gateway → 固定列表中的 gRPC ASR Worker。Mock Worker 支持返回 partial/final；Gateway 负责实时转发、会话登记、总接入上限、各 Worker 名额预留及生命周期收尾。每个 WebSocket 尝试固定一个 Worker、对应一次 RPC；v1 为单次识别，v2 由存活客户端持有问诊身份并在有限预算内重新接入和重放，新的接入重新参与 Worker 选择。不包含病历生成或服务端持久化恢复。

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

WebSocket 升级后启动独立心跳，Start 校验完成后启动三个 I/O goroutine。Worker 建流由 Sender 执行，Reader 和期限协调者不等待建流：

```text
WebSocket Reader → 有界音频 FIFO → Worker Sender → gRPC Send

gRPC Recv → download → 带期限的 WebSocket Write

三个业务执行流 → Session 协调者 → 取消 / 关闭 / 等待退出

独立 Ping/Pong → 失败时取消 Session 并中断传输
```

| 角色 | 职责与等待边界 |
| --- | --- |
| Reader | 独占 WebSocket 读取。立即尝试入队，满时报告过载；合法 End 关闭队列输入，随后继续监测断开和非法后续数据 |
| 音频队列 | 同时限制字节数和条目数，复制成功入队的音频，FIFO 出队时移除引用；只关闭输入时保留积压供 Sender 排空 |
| Sender | 建立 RPC，将流对象通过单槽通道交给 download，独占 Send/CloseSend；发送前建立音频序号水位，空队列等待使用 RPC Context；排空后半关闭请求方向 |
| download | 可取消地等待流对象，随后校验并消费 Worker 处理进度，不转发为文字；结果继续串行写入，每条有独立期限，写入期间不继续 Recv |
| 协调者 | 处理失败及发送完成、Worker EOF 事件，复用一个计时器等待最近的音频/End 截止时刻；执行清理并等待业务执行流和心跳退出 |

实现入口：[session.go](../internal/gateway/session.go)、[upload.go](../internal/gateway/upload.go)、[audio_queue.go](../internal/gateway/audio_queue.go)、[download.go](../internal/gateway/download.go)。

[processing_progress.go](../internal/gateway/processing_progress.go) 独立跟踪全部已接纳但未确认处理的音频，包含已出队和正在发送的部分。固定环形表只存到达时间的单调偏移，确认后释放槽位；不保存音频副本，不为每块音频创建计时器或 goroutine。队列可获取进度锁，反向获取禁止，所有网络操作都在锁外。

## Worker 进度协议

每个 RPC 的请求 `audio_seq` 从 1 连续递增。独立 `ProcessingProgress.processed_through_seq` 确认此前所有音频均已处理，包含静音；允许累计和重复确认，拒绝倒退、越过已开始发送的水位或与文字结果混装。开始发送水位在 Send 调用前建立，确认可以先于 Send 返回。

进度表示处理完成，不能用接收确认代替。批量周期和返回延迟计入等待预算。协议新增字段需要 Gateway 与 Worker 配套升级：旧 Worker 的缺失确认不会被视作成功。v1 Start/result 格式保持原样，分段 `isFinal` 不代表会话完成。v2 的安全 checkpoint 与此处理水位独立，整个问诊完整性由客户端结果覆盖、End 完成及显式缺口共同决定。

## 配置与资源边界

| 配置 | 默认值 | 含义 |
| --- | ---: | --- |
| Heartbeat.Interval / Timeout | 2 秒 / 3 秒 | 计划探测周期 / 写 Ping 加等待匹配 Pong 的总预算，两端独立配置 |
| Heartbeat.Disabled | false | 显式禁用主动探测，适用于对照或由调用方接管；仍须持续 Read 回应对端 |
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

正常结束依次经历：Reader 接收 End → Sender 排空队列 → 开始并完成 CloseSend → 收齐结果及 Worker EOF → 停止后续心跳并等待在途探测 → WebSocket 正常关闭 → 等待执行流退出 → 归还 Worker 名额 → 注销。CloseSend 返回和 Worker EOF 的观测顺序可以交错，协调者需要同时确认两者。Worker 在开始半关闭前就返回 EOF，按失败处理。

正常完成还要求全部已接纳音频均获处理确认。已到期的失败不能被迟到确认或 End 重复通知清除；已有音频全部确认后，音频期限撤销，End 期限仍独立存在。期限约束协调者等待业务完成的过程，之后的关闭握手和清理仍须单独观测，不构成无条件的总退出时间上界。

Send EOF 仅表示发送停止，不能当作 RPC 成功。Send EOF 或 CloseSend 错误之后，Recv 仍负责给出最终接收状态。已知发送失败不会与 Worker EOF 合并成正常完成。

音频满队列立即取消 RPC，尝试返回 WebSocket 1013；空音频块或非法控制消息按协议错误处理。写入超时取消 RPC 并清理连接；它可能先使 Reader 观察到断开，收尾后会补充写入超时原因。异常结束允许存在未处理音频或未送达结果，但不能报告正常完成。

处理等待超时尝试返回 1013 / `processing_timeout`；End 超时返回 1011 / `end_timeout`；进度记录满返回 1013 / `progress_capacity`。这些路径均先取消 RPC，再尽力通知客户端并完成清理。非法进度或缺失尾部确认按 Worker 失败 1011 处理。

容量不足时优先保护已接纳问诊，升级前立即返回 HTTP 503，拒绝请求不创建 Worker RPC，不提供接入等待或抢占已有会话。当前分别检查 Gateway 的 `MaxSessions` 与选定 Worker 的配额；这些仍是静态配置，不会自动感知 Worker 剩余处理能力，实验候选值 6 未设为生产默认容量。

会话在升级前登记，所有执行流与连接清理完成后才注销。StopAccepting 拒绝新会话，Wait 仅等待注销；Abort 取消会话并关闭原始传输。服务关闭先共享 5 秒自然排空预算，必要时进入 2 秒强制清理等待预算；同步关闭调用不能由等待预算抢占。

## 心跳与退出协调

[ADR-008](adr/ADR-008-websocket-heartbeat-lifecycle.md) 采用双端独立 Ping/Pong，默认两秒周期、三秒单次预算，最多一个在途 Ping。Gateway 从升级成功后开始，覆盖等待 Start、慢建流、传输和 End 后等待结果；客户端从 Dial 成功后开始。业务 Reader 是唯一读取者，也负责消费控制帧；没有另建心跳 Reader。正常空闲继续保活，处理期限仍独立衡量 Worker 进度。

`Monitor.Stop` 幂等关闭后续探测入口，不取消在途 Ping 的 Context；`Wait` 等它按自身预算退出。正常收尾先停用并 join，再完成关闭握手。业务失败先取消 RPC，再 join 心跳；在途探测也失败时直接 CloseNow，保留先选定的业务错误，避免继续等待关闭握手。显式 Abort 可以立即中断原始传输。正常停止可能增加当前探测的剩余等待，但五秒检测目标不代表整个收尾硬上界。

活动会话心跳失败携带独立 `websocket heartbeat failed` 原因，取消 Session 并中断连接；客户端取消发送、退出接收并返回错误。正常收到 1000 时，不因同时结束的 Ping 重判为失败。心跳错误可能来自读写争用、本地调度或网络，不直接改变 Worker 接入资格；对端不一定得到关闭原因。

实现：[公共探测组件](../internal/wsheartbeat/heartbeat.go)、[Gateway 协调](../internal/gateway/session.go)、[客户端](../internal/wsclient/client.go)。两端命令通过 `TIDE_HEARTBEAT_INTERVAL` / `TIDE_HEARTBEAT_TIMEOUT` 在启动时配置，见 [命令说明](command.md)。库组件只读取显式 Config，不隐式读取环境。`Client.Run` 仍是单次会话；v2 的 `RunRecoverable` 另负责跨尝试恢复。其 `AudioSource` 必须响应 Context，文件/内存 Reader 适配要求 Read 能及时返回。

## 客户端有限恢复

[ADR-009](adr/ADR-009-bounded-recovery.md) 已实现：业务 Session 留在客户端，各 RecognitionAttempt 使用独立连接/RPC，先取消并等待本地旧尝试退出再重试；Gateway 注册表继续按本次接入管理名额。客户端协调 goroutine 独占结果、缓存和缺口，采集与网络 I/O 独立。

可恢复 Worker 显式实现 `RecoverableRecognize`：Ready 后接收带全局采样点偏移的 PCM，提供不可变、连续覆盖且无需前文音频即可重启的 checkpoint。客户端保存结果与断点后才释放 PCM；progress 不能代替 checkpoint。旧尝试和已提交重复结果不再次追加。Mock 使用人工独立片段，真实模型契约未验证。

默认缓存十五秒 PCM / 最多 4096 块，一轮恢复总预算十秒，包含重试间隔、Ready、重放和追赶。只有安全断点追上采集末尾才结束一轮预算，End 后还要求正常完成。缓存不足且未 End 时可在同一剩余预算中从新片段继续，并保留历史缺口；End 后禁止跳过尾部。预算耗尽返回中断报告并停止自动重试，不能用最后一次 1000 消除旧缺口。

完整协议、消息、配置与 API 使用见 [恢复协议](recovery-protocol.md)。实现：[协调器](../internal/wsclient/recovery.go)、[尝试 I/O](../internal/wsclient/recovery_attempt.go)、[Gateway 协议校验](../internal/gateway/recovery.go)、[Mock 契约](../internal/mockasr/recovery.go)。PCM 上限不包含固定通道/发送缓冲，也不是总会话内存上限；结果随问诊增长。Source 取消及观察回调及时返回是接口条件。

## 已验证范围

[EXP-001](experiments/EXP-001-send-stall-disconnect.md) 验证了 Send 停滞时的断开退出及两种容量上限触发的过载失败。[EXP-002](experiments/EXP-002-slow-websocket-write.md) 验证了慢写期间的客户端关闭、Abort 和写入期限退出。回归测试覆盖顺序、结束协调和并发取消。

[EXP-003](experiments/EXP-003-streaming-load.md) 完成了 1/8/32 会话短负载和 8 会话一分钟连续输入的初测，并验证 Worker 抖动与持续变慢时的表现。应用队列较短时，gRPC 内部仍可能积压，当前队列上限不构成端到端延迟保证。

上述 EXP-001～003 记录各自版本的历史验证，EXP-003 的堆数据不包含本次新增进度表，不能直接作为当前版本的资源测量值。

[EXP-004](experiments/EXP-004-processing-progress-deadline.md) 的生产对照覆盖正常、抖动、模拟静音、批量进度、持续慢处理、单块停滞和 End 后无 EOF。14 次运行中 8 次正常完成、6 次期限退出，全部清理后登记数归零。协议边界、记录表复用、迟到确认和并发进度由回归测试覆盖。

[EXP-005](experiments/EXP-005-tcp-slow-client-cleanup.md) 补充 18 次真实 loopback TCP 慢读、RST、Abort 和恢复读取实验，全部最终注销。默认 2 秒写入期限下能结束慢写；将写入期限设为 8 秒后，RPC 约在处理/End 的 3/5 秒期限取消，会话却接近第 8 秒才注销。当前没有独立的异常收尾预算，尽力发送关闭通知可能延迟资源释放。已确认暂时保留这一取舍，不承诺整个收尾在 5 秒内完成，也不保证关闭原因送达；后续需要延长写入期限或发现清理占用影响接入时再评估。

[EXP-008](experiments/EXP-008-worker-selection.md) 验证固定 Worker 预留和两种候选分配的行为：异构组中最小预留比例改善本轮短负载回显延迟，同容量组表现接近；32 个流式会话全部正常完成并归还名额。逻辑重放、短回显和微基准不代表长期或真实模型容量，[ADR-006](adr/ADR-006-least-reserved-ratio-selection.md) 据此确认当前推荐最小预留比例策略。

[EXP-009](experiments/EXP-009-multi-worker-capacity.md) 延长多 Worker 容量验证：配额 1/3 在本机 Mock 下完成四会话五分钟正常输入，并在两轮周期暂停中确认十次恢复；1/4 正常负载达标但有三次暂停恢复未确认，1/5 出现处理超时。持续降速时 B 仍失败，A 在本实验中正常完成。全部名额与活动流最终清理；[ADR-007](adr/ADR-007-experimental-capacity-margin.md) 据此确认 1/3 为上述模拟条件下的保守实验配额，1/4 保留为正常负载下的较高利用率候选。该决策不改变应用启动默认值，也不代表最大稳定容量。M5 按此实验与基础范围收尾；M6 恢复仍通过相同接入与名额机制。

[EXP-010](experiments/EXP-010-silent-disconnect.md) / [EXP-011](experiments/EXP-011-heartbeat-interval.md) 提供候选检测与周期成本依据；[EXP-012](experiments/EXP-012-heartbeat-lifecycle.md) 保存生产实现的生命周期回归及收尾失败复现。候选性能数据不直接当作当前生产代码的性能测量。

[EXP-013](experiments/EXP-013-recovery-checkpoint.md) 用测试客户端状态模型和可控 Worker 验证处理进度不能代替恢复断点，以及迟到/重复结果、断点停滞导致缓存不足的边界；另用生产 Gateway 验证旧清理占用与接入拒绝。该实验提供候选反例，最终协议随后由 ADR-009 确认，实际链路由 EXP-014 验证。

Gateway 仍没有 Start/建流专用期限；v2 客户端从 Dial 到 Ready 另有三秒预算，并在恢复期间服从剩余总预算。当前没有结果持久化。无未确认音频且未 End 的连接不会仅因无文字输出而超时。WAN/TLS 慢读、临床量级会话时长及共享模型资源下的稳定容量尚未验证；正常转录可见延迟 P95 目标 1 秒仍需真实模型验证。

[EXP-014](experiments/EXP-014-production-recovery.md) 验证实际 Client/Gateway 链路的 Worker 故障、WebSocket checkpoint 交付丢失、End 关闭丢失、缓存耗尽与预算终止。三十个样本全部满足预设语义与清理，包含十五个按预期不完整结果；加速预算不能证明默认预算下的持续追赶。M6 仍需联合弱网及实例故障负载。

[EXP-015](experiments/EXP-015-continuous-recovery.md) 使用默认心跳、十五秒缓存和十秒恢复预算，在十六秒持续输入中注入明确 Worker 故障和两秒接入不可用。八个正式样本中，40 ms/100 ms 音频档约四秒追上，80 ms 和 100 ms 档继续提交结果但预算内未追上，明确返回尾部缺口；全部清理完成。没有触及缓存上限或先触发 M4 保护，说明本组边界来自处理余量与总预算。它没有测静默失联的检测积压，M6 仍需该联合负载及实例故障验证。
