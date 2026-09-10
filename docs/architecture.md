# Tide 当前架构

Updated: 2026-09-11
Source: cb22071224a5a04f69eecfe1032e866f880474bb
Related: [ADR-002](adr/ADR-002-streaming-io-backpressure.md)

## 系统边界

当前实现为 WebSocket Client → Go Gateway → 单个地址的 gRPC ASR Worker。Mock Worker 支持返回 partial/final；Gateway 负责实时转发、会话登记、接入上限和生命周期收尾。一个 WebSocket 会话对应一次 RPC，不包含重连恢复、Worker 调度或病历生成。

音频约定为 16 kHz、单声道、16-bit PCM，即每秒 32,000 字节；客户端默认每块 3,200 字节。

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
| Sender | 独占 Send/CloseSend；空队列等待使用 RPC Context，取消 RPC 能唤醒等待；排空后半关闭请求方向 |
| download | 串行 Recv 和结果写入，每条结果获得独立写入期限；写入期间不继续 Recv |
| 协调者 | 处理失败及发送完成、Worker EOF 事件，执行清理并等待三个执行流退出 |

实现入口：[session.go](../internal/gateway/session.go)、[upload.go](../internal/gateway/upload.go)、[audio_queue.go](../internal/gateway/audio_queue.go)、[download.go](../internal/gateway/download.go)。

## 配置与资源边界

| 配置 | 默认值 | 含义 |
| --- | ---: | --- |
| MaxMessageBytes | 1 MiB | 单条入站 WebSocket 消息上限 |
| AudioQueueMaxBytes | 64,000 | 等待发送的音频字节上限，约 2 秒音频 |
| AudioQueueMaxChunks | 128 | 等待发送的音频块数量上限 |
| ResultWriteTimeout | 2 秒 | 单次结果写入预算，不是 Worker 结果间隔或整段问诊时长限制 |
| MaxSessions | 必须显式配置正数 | 已登记且尚未清理完成的会话上限 |

队列和写入参数的零值使用默认配置，负数被拒绝。默认值是实验初值；音频时长不等于实际排队时间。队列上限不包含正在读取、发送的音频和协议缓冲，也不是单会话堆内存测量值。

## 完成与失败语义

正常结束依次经历：Reader 接收 End → Sender 排空队列 → 开始并完成 CloseSend → 收齐结果及 Worker EOF → WebSocket 正常关闭 → 等待执行流退出 → 注销。CloseSend 返回和 Worker EOF 的观测顺序可以交错，协调者需要同时确认两者。Worker 在开始半关闭前就返回 EOF，按失败处理。

Send EOF 仅表示发送停止，不能当作 RPC 成功。Send EOF 或 CloseSend 错误之后，Recv 仍负责给出最终接收状态。已知发送失败不会与 Worker EOF 合并成正常完成。

音频满队列立即取消 RPC，尝试返回 WebSocket 1013；空音频块或非法控制消息按协议错误处理。写入超时取消 RPC 并清理连接；它可能先使 Reader 观察到断开，收尾后会补充写入超时原因。异常结束允许存在未处理音频或未送达结果，但不能报告正常完成。

会话在升级前登记，所有执行流与连接清理完成后才注销。StopAccepting 拒绝新会话，Wait 仅等待注销；Abort 取消会话并关闭原始传输。服务关闭先共享 5 秒自然排空预算，必要时进入 2 秒强制清理等待预算；同步关闭调用不能由等待预算抢占。

## 已验证范围

[EXP-001](experiments/EXP-001-send-stall-disconnect.md) 验证了 Send 停滞时的断开退出及两种容量上限触发的过载失败。[EXP-002](experiments/EXP-002-slow-websocket-write.md) 验证了慢写期间的客户端关闭、Abort 和写入期限退出。回归测试覆盖顺序、结束协调和并发取消。

这些是局部故障验证，不是生产容量 benchmark。当前仍没有 Start/建流专用期限、覆盖所有 Worker 停滞的期限、静默断网心跳发现、结果持久化或恢复协议。持续音频、多会话与真实慢读负载下的资源趋势和稳定容量尚未验证。
