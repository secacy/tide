# 有限恢复协议与客户端使用

依据：[ADR-009](adr/ADR-009-bounded-recovery.md)。适用于客户端进程持续存活、Worker 明确实现可恢复接口的情形。

## 身份与职责

`Client.Run` 保留原 v1 单次识别；`Client.RunRecoverable` 使用 v2，返回整个问诊的 `Transcript`。同一次调用持有稳定 `sessionId`，每次网络尝试生成新 `attemptId`。Gateway 注册表的 `session_id` 日志字段仍表示本次接入的本地登记身份，不是客户端提供的问诊身份。

客户端的协调 goroutine 独占结果、缺口、缓存和重试状态。采集 goroutine 按输入音频时钟推进；每次尝试的 Reader、Sender 和心跳负责网络 I/O。切换时先取消并等待本地旧尝试退出，再重新连接。服务端旧清理可能仍占名额，新接入照常受 HTTP 503 拒绝。

## WebSocket v2

仍使用现有 `/v1/asr` 路由，通过 Start 的应用协议版本区分；路由名称不表示恢复版本。音频固定为 16 kHz、单声道、16-bit little-endian PCM；音频位置用全问诊采样点数和半开区间表示。

| 方向 | 消息 | 约束 |
| --- | --- | --- |
| Client → Gateway | `{"type":"start","version":"v2","sessionId":"visit","attemptId":"try-2","fromSample":1600}` | 身份长度 1–128 字节；起点是已提交断点或明确放弃历史后选定的新起点 |
| Gateway → Client | `{"type":"ready","sessionId":"visit","attemptId":"try-2","fromSample":1600,"throughSample":1600}` | Worker 确认可恢复接口；客户端收到后才发送 PCM |
| Client → Gateway | Binary：8 字节 **big-endian uint64 起点** + PCM | 仅头部使用大端；PCM 仍小端。音频必须非空、偶数字节且连续 |
| Gateway → Client | `{"type":"checkpoint","sessionId":"visit","attemptId":"try-2","fromSample":1600,"throughSample":3200,"text":"稳定文字"}` | 原子提交文字与覆盖范围；静音允许省略 text。不能混装普通 result |
| Client → Gateway | `{"type":"end","throughSample":3200}` | 末尾必须等于本次已发送音频末尾；允许空尾部重试 |

客户端校验身份、连续性和已发送范围后，在同一个所有者中保存结果、推进断点并释放 PCM。不同尝试身份的迟到结果忽略；当前尝试已经覆盖的重复范围不再次追加。跨问诊身份、范围有洞或越界视为协议错误。Gateway 要求 Worker 自身的 checkpoint 连续推进；它不是跨连接的去重账本。

## Worker 契约

新 RPC 为 `ASRService.RecoverableRecognize`，复用原请求/响应类型的新增字段。首条请求只包含 `recovery_start`；服务端先发送独立 `ready`。后续请求携带 PCM、全局 `start_sample`，以及本 RPC 从 1 开始的 `audio_seq`。原 `progress` 仍确认处理水位并驱动 M4 处理期限，不能使客户端删除音频。

checkpoint 必须提供不可变结果、连续覆盖和可独立重启的末端：首版要求从该末端恢复时无需更早音频或遗留解码器状态。包含静音覆盖。请求半关闭后，Worker 必须补齐剩余 checkpoint 再正常 EOF；Gateway 还检查 End、进度及发送侧完成条件。旧 Worker 的 Unimplemented 映射为 WebSocket 1008 `recovery_unsupported`；非法 checkpoint 或 EOF 缺少覆盖映射为 1008 `recovery_protocol`，客户端不重试。普通 Worker/网络故障在剩余预算内重试。

Mock 只提供人工独立片段，不验证真实模型的上下文依赖、断句质量或重放效果。v2 首版只展示稳定 checkpoint，不包含普通 partial 文本。

## 缓存、预算与结果

`RecoveryConfig` 零值为：PCM 15 秒 / 480,000 字节、最多 4096 块；每轮恢复总预算 10 秒；重试间隔 100 ms；每次从 Dial 到 Ready 最多 3 秒。初次建连 Ready 超时后开启恢复预算；恢复期间的 Ready 等待同时受剩余总预算约束。库调用者显式配置，运行中不热更新。

缓存上限只计未提交 PCM：采集通道、一次网络发送和部分切块还会持有有界音频引用，WebSocket/gRPC 有自身缓冲；结果报告随问诊增长。不能将 480,000 字节解释为客户端总内存上限。单块必须能放进缓存。

一轮恢复从首次故障或缓存不足开始，接入拒绝、退避、重放和追赶共享预算。只有安全断点追上当时已经采集的末尾才结束这轮预算；End 后还要等待正常完成。新 RPC、Ready 和从最新音频继续都不会自行刷新预算。

缓存不足且未 End 时，取消旧尝试，保留新采集片段；新尝试从当前仍保留的最早新片段起点 R 建立。其 Ready 后记录旧断点 C 到 R 的 `buffer_exhausted` 缺口。若期间再次溢出则重新选择片段，但预算不延长。已观察到输入 EOF 后禁止这一跳转；被放弃的历史无法再补，直接返回不完整结果。

预算耗尽返回 `ErrRecoveryBudget`，停止采集和自动重试；无法补齐或曾留下缺口返回 `ErrIncomplete`。终止时未提交的尾部记录为 `interrupted`。即使全部音频已覆盖，End 关闭一直失败也可能返回预算耗尽，此时报告不完整但不虚构音频缺口。检查 `Complete`、`Ended` 和返回错误，不能只检查最后一次 1000 或 `Gaps` 是否为空。

## 调用示例

```go
client, err := wsclient.New(wsclient.Config{
    URL: "ws://localhost:8080/v1/asr",
    ChunkBytes: 3200,
    Realtime: true,
    Recovery: wsclient.RecoveryConfig{
        Observe: func(checkpoint wsprotocol.RecoveryMessage) {
            // 在此展示已经提交的稳定文字；必须及时返回。
        },
    },
})
if err != nil { return err }
report, err := client.RunRecoverable(ctx, wsclient.ReaderSource{Reader: file})
// 即使 err 非空，也保留 report.Segments，并展示 report.Gaps 和中断状态。
```

`AudioSource.Read(ctx, p)` 必须响应取消。`ReaderSource` 仅适用于能及时返回的文件/内存 Reader；任意阻塞设备需要调用方提供可取消的采集实现。`Observe` 在协调 goroutine 执行，不能执行可能长时间阻塞的 UI、磁盘或网络操作。返回前会取消并等待采集、当前尝试和心跳退出；调用方违反上述接口条件时无法保证及时返回。
