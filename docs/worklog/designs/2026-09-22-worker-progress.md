# Worker 处理进度与积压边界（计量已接入，积压限制待实现）

## 问题与已有依据

第四阶段已完成发送、尾部等待、结果写回期限和单实例准入。暂停实验仍显示：3 秒暂停期间单次 Send 最大耗时仅 0.131–1.311ms，结果等待却可达到约 2.81 秒。Send 返回不能证明音频已被处理，现有期限不能完整限制输入阶段的下游积压。

本步先建立处理进度信号，后续再实现未确认音频的计量与限制。截至 2026-09-23，协议、生成代码、Gateway 下载分流、Mock 进度发送及计量器链路接入已完成并验证；积压限制和改造后实验结果尚未完成。

## 方案比较与选择

- 仅依赖发送期限：实现已有，但无法识别 Send 成功之后的处理落后。
- 增加有界 channel：可限制新增的本地排队，不能单独限制已经交给 gRPC/Worker 的音频；增加读写协程与收尾责任。暂保留逐块读取、串行发送。
- 规定多久必须产生识别文本：简单，但静音或文本不更新不一定意味着未处理音频，不适合作为通用进度依据。
- Worker 显式汇报累计处理字节数：可以独立于文本观察处理位置，代价是扩展内部协议，并要求 Worker/未来适配器提供有明确含义的进度。选择此方案，先验证 Mock 链路。

## 协议第一小步

保留现有 StreamingRecognizeResponse 的 1、2、3 号字段，新增 `AudioProgress progress = 4`；新增消息 AudioProgress，其中 `uint64 processed_audio_bytes = 1`。

progress 存在时，这条响应只表示进度，原文本字段应为空/false；progress 不存在时，按已有文本结果解析。Gateway 的分流和校验在后续小步实现，在此之前 Mock 不发送新消息。相比完整改为 oneof，这样可以保留已有文本构造与测试；代价是字段互斥依赖显式校验，而非 protobuf 结构保证。

processed_audio_bytes 表示本次流从起点连续完成处理的原始音频字节数，不包括控制消息、传输编码和消息头。按流单调不减，不能跨会话共享。仅 Recv 成功或进入内部推理队列不算完成处理；Mock 在 ProcessingDelay 完成后推进。静音也应有处理进度，不能依赖 PartialTexts 是否用尽。

进度不是文本定稿，不代表客户端已收到对应识别结果，也不代替响应流正常结束。未来真实模型如不能提供可信进度，需要明确能力限制，不能用入队确认冒充处理确认。

## 后续小步与验证计划

1. 开发者修改 proto 并重新生成代码；助手检查字段语义与构建。此步不启用新消息。
2. Gateway 分流进度与文本，再让 Mock 在处理完成后独立汇报进度。助手补充进度单调、暂停/取消、静音或文本耗尽、尾部结果不受影响的测试。
3. 维护已接收音频与已确认处理位置，定义未确认音频预算、报告粒度和超限策略。计数先后顺序必须允许 Worker 的确认早于本地 Send 返回；不能简单用 Send 返回后的计数校验确认。
4. 用正常、短暂停顿、持续降速/停滞场景验收，记录最大未确认音频量、超限退出、清理与名额复用。阈值、数据与收益待实验确定。

该差值包括传输、处理及确认返回中的音频，不能直接称为 Worker 队列长度，也不是端到端转录延迟。限制该值不等于限制客户端、系统缓冲及模型内部的全部内存。音频时长换算依赖约定格式；不在本步宣称总内存上界或稳定容量。

## 2026-09-23 核对与下一小步

- 原文本字段编号 1/2/3 保留；新增 progress 为 4，累计值为 uint64，注释符合约定。
- 在临时目录重新执行 protoc，两个生成文件与工作区内容逐字节一致；没有覆盖开发者文件。
- `go test ./... -run '^$'` 通过（仅构建，不是全量行为回归）。
- `go test -race ./internal/gateway -run '^(TestGatewayNormalEndPreservesTail|TestGatewayClientDisconnectIsolated)$' -count=1 -timeout=30s` 通过。
- 下一小步仅修改 download：Recv 错误处理之后、文本转换之前，检测 progress 是否存在。存在则校验 segment_id/text 为空、is_final 为 false；非法混合响应归为 resultWorkerFailed，合法进度直接继续接收，不向 WebSocket 发送。不以累计值是否大于零判断消息类型。
- 暂不保存进度、不校验单调性或音频总量、不新增协程，也不让 Mock 发送进度。先补好分流入口，后续接入计量和发送；当前不能据此宣称积压已受控。
- 选择在现有 download 内直接分支，避免为两类固定消息引入额外路由与同步；完成后由助手补分流、非法混合字段和零进度的行为测试。

## 2026-09-23 下载分流验收与 Mock 发送计划

开发者已完成 download 分流：按 progress 是否存在判断，非法混合消息归为 resultWorkerFailed，合法进度继续接收，不向客户端写回。当前不保存累计值，也不约束积压。

助手新增 [progress_routing_test.go](../../../internal/gateway/progress_routing_test.go)：

- 零值、非零值和重复进度不开始结果写入；后续 Recv 原始错误保留。
- segment_id、text、is_final 单独混入及全部混入时，均归为 Worker 失败并停止接收。
- WebSocket TCP + gRPC bufconn 完整链路中，进度与文本交替，三条预期文本按序返回，尾部前后进度不泄漏，正常关闭为 1000。
- 混合消息导致 1011 并取消 Worker；进度之后、合法 end 之前的 Worker EOF 仍为 1011。
- 三种链路场景均观察 Worker 退出和 tracker 等待完成，不依赖服务主动取消。

`go test -race ./...` 通过。本次为行为回归，不提供吞吐、容量或延迟改善结论。

下一小步由开发者实现 Mock 发送：每块有效音频完成 ProcessingDelay 并更新 totalBytes/processedChunks 后，发送一次累计进度，位置在 partial 循环之前。空块、非法 PCM 及处理期间取消不得新增该块进度；PartialTexts 耗尽不影响汇报。发送失败立即返回，不另开 goroutine。

方案取舍：逐块确认容易验证且没有额外定时器；按当前 100ms 音频块实时输入时约每秒 10 条进度，但这不是所有负载下的发送速率上限。固定周期/批次汇报能减少消息数，却引入确认滞后与尾部刷新规则，后续需要时再调整。附带于文本会再次依赖文本产出，不采用。

新增 sendProgress(stream, processedAudioBytes uint64) 辅助方法只构造并发送 progress，不调用 ResponseDelay。此时进度代表每块模拟处理完成；ResponseDelay 仍可能延后文本，进度不证明文本已生成或交付。真实模型适配必须重新确认进度依据。Mock 启用后，助手负责更新旧测试对响应数量的假设，并补处理前不提前确认、文本耗尽/暂停/取消等测试。

## 2026-09-23 Mock 初稿检查（错误传播待修正）

Mock 已在 ProcessingDelay 完成、累计计数更新后调用 sendProgress；消息只携带 progress，发送成功路径符合约定。sendProgress 使用 `%w` 保留底层错误，但 StreamingRecognize 调用处又通过 `status.Errorf(codes.Internal, ..., err)` 将所有失败改成 Internal 并丢失原始错误链。此处应直接返回 sendProgress 已包装的错误，与 sendPartial 的调用方式一致。

助手更新了 worker/pause/stall 测试夹具：保留全部响应与发送时刻，另外提供仅文本的视图，原有文本数量与时间断言继续生效。新增 [progress_test.go](../../../internal/mockasr/progress_test.go) 覆盖变长静音 PCM 累计、空块、非法 PCM、文本耗尽、逐流计数重置、处理完成时刻、ResponseDelay 分离、暂停恢复、永久停读及发送失败。

验证结果：

- 全量 race 运行中 Gateway 包通过，Mock 包未通过。
- 新增暂停测试自身的运行中记录读取曾触发 race；已通过加锁快照修正。随后重新执行 `go test -race ./internal/mockasr`，没有报告 race，仅剩发送错误保留测试的 4 个子用例失败。
- 普通错误、Canceled、DeadlineExceeded、Unavailable 均被调用处改写为 Internal；errors.Is 也无法再匹配原始错误。不是 4 个独立实现问题，而是同一处重新包装造成。
- 其余新旧 Mock 行为测试通过。尚不能标记本步完成或全量回归通过；等待开发者将该调用处改为 `return err` 后复验。

## 2026-09-23 Mock 修正后验收

调用处已改为直接 `return err`。全量 `go test -race ./...` 通过，包括原先失败的四个错误保留子用例；Gateway 与 Mock 均通过。Mock 逐块汇报至此完成本步验收，当前 Gateway 对合法进度仍直接 continue，尚无累计计量与超限行为。

## 单会话音频进度账目设计（独立组件已完成）

目标是保存网关已完整接收的音频字节数与 Worker 累计确认的处理字节数，获得未确认处理音频量，之后才决定限制策略。每个 session 将独立拥有一个 audioProgress；它不是 sessionTracker，不管理会话注册或连接生命周期。

并发方案比较：

- 单会话 mutex：将两项计数的校验、更新、快照作为整体保护，临界区短，便于维护不变量；选用此方案。锁不跨会话共享，不覆盖任何 I/O。
- 独立 atomic 计数：能保护单个值，却仍需协调组合读取、进度合法性校验及更新，不作为当前默认选择。
- 将计数事件全部交给协调 goroutine：状态可以集中写入，但需要新增事件顺序、事件传递和退出责任；当前仅计量，不引入该复杂度。

本次只新增 internal/gateway/audio_progress.go 中的独立类型及方法，不接入 session/upload/download，不增加阈值、计时器或 goroutine：

- audioProgress：包含 mu sync.Mutex、receivedBytes uint64、processedBytes uint64；零值可用，使用后不可复制。
- audioProgressSnapshot：receivedBytes、processedBytes、pendingBytes 三项 uint64，作为持锁期间取得的值快照；pendingBytes 由 receivedBytes - processedBytes 计算，不额外存入计量器。
- addReceived(n uint64) error：增加收到的原始音频字节数，零允许；加法前检查 uint64 溢出，失败不改状态。
- acknowledge(n uint64) error：n 是累计值，合法范围为 processedBytes <= n <= receivedBytes；重复值允许，回退或超出接收总量返回错误且不改状态。先验证再赋值。
- snapshot() audioProgressSnapshot：在同一把锁下取得两个计数及差值，不返回内部指针。

所有方法使用指针接收者，并用同一把锁保护状态。每个字段、类型和方法应注明含义、并发/零值约定及错误时状态不变。

将来接入时，upload 必须在完整读取音频后、调用 Send 前记录 receivedBytes。否则 Worker 的确认可能先于本地 Send 返回被 download 处理，造成合法进度被误判为越界。记录的是已接收量，发送失败不能据此回滚成未接收；现有会话失败清理逻辑仍负责结束该流。此顺序在本次仅解释，下一小步再实现。

待助手补充：初始/重复确认、变长块、回退/越界及溢出时状态不变、快照一致性与并发 race 测试。该对象只计量，不保证实际结果延迟、总内存或稳定容量。

## 2026-09-23 独立计量器验收与接入计划

开发者已实现 audioProgress，校验顺序、溢出判断、同锁更新与快照均符合设计。助手按用户要求补充了三个方法的注释，未修改业务逻辑。

新增 [audio_progress_test.go](../../../internal/gateway/audio_progress_test.go) 覆盖累计/增量含义、重复确认、回退/越界失败后状态不变、uint64 极值与溢出、值快照和实例独立性，以及并发累加/确认/快照一致性。`go test -race ./internal/gateway -run '^TestAudioProgress' -count=1 -timeout=30s` 通过。此轮仅验证未接入的独立组件，没有重复运行网络回归，也没有容量或积压控制结论。

下一小步由开发者接入现有链路，不增加阈值或队列：

1. session 增加 `progress audioProgress` 值字段；零值可用，不增加构造参数，也不放到共享 Gateway 中。
2. upload 的二进制消息分支中，在 sendWithTimeout 之前调用 `s.progress.addReceived(uint64(len(data)))`。失败立即返回，不能继续 Send 或忽略错误；成功后释放计量器锁再执行 I/O。发送失败不回滚已接收计数。
3. download 在进度消息互斥校验通过后调用 acknowledge，失败按 resultWorkerFailed 返回并用 `%w` 保留原因，成功再 continue。原文本分支保持不变。
4. 接收计数溢出属于本地计量失败，不应归为 Worker 报错或客户端主动断开。新增 `resultInternalFailed`，upload 对该错误返回此类别；finish 先取消 RPC，再尝试 1011、reason 为 `internal error` 的关闭，随后取消 WS，并处理关闭错误。沿用现有集中收尾方式，不在 upload 内直接关闭资源。

接入取舍：按值放入 session 避免可选指针/初始化遗漏；方法在既有 upload/download 中直接调用，短锁内计量，锁外 I/O。暂不强制正常 EOF 时 processedBytes 必须等于 receivedBytes，兼容只返回文本的已有 Worker；若后续要求 Worker 必须提供进度，再明确能力和协议规则。

接入后的助手测试计划：确认可早于本地 Send 返回而不被误拒、非法进度退出与清理、正常尾部和不带进度 Worker 回归，以及计数溢出在发送前终止。当前仍不能宣称积压受控。

## 2026-09-23 计量接入验收

开发者已完成 session 独立持有计量器、upload 在 Send 前登记、download 保存并校验累计确认，以及 resultInternalFailed 的集中收尾。检查未发现需要修改的实现问题；助手只更新测试和文档。

新增 [progress_integration_test.go](../../../internal/gateway/progress_integration_test.go)，并调整 [progress_routing_test.go](../../../internal/gateway/progress_routing_test.go)：

- 下载分流单测先登记 3200 字节，再接收 0/3200/3200 的累计确认；最终接收量与处理量均为 3200，未确认量为 0。
- 受控 gRPC 接口替身在 Send 返回前交付确认和文本。完整 session.run 测试在客户端收到文本后检查 Send 仍未返回，且累计接收/处理均为 3200；随后允许 Send 返回并发送 end，正常关闭为 1000。该测试固定事件顺序，不依赖自然竞争概率，也不代表真实网络延迟。
- 真实 gRPC 编解码（bufconn）与 TCP WebSocket 链路覆盖确认超过接收量、确认倒退；均异常关闭为 1011，Worker 观察到取消，tracker 等待完成，服务 context 保持有效。
- 下载端非法确认不修改最后合法计数，并保留底层校验错误链。
- 发送失败后，完整接收的 3200 字节仍保留在账目中，不回滚；上传返回原有 Worker 失败原因。
- 通过预置极值模拟计数溢出：完整 run 在调用 Worker Send 前失败，累计值保持不变，客户端收到 1011 / internal error，RPC 被取消、Recv 退出，run 返回保留错误链的溢出原因。
- 全量 `go test -race ./...` 通过，包含既有正常尾部、不带进度 Worker、会话隔离和清理回归；`git diff --check` 通过。

本步收益是运行链路具备可信的未确认音频计量和非法进度处理，不是已限制积压。下一步建议在增加积压阈值之前，对正常、短暂停顿和持续降速运行记录接收/确认/未确认音频变化，形成当前协议版本下的对照基线；需说明采样峰值是否可能漏掉瞬时变化。旧实验版本尚无进度消息，不能将旧时序数据直接当作当前版本的计量基线。

## 2026-09-23 积压基线完成

[EXP-004-10](../../experiments/progress-backlog-baseline.md) 已完成九次正式测量。正常、暂停 500ms 和持续降速的网关未确认量观测峰值分别为 0.1、0.5、4.3 秒音频。持续降速尾部等待为 10.134–10.153 秒，最大 Send 为 1.202–1.208 秒，仍在已有操作期限内。

实验还明确了计量边界：降速组客户端 end Write 返回后，网关账目内未确认量为 4.1 秒，但另有 1 秒已完成客户端 Write 的音频尚未登记到网关。因此以后限制 pendingBytes，只能说明对网关已读取音频的控制，不能直接宣称所有上游缓冲、端到端延迟或总内存都已受控。

周期采样不改变生产代码，但可能漏峰；实际最大样本间隔 49.61ms，已原样记录。全量 race 验证含三个完整场景，正式测量另行关闭 race；保存原始日志、汇总、CSV、源码哈希和 patch。尚未加入积压限制，下一步应先比较等待、拒绝/结束和丢弃音频等策略的业务代价，再确定预算及控制点。
