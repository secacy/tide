# 第六阶段：唯一 Worker 接收任务与有序事件交接

日期：2026-10-06。状态：接收任务独立及部件组合验收通过，生产协调接入待实现。依据 `0350ff2`、[结果缓冲验收](2026-10-06-result-retention.md#结果缓冲验收2026-10-06)和[内部输入协调验收](2026-10-06-session-upload-coordination.md#内部协调组合验收2026-10-06)。核心由开发者实现，助手负责测试。

## 能力目标与两次编码的范围

目标是让原 Worker 在客户端断开期间继续被读取，识别结果进入有限保留区，处理进度用于内部保护，退出原因由会话统一处理。

这项接入分两次编码：本步实现 workerReceiver，固定阻塞 Recv、事件交付、取消及终态发布契约；随后将它与 resultBuffer 一起并入现有 runCoordinator，补齐双向完成/错误裁决、处理进度保护、尾部期限及结果保留与清理的关系。之后再接客户端投递、确认和真实恢复，并安排端到端验证。

本步包含接收任务和响应解析，不提前改变现有上传协调的退出语义。助手会联动实际 workerReceiver 与 resultBuffer 验证数据交接，但那是部件组合夹具，不能代替下一轮生产协调者接入。

## 为什么需要独立接收任务

现有 v1 download 在 Recv 后直接写 WebSocket，写失败即报告客户端断开。新的逻辑会话需要在断开期间保留 Worker 并继续接收结果；读取任务应将响应交给协调者，由协调者维护结果序号、有限存储及保护状态。

已核对本地 grpc-go v1.83.2 的 stream.go：同一流允许一个 Send 和一个 Recv 并发，但不允许多个 Recv 并发；Recv 的 io.EOF 表示 RPC 正常完成，其余读取错误携带 RPC 失败状态。Send 返回 io.EOF 时需要通过 Recv 取得服务端状态，不能把发送方向 EOF 当成识别成功。

| 方案 | 优点 | 代价及本轮选择 |
| --- | --- | --- |
| 协调者直接 Recv | 数据流直观 | 等下一条响应时妨碍音频命令、恢复和 Timer；不采用 |
| 接收任务直接操作 resultBuffer，再与 ACK 入口加锁协调 | 少一次事件交接 | 接纳结果、确认释放和会话终止分散到多个所有者，增加锁及跨状态一致性约束；不采用 |
| 单个接收任务串行 Recv，协调者接管每条事件 | I/O 与状态所有权清楚，结果/进度保持 Worker 顺序 | 需要可取消的事件交付和可靠的终态发布；采用 |

不为每次 Recv 再启动一个 goroutine。RPC 取消由生命周期拥有者负责，stream 必须响应原 RPC 的取消；不合作的 Recv 不能靠等待方超时被强制回收。

## 普通事件与接收终态

响应分为普通识别更新、累计处理进度两类，作为普通事件交付。EOF、读取错误、非法响应或取消则结束接收任务，将终止原因写入 err，最后关闭 done。

events 使用无缓冲通道。接收任务交付完当前事件后才调用下一次 Recv，不创建额外响应队列。done 关闭前写入 err，生命周期拥有者和协调者必须观察到 done 关闭后才能读取 err；之后 err 不再修改。events 保持打开，不能使用 range events 等待退出。

将终态保存在 err 并用 done 发布，是为了让接收任务在协调者已经退出时仍能完成收尾。若把每条响应和最后错误都无条件写入一个通道，取消后无人读取时会卡住；若仅靠一个小缓冲，也可能被普通响应占满。当前方案让普通事件的交付响应取消，终态发布不依赖消费者继续接收。

正常情况下，最后一个普通事件完成通道交接后，接收任务才可能读到 EOF。未来协调者需要在接收事件的分支内同步保存/处理，再回到 select，因而随后观察到 done 时，前面的事件已经处理。done 可能在当前事件处理过程中关闭；这不表示该事件可以被跳过。

## 文件、结构体与方法

新增 `internal/gateway/worker_receiver.go`，复用 workerStream 及当前 protobuf 响应。

```go
// workerReceiveKind 区分 Worker 的普通响应事件，零值无效。
type workerReceiveKind uint8

const (
    workerReceiveInvalid  workerReceiveKind = iota // 非法零值，不交给协调者。
    workerReceiveResult                           // 一次识别文本更新。
    workerReceiveProgress                         // 累计音频处理进度。
)

// workerReceiveEvent 是一条响应的内部值表示。
// 只携带解析后的字段，不携带 protobuf 指针；字符串保持只读。
// 接收任务不分配结果序号，成功进入 resultBuffer 时才分配。
type workerReceiveEvent struct {
    kind           workerReceiveKind // 响应类别。
    segmentID      string            // result 的片段标识。
    text           string            // result 的此次更新文本。
    isFinal        bool              // result 的片段定稿标志。
    processedBytes uint64            // progress 的累计处理字节数。
}

// workerReceiver 独占一个原 Worker 流的 Recv。
// 通过构造器创建、以指针使用；run 恰好启动一次，不能复用。
type workerReceiver struct {
    events chan workerReceiveEvent // 无缓冲，run 发送，唯一协调者接收；不关闭。
    done   chan struct{}           // run 在停止 I/O 并写好 err 后关闭，恰好一次。
    err    error                   // 仅 run 写；观察到 done 关闭之后才可读取。
}

// newWorkerReceiver 创建独立事件和完成通道，不启动任务或 I/O。
func newWorkerReceiver() *workerReceiver

// decodeWorkerResponse 把一条成功读取的非 nil 响应解析成普通事件。
// nil 响应、progress 混带文本字段返回零值和 errInvalidWorkerResponse。
// 保持普通识别字段原值；进度为零合法，不在这里检查进度单调性/上界。
// 不修改 response，不分配结果序号，不操作缓冲或会话状态。
func decodeWorkerResponse(
    response *asrv1.StreamingRecognizeResponse,
) (workerReceiveEvent, error)

// run 串行读取并交付普通事件，恰好启动一次。
// rpcCtx 必须属于创建 stream 的原 RPC，不绑定某代 WebSocket。
// rpcCtx 和 stream 的 nil 接口是编程错误，入口 panic；typed nil 属于调用方违约。
// stream 必须响应 RPC 取消。run 不调用 Send/CloseSend，也不主动取消 RPC。
// 正常 EOF 将 err 设为 nil；其他终止保存错误身份，最后关闭 done。
// done 只表示接收任务退出，不能表示整个会话已成功或已完成清理。
func (r *workerReceiver) run(rpcCtx context.Context, stream workerStream)
```

新增带说明的内部哨兵错误 errInvalidWorkerResponse。需要描述具体格式问题时可以用 fmt.Errorf 和 %w 包装，调用方能够 errors.Is 判断。

err 的初始 nil 不能被当作“已经成功”。读取前必须先等待/观察 done；通道关闭为写入 err 与后续读取建立同步关系。不要在 run 尚未退出时轮询 err，也不要通过轮询 len(events) 判断接收是否完成。

## 响应解析规则

1. response==nil：明确拒绝，不能通过 nil 安全的 Get 方法把它伪装为空文本结果。
2. response.Progress!=nil：segmentID、text 必须为空，isFinal 必须为 false，否则拒绝；合法时产生 progress 事件，processedBytes 取原累计值，其余字段保持零值。
3. 没有 progress：产生 result 事件，复制 segmentID/text/isFinal 的值，processedBytes 保持零值。沿用目前文本字段的语义，不在此新增非空限制。

进度的单调性、是否超出已接纳/交付的音频范围，需要与协调者的音频状态一起检查，接收任务不并发读取它们。普通结果的 isFinal 只表示片段定稿，不能使接收循环提前退出。

## run 的处理顺序

1. 校验必要参数，登记 defer close(r.done)。非法参数 panic 不属于正常启动后的收尾保证。
2. 每轮开始检查 context.Cause(rpcCtx)。已取消则保存该原因并返回，不再调用 Recv。
3. 同步执行 stream.Recv。调用期间依赖原 RPC 取消解除阻塞，不创建逐次后台读取任务。
4. Recv 返回后先检查 context.Cause。若此时已取消，保存取消原因，丢弃本次响应并返回；这避免底层返回 nil/io.EOF 掩盖已经发生的业务超时。
5. 处理读取错误：errors.Is(err, io.EOF) 时保存 nil 并返回；其他非 nil 错误原样保存并返回。错误优先于 response，遇到 response 与 err 同时非 nil 也不交付响应。
6. 读取成功则 decodeWorkerResponse，解析失败保存解析错误并返回。
7. 通过 select 在 `r.events <- event` 和 `rpcCtx.Done()` 之间等待：交付成功后清除本轮 event 的字符串引用，再进入下一轮；取消则保存 cause 并返回。等待交付时不能继续 Recv 或把响应追加进另一队列。
8. 所有已启动的退出路径先确定 r.err，再由 defer 关闭 done。

与 workerUploader 的结果责任不同：上传任务接收了一次命令，需要给该命令唯一结果；接收任务的普通事件流可以在整场取消后停止交付，终态仍由 err/done 发布。取消与交付同时就绪时，select 可能完成一次交付；下一轮会再检查取消。未来协调者同样要检查自身生命周期，不能宣称取消瞬间后绝无事件交接。

取消检查也定义了结果的观察点：检查之后才发生的取消可能与 EOF/错误发布竞争，协调者在决定整场结果时仍需执行统一的取消/超时优先级判定。receiver 保存的是接收方向事实，不独自裁决完整业务结果。

## 有界性与资源责任

接收任务至多持有一个尚未交付的普通事件；事件交付后协调者可能仍在处理它，同时接收任务读取下一条响应。因此无缓冲通道不等于没有在途数据。resultBuffer 会接管字符串副本；gRPC 当前解码对象、协调者当前事件和框架缓冲仍需纳入后续链路预算。

原响应和其字符串保持只读，接收任务不用额外字符串复制，也不把 protobuf 指针传给协调者。后续设置 gRPC 单响应上限，避免先解码巨大消息才发现结果预算不足；本步不声称总内存已严格受结果缓冲预算限制。

会话终止时由唯一生命周期拥有者取消原 RPC，然后等待 uploader.done 和 receiver.done。即使协调者不再消费 events，取消分支也应允许接收任务退出。若 Recv 在取消后仍有收尾过程，done 必须等到底层真正返回，不能提前关闭。

解析/读取失败本身不在 receiver 内取消 RPC；下一轮协调者处理 done 后决定如何取消并清理另一个方向。独立运行时调用方也必须承担这项责任，不能把 receiver.done 当成上传任务已经退出。

## 助手验收计划与下一轮接入约束

开发者实现 worker_receiver.go。助手随后测试：

- 构造不启动、对象通道独立，必要参数校验。
- 文本与进度交错保持读取顺序；同片段多次更新均交付，final 后继续读取。
- nil 成功响应、progress 混带三个文本字段分别拒绝；零进度合法；响应带错误时以错误为准。
- 无消费者时第一条事件等待交付，不启动下一次 Recv；交付后才继续读取。
- 最后一条普通事件交接后才发布正常 EOF，原读取错误身份保留；done 之后读取 err。
- 启动前取消、Recv 阻塞中取消、事件交付等待中取消、底层取消后返回 nil/EOF，原因与退出行为正确。
- 没有事件消费者时仍能取消并发布终态；模拟 Recv 延后收尾时 done 不提前关闭。
- 由单一协调夹具将真实 receiver 事件写入 resultBuffer：文本依序编号，进度不占结果序号；满额后夹具明确取消并等待 receiver，不能在汇报或下一次读取处遗留任务。

下一轮在同一协调循环处理这些事件，重点解决：

1. 普通结果接管成功后才分配序号；断开不停止接收，结果超限由协调者明确终止。
2. 处理进度与音频接纳/交付状态统一校验，恢复/重复音频不重复增加计量，并继续接入积压保护。
3. Send 的 EOF 与 Recv 最终状态分别处理；正常结束需结合合法 end、上传和接收事件，不能因 select 先读到某个 done 就跳过已交付结果。
4. 尾部期限从合法 end 起计算，断开、恢复和重复 end 不重置；普通问诊等待响应不能直接套用这条期限。
5. Worker 计算完成与客户端结果交付完成分别处理。正常 EOF 后仍有未确认结果时，要保留投递/恢复所需数据和有界终态信息，不能直接把 resultBuffer 清空当作整场完成。

## 接收任务验收（2026-10-06）

开发者完成 `workerReceiver`，助手补充错误哨兵及零值注释、编写 [接收任务测试](../../internal/gateway/worker_receiver_test.go)。新增 8 个顶层测试、23 个叶级场景；与结果缓冲、内部输入/上传及恢复状态等相关部件合计 116 个顶层、329 个叶级场景（含父测试共 389 项）通过定向 race，无失败或跳过。运行命令：

```sh
go test -race ./internal/gateway \
  -run '^Test(WorkerReceiver|DecodeWorkerResponse|ResultBuffer|SessionUpload|WorkerUploader|SendWithTimeout|AudioInputBuffer|AudioInputState|SessionControl|ResumableSession|SessionRegistry|ResumeState|SessionIdentity)' \
  -count=1 -timeout=45s
```

测试确认文本、进度与片段定稿按 Worker 顺序交接，普通事件无缓冲，未交付前不会启动下一次 Recv；EOF、读取错误、非法响应和取消分别发布终态。取消时若底层 Recv 仍在收尾，`done` 保持未关闭，直到底层真实返回；没有事件消费者时，等待交付也可因取消退出。夹具让真实接收任务将结果依序写入 `resultBuffer`，进度不占结果序号；缓冲满时由夹具取消原 RPC 并等待接收任务退出。

这些是独立任务与部件组合的正确性结果，没有正式负载、性能数据，也没有把 receiver 接入生产 `runCoordinator`。原 RPC 的取消与双向终态判定仍由下一轮协调者负责；客户端确认、真实连接恢复和端到端资源清理尚待实现与验证。
