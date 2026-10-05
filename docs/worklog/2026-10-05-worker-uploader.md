# 第六阶段：唯一 Worker 上传任务

日期：2026-10-05。状态：设计与实现指导，等待开发者实现。依据 `de37f05` 和[有界音频接管验收](2026-10-05-audio-input-buffer.md#有界音频接管验收2026-10-05)。核心由开发者编写，助手补测试和验收。

## 目标与方案选择

audioInputBuffer 已能保存已接纳音频并限制预算。下一步执行实际 stream.Send，同时保持协调者可处理恢复、期限和停服。上传任务负责阻塞 I/O，协调者负责缓冲和状态；两者通过任务和完成事件交接。

| 方案 | 好处 | 代价及决定 |
| --- | --- | --- |
| 在协调者中直接 Send | 调用顺序简单 | Worker 阻塞时会妨碍恢复、Timer 和取消命令处理；不采用 |
| 每块创建一个 goroutine | 容易将单次阻塞移出协调者 | 仍需限制任务数并保证同一 stream 的发送顺序，不能并发 Send；没有必要 |
| 每场一个上传任务，串行 Send/CloseSend | 顺序与所有权集中，不增加逐块 goroutine | 需要明确任务交付、结果和退出规则；本轮采用 |

已核对本地依赖 grpc-go v1.83.2 的 stream.go：同一 stream 不支持并发 SendMsg，也不支持 CloseSend 与 SendMsg 并发；发送与接收可分别由一个任务执行。SendMsg 成功不保证服务端已经收到，更不表示处理完成；CloseSend 后仍需接收响应来判断最终 RPC 状态。发送过的消息也不应修改，依赖库可能延后读取它。

沿用已有 sendWithTimeout，不重写发送超时机制。它在调用 goroutine 中同步等待 Send 真正返回，超时通过取消 RPC 解阻塞；替身及适配器必须遵守 RPC 取消后退出的契约。CloseSend 在同一上传任务中串行执行，本轮没有给它新增独立期限，也不承诺任意自定义 stream 都可被强行终止。

## 所有权与通道

- 协调者独占 audioInputBuffer 的 offer/take/complete/acceptEnd；上传任务不持有缓冲对象，也不读取其中字段。
- 协调者 take 后，借用仍占预算，借出的 payload 只读。上传任务为本次 Send 创建请求，不修改或复用该请求及其 data。
- jobs 为无缓冲通道：发送成功表示上传任务已接收。协调者同一时刻最多保留一个待交付或等待结果的任务，收到并处理结果后才能交付下一个。
- results 容量固定为 1，且只有上传任务写入。任务被接收后恰好发布一次结果，即使 RPC 已在执行期间取消。
- done 只由上传任务退出时关闭。jobs 和 results 都不关闭；协调者不能用 range results 等待结束。

结果通道容量 1 的依据是“一次只允许一个未处理任务”，不是任意的缓冲优化。此前结果必须已被协调者取走，才可能提交下一任务，因此新结果有自己的空位。协调者即使停止读取，也不会阻塞这一个结果的发布。违反这个交付约束后，容量 1 不再提供该保证。

结果只携带类型、音频起点与错误，不携带 payload。发布前清除本轮 job.chunk 中对音频的引用，不再主动持有借用数据；gRPC/跟踪库是否仍持有引用不属于本缓冲区的预算保证，且不得通过复用数据试图强制回收。

## 文件、类型和方法

新增 `internal/gateway/worker_uploader.go`。使用已有 workerStream、bufferedAudio 和 sendWithTimeout。

```go
// workerUploadKind 表示上传任务支持的操作。
type workerUploadKind uint8

const (
    workerUploadInvalid   workerUploadKind = iota // 无效零值，不能执行 I/O。
    workerUploadAudio                            // 发送一块只读音频。
    workerUploadCloseSend                        // 半关闭输入并结束上传任务。
)

// workerUploadCommand 是协调者交付的一次操作。
type workerUploadCommand struct {
    kind  workerUploadKind // 操作类型。
    chunk bufferedAudio    // audio 时为 take 的借用；CloseSend 时必须为零值。
}

// workerUploadResult 是一个已接收操作的唯一结果，不携带音频引用。
type workerUploadResult struct {
    kind   workerUploadKind // 对应操作，不能将 CloseSend 成功误认作音频发送成功。
    offset uint64           // audio 的起点；合法 CloseSend 为 0。
    err    error            // 原发送结果、取消原因或内部命令错误。
}

// workerUploader 管理一场会话的串行上传任务。
// 通过构造器创建，以指针传递；run 恰好启动一次。
// 协调者收到并处理前一结果后，才可提交下一任务。
type workerUploader struct {
    jobs    chan workerUploadCommand // 无缓冲，唯一协调者发送，run 接收。
    results chan workerUploadResult  // 容量 1，run 写入，唯一协调者读取。
    done    chan struct{}            // run 退出时关闭；不表示 Worker 响应流结束。
}

// newWorkerUploader 创建三个独立通道，不执行 I/O、不启动任务。
func newWorkerUploader() *workerUploader

// run 串行执行发送和半关闭操作，由生命周期拥有者恰好启动一次。
// rpcCtx/cancelRPC 必须属于创建 stream 的同一 RPC，不绑定某次 WebSocket。
// stream 必须有效且响应 RPC 取消，sendTimeout 必须为正。
// rpcCtx/cancelRPC/stream 的 nil 接口和非法期限属于编程错误，入口 panic。
// 每个已接收任务发布一次结果；空闲取消可直接退出；退出前关闭 done。
// 不调用 Recv，不操作缓冲，不在正常半关闭后取消 RPC。
func (u *workerUploader) run(
    rpcCtx context.Context,
    cancelRPC context.CancelCauseFunc,
    stream workerStream,
    sendTimeout time.Duration,
)
```

stream 的有效性由启动方保证，包含 nil 动态指针的接口也不是有效 stream；本步入口无需新增反射校验。已构造 uploader 的通道及容量保持不变，不允许外部替换、关闭，或为同一对象重复启动 run。

新增 errInvalidWorkerUploadCommand 哨兵错误，并注明：未知 kind、空音频、带非零 chunk 的 CloseSend 均属于内部非法命令。合法 CloseSend 的 chunk.offset=0、chunk.data=nil；零长度但非 nil 的 data 同样不符合零值约定。

## run 的实现顺序

1. 检查必要参数，登记 defer close(u.done)。参数不合法的 panic 不在正常退出保证内。
2. 每轮开始先检查 context.Cause(rpcCtx)。若已取消，直接退出，避免明知取消仍继续接任务。
3. select 等待 rpcCtx.Done 或从 jobs 接收一个命令。内部约定 jobs 永不关闭；可用双返回接收并将意外关闭视为编程错误，不能解释为客户端 end。
4. 一旦接收命令，就负责给它一次结果。先准备 kind/offset，再检查 context.Cause；若已取消，发布带该原因的结果并退出，不执行 I/O。这覆盖 select 在取消与交付同时就绪时接收了命令的情况。
5. 校验命令。非法命令发布 errInvalidWorkerUploadCommand 后退出，不调用 Send/CloseSend。上下文取消优先于命令校验。
6. audio 命令构造新的 StreamingRecognizeRequest{Data: job.chunk.data}，调用 sendWithTimeout，结果保留其错误身份，不重试、不调用 complete。
7. CloseSend 命令调用 stream.CloseSend，随后若 RPC 已取消则使用 context.Cause 作为错误，否则保留原调用结果。CloseSend 不消费音频，也不验证缓冲是否排空：该前提由协调者保证。
8. 保存结果后清空 job.chunk，直接执行 u.results <- result。这里不 select rpcCtx.Done，否则发送超时自己取消 RPC 时，可能把最重要的超时结果丢弃。
9. audio 成功后进入下一轮；audio 失败、非法命令，或执行过 CloseSend（无论成功失败）都在发布结果后退出。正常 CloseSend 后不能 defer cancelRPC，否则会中断尚未返回的识别结果。

命令被接收后，取消发生在执行前检查之后时仍可能开始 I/O；sendWithTimeout 继续执行自己的取消检查，最终以其返回值汇报。不能声称取消瞬间后绝无任何 I/O 调用；需要保证的是结果明确、后续不重试、合作式取消能够结束任务。

## 错误与完成的语义

| 情况 | 上传任务行为 | 协调者后续责任 |
| --- | --- | --- |
| audio 结果 err=nil | 发布成功，继续等任务 | complete(offset) 释放预算，再安排下一块 |
| audio 返回 io.EOF 或其他错误 | 发布原错误并退出，不重发 | 不 complete；停止继续上传，结合 Recv 权威状态及现有终止规则收尾 |
| Send 超时 | sendWithTimeout 取消 RPC、等待 Send 返回；发布 ErrWorkerSendTimeout 后退出 | 保留超时分类并执行终止清理，不将其改写成普通 canceled |
| 空闲时 RPC 取消 | 直接退出，不凭空创建结果 | 已发起的会话终止继续等待 done |
| 已接收任务在取消后被检查 | 发布取消原因后退出 | 该任务未成功，不 complete |
| CloseSend 成功 | 发布半关闭结果，上传任务退出 | 继续接收尾部结果，不能判整场成功 |

uploader 不因为普通发送错误或 io.EOF 主动增加一次 cancelRPC；最终 RPC 错误仍应尽量由接收方向识别。已有 sendWithTimeout 的超时取消保持不变。任务退出也不等于整个 RPC 已完成或会话名额可以归还。

发布结果先于关闭 done，但接收方 select 不保证先读到 results。未来协调者将 results 用于操作结果，done 用于退出等待；如主循环观察 done，必须先处理仍待消费的结果，再判断是否异常退出，不能直接覆盖或丢弃结果。生命周期已经终止时，缓冲留待统一清理，也不能将未处理成功事件当成重发依据。

## 与缓冲区连接的方式

后续协调者通过 take 保存一个待交付任务，在自己的 select 中启用 jobs 发送分支；不能在控制命令处理函数中阻塞等待 jobs 交付。等待交付期间，缓冲仍记为 inFlight，占用预算。交付后关闭该发送分支，等待结果，避免同一块被交付两次。

只有成功 audio 结果才调用 complete。全部音频 complete 且 inputDrained 为 true 时，再安排一次 CloseSend 命令，并记录已经安排/执行，防止持续为真的排空条件重复触发。接收和状态判定继续由各自任务/协调者负责。

取消顺序为：停止安排新任务，取消属于原流的 RPC，等待 uploader.done，再释放剩余缓冲及其他资源。因为已接收任务的结果有独立空位，收尾方无需为帮助它退出而无限读取事件。若 stream 不响应取消，等待超时只能报告清理未完成，不能将仍在运行的任务当成已释放。

## 本步范围与助手验收计划

本轮实现上传任务部件，先不修改 v1 session/upload，不给现有 runControl 塞入半套 Worker 生命周期。助手将使用小型协调夹具串行调用 audioInputBuffer，再通过 uploader 的通道执行可控 stream，验证二者交接；后续再合并到真实逻辑会话协调者。

测试计划：独立通道与构造不启动；音频 FIFO 且无并发 Send；结果只含元数据；发送阻塞期间预算保留；协调夹具继续处理其他事件；成功后完成释放并复用预算；错误/EOF 不释放或重试；超时取消仍完整发布错误；结果暂不消费时任务可退出；空闲取消；取消与已接收命令的结果责任；音频全完成后半关闭一次；半关闭不取消仍可接收尾部的 RPC；非法命令不做 I/O；结果先发布、done 随后关闭。可控替身不自动证明真实 gRPC 或生产恢复行为，实际网络接入另验收。

本步仅记录设计，未修改运行代码或新增运行结果。接口、字段和方法均需保留解释所有权、参数、取消与结果语义的注释。
