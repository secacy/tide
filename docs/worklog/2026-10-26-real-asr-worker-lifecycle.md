# 第七阶段：真实 Worker 的 RPC 与模型生命周期

日期：2026-10-26。承接[文本映射器验收](2026-10-25-real-asr-worker-contract.md)。状态：Worker 纯 Go 核心及可控 Engine/真实 TCP gRPC 验收完成，见文末；原生组装和 Gateway 闭环待实现。本步没有新增真实模型运行数据。

## 目标与当前位置

已有本地原生探针和通过测试的 snapshotMapper。本步在主模块 `internal/realasr` 实现完整的 Worker RPC 流程：接收 PCM、调用模型、发送可修订快照、正常尾部刷新、异常清理。以可控 Engine 验证这条流程，然后接原生实现和真实网络入口，最后做 Gateway/客户端短音频闭环。

这是首个单场短音频接通版本：16kHz、单声道、PCM S16LE，同一 Worker 最多一场，默认每块最多 3200B、每场最多 960000B（30 秒）、文本最多 4096B。不发送 AudioProgress，专用 Gateway 显式关闭 MaxPendingAudioBytes。配置是开发保护参数，不是容量实验结果。

## 方案比较与选择

| 选择点 | 方案及价值 | 本次取舍 |
| --- | --- | --- |
| 执行方式 | 一个 RPC 流程串行 Recv → Accept/Decode → Send；所有模型调用及释放顺序清楚 | 采用；Send 慢会推迟后续输入处理，后续通过实测判断是否拆分 |
| 执行方式 | 接收、推理、发送三个任务，各自推进 | 可重叠计算和网络等待，但需增加两侧有界队列、退出协调及对象所有权；本次不增加 |
| 原生依赖 | Worker 直接 import sherpa | 组装直接，但普通主模块测试会依赖原生构建；选择小型 Engine 接口，由后续独立入口注入实现 |
| 模型资源 | 每场独立创建 recognizer 和 stream | 首版采用，结束后全部释放；重复加载有成本，不能把该版延迟当成预热后的服务性能 |
| 模型资源 | 预加载并共享 recognizer，每场建 stream | 减少加载成本，但需另外明确并发支持、进程关闭和共享对象生命周期；后续优化 |
| 并发准入 | 忙时立即拒绝 | 首版采用一个名额；等待队列会增加延迟与取消语义，当前没有容量依据支撑多场推理 |

双向流允许两端独立上传和接收，不要求 Worker 内部必须拆成两个 goroutine。客户端/Gateway 已并行上传和接收结果；Worker 可以处理一块就返回当前快照，然后继续接下一块。

Engine 只覆盖当前流式模型操作与可控测试所需的失败点，不承诺它是所有 ASR 后端的统一插件标准。模型特定的前后 padding 由 Engine 实现负责，避免把 Zipformer 的 300ms/600ms 配置写成所有 Worker 的通用规则。

## 文件与接口骨架

新增 `internal/realasr/engine.go`：

```go
// Engine 拥有一场识别所需的模型资源；由 RPC 执行流程串行调用。
// 所有方法同步返回；接口本身不承诺正在执行的原生调用可被取消。
type Engine interface {
    // Accept 接收非空、16kHz 单声道 float32 样本；调用返回后不持有调用方切片。
    // 所需前置 padding 由具体实现处理，不能计入原始音频字节。
    Accept(samples []float32) error
    // Ready 表示现在能否执行一步 Decode，不代表原始输入全部处理完成。
    Ready() (bool, error)
    // Decode 同步执行一步解码，仅在 Ready=true 时调用。
    Decode() error
    // Result 返回当前片段的完整可修订文本快照，可以为空。
    Result() (string, error)
    // Finish 声明正常输入结束，由实现提交所需尾部 padding 和 InputFinished。
    // 仅调用一次；返回后仍需 Ready/Decode 排空并读取最终快照。
    Finish() error
    // Close 释放本实例拥有的资源，恰好调用一次；不得与其他方法并发。
    // 不执行正常尾部刷新；只在它返回后才能归还 Worker 名额。
    Close()
}

// EngineFactory 每次成功返回一个独立 Engine，将所有权交给调用者。
// 返回错误时必须返回 nil 并自行清理部分创建的资源；禁止返回带类型的 nil。
// ctx 用于协作取消；同步原生创建若不能中断，调用者仍须等待其返回。
type EngineFactory func(ctx context.Context) (Engine, error)
```

与探针的区别：为失败提供显式 error 返回，padding 移入具体实现。后续 sherpa 包装层可以在无错误返回的原生调用之后返回 nil，但不能因此声称原生崩溃或卡住能够转成普通 error。构造器需检查返回指针，创建 stream 失败时释放已创建 recognizer。

首个原生实现应保持已测顺序：创建后在首次 Accept 中先提交 4800 个零样本，再提交首块；Finish 中先提交 9600 个零样本，再 InputFinished；不开启 endpoint reset。绑定参数来自[探针源码](../../experiments/asrprobe/native.go)和[探针流程](../../experiments/asrprobe/probe.go)，没有新增模型行为假设。

新增 `internal/realasr/worker.go`：

```go
// Config 的零值采用首轮短音频开发默认值；负值非法。
type Config struct {
    MaxAudioBytes int // 整场原始 PCM 上限，默认 960000；须为偶数。
    MaxChunkBytes int // 单个请求的 PCM 上限，默认 3200；须为偶数。
    MaxTextBytes  int // 单条完整快照上限，默认 4096。
}

// Worker 实现流式识别服务；通过 New 创建后以指针使用，不得复制。
type Worker struct {
    asrv1.UnimplementedASRServiceServer
    cfg     Config
    factory EngineFactory // 只在取得名额后调用。
    active  chan struct{} // 容量固定 1；占位覆盖创建、运行及实际释放全过程。
}

// New 校验并复制配置，创建一个名额；不加载模型、不启动 goroutine。
func New(cfg Config, factory EngineFactory) (*Worker, error)

// StreamingRecognize 拥有本 RPC 的 Engine、mapper 和输入计数。
// 正常尾部刷新及 final 发送成功后返回 nil；异常路径不补发 final。
func (w *Worker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error

// drain 执行当前可运行的解码步骤，每步之后尝试发送 partial。
// 这里的排空仅指暂时没有可执行解码步骤，不是已处理字节确认。
func (w *Worker) drain(stream asrv1.ASRService_StreamingRecognizeServer,
    engine Engine, mapper *snapshotMapper) error

// sendSnapshot 读取完整模型快照，经 mapper 转换；nil 响应不发送。
// final=true 只用于 Finish 和最终 drain 均成功之后。
func (w *Worker) sendSnapshot(stream asrv1.ASRService_StreamingRecognizeServer,
    engine Engine, mapper *snapshotMapper, final bool) error

// pcm16Samples 将已经校验对齐的 PCM S16LE 转为新建 float32 切片。
// 调用方保证非空且长度为偶数；本函数不解析 WAV、不重采样。
func pcm16Samples(data []byte) []float32
```

上述为带注释的声明骨架，需补全函数体和 imports 后编译。New 拒绝 nil factory，配置归一化后必须为正；两个音频字节上限须为偶数。PCM 转换使用 `float32(int16(binary.LittleEndian.Uint16(data[i:i+2]))) / 32768`，保持有符号和小端语义。

## RPC 的执行顺序

1. 检查 stream.Context()。尝试以非阻塞 channel 写入取得唯一名额，忙时返回 ResourceExhausted；取得后再次检查取消。立即 defer 归还名额。
2. 调用 factory(ctx)。失败时按下表结束；成功后立即 defer engine.Close()，再检查取消。后注册的 defer 先执行，因此 Close 返回后才归还名额。factory 按契约不应返回 nil,nil；若发生，返回 Internal，不调用 nil 对象。
3. 创建本 RPC 的 snapshotMapper("1", cfg.MaxTextBytes)，初始化原始输入计数 total=0。配置已校验，但仍处理构造错误。
4. 循环 Recv，每次阻塞前后检查取消。nil 请求视为非法；空 data 跳过；非空 data 先校验偶数字节、单块和整场上限，再转换样本。总量检查用 `len(data) > MaxAudioBytes-total`，避免加法溢出。
5. 调用 engine.Accept(samples)，成功后 total 增加原始块字节数，然后 drain。total 仅为本地输入限额计数，不作为模型处理进度发送。一次只持有当前请求和转换切片，不增加应用音频队列。
6. Recv 返回 io.EOF 且当前未取消时，total=0 返回 InvalidArgument；有音频则 Finish → drain → sendSnapshot(final=true)。最后检查取消并返回 nil。这里的空输入与有样本的全静音不同，全静音可以正常 final。
7. 任一调用失败直接返回相应错误，经 defer 清理。发生错误或取消后，不再补做 Finish、补发 final 或执行新解码。

drain 每轮：检查 ctx → Ready → 检查 ctx/错误 → 若 false 则返回 → Decode → 检查 ctx/错误 → sendSnapshot(final=false)，然后下一轮。sendSnapshot：检查 ctx → Result → 检查 ctx/错误 → mapper → 有响应则检查 ctx/Send/检查 ctx。

统一规则：在 Factory、Recv、Accept、Ready、Decode、Result、Finish、Send 的前后检查 context；观察到取消后优先返回 context 对应的 gRPC 错误。检查点保证停止后续工作，不能使检查之后恰好发生的取消与调用启动变成原子操作。Close 属于必须完成的清理，不因 ctx 已取消而跳过。

## 错误与取消边界

| 情况 | 返回 |
| --- | --- |
| RPC ctx 已取消或到期 | `status.FromContextError(ctx.Err()).Err()` |
| Worker 正忙、单块/整场音频超过配置 | ResourceExhausted，固定原因文本 |
| nil 请求、奇数字节、没有任何原始音频就 EOF | InvalidArgument，固定原因文本 |
| 模型创建或方法失败、nil Engine、mapper 拒绝模型输出 | Internal，固定阶段说明，不在客户端错误中插入识别原文或原生错误内容 |
| Recv/Send 传输错误且 ctx 未取消 | 保留原始传输错误；不再刷新或重试 |

本步对文本超限沿用“模型输出不满足适配约束”的 Internal 分类；后续若产品需要区分预算耗尽，再增加明确错误类型。不能一边吞掉错误一边报告正常完成。

**context 取消是停止请求，不是当前原生调用已经停止。** 若取消时 Decode 仍执行，保持名额和模型对象，等待 Decode 返回，检查取消后 Close，最后归还名额。不能把 Decode 放到后台 goroutine、select 收到 ctx.Done 就并发 Close。即使客户端已经观察到 RPC 错误，Worker 的本地推理也可能仍未返回。

同理，厂商原生库若永久卡住，本流程可能一直占着名额。硬终止需要另行进程隔离，不能通过一个额外 goroutine 获得安全强制释放能力。

串行 Send 可能因对端接收慢而阻塞；首轮由 Gateway/调用方的 RPC 取消和已有期限处理，直接 Worker 测试必须设置 RPC deadline。仅给处理函数创建子 context 不会自动中断底层 stream.Recv/Send，因此本步不添加误导性的局部超时。后续独立入口还须设置 gRPC 接收消息上限；业务 MaxChunkBytes 检查发生在解码请求之后，不等于限制此前的网络分配。

## 验证安排与后续

开发者本步实现 Engine/Factory 契约及 Worker 纯 Go 核心，不导入 sherpa。助手随后补可控 Engine 与真实 TCP gRPC 组合测试，覆盖：

- PCM 符号/端序/短末块、空块、输入限额及超限块未交给模型；输入过程中收到 partial。
- 同片段改写/撤回、尾部 Finish/drain/最后 Result/final/EOF 的顺序；静音成功、空输入失败。
- 模型各阶段、结果映射、传输失败不会产生伪成功；所有响应不含 progress。
- 单名额拒绝重入、创建失败归还；Decode 期间取消后不并发释放，Close 尚未返回时名额不能复用，完成释放后新场可接入。

这些替身测试证明 Worker 的调用和生命周期契约，不证明真实原生库可取消。下一步将 native Engine 与启动命令放入独立构建入口（与既有主模块连接），再以固定素材通过现有 Gateway/v2 客户端接通；原生加载、逐步结果、尾部、取消等待和实际释放分别记录。长对话切段、可信处理进度、完整录音和质量对照仍保留。

## 实现评审与验收（2026-10-26）

开发者完成 [Engine 契约](../../internal/realasr/engine.go)和 [Worker 核心](../../internal/realasr/worker.go)。评审确认实现遵循上述串行调用、合法 EOF 收尾、错误/取消优先级和资源所有权设计，未发现需要修改的核心逻辑问题。助手补充 Engine 的非 nil 成功值及 Close 返回后归还名额的注释，没有改动 Worker 逻辑。

助手新增 [可控模型测试](../../internal/realasr/worker_test.go)和 [TCP gRPC 测试](../../internal/realasr/worker_network_test.go)：

- 输入：验证 PCM 符号、小端和独立转换切片，空块跳过、短末块保留、预算恰好用尽仍完成；非法/超限块在 Accept 之前拒绝，空输入与有样本的静音分别结束。
- 结果及尾部：输入尚未结束时交付 partial；同 ID 文本可改写或撤回为空；Finish 后多步 Decode 被排空；即使没有可执行解码步骤，也读取最后快照并发送 final。模型方法、映射和 Recv/Send 错误不会被伪装为正常成功，传输错误按约定保留，模型错误不带原始细节。
- 生命周期：取消在 factory、Recv、各模型方法和 Send 返回时优先于相伴错误；预先取消/到期不创建模型，创建失败归还名额。使用 synctest 与通道控制创建、Decode 和 Close 的阻塞点，分别确认唯一名额未提前释放。
- 真实连接：客户端在关闭输入前收到可修订 partial，半关闭后收到 final 和正常 EOF；静音可完成、空输入返回 InvalidArgument。客户端取消先返回后，服务端在受控 Decode/Close 仍阻塞时拒绝新 RPC 且不再创建模型；两个阻塞解除、handler 实际退出后，同一实例的新 RPC 正常完成。

验证：最终 `go test -race -count=1 ./internal/realasr` 和 `go vet ./internal/realasr` 通过，差异格式及文档链接检查通过。首次纯函数/受控流程测试通过；加入网络测试后的首轮被沙箱禁止本机 bind 阻止，四个网络场景未进入业务断言，允许本机监听后复测通过。补充创建期间名额断言后最终复验通过，没有通过修改核心逻辑消除测试失败。

本轮没有执行真实模型，没有修改其他业务包或主模块依赖，未扩大到全项目回归。测试使用同步可控 Engine，不证明 sherpa 原生调用可以中断，不产生识别质量、推理延迟或并发容量结论。下一步实现原生 Engine 及隔离构建的启动入口，然后验收真实模型经 Gateway 到客户端的短音频闭环。
