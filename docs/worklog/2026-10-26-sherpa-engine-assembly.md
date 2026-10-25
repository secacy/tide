# 第七阶段：sherpa 原生 Engine 与运行入口

日期：2026-10-26。承接 [Worker 核心验收](2026-10-26-real-asr-worker-lifecycle.md)。状态：按开发者随后明确委托，助手完成原生适配、独立入口、测试及本机真实模型/gRPC 进程验收，见文末；Gateway/v2 客户端闭环仍待接通。前半部分保留实现前方案。

## 本步能力与分工

已有文本映射器、纯 Go Worker，以及可控 Engine/真实 TCP gRPC 验收；真实模型仍只在独立探针里运行。现在把已验证的 sherpa 调用包装为 realasr.Engine，再由独立命令将它交给 Worker。

开发者本轮实现原生 Engine/Factory 和下述模块声明；助手在开发者完成后评审，补充启动入口、测试与运行验证。以可运行 Worker 为一个能力验收，不把每个包装方法拆成单独阶段。之后接现有 Gateway/v2 客户端，验证固定短音频闭环。

## 依赖布局的备选与选择

| 方案 | 优点 | 代价及决定 |
| --- | --- | --- |
| sherpa 直接加入主模块 | 一个模块，构建入口集中 | 把原生依赖带入主模块依赖图；本次不选 |
| 主模块内使用构建标签隔离原生文件 | 默认构建可排除原生实现 | 仍需维护同一模块的原生依赖与标签组合；本次不选 |
| 独立 Worker 模块，通过本地 replace 引用主模块 | 原生构建单独维护，复用现有协议与核心，不影响根目录常规测试 | 多一个 go.mod/go.sum，需单独构建和测试；本次采用 |
| 将探针直接扩成服务 | 可复用现成原生代码 | 混合实验取证与运行服务职责；保留探针作为独立行为依据 |

计划布局：

```text
internal/realasr/                 # 已有：主模块的协议与生命周期核心
experiments/asrprobe/             # 已有：独立模型行为探针
workers/sherpa/
    go.mod
    go.sum                       # 后续 tidy 生成
    internal/native/
        factory.go               # 本轮开发者实现：配置和 Factory
        engine.go                # 本轮开发者实现：原生对象和方法
    cmd/asr-worker/               # 助手随后补齐启动入口
```

新建 `workers/sherpa/go.mod`，初始声明：

```go
module github.com/secacy/tide-artisan/workers/sherpa

go 1.26

require (
    github.com/k2-fsa/sherpa-onnx-go-macos v1.13.8
    github.com/secacy/tide-artisan v0.0.0
)

replace github.com/secacy/tide-artisan => ../..
```

模块路径保持在主项目命名空间内，文件也位于该仓库，可以导入 `github.com/secacy/tide-artisan/internal/realasr`；无需复制核心、protobuf 或改为 public 包。写完实际 imports 后，在新模块运行 `go mod tidy`，生成自己的间接依赖和 go.sum。根模块和探针模块的依赖不变，不新增 go.work。当前构建目标限定本机 macOS+cgo，Linux/GPU 入口后续另行验证。

## Factory：配置先检查，模型按场创建

`factory.go` 建议骨架（需补 imports 和函数体）：

```go
// Config 描述当前固定 int8 Zipformer 后端，不包含网关或 RPC 限额。
type Config struct {
    ModelDir string // 四份模型/词表文件所在目录，必填；不包含音频输入。
    Threads  int    // 原生 CPU 线程数；0 默认 2，合法范围 1～16。
}

// NewFactory 校验并复制配置，将目录固定为绝对路径，检查预期文件。
// 它不创建 recognizer/stream；成功返回的工厂每次创建独立 Engine。
func NewFactory(cfg Config) (realasr.EngineFactory, error)

// newEngine 在同一执行流程内创建 recognizer 和 stream。
// 成功移交完整所有权；失败或创建期间取消时，清理已取得资源。
func newEngine(ctx context.Context, cfg Config) (*engine, error)
```

NewFactory 检查目录非空、线程范围和以下四份文件可打开、为非空普通文件，然后捕获归一化后的配置值。不要捕获调用方可变指针，不在每次 RPC 中重新进行路径解析。

```text
encoder-epoch-99-avg-1.int8.onnx
decoder-epoch-99-avg-1.int8.onnx
joiner-epoch-99-avg-1.int8.onnx
tokens.txt
```

模型配置沿用 [探针原生实现](../../experiments/asrprobe/native.go)：16kHz、feature dim 80、三个 int8 模型、tokens、CPU、greedy_search、配置线程数，EnableEndpoint=0，不调用 Reset。文件名/格式检查不等于权重摘要校验；正式接通仍使用探针固定 revision/摘要的资源，由助手在运行验证时核对并保存。

创建顺序：检查 ctx → NewOnlineRecognizer → 检查 nil/ctx → NewOnlineStream → 检查 Go 返回值/ctx → 返回 Engine。recognizer 一旦取得，就必须在后续失败路径释放；stream 一旦取得，按 stream 后 recognizer 顺序释放。工厂返回错误时必须返回真正的 nil Engine，不能将 nil 的 *engine 转成非 nil 接口返回。闭包应显式处理 `newEngine` 的错误后，再把成功指针作为 realasr.Engine 返回。

## Engine：包装调用与状态

`engine.go` 建议结构与方法：

```go
// engine 只由一场 RPC 串行使用，不加锁，也不启动后台推理任务。
type engine struct {
    ctx          context.Context          // 原 RPC 的取消信号；不拥有 cancel。
    recognizer   *sherpa.OnlineRecognizer  // 本场独占，晚于 stream 释放。
    stream       *sherpa.OnlineStream      // 本场独占，先释放。
    leadingPadded bool                     // 已实际提交前置零样本，不代表原始音频处理进度。
    finished     bool                     // 已调用 InputFinished，禁止再次输入。
    closed       bool                     // 已开始释放，禁止再使用。
}

var _ realasr.Engine = (*engine)(nil)

// check 检查未关闭及 ctx；不调用原生 API。
// 不因 finished=true 拒绝 Ready/Decode/Result，尾部仍要继续读取。
func (e *engine) check() error

func (e *engine) Accept(samples []float32) error
func (e *engine) Ready() (bool, error)
func (e *engine) Decode() error
func (e *engine) Result() (string, error)
func (e *engine) Finish() error
func (e *engine) Close()
```

各方法应补中文注释，说明下面的约束。状态错误使用固定信息，不包含识别原文。无需引入锁、通道、sync.Once 或新的处理进度字段。

| 方法 | 调用顺序与约束 |
| --- | --- |
| Accept | check；拒绝 finished 和空 samples；首次先提交 4800 个零样本，再设置 leadingPadded；检查 ctx 后提交原始 samples，再检查 ctx |
| Ready | check → IsReady → check；允许 Finish 后继续调用，不推断处理字节 |
| Decode | check → Decode → check；Ready 前提由 Worker 保证，无须再重复检查 |
| Result | check → GetResult/Text → check；返回完整快照，不追加、不分段、不定稿 |
| Finish | check；拒绝重复 Finish 和未提交过输入前置 padding 的状态；提交 9600 个零样本，检查 ctx，再 InputFinished；设置 finished 后检查 ctx |
| Close | 已关闭则直接返回；设置 closed，先 DeleteOnlineStream 再 DeleteOnlineRecognizer，各自非 nil 才删除并置 nil；不检查 ctx，不调用 Finish |

4800/9600 分别为当前 16kHz 配置的 300ms/600ms，沿用已测探针，不是所有模型通用参数。零 padding 都由适配器提供，Worker 的原始音频预算不包含它们。Accept 的空切片必须在进入绑定前拒绝，因为固定版本通过 `&samples[0]` 向 C 传样本地址。

每个原生调用前后都检查 ctx；Accept/Finish 各包含两个原生调用，因此中间也要检查，观察到取消后不再启动下一次调用。正在执行的调用仍须同步返回，才能继续清理。Close 可做串行幂等防护，但不能因此被其他 goroutine 并发调用；Worker 仍负责恰好一次正常调用。

## 绑定的实际可观察边界

核对对象为本机已固定的 `sherpa-onnx-go-macos v1.13.8/sherpa_onnx.go`，版本与[探针归档](../experiments/results/real-asr-probe-2026-10-25/README.md)一致：

- NewOnlineRecognizer 检查底层 C 指针，为 nil 时返回 Go nil；newEngine 必须处理。
- NewOnlineStream 总会先创建 Go 包装对象，再赋值内部 C 指针；内部字段不公开。检查 Go 指针非 nil 不能证明底层指针非 nil，不宣称已经完整处理该分配失败分支。
- AcceptWaveform、Decode、InputFinished 等方法没有 error 返回。GetResult 在绑定内部直接读取原生结果指针后转换为 Go 数据；适配层拿到结果之后再检查 Go 指针，也无法兜底该内部失败。

因此这里的 error 表达配置、状态、可观察创建失败和 context，不会把所有底层崩溃、内存错误或卡住变成普通 RPC 错误。首版不修改第三方绑定、不用 unsafe 读取私有字段，也不以 recover 声称解决 C 故障。若后续需要覆盖这些原生失败，另行比较受控绑定补丁与进程隔离；目前仅在固定模型、本机单场范围接通。

## 启动入口与验收计划

原生实现完成后，助手在同一独立模块补 `cmd/asr-worker`：校验参数、创建 Factory 和现有 realasr.Worker、注册已有 protobuf 服务、本机监听。初始参数为 model-dir 必填、threads=2、listen=127.0.0.1:50051；Worker 沿用 30 秒/3200B/4096B 配置和单名额，gRPC MaxRecvMsgSize=4096，允许当前 3200B PCM 请求及 protobuf 开销。

启动时只检查配置/文件，不预热加载模型，因此“开始监听”不等于“模型成功加载”。模型每场创建，在首轮报告中将加载与识别、尾部分别记录。参数解析/帮助不应先调用原生创建。暂不增加 debug HTTP、模型选择器或 Linux/GPU 兼容层。

停服先给予五秒自然完成窗口，超时后取消 RPC/关闭连接，再等待实际 handler 清理。本地已核对 gRPC v1.83.2 的 `WaitForHandlers(true)` 可用于让 Stop 等待 handler；入口将配合设置并验证，而不是把 Stop 返回默认解释为所有模型已释放。原生永久卡住可能导致实际退出继续等待，五秒不是进程硬退出保证；超时中断不报告正常完成。

验证由助手完成：配置/文件/取消边界测试；以固定资源运行真实 Engine 的语音、静音、短末块、正常释放与取消；通过启动命令接 gRPC，再接 Gateway/v2 客户端。完整输入和实时结果、尾部和最终结束、释放与再次接入分别核实。不能用此前可控 Engine 的取消结果代替原生证据，也不能凭本轮单场运行推算并发容量或识别准确率。

本步不要求开发者编写测试或启动命令；完成 factory.go、engine.go 及模块声明后，由助手把其余组装和验证接上。根项目的常规测试继续独立执行，新模块必须单独构建和验收。

## 委托实现与验收（2026-10-26）

开发者随后将本步实现委托助手。完成 [workers/sherpa](../../workers/sherpa/README.md) 独立模块：Factory 配置/文件校验与失败清理、串行原生 Engine、按场模型加载、gRPC 启动入口与实际释放后名额归还；根模块与探针的 go.mod/go.sum 均未修改，未新增 go.work。模块通过本地 replace 复用既有 Worker 和 protobuf，没有复制核心。

### 验证范围

- 独立模块 `go test -race -count=1 ./...` 与 `go vet ./...` 通过。普通测试检查参数/帮助、模型资源拒绝、取消/关闭状态和空样本守卫、Factory 不提前创建模型，以及真实 TCP 下自然停止、监听异常和强制停止后继续等待 handler 清理。
- 显式真实 Engine 测试通过：2.035s 静音含短末块，Finish 后仍可排空/读空文本，重复 Finish/结束后输入被拒绝；另一次在真实模型接收一块后取消，Finish 返回取消，Close 返回并清空拥有指针。没有声称取消恰好发生在 C 调用内部。
- 非 race Worker 可执行程序的真实进程测试通过：帮助退出 0、非法线程与缺失文件非零退出；同一服务进程顺序执行下表场景，最后空闲 SIGTERM 退出 0。强制停服等待由受控 handler 测试证明，本轮真实模型进程没有做“推理中 SIGTERM”实验。

### 真实 gRPC 运行

窗口 **2026-10-26 02:08:42–02:09:03（Asia/Taipei）**。Go 1.26.5、darwin/arm64/cgo、CPU 两线程；绑定 v1.13.8，模型 revision 与七份资源 SHA 均匹配探针。五次模型在每场重新创建，操作系统缓存未清除，不称五次冷启动。输入为文件内容的 100ms 分块排期，不是实时麦克风采集。

| 场景 | 原始 / 客户端 Send 成功字节 | 响应 / final | RPC 发起→首条非空文本 | CloseSend 开始→EOF | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| 示例 0，10.053125s | 321700 / 321700 | 17 / 1 | 1029.341ms，全部输入前 | 50.477ms | 正常完成 |
| 示例 1，5.1s | 163200 / 163200 | 12 / 1 | 1327.360ms，全部输入前 | 33.809ms | 正常完成 |
| 静音，2.035s | 65120 / 65120 | 1 / 1 | null | 46.934ms | 空文本正常完成 |
| 示例 0，首条 partial 后取消 | 321700 / 35200 | 1 / 0 | 1028.303ms | null | Canceled，无 final |
| 取消后探测 0 | 65120 / 3200 | 0 / 0 | null | null | Worker 忙，ResourceExhausted |
| 取消后探测 1 | 65120 / 65120 | 1 / 1 | null | 46.761ms | 释放后新场完成 |

前两场的 17/12 条包含一次额外 final，与探针的文本变化数不直接当作新增识别收益。所有消息同 segment ID，无 progress。客户端消费的是整段快照，最后文本与探针相同；仍含 `TODAY IS LIBR` 和 `叫叫` 等原始内容，无独立人工参考，CER 继续为 null。

五次 model_created 和五次 engine_closed 按 ID 交替对应；加载 468.975–515.658ms，Close 包围段 4.721–5.831ms。记录证明这些释放调用返回，不证明无内存泄漏。取消后探测 0 保留了先前会话尚未归还名额时的忙状态；探测之间人为等待 50ms，不能据此报告“恢复耗时 50ms”。忙时客户端 Send 返回的 3200B 只是传输调用成功，不表示 Worker 接纳或模型处理。

首字口径包含本场模型加载、客户端排期和网络等待，与旧探针从加载后计时不同；尾部还包含 final 发送、模型释放和 RPC 结束，不能与旧探针的本地刷新耗时直接相减作为优化。没有新增性能对照、稳定容量、临床准确率或可信处理字节结论。

### 证据与限制

[证据归档](../experiments/results/sherpa-worker-smoke-2026-10-26/README.md)含六次 RPC 原始结果/摘要、进程日志、两项真实验证输出、构建前源码摘要与源快照、Go 构建/二进制/原生库摘要。40 文件逐份读回 SHA 一致，搬移后冻结脚本复核一致，摘要与逐条消息/创建释放日志独立核对通过；运行后编写的复核脚本拒绝伪造进度、错误文本和错误输入量三个反例。所有已登记构建前 Go 源码/依赖摘要与运行后文件一致；权重、WAV、可执行文件及动态库不纳入归档，保留固定资源下载与构建信息。

常规测试、真实 Engine 和真实进程首轮均通过，没有替换不利结果；忙时探测原样保留。准备阶段一次 gofmt 使用了错误相对目录，随后从模块目录更正，未作为测试成功或业务失败计数。依赖从已有本地缓存取得，没有改动第三方绑定或新增网络模型服务。

下一步：专用 Gateway 显式关闭未知处理进度预算，验证真实模型 → Worker → Gateway → v2 客户端的短音频、快照替换、正常 completed 与取消清理。长对话分段、可信处理进度、完整录音/延后转录、说话人及质量对照仍未完成。
