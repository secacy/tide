# 分段 Worker 与原生流生命周期接入指导

日期：2026-10-27。设计代码依据 `72c1459`。状态：后续经开发者委托，助手已完成本组实现、可控/网络验收和真实短验证，见[接入验收](2026-10-27-segmented-worker-validation.md)；120 秒实验仍待实施。以下保留原实现指导。[分段计数](2026-10-27-segment-cursor-validation.md)已先行通过组件测试。

## 本次能力和后续两步

本次要接通一个完整能力：同一 RPC 内，一段达到音频上限就正常刷新、定稿和释放模型流；后续音频使用新的流，复用原 recognizer。整个过程中持有同一个 Worker 名额。客户端 end 落在片段中间或恰好边界时，都正确完成整场。

实现完成后，助手补可控 Engine、TCP gRPC 和 Gateway/v2 组合测试；然后登记并运行真实模型 120 秒、四段验证。这一轮不先增加新的并发负载或端点策略。

## 为什么选择扩展接口

| 做法 | 取舍 |
| --- | --- |
| 给所有 Engine 强制增加分段方法 | 统一接口，但旧短场工厂、测试替身和包装器都必须支持并不需要的能力 |
| 在 Accept 内隐式创建下一流 | Worker 难以知道换流何时成功、何时能更换 mapper/编号；不采用 |
| 类型断言判断可选分段能力 | 能减少一个入口，但配置行为依赖运行时对象；不采用 |
| **独立分段接口、工厂类型和显式构造入口** | 旧模式保持短场语义，新模式编译期要求分段能力；增加少量构造与分支，本轮采用 |

固定分段与端点重置的比较沿用[总设计](2026-10-27-long-dialogue-segmentation-design.md)。本轮不会把删除模型流简化成 Reset：首次选择删除并新建流，是为了建立明确的旧对象释放边界。流状态重建的时间与 RSS 表现仍需测量。

## 1. 定义分段接口和入口

在 `internal/realasr/engine.go` 增加下面的独立接口；原 Engine 和 EngineFactory 方法集合保持。Engine 注释中的 Finish 改为“结束当前模型流输入”；原单段模式的当前流仍覆盖整场。

```go
// SegmentedEngine 在一场 RPC 内串行复用模型，逐段拥有不同模型流。
// 工厂成功返回时，第一段模型流已经存在，与旧 Engine 的初始化方式一致。
// 除 Close、ReleaseSegment 外，其余方法必须有当前流，不能隐式重建。
type SegmentedEngine interface {
    Engine

    // ReleaseSegment 实际释放当前模型流，保留模型实例。
    // 串行调用，重复释放空流无操作；即使 ctx 已取消也必须执行清理。
    // 不调用 Finish、不输出 final、不创建下一流，不检查业务 final 是否发送。
    // 正常路径由 Worker 保证先完成 Finish/drain/final；异常由整场 Close 清理。
    ReleaseSegment()

    // StartSegment 要求原流已释放、整场未 Close 且 ctx 有效。
    // 只创建一份新模型流，成功后可 Accept；不会重新加载 recognizer。
    // 失败不留下可继续使用的半成品流，调用方结束整场，不重试或回退旧段。
    // 调用中的原生创建不能硬取消，返回后仍须检查 ctx 并清理。
    StartSegment() error
}

// SegmentedEngineFactory 成功返回带首段流的独立 Engine；不返回动态 nil。
// 失败自行清理部分资源。ctx 作用与原 EngineFactory 一致。
type SegmentedEngineFactory func(ctx context.Context) (SegmentedEngine, error)

// NewSegmented 显式启用固定长度分段；整场配置归一化规则与 New 一致。
// maxSegmentBytes 必须为正偶数，且不大于归一化后的 cfg.MaxAudioBytes。
// 每段上限不采用隐式默认值；构造不创建模型、不启动任务。
func NewSegmented(cfg Config, maxSegmentBytes int,
    factory SegmentedEngineFactory) (*Worker, error)
```

Worker 增加两个私有字段：`segmentedFactory SegmentedEngineFactory` 和 `maxSegmentBytes int`。原 `factory EngineFactory` 保留。两个构造入口各设置一个工厂，拒绝 nil；运行时不会同时设置。公共 `Config` 不增加另一个同名含义的音频字段，整场预算继续来自 `cfg.MaxAudioBytes`。

抽出 `normalizeConfig(cfg Config) (Config, error)`，复用当前默认值和校验；`NewSegmented` 再调用 newSegmentCursor 检查两个上限。保留现有 New 中合法配置的行为，不顺便增加新限制。

## 2. 收拢整场名额，分开单段与分段执行

建议把现有 StreamingRecognize 的非阻塞准入保留在最外层，取得名额后注册归还 defer，再根据构造时选定的工厂进入对应分支：

```go
// StreamingRecognize 取得一次整场名额，等待分支清理实际结束后归还。
// 分段结束和 ReleaseSegment 都不能直接归还此名额。
func (w *Worker) StreamingRecognize(stream asrv1.ASRService_StreamingRecognizeServer) error

// recognizeSingle 承接现有取得名额之后的单段流程，沿用原协议和错误。
// 工厂成功取得 Engine 后立即 defer Close，包括创建返回时已经取消的情况。
func (w *Worker) recognizeSingle(stream asrv1.ASRService_StreamingRecognizeServer) error

// recognizeSegmented 每场只调用一次分段工厂；串行拥有 engine、cursor 和 mapper。
// Close 的 defer 在本函数内执行完毕后，外层才能释放整场名额。
func (w *Worker) recognizeSegmented(stream asrv1.ASRService_StreamingRecognizeServer) error

// finishSegment 完成当前流的 Finish、剩余解码和最后一次快照发送。
// 按现有规则在每个模型/I/O 调用前后检查取消；任一步失败立即返回。
// 不释放流、不换段、不归还整场名额；调用方在成功后执行 ReleaseSegment。
func (w *Worker) finishSegment(stream asrv1.ASRService_StreamingRecognizeServer,
    engine Engine, mapper *snapshotMapper) error
```

原 drain、sendSnapshot 和 pcm16Samples 可以直接复用；调用它们时，Engine 必须仍有当前流。单段与分段流程可以复用 finishSegment，避免两个尾部实现逐渐不一致。两个分支各自保留明确的工厂返回/取消检查，不必引入通用回调框架压缩几行代码。

**defer 的所有权：** 分支创建成功即 defer engine.Close；分支返回会先执行 Close，再返回最外层，最后由外层归还 active。即使中间已经 ReleaseSegment，整场 Close 仍需释放 recognizer。不能用“发送了最后 final”替代实际 Close。

## 3. 分段循环的局部状态和顺序

本轮无需新的并发任务。函数局部保存：

```go
cursor        *segmentCursor // 已提交的原始数量和编号，不代表解码进度。
mapper        *snapshotMapper // 当前段快照；释放旧段后置 nil，不保留历史。
segmentActive bool            // 是否拥有可用于当前段收尾的模型流。
```

工厂已创建首段流，因此初始化 cursor 为第 1 段，mapper ID 为 `"1"`，segmentActive=true。这个布尔量区别于 cursor 的满额计数：满额时可能还没收尾；final 及释放都成功后才设 false。它由当前串行循环独占，不需要 atomic 或锁。

### 每个请求

1. Recv 前后检查 context，继续沿用现有 nil 请求、空块、奇数字节和单块超限规则。空块直接忽略，不创建下一流。
2. 对整个非空块先调用 cursor.plan(len(data))。超出整场上限时直接报原有 `ResourceExhausted / audio stream limit exceeded`，不创建下一流、不提交前缀。
3. 当该块仍有剩余字节：如果 segmentActive=false，则 StartSegment 成功后 cursor.advance，再创建新编号的 mapper，最后设 active=true。任一步失败结束整场，由 defer Close 清理。StartSegment 返回后必须先检查取消，不能继续推进编号或 Accept。
4. 对这份剩余量重新 plan 得到正数 n，转换 `data[:n]`，同步 Accept。保留模型错误与取消检查；成功后 commit(n)，再 drain。commit 若意外失败是内部契约错误，终止 RPC，不能再次提交该前缀。
5. 当 `cursor.segmentBytes == cursor.maxSegmentBytes`：finishSegment 成功后 ReleaseSegment，等待返回；再检查取消，清空 mapper，segmentActive=false。达到上限时立即收尾，不等待下一次 Recv。
6. 将 data 前移 n 字节；若仍有字节，下一轮先启动下一段；否则继续 Recv。

一个请求可能跨多个段，不能假设 `MaxChunkBytes <= maxSegmentBytes`；分段预算很小时同样要成立。plan 校验的是尚未提交的请求余量，推进 data 后再计算，不能把原始请求长度反复计入预算。

### EOF

| 条件 | 处理 |
| --- | --- |
| cursor.totalBytes==0 | 沿用空输入错误；不 Finish，不伪造空段 final |
| segmentActive=true 且已有输入 | finishSegment 成功后 ReleaseSegment，然后检查取消并正常返回 |
| segmentActive=false | 前一段已在满额时完成刷新和释放；检查取消后直接返回成功 |

边界 EOF 不调用 StartSegment 或 advance，也不重复发送旧 final。片段达到整场总上限后，如果又收到非空音频，仍按整场超限失败；已经发出的片段 final 不会使超限整场变成成功。

### 错误与取消

旧段 final 发送失败时不 StartSegment；调用方无法确定客户端是否收到该消息，因此不补发，整场失败。StartSegment、Finish、Decode 等错误继续用固定的 Internal 描述，不向客户端回传原生详情或识别文本。传输错误保持现有规则；观察到取消优先返回 context 对应状态。

任意异常都由 defer Close 释放当前剩余资源，包括没有输入的初始流、新段创建成功后取消，以及新段尚未 Accept 就发生错误。错误之后不补做正常 Finish，不创建下一段，不发整场成功。ReleaseSegment 和 Close 的释放过程不被 ctx.Err 提前跳过。

## 4. 原生 Engine 的两级释放

在 `workers/sherpa/internal/native/engine.go` 让现有私有 engine 实现 SegmentedEngine；同一实现供旧工厂和新工厂使用，旧入口只暴露较小方法集合。

```go
// ReleaseSegment 同步删除当前 online stream；即使已取消仍清理。
// 保留 recognizer；stream==nil 时无操作，重复调用不重复删除。
func (e *engine) ReleaseSegment()

// StartSegment 检查未关闭、ctx 有效、recognizer 存在且 stream==nil。
// 新流成功建立后重置 leadingPadded=false、finished=false。
// 创建前后检查取消；失败清理本次创建的流，不留下半成品。
func (e *engine) StartSegment() error

// Close 先标记整场关闭，再 ReleaseSegment，最后删除 recognizer。
// 重复 Close 无操作，不能由于 context 取消而跳过释放。
func (e *engine) Close()
```

StartSegment 使用局部变量接收 NewOnlineStream 的返回值；若原生创建返回后发现取消，删除该局部流再返回。直到所有成功条件满足才把流交给 e.stream。此绑定无法检查内部 C 指针的限制保留，不把 Go 包装指针非 nil 当作严格原生分配保证。

为原有 Accept、Ready、Decode、Result、Finish 增加“当前流存在”的检查，先执行已有 closed/context 检查，再拒绝无流，避免释放后误调用进入 C。可以新增 checkStream 调用原 check 再检查 stream；原 check 继续只检查关闭和取消，StartSegment 需要在无流状态下通过它。ReleaseSegment 不调用这些带 context 的检查。Accept 的空样本检查、Finish 的重复及无输入检查仍保留。

前置 300ms、尾部 600ms padding 改为**每段**独立。Result 注释更新为“当前段完整可修订快照”。新流不继承旧文本；不要把旧段 final 拼进下一段 Result。

工厂增加 `native.NewSegmentedFactory(cfg Config) (realasr.SegmentedEngineFactory, error)`。提取共享的配置校验/复制辅助函数；两个入口都调用现有 newEngine 建立 recognizer 和首段流，并保持错误时返回真正 nil。旧 NewFactory 的配置规则和创建顺序继续保持，避免用断言将旧工厂结果转成新接口。

## 5. 显式组装与日志包装

本轮同时提供一个可实际启动的入口，避免只在内存测试中接通分段。建议在 sherpa Worker CLI 增加：

- `--max-audio-bytes`：整场上限，默认仍为 960000，正偶数。
- `--segment-audio-bytes`：0 表示旧单段模式，正偶数表示分段模式；不得大于整场上限。

例如后续 120 秒实验的明确配置为 `--max-audio-bytes=3840000 --segment-audio-bytes=960000`，当前不启动该实验。两个字段进入 config 和 listening 配置日志，解析阶段拒绝非法组合；单块 3200B、文本 4096B 和 gRPC 接收上限 4096B 继续适用。

segment=0 的分支使用旧 NewFactory/realasr.New/日志包装，分段分支使用 NewSegmentedFactory/realasr.NewSegmented。共用剩余服务启动与退出流程。

**注意现有 loggedEngine 只嵌入 realasr.Engine，会隐藏分段方法。** 新增明确的 `loggedSegmentedEngine`，嵌入 realasr.SegmentedEngine，Close 仍在底层 Close 实际返回后记录 engine_closed；新工厂静态返回分段接口。不要经旧 loggedFactory 包装后再断言，也不能漏掉 StartSegment/ReleaseSegment 的透传。首版可以只保留已有模型创建/释放日志，真实逐段流观测由助手随实验工具补充，不能用 model_created 数充当流创建数。

## 建议编码顺序与验收

按本组能力连续实现：接口及构造 → 原生分段方法/工厂 → Worker 外层名额和分段循环 → CLI 显式模式及包装。可以分次编译，但这些部分接通前不报告“真实模型已支持长对话”。测试由助手负责，不需要开发者先写测试替身。

写完后助手重点验收：

- 一个请求跨两段或多段；partial/final 的 ID 正确，原始样本不丢不重；片段 final 后下一段仍能上传。
- 满段后立即 Finish/drain/final/释放，边界 EOF 不多一空段；最后短段、全静音、空块和整场空输入。
- 每场工厂调用一次、名额取得一次；ReleaseSegment 阻塞期间其他 RPC 仍报忙；整场 Close 实际返回后才放名额。
- Finish、final 发送、换流、取消及整场超限的失败路径；已完成旧段保留，失败整场不冒充正常完成。
- 原生无流状态保护、取消后释放和重复 Close；日志包装能完整透传接口；CLI 默认旧模式和明确分段参数均可用。
- 旧模式回归、TCP gRPC 以及 Gateway/v2 多片段正常完成与 ACK 清理。真实 SDK 调用及资源趋势另用显式模型实验验证，纯 Go 替身不能代替。

本轮没有新增测试结果或长音频数字。已有计数组件通过不能替代以上集成验收。
