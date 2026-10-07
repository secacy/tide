# 第六阶段：v2 新建与恢复握手

日期：2026-10-07。状态：首条消息协议与限时读取部件已实现并验收，公开 v2 入口待接。下文保留设计依据，末节记录实际实现与测试证据。

## 当前位置和目标

completed/completed_ack 已完成内部链路和真实 WebSocket 验收。现有公开入口仍是 /v1/asr，handler 在升级前通过 tracker.tryEnter 占位，defer leave 与单条连接绑定。可恢复逻辑会话、注册表和候选连接交接尚未接入公开入口。

下一能力是客户端通过公开入口新建或接回同一逻辑会话。分三步推进：

1. **本步：首条消息协议与限时读取。** 完整读取一条文本消息，严格解析为 start 或 resume；成功只产生握手请求值，不创建 Worker、不查注册表、不启动 attachment。
2. **入口接线：双层准入、注册与所有权。** 增加独立 /v2/asr 路由；新建占逻辑名额，恢复复用原名额；临时连接另受握手预算限制。统一停服与实际资源清理后归还名额。
3. **公开网络闭环：保护与客户端。** 组合入口失败分类、空闲/断连检测与恢复，再接客户端有限音频缓存、结果去重和自动重连，最后进行故障实验。

后两步仍需单独设计和验收，尤其是停服与接管并发、初始 ready 丢失后的身份未知场景、活动对象撤销后的有界终态查询。当前完成协议不自动解决这些问题。

## 方案与权衡

| 选择 | 收益 | 代价与结论 |
| --- | --- | --- |
| 原 /v1/asr 中按首条消息混合 v1/v2 | 外部路径少 | 输入格式、生命周期、准入规则同时分叉；当前采用独立 /v2/asr，便于逐步验收，代价是维护两个版本入口 |
| 用 URL 查询参数传恢复凭据 | 升级前容易路由 | 凭据更容易进入访问日志；采用升级后的首条应用消息 |
| 将握手与已附着输入放进一个解析器 | 入口函数少 | 不同阶段允许的消息混杂；采用 DecodeV2Handshake 与已有 DecodeV2Control 分开，复用底层 JSON/数值校验 |
| 恢复也重新占逻辑会话名额 | 可以照搬 v1 handler | 满额时已保留会话无法恢复，且重复计数；采用逻辑名额与临时连接名额分开管理，后续接线时落实 |

握手解析回答“客户端提出了什么请求”；凭据校验回答“是否有权访问该会话”；协调命令回答“此刻能否接管连接”。三个判断分别完成。lookup 成功、token 匹配均不能越过协调者直接修改 resumeState。

## 首条应用消息

新建使用已有 StartMessage，Version 固定为 v2：

```json
{"type":"start","version":"v2"}
```

恢复新增 ResumeMessage：

```json
{"type":"resume","version":"v2","sessionId":"原 ready 的 sessionId","resumeToken":"原 ready 的 resumeToken","appliedSeq":"12"}
```

appliedSeq 是客户端已经连续应用的结果序号，不是最后收到或最大见到的序号。12 表示 1～12 均已应用。Gateway 在现有 requestResumeConnection 中继续检查它是否处于可恢复范围，并原子提交确认与新代连接。

恢复请求不自报 generation；新代次由服务端分配。也不使用客户端自报的音频 offset 改写服务器位置：恢复成功后 ready.nextOffset 告诉客户端从哪里继续。

## 协议类型与解析约束

在 protocol.go 增加 MessageTypeResume；新增 internal/wsprotocol/handshake_v2.go：

```go
// ErrInvalidV2Handshake 表示首条消息的 JSON、类型、版本或字段非法。
// 可用 errors.Is 判断；错误文本不得包含恢复凭据或原始报文。
var ErrInvalidV2Handshake = errors.New("invalid v2 handshake")

// ResumeMessage 是新连接请求接回原逻辑会话的首条消息。
// 包含恢复凭据，禁止把整个结构体写入普通日志。
type ResumeMessage struct {
    Type        MessageType `json:"type"`              // 固定 resume。
    Version     string      `json:"version"`           // 固定 v2。
    SessionID   string      `json:"sessionId"`         // 原 ready 返回的稳定 ID。
    ResumeToken string      `json:"resumeToken"`       // 原 ready 返回的恢复凭据。
    AppliedSeq  uint64      `json:"appliedSeq,string"`  // 连续已应用结果位置，0 合法。
}

// V2HandshakeKind 区分新建与恢复请求，零值非法。
type V2HandshakeKind uint8
const (
    V2HandshakeInvalid V2HandshakeKind = iota
    V2HandshakeStart  // 请求新建逻辑会话。
    V2HandshakeResume // 请求接回原逻辑会话。
)

// V2Handshake 是完成线格式校验的请求值，不代表身份或附着资格有效。
// 不保存原始报文；含凭据，不得整体输出到普通日志。
type V2Handshake struct {
    Kind        V2HandshakeKind // 决定下面哪些字段有效。
    SessionID   string          // 仅 resume，原样保留，不裁剪或规范化。
    ResumeToken string          // 仅 resume，原样保留。
    AppliedSeq  uint64          // 仅 resume，连续已应用结果序号。
}

// DecodeV2Handshake 解码一条完整的首条文本消息，不涉及网络或会话状态。
// data 必须是单个 JSON 对象；失败返回零值和 ErrInvalidV2Handshake。
func DecodeV2Handshake(data []byte) (V2Handshake, error)
```

内部 wire 结构体的 type/version/sessionId/resumeToken/appliedSeq 全部用 json.RawMessage，区分字段缺失与 null/默认零值。直接反序列化到 ResumeMessage 不足以验证必需字段。

解析顺序和规则：

1. 复用 decodeSingleJSONObject，要求单个完整对象；拒绝数组、null、多对象和尾随垃圾。
2. 复用 decodeRequiredString 读取 type/version，要求 type 精确为 start 或 resume，version 精确为 v2；空值、缺失、null 和非字符串拒绝。
3. start 不得携带 sessionId/resumeToken/appliedSeq 中任一字段，即使是 null。避免恢复请求被误写成 start 后悄悄创建新问诊。成功只设置 Kind。
4. resume 的 ID/token 必须是存在且非空的字符串，保留原值；解析器不校验 Gateway 内部身份编码，也不查找或认证。空白字符串不当作空串规范化，它无法通过后续精确身份查找/凭据匹配。
5. appliedSeq 必须存在，复用 decodeUint64String；非空 ASCII 十进制字符串，允许 0、前导零及 uint64 最大值，拒绝数字类型、符号和溢出。
6. 其他未知字段忽略，重复同名字段沿用现有 encoding/json 后值覆盖行为；start 上拒绝恢复字段是明确的已知字段分支约束。
7. 所有失败都返回 V2Handshake{}，errors.Is(err, ErrInvalidV2Handshake) 为 true。底层校验 helper 目前返回 ErrInvalidV2Input，在握手边界改为本步哨兵并附固定字段名/原因。不要直接拼接报文、未知 type/version 或 token 值。

## 限时首条读取

新增 internal/gateway/handshake_v2.go，复用 connectionReadConn，不新增拥有连接的对象：

```go
// ErrHandshakeTimeout 表示 v2 首条消息读取/校验超过本次期限。
var ErrHandshakeTimeout = errors.New("handshake message timeout")

// errInvalidV2HandshakeReaderConfig 表示 nil 依赖、非正期限或非正消息上限。
var errInvalidV2HandshakeReaderConfig = errors.New("invalid v2 handshake reader config")

// readV2Handshake 同步读取并校验恰好一条首条文本消息。
// ctx 是入口操作生命周期；timeout 限制本次读取和解析，必须为正。
// maxMessageBytes 是握手消息上限，必须为正，在第一次 Read 前设置。
// 连接仍由调用方拥有；本函数不写 ready、不关闭连接、不创建 goroutine。
// 返回前必须等待实际 Read 返回；错误时握手值始终为零。
func readV2Handshake(
    ctx context.Context,
    conn connectionReadConn,
    timeout time.Duration,
    maxMessageBytes int64,
) (wsprotocol.V2Handshake, error)
```

实现顺序：参数校验 → 检查父 ctx 已取消 → 建立 context.WithTimeout → 设置读取上限 → 同步 Read 一次 → 判定取消/超时/读取错误 → 检查 MessageText → DecodeV2Handshake → 再判定取消/超时 → 返回请求值或解析错误。没有重试循环，不能吞掉非法首条消息继续等待。

取消优先级：父 ctx 的 context.Cause 优先，其次子 context 的期限（映射为 ErrHandshakeTimeout），然后才是读取/协议错误。检查子 context 状态时尚未执行 cancel；用 defer cancel 释放计时资源，避免把主动取消误判为失败。保留底层读取错误的 errors.Is 身份，非文本帧归 ErrInvalidV2Handshake。

期限到达只是请求取消；实际 Read 必须返回后该函数才能结束，不能再启动一个 goroutine 竞速后遗留读取者。成功返回之后，未来入口才允许启动 connectionReader，确保同一条连接不出现两个读任务。

这里的 timeout 只覆盖首条读取/校验，不宣称覆盖建流或候选接管。入口接线时还要建立总操作预算和临时连接名额释放规则，不能逐步重置期限而无限延长握手。逻辑会话生命周期继续来自 Gateway，不能绑定这一临时握手 context。

## 本步验收与随后接线

开发者提供两个核心文件的类型与接口草稿后明确委托助手完成本步；助手已实现并补协议、受控读取和真实 WebSocket 测试。验证合法新建/恢复、字段缺失与类型错误、零/最大序号、失败零值与凭据不泄露；验证读取前限额、只读一条、父取消/自身超时/传输错误优先级，以及实际 Read 阻塞时不提前返回。真实网络验证首条后发送的下一条消息未被握手函数消费。此时仍不能标记公开 v2 路由可用。

下一步公开入口必须共同满足：

- 新建：合法 start 后申请逻辑名额、选择 Worker 一次、创建身份/Worker/逻辑会话、登记并启动。失败回滚已取得资源；注册时机与开始接受恢复命令须协调。
- 恢复：按 ID 查注册表、验证 token，提交 requestResumeConnection；不申请第二个逻辑名额、不重新选择或创建 Worker。失败不回退为新建。
- 候选交接：回复成功以前入口持有 socket 关闭责任；成功以后仅 attachment 关闭。ready 由既有唯一 writer 发送，入口不另写一份。
- 完成清理：controlDone 不代表资源已释放；统一运行器等待全部任务/连接后撤销登记、归还逻辑名额。候选握手及退出中连接也须有边界。
- 停服：在新建和恢复交接处阻止新的接管，并等待握手与逻辑会话实际退出；不能只在查表前检查一次停止状态。

这些是后续接线约束，不在本步新增占位 Gateway 字段或修改原 v1 handler。


## 实现与验收（2026-10-07）

已补齐 [协议解析](../../internal/wsprotocol/handshake_v2.go)、[限时读取](../../internal/gateway/handshake_v2.go)及 MessageTypeResume。解析复用现有单对象/字符串/数值 helper，在握手边界转换为固定原因的 ErrInvalidV2Handshake，避免透传报文值；ID/token 只做必需字符串检查，认证仍由后续入口负责。start 上已知恢复字段即使为 null 也被拒绝。

读取在第一次 Read 前设置消息上限，只同步读一条，按父取消原因 → 本次期限 → 读取/协议错误判定；defer cancel 释放子 context。函数返回前等待实际 Read，无额外读取任务，不写 ready、不主动关闭连接。公开路由、注册表和准入本步未接入。

| 证据 | 验证结果 |
| --- | --- |
| 严格协议 | 必需字段缺失、null、非字符串、空值、旧版本、未知类型、多对象和非法数值被拒绝；失败始终返回零值。start 恢复字段混入被拒绝 |
| 数值与身份 | 0、前导零、超过 JavaScript 安全整数范围的数值及 uint64 最大值精确保留；ID/token 不裁剪；未知字段与重复字段行为符合约定 |
| 凭据处理 | 非法 JSON、未知 type/version、错误 token 类型和错误 seq 等五种错误场景没有输出凭据标记或原始报文 |
| 受控读取 | 非法参数、父预取消/预过期不读取；合法与失败返回只读一次、不写/主动关闭，子 context 已释放；验证七种父取消/自身期限与返回数据/错误的竞争分类 |
| 实际退出 | 虚拟时间触发超时或父取消后，替身 Read 仍被阻塞时函数没有返回；解除实际读取阻塞后才返回确定错误。测试失败时也解除阻塞并等待任务，避免残留测试 goroutine |
| 真实连接 | 新建和恢复首条消息之后，已发送的二进制音频帧仍留给下一次读取；子 context 正常释放后连接仍可继续读取 |
| 真实失败 | 二进制首条、非法文本、消息超限、对端直接关闭被正确拒绝/分类；静默连接触发配置的 100ms 读取期限。该测试不是性能实验 |

新增 **15 顶层、101 叶级**全部通过：协议包 72 叶级，Gateway 29 叶级，其中 **7 个真实 WebSocket 场景**。Gateway/协议两个包的完整 race 回归 **337 顶层、1016 叶级：1004 通过、0 失败、12 显式实验跳过**，无数据竞争报告。数量只表示正确性覆盖规模，不代表公开恢复成功率或恢复性能。

验证命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./internal/gateway ./internal/wsprotocol \
  -count=1 -timeout=90s -json
```

原始结果本地保留：

- `/private/tmp/tide-handshake-first.jsonl`：首次无网络定向验证，12 顶层/94 叶级全部通过。
- `/private/tmp/tide-handshake-network-first.jsonl`：首次网络验证被沙箱拒绝回环端口 bind，中止并失败；不能记为完整通过。
- `/private/tmp/tide-handshake-accepted.jsonl`：按权限流程授权后两个包完整复验，统计如上。
- `/private/tmp/tide-handshake-cleanup-check.jsonl`：之后补强阻塞读取测试的失败清理，定向复验两种等待实际退出场景；核心行为未更改。

下一步把已验收的首条读取接入独立 /v2/asr，组合临时连接准入、逻辑会话准入、身份查找与凭据认证、唯一建流和原子候选交接。届时以公开入口真实新建/恢复及名额清理证明能力，不能用当前内部读取测试替代。
