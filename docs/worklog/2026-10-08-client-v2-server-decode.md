# 第六阶段：v2 服务端消息解码

日期：2026-10-08。状态：讨论后不再作为独立实现步骤；必要消息解析随单连接 v2 运行器接入。下文原方案保留为设计记录，不再是待实现清单；没有新增业务代码或测试结果。

## 范围调整（2026-10-08）

开发者指出当前客户端和服务端都由项目维护。现有服务端通过固定结构体序列化必填字段，相关字段没有 omitempty；正常输出不会因网络分片而成为“缺少一个字段但仍然合法”的 JSON。完整 WebSocket 消息读取遇到传输中断应处理读取错误，字段缺失主要是实现或版本不一致的问题。

因此不再要求先实现独立 server_v2.go、逐字段 RawMessage 校验及全字段缺失矩阵。下一步直接接单连接 v2 运行器，消息解析使用现有消息结构体：读取 type 分派，再 json.Unmarshal 到对应类型，检查解析错误和支持的消息类型；uint64 字段已有 string 标签，可直接保持精度，无需经过 float64。

运行器仍执行文本消息检查、读取上限、ready 前后阶段约束，以及既有 v2ClientState 的身份、代次、授权位置、结果连续性和完成判断。结果必须成功应用后才提交确认。缺字段与零值的全量区分暂缓，接受当前双方受控实现下的这项限制；以输出往返和真实端到端验收检查两端一致性。以后有独立升级、外部接入或具体兼容问题时再增加严格字段校验。

以下各节记录调整前的方案与取舍，不表示仍需先完成该部件。

## 当前能力与本步目标

客户端有界音频缓存、协调状态及两者的串行组合已验收。下一组目标是将它们接到真实 WebSocket：完整读取服务端消息、验证格式、交给状态对象判定，再执行音频写入与结果应用。

本步先实现 internal/wsprotocol/server_v2.go，把完整 JSON 文本解析成确定的服务端消息值，覆盖 ready、audio_ack、result、completed 和 error。首部业务判断继续由 v2ClientState 负责；网络消息类型和读取大小上限由随后 reader 负责。

验收完成后直接设计单连接 v2 运行器，组合源读取、唯一网络 writer、reader、结果应用及共同退出，跑通正常端到端完成；之后再接有限重试与断线恢复。源取消、结果回调退出及在途资源预算在运行器设计时一起明确。

## 为什么需要这一步

ReadyMessage 等现有结构体描述输出格式，但直接 json.Unmarshal 无法区分缺失字段与合法零值。例如漏掉 nextOffset 会得到 0，漏掉 inputEnded 会得到 false；首次 ready 的这些值恰好可能合法。只依靠状态检查，会把残缺消息当作完整消息。

另一个风险是 uint64 位置。协议已经规定十进制字符串，不能先解析成 float64 再转换，否则大整数可能失去精度。解码器要保留字段原始类型，确保数字字符串、布尔值和普通字符串各自符合协议。

## 方案与选择

| 方案 | 收益 | 代价与决定 |
| --- | --- | --- |
| 直接解到已有消息结构体 | 代码最少 | 缺失/null 与零值难以区分，不满足必填字段检查 |
| 为每类消息新增指针字段 wire 结构体 | bool/string 指针能区分部分缺失场景 | 仍需处理十进制字符串、null 和固定错误分类；结构声明重复较多 |
| 单对象解析成 map[string]json.RawMessage，再逐字段验证 | 直接区分缺失、null、类型错误；可复用现有数值/字符串工具 | 有字段 map 分配；首版采用，先保证正确性，后续依据实验再优化 |

输出采用一个带 Type 的值结构体，内含五种已有消息类型。调用方按 Type 访问对应字段，不需要 any 或类型断言。只有对应分支有值，其余保持零值；解码失败返回整个结构体零值，避免部分身份或完成字段进入调用方。

## 公开接口与辅助函数

在 internal/wsprotocol/server_v2.go 中实现：

```go
// ErrInvalidV2ServerMessage 表示服务端应用消息的 JSON 格式或必需字段非法。
// 可用 errors.Is 分类；错误文本不得包含原消息、凭据或服务端字段值。
var ErrInvalidV2ServerMessage = errors.New("invalid v2 server message")

// V2ServerMessage 是一条通过格式校验的服务端消息，不代表业务已接纳。
// Type 指定唯一有效的消息字段，其他字段为零值。
// Ready 包含凭据，因此不得把整个值输出到普通日志。
type V2ServerMessage struct {
    Type      MessageType            // ready/audio_ack/result/completed/error。
    Ready     ReadyMessage           // 仅 Type=ready 有效。
    AudioAck  AudioAckMessage        // 仅 Type=audio_ack 有效。
    Result    SequencedResultMessage // 仅 Type=result 有效。
    Completed CompletedMessage      // 仅 Type=completed 有效。
    Error     V2ErrorMessage         // 仅 Type=error 有效。
}

// DecodeV2ServerMessage 解析一个完整 JSON 对象，检查消息类型和必需字段。
// data 的大小与 WebSocket 文本类型由调用方限制；函数不读网络、不改变会话。
// 成功返回独立消息值；失败返回零值与 ErrInvalidV2ServerMessage 类错误。
// 合法 error 消息同样返回消息和 nil，业务拒绝由调用方解释。
func DecodeV2ServerMessage(data []byte) (V2ServerMessage, error)

// decodeV2Ready 从已验证的单对象字段表解码 ready，保留不透明身份字符串。
func decodeV2Ready(fields map[string]json.RawMessage) (ReadyMessage, error)

// decodeV2AudioAck 解码累计音频位置和显式输入结束标志，不释放任何音频。
func decodeV2AudioAck(fields map[string]json.RawMessage) (AudioAckMessage, error)

// decodeV2SequencedResult 解码一条更新；允许空文本及空片段标识。
func decodeV2SequencedResult(fields map[string]json.RawMessage) (SequencedResultMessage, error)

// decodeV2Completed 解码固定完成范围，允许 finalOffset/lastSeq 为 0。
func decodeV2Completed(fields map[string]json.RawMessage) (CompletedMessage, error)

// decodeV2ServerError 解码入口拒绝；未知但非空的 code 原样保留，不判重试。
func decodeV2ServerError(fields map[string]json.RawMessage) (V2ErrorMessage, error)

// decodeV2ServerBool 检查必需的原始 JSON 布尔值，拒绝缺失、null 和字符串。
// field 只能传入代码中的固定字段名，用于安全的错误定位。
func decodeV2ServerBool(raw json.RawMessage, field string) (bool, error)

// invalidV2ServerMessage 只包装本协议哨兵及由代码指定的固定原因。
// 不拼接底层解析错误，也不引用服务端传来的 type/code/token/text 等值。
func invalidV2ServerMessage(reason string) error
```

这些辅助函数放在同一文件即可，不需要新增通用解析框架。各消息分支在字段全部通过后，用明确的 Type 常量构造已有消息结构体，然后再构造 V2ServerMessage。

## 字段契约

全部消息必须有精确拼写的 type 字段，其值为相应小写类型字符串。只接受下表五种类型；start、resume、end、result_ack、completed_ack 属于客户端到服务端方向，此处拒绝。

| 类型 | 除 type 外的必需字段 | 值约束 |
| --- | --- | --- |
| ready | sessionId、resumeToken、generation、nextOffset、inputEnded、ackedResultSeq | 身份为非空字符串；generation 是非零 uint64 十进制字符串；两个位置允许 0；inputEnded 为 JSON bool |
| audio_ack | generation、nextOffset、inputEnded | generation 非零；nextOffset 允许 0；inputEnded 必须显式存在 |
| result | seq、segmentId、text、isFinal | seq 非零；segmentId/text 必须是字符串但允许空；isFinal 必须为 JSON bool |
| completed | generation、finalOffset、lastSeq | generation 非零；finalOffset/lastSeq 允许 0，包括空输入或无结果的完成 |
| error | code、message | code 为非空字符串，未知值保留；message 必须是字符串，允许空，文案不决定重试 |

所有 uint64 字段均接受解码后的 ASCII 十进制数字字符串，包括前导零、JSON 转义数字及 MaxUint64；拒绝 JSON 数字、符号、空白、浮点、小数/指数表达、非 ASCII 数字和溢出。复用 decodeUint64String 可以保持与已有 v2 输入规则一致。

字符串规则明确区分“字段不存在”与“值为空”。身份原样保留，不 TrimSpace，不把 token 当 UUID/hex 做格式猜测。结果的空字符串与现有 resultBuffer 接受空更新的范围一致，不能因本步解码新增业务限制。

只接受 true/false 两个 JSON 布尔值。可对 RawMessage 做 bytes.TrimSpace 后直接比较；不要只 json.Unmarshal 到 bool 后检查错误，因为 null 可能留下默认 false。

未知字段忽略，以保留扩展空间。重复的同名字段沿用现有 v2 解码的后值覆盖规则，由字段 map 决定最终值：最终为 null 仍须拒绝。字段名区分大小写；仅提供 NextOffset 不能代替 nextOffset。这里的严格校验指必填、类型、整数范围与消息方向，不承诺拒绝所有扩展字段或重复键。

## 实现顺序与错误边界

1. 调用现有 decodeSingleJSONObject，要求一个完整对象。允许首尾空白；拒绝空输入、null/数组/标量、多个 JSON 值和尾随垃圾。
2. 将对象解成 map[string]json.RawMessage，使用 decodeRequiredString 读取 type，再按五种已知类型分派。不要通过 map[string]any 引入 float64。
3. 每个分支逐个检查所需字段，复用 decodeRequiredString、decodeUint64String，补布尔检查及非空/非零约束。
4. 所有字段通过后构造返回值；失败时外层统一返回 V2ServerMessage{}。对应消息内的 Type 与外层 Type 必须一致。
5. 不将底层工具的错误透传。它们目前带 ErrInvalidV2Input，且部分数值/JSON 错误包含输入。转换成固定原因，例如 invalidV2ServerMessage("invalid ready generation")；错误只保留 ErrInvalidV2ServerMessage 的分类，既不 `%v` 拼原错误，也不 `%w` 包装原错误链。

“合法 error 帧”和“解码出错”要分开：例如 code=session_busy 的完整消息返回 Type=error、Error.Code=session_busy、err=nil。未来运行器结合会话身份和剩余预算决定是否重试。一个未知但非空的 code 同样能解析成功，首版运行器按未知拒绝停止自动尝试；本解码函数不启动任何动作。

同样，解码器允许 generation=3 的 ready，也允许任何格式正确的 uint64 位置。它不认识当前会话，身份是否一致、代次能否前进、是否超出授权、结果是否连续以及 completed 是否可信，仍交给 v2ClientState。

## 网络接线时需要保持的条件

reader 在调用本函数前确认 WebSocket MessageText，并应用明确的 ReadLimit；HTTP 升级前的错误体读取也要有独立上限。协议函数接收的是完整字节切片，本身不提供网络或内存总预算。

后续 reader 仍需识别阶段：ready 前只允许 ready 或入口 error；ready 后按业务协议分派，重复 ready 不能再次建身份。解码成功不等于阶段合法，连接关闭也不等于 completed。reader 事件携带固定尝试 ID/代次，由协调者再决定是否接纳。

返回的字符串归解码结果所有，调用方复用原 data 不应改变消息内容。实现使用标准 JSON 解码即可，不引入 unsafe 字节到字符串转换。

## 验收安排

开发者实现上述一个文件；助手评审并补测试，覆盖：

- 五类合法消息、空输入/无结果相关零值、显式 false、空结果字符串、中文与转义内容。
- 每个必需字段的缺失、null、错误 JSON 类型；generation/seq 的 0、数值字段的符号/空白/小数/指数/溢出及大于 2^53 的精确值。
- 单对象边界、错误消息方向、未知 type、字段名大小写、扩展字段、重复字段最后值合法/非法。
- 未知非空错误码保留，合法 error 返回 nil 解码错误，缺少 code 的 v1 error 不能冒充 v2。
- 所有失败返回完整零值；errors.Is 仅匹配服务端解码哨兵，不意外匹配 ErrInvalidV2Input；错误不包含凭据、原报文或恶意字段值。
- 五类既有输出结构体经 json.Marshal 后往返解析一致，非对应消息字段全为零；原 data 被修改后返回值保持有效。
- 在 wsclient 测试中组合解码器与真实 v2ClientState：缺字段的 ready 在解析层拒绝；格式正确但确认超过 offeredOffset 的 ACK 在状态层拒绝；合法 ready/音频 ACK/result/completed 能按既有规则提交。两类拒绝都不修改状态或缓存。

本轮只记录计划，不预写通过数量。协议正确性与状态组合通过后，继续单连接真实网络验收，再测自动恢复成功率、重放范围和耗时。
