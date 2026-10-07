# 第六阶段：v2 入口错误与客户端判断边界

日期：2026-10-07（设计）；2026-10-08（实现与验收）。状态：开发者明确委托助手实现；稳定入口错误码及公开接线已验收，客户端判断与自动恢复仍待实现。下文保留设计依据，末尾记录验收结果。

## 能力目标与当前位置

公开 v2 已能新建、恢复原 Worker、重放结果、确认完成，并在输入无进展时进入有限恢复窗口。当前 handler_v2.go 的 v2EntryErrorText 只返回英文提示；未来客户端若据此决定重试，会依赖可变文本，且无法可靠区分临时冲突、结果缺口和恢复资格丢失。

本步让入口拒绝携带稳定 code，并固定客户端对初始 ready 丢失、完成确认丢失的判断规则。实现范围是协议类型、统一映射和公开入口接线；客户端执行这些规则在后续实现。成功交接后的主动错误通知仍未接入，传输中断时也不能保证收到错误帧。

后续两步：客户端有界音频缓存、结果应用位置及有限重连状态机 → 同一存活 Gateway/Worker 范围的端到端故障实验。初始请求幂等和清理后的终态查询是独立扩展；这里明确首版边界，不将它们记录为已实现。跨实例恢复仍是第六阶段待处理范围。

## 为什么需要先定协议

例如旧连接仍在 CloseNow，恢复请求得到 session busy，此时原会话仍可在期限内恢复；若客户端的 appliedSeq 早于已释放的结果，继续相同请求只能反复失败。另一个场景是 completed_ack 已到达服务端并触发清理，但客户端只看见连接断开；此后查不到注册项不能用来推断识别失败。

代码对应事实：新建提交注册后才由运行器写 ready；handler 交接成功后不再写连接。resumeV2 把查不到 ID 与 token 不匹配合并为 errResumeUnavailable。runV2Session 在实际清理后删除注册项，没有历史终态存储。CompletedAckMessage 已约定客户端应用全部结果并记录成功后才确认。

## 方案与权衡

| 选择 | 优点 | 代价与本轮决定 |
| --- | --- | --- |
| 匹配 message 文本重试 | 无需改协议 | 修改文案就会改变控制行为；拒绝采用 |
| 稳定 code + 展示 message | 分支可测试，文案不决定重试，内部错误不泄漏 | 要维护公开类别；本轮采用 |
| 所有错误增加 retryable 布尔值 | 客户端表面上简单 | 是否重试还取决于是否有身份、结果完成事实和剩余预算；本轮不增加，客户端后续按 code 与自身状态判断 |
| 给 start 增加幂等键，丢失 ready 后重复查询同一场 | 可找回已提交但身份未知的会话 | 新增键的身份绑定、冲突、并发提交、存储预算和有效期；首版先要求 ready 前不上传音频，尚无身份时只允许有界重试新建 |
| 清理后保留有界终态记录供查询 | 能回答部分“这场最后怎么样” | 需要新的条目/字节预算、TTL、认证、驱逐与进程丢失语义；暂缓，不能把缺失记录解释为失败 |
| 客户端先核对并记录 completed，再发送 completed_ack | 已有协议能完成当前存活客户端的正常闭环 | 不能恢复客户端自己丢失的完成记录；首版采用，不承诺跨进程终态查询 |

## 公开错误格式

在 internal/wsprotocol/error_v2.go 新增独立类型，保持现有 v1 ErrorMessage 的字段不变：

```go
// V2ErrorCode 是稳定的入口拒绝类别；客户端不得按 Message 判断重试。
type V2ErrorCode string

// V2ErrorMessage 表示本次 v2 入口操作未交接成功。
// 它不证明原逻辑会话已经终止，也不承诺错误消息一定送达。
type V2ErrorMessage struct {
    Type    MessageType `json:"type"`    // 固定 error。
    Code    V2ErrorCode `json:"code"`    // 稳定公开类别，必须非空。
    Message string      `json:"message"` // 固定安全提示，不包含内部 err.Error()。
}
```

例：`{"type":"error","code":"session_busy","message":"session busy"}`。

常量使用 V2Error 前缀，例如 V2ErrorSessionBusy。以下完整映射是本步实现清单；每个常量的注释写明其含义。

| code / 常量后缀 | 内部原因 | 固定 message | 后续客户端行为 |
| --- | --- | --- | --- |
| service_stopping / ServiceStopping | errGatewayStopping | service is stopping | 当前实例停止服务；按既有身份和总预算处理，不承诺可在别的实例恢复 |
| handshake_limit / HandshakeLimit | errHandshakeLimit | handshake limit exceeded | 退避后重试本次入口操作 |
| session_limit / SessionLimit | errSessionLimit | session limit exceeded | 仅新建会申请逻辑名额；预算内退避重试新建 |
| invalid_handshake / InvalidHandshake | ErrInvalidV2Handshake、websocket.ErrMessageTooBig | invalid handshake | 停止当前自动尝试，修正协议/配置 |
| entry_timeout / EntryTimeout | ErrHandshakeTimeout、未被更具体入口类别包装的 context.DeadlineExceeded | entry timeout | 若确实收到该错误，当前入口未交接；预算内重试原操作 |
| resume_unavailable / ResumeUnavailable | errResumeUnavailable、errResumeClosed、errResumeExpired、errResumeGenerationExhausted | resume unavailable | 停止恢复原会话；不能推断此前成功或失败，不自动新建替代 |
| session_busy / SessionBusy | errResumeAlreadyAttached、errConnectionRetiring | session busy | 带原 ID/token/应用位置退避重试；不会续服务器期限 |
| replay_gap / ReplayGap | errResultReplayGap | result replay gap | 所需结果已不在可重放范围；停止自动恢复，不伪造已应用位置 |
| invalid_resume_position / InvalidResumePosition | errResultAckAhead | invalid resume position | 声明位置超过服务端授权范围；停止自动恢复，不能跳过结果 |
| worker_unavailable / WorkerUnavailable | errV2WorkerUnavailable | worker unavailable | 本次新建未交接；预算内重试新建 |
| internal_error / InternalError | 其他未知原因 | internal error | 首版停止自动重试并报告固定错误；内部原因留在内部 |

“重试”均不保证成功，也不重新开始整场重试总预算；未知 code 按未知错误停止自动重试。本步只建立错误协议，重试次数、退避和预算由客户端下一步实现。

## Gateway 接线

建议新增 internal/gateway/entry_error_v2.go，集中实现两个函数：

```go
// v2EntryError 将非 nil 内部入口错误映射为固定公开消息。
// 使用 errors.Is 识别包装错误，不操作会话、不携带凭据、音频或后端错误文本。
func v2EntryError(err error) wsprotocol.V2ErrorMessage

// writeV2EntryHTTPError 在 WebSocket 升级前发送入口拒绝。
// 仅由尚未升级的 handler 调用；状态码为 503，响应体使用相同公开错误格式。
func writeV2EntryHTTPError(w http.ResponseWriter, err error)
```

v2EntryError 使用确定的映射顺序：显式入口类别优先，通用 context.DeadlineExceeded 兜底在后。特别是 errV2WorkerUnavailable 包装了 context.DeadlineExceeded 时，只要入口未超时，它仍是 worker_unavailable；不能因为底层原因包含 DeadlineExceeded 就误报 entry_timeout。prepareV2Worker 已在包装建流错误前检查入口/RPC context，继续保留该顺序。

writeV2EntryHTTPError 设置 `Content-Type: application/json; charset=utf-8`，再写 503 和 JSON。不要使用 http.Error 发送 JSON 字符串，因为其响应头和格式语义是纯文本。HTTP 写响应依旧依赖现有 HTTP 服务配置，不借此承诺新的写入时限。

修改 handler_v2.go：

1. 升级前 tryEnterHandshake 失败，调用 writeV2EntryHTTPError，保持名额失败不进入后续链路。
2. 已建立 entryCtx 后、Accept 前的取消检查分清来源：Gateway 已取消时使用 service_stopping；请求本身已取消时直接返回；独立入口期限耗尽且请求仍有效时映射 entry_timeout。客户端取消不等于服务停服。
3. 升级后且入口仍拥有连接的失败分支，用 v2EntryError 构造 JSON 并沿用 ResultWriteTimeout 收尾预算。保留原有“服务/请求取消或已收到 WebSocket CloseStatus 时不强行提示”的判断。
4. 删除 v2EntryErrorText，固定提示只保留一处映射；HTTP 与 WebSocket 拒绝使用相同 code/message。
5. 保留成功交接的所有权：startV2/resumeV2 返回 nil 后不再输出入口错误；运行器仍是 ready 和业务输出的唯一写入者。

实验入口禁用的 404 以及 websocket.Accept 自行产生的升级错误不包装成本协议。客户端还必须能处理没有 JSON 错误、没有应用错误帧、提前断开的结果；不能将“未收到错误”当作提交成功或失败的证据。运行中 Worker 失败等终止通知并未因新增 V2ErrorMessage 自动覆盖。

## 两个客户端边界

### 首次 ready 丢失

未来客户端必须收到并保存首个合法 ready 的身份后才上传音频。如果 start 后断开且尚未得到身份，本次新建可能已提交，但还没有本客户端上传的音频。客户端先关闭旧连接，再在同一重试总预算内退避重试 start；新建可能被原空会话占用的名额拒绝，这是明确的代价。原会话由断开窗口或输入进展期限清理，不要求为了新请求提前释放。

该规则只适用于尚未拿到身份、且未发送音频的初始阶段。已有 ID/token 后，即使下一代 ready 丢失，也继续用原身份恢复；不能退回 start。旧恢复可能已安装，因此允许先收到 session_busy；保持已应用结果位置，不把未收到的新代信息当成确认。

### completed / completed_ack 附近断开

客户端只有在已发送合法 end、确认 completed 属于当前代、finalOffset 与本地输入终点一致、结果已经连续应用至 lastSeq 后，才记录本场成功并发送 completed_ack。这是顺序要求：先记录完成事实，再确认服务端释放资源。

- 没有完整收到/验证 completed：即使全部音频已 ACK，也不能宣告整场成功；仍在原会话范围内恢复结果与完成通知。
- 已记录完成事实：completed_ack 发送失败或连接关闭不撤销本地业务成功；服务端可能收到 ACK，也可能靠固定保留/恢复期限清理，不保证即时归零。
- 没有本地完成事实，恢复又返回 resume_unavailable：仅报告无法继续恢复、最终结果未知；不能把缺失注册项当作成功或失败证明。

这里“记录”是首版存活客户端的状态与结果保存契约，不自动承诺持久化或客户端进程重启恢复。本步不新增终态查询 API、墓碑缓存或 start 幂等键；后续如果要支持清理后查询，须独立实现预算、TTL、认证及查询缺失的语义。

## 验收安排

开发者先完成上述协议类型、错误映射和 handler 接线。助手随后评审并补测试：

- 11 类公开 code 和固定提示；包装错误仍可识别，未知内部错误不会暴露 token/后端文本；Worker 包装的 DeadlineExceeded 不误分类。
- HTTP 临时握手满额/停止接入的 503 JSON；WebSocket 非法握手、满额新建、身份认证拒绝、busy 和恢复位置错误对应正确 code；原会话不因候选失败被释放或续期。
- 不存在 ID 与错误 token 的公开响应保持一致，禁止通过错误类别枚举会话。
- 仍允许超时/消息超限路径只有断开；提示写入失败后实际释放握手资源。
- 原有 v1 错误格式、公开恢复和完成闭环回归。

初始 ready 丢失后的自动重试、completed 记录与 ACK 丢失的客户端决策，将在客户端实现时用故障注入验收。本步的协议与服务端测试通过后，只能标记错误分类接通，不能记作自动恢复能力或恢复成功率。

## 实现与验收（2026-10-08）

按明确委托完成已有错误类型/映射骨架，接通 handler 并补注释与测试。新增 [error_v2.go](../../internal/wsprotocol/error_v2.go) 的 11 类 V2ErrorCode；[entry_error_v2.go](../../internal/gateway/entry_error_v2.go) 统一固定 code/message，显式建流错误优先于通用 DeadlineExceeded。HTTP 拒绝返回 503 JSON；尚未交接的 WebSocket 失败沿用独立有界写入预算。升级前请求取消直接返回，与 Gateway 取消的 service_stopping 分类分开。

删除旧文本映射，结果缺口独立返回 replay_gap，位置超前仍返回 invalid_resume_position。已有公开入口/输入进展网络测试迁移为同时检查 code 与 message，正常交接、恢复、尾部结果及 completed_ack 清理继续走原拥有者。

新增 [entry_error_v2_test.go](../../internal/gateway/entry_error_v2_test.go)、[entry_error_v2_websocket_test.go](../../internal/gateway/entry_error_v2_websocket_test.go) 和 [error_v2_test.go](../../internal/wsprotocol/error_v2_test.go)，验证如下：

| 证据 | 结果与范围 |
| --- | --- |
| 18 个内部原因，各直接及包装路径 | 36 叶级全通过；覆盖 11 个公开类别及统一恢复不可用分支，未知错误/包装详情不进入固定响应 |
| 4 类明确入口错误同时包含 DeadlineExceeded | 优先保持各入口类别，Worker 下游超时不误报 entry_timeout |
| HTTP 格式、满额/停止/两种取消、写入失败 | 8 叶级全通过；503 的 Content-Type 与三字段 JSON 正确，请求取消不写停服提示，失败写入不追加响应或泄漏名额 |
| v2 错误线格式与 v1 兼容 | 2 叶级全通过；v2 固定 type/code/message，v1 仍仅 type/message |
| 真实 HTTP 升级拒绝 | 握手满额和停服两个网络场景均得到 503 JSON，不启动 Worker |
| 真实 WebSocket + TCP gRPC 的认证拒绝 | 缺失会话与错误 token 的响应逐字相同；两次拒绝后原连接仍上传 ab 并完成，Pick=1、RPC=1 |
| 真实 WebSocket + TCP gRPC 的非法恢复位置 | replay_gap 与 invalid_resume_position 各一场；正确位置重试仍 generation=2、nextOffset=2、ackedResultSeq=1，身份不变，Pick=1、RPC=1，后端只收到 ab、cd，确认后注册与计数归零 |
| 真实 WebSocket 的建流失败 | 下游 DeadlineExceeded 与不透明后端错误均返回 worker_unavailable，不公开原错误；失败启动 RPC 与名额归零。此处 gRPC 建流由可控替身提供 |
| 真实 WebSocket 对端在建流失败回复前关闭 | 服务端退出并取消失败 RPC，临时握手/逻辑计数和注册归零；不要求对端收到应用错误帧，也不推断底层 Write 是否曾被本机缓冲接纳 |

新测试共 12 顶层 / 58 叶级，包括 8 个新增真实 HTTP/WebSocket 网络场景，其中 3 个经过真实 TCP gRPC 的认证/非法位置组合。测试数量表示已覆盖行为，不是业务恢复成功率。

验证记录：

- 首轮受控分类与 HTTP 测试：5 顶层 / 48 叶级全通过。
- 加入线格式、网络组合及既有公开入口/输入进展回归的定向运行：19 顶层 / 80 叶级全通过，其中本步新增 58 叶级。以上均一次通过，无需修正失败断言或生产逻辑。
- 最终全项目 `go test -race -p 1 ./... -count=1 -timeout=90s -json`：636 顶层 / 2401 叶级，2389 通过、0 失败、12 既有显式实验跳过，无数据竞争报告。本步新增 58 叶级全部通过。叶级计数不重复计入带子测试的父项。
- 环境变量为 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off`；定向原始 JSON 临时记录 `/private/tmp/tide-entry-errors-focused.jsonl`，最终完整记录 `/private/tmp/tide-entry-errors-all.jsonl`。可按命令和源码复现，包耗时不作为业务指标。

这一步提供自动恢复所需的稳定入口判断依据。消息超限/读取超时仍可能仅关闭连接；成功交接后的运行中终止尚无新增错误帧。原有协调者/期限回归继续验证候选失败不获得连接、不同代次隔离及实际清理；本步没有改变服务器恢复截止点。

首次 ready 丢失、完成事实记录与 ACK 丢失的客户端决策仍只是上述契约，尚无客户端故障注入证据；start 幂等和清理后终态查询未实现。下一步进入存活客户端的有限音频重放、连续结果应用与有总预算的自动重连，再按故障种类开展实验。公开 v2 实验开关仍默认关闭。
