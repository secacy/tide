# 第六阶段：实例异常的处理边界与验收设计

日期：2026-10-11。状态：代码核对与验收设计完成；Worker 在线/离线保留期、Gateway 受控停止、全新 Gateway 拒绝旧恢复，以及独立进程 SIGKILL/原地址重启均已通过当前测试范围的完整客户端验收。设计参照版本 `56e08ee`，Worker 测试基于 `bb72836`，Gateway 测试基于 `4f37c11`，进程测试基于 `40b26e9`；生产逻辑未修改，没有新增性能结论。下文分别保留各步当时的范围与待办。

## 为什么进入这一步

EXP-006-01 已验证原 Gateway 和原 Worker 流存活时的有限断网恢复。实例异常会破坏这个前提：Gateway 重启丢失内存注册表、音频和结果保留状态；Worker 流失败后，当前协议没有恢复该流识别上下文的接口。连上重新启动的服务，不代表原场问诊已经续上。

本能力目标：在原场尚未确认 completed 时，实例异常不能被误报为完整完成，不能自动新建一场来掩盖中断；存活端完成资源清理，服务重新可用后，显式发起的新会话可以正常完成。已经合法接纳 completed 的客户端保留完成事实，后续确认失败另行报告。

## 现有代码已经做了什么

- `sessionRegistry` 只保存进程内对象引用。`resumeV2` 查找并认证原会话，不创建 Worker RPC；身份缺失或恢复资格已关闭映射为 `resume_unavailable`。
- `runCoordinator` 观察 Worker 接收错误后终止原场。`runWithConnection` 通知 Worker 与连接停止，等待任务退出后释放缓冲；`runV2Session` 随后撤销注册并归还名额。
- Gateway 的 `serve` 在退出时停止接入、取消已有会话，再等待 HTTP 和会话清理。这是受控退出路径，不能当作进程被强制终止时也执行了这些清理代码。
- 客户端只在原场身份已经建立且业务尚未完成时有限重试；`resume_unavailable` 和 `service_stopping` 均不重试，也不回退 `start`。服务一直不可达时可能先耗尽恢复预算，不能要求所有故障都返回同一个错误码。
- 当前 `runV2Session` 丢弃运行器返回原因；客户端通常先观察连接断开，再得到恢复拒绝。现有稳定错误码主要用于入口，不能据此声称客户端已经能准确区分 Worker 崩溃、Gateway 重启和会话过期。

上述为源码核对。已有部件测试不替代本轮完整客户端、Gateway、Worker 故障验收。

## 方案选择与代价

| 方案 | 能解决什么 | 代价与限制 | 本轮选择 |
| --- | --- | --- | --- |
| 原场明确中断；服务恢复后由调用方显式新建 | 保留已应用文本，避免静默拼接；沿用现有清理与重试边界 | 不能无缝继续原问诊；未确认部分可能缺失或状态未知 | 采用，先验证已有机制 |
| 新建 Worker 流并重放最近缓存 | 在上下文要求较弱的模型上可能补回一部分音频 | 已确认音频可能已被客户端释放；有限片段不一定够重建上下文；分段和文本可能变化，原流序号不能直接跨流去重 | 暂不采用，需另定跨流语义 |
| 持久保存音频、结果与恢复进度，配合模型重建或检查点 | 扩大到进程重启或跨实例恢复 | 需要定义持久提交时点、所有权转移、模型状态重建和结果一致性；仅把会话 ID 放入共享存储并不足够 | 超出本轮，保留为后续设计 |

选择依据是已有协议只承诺同一存活原场的恢复。先把这个边界验证清楚，避免为实例异常直接引入尚无模型依据的迁移机制。这里的服务恢复指能处理新的请求，和原场恢复分别验收。

## 不能把 audio_ack 当作持久检查点

例如 Gateway 已确认前 32000 字节，但 Worker 只报告处理到 25600。客户端可以依照现有契约释放已确认缓存；此时 Gateway 异常退出，不能假设客户端还能重放中间 6400 字节，也不能声称这部分已经识别完成。

即使 Worker 曾报告处理完成，也不代表相应结果已被客户端应用。当前公开结果没有对应音频范围，`V2RunResult` 在失败时也不暴露完整音频进度。因此：

- 客户端保留已经成功应用的结果，`Completed=false` 表示没有确认整场完整完成，并不等于所有音频都丢失。
- 测试可以使用位置 Worker 和采集记录统计精确字节范围，但那是测试端证据，不是客户端产品已经具备的缺口诊断能力。
- 实际崩溃导致最后处理位置无法观测时，应记录未知范围或可证实的上下界，不用崩溃前最后一次采样伪造精确丢失量。

## 分两层验收

先进行真实 WebSocket / TCP gRPC 的受控故障集成测试，复用生产客户端和 Gateway；再补独立进程强制退出。两层分开报告，不把 `cancel()` 或 `grpc.Server.Stop()` 称为已经测过进程崩溃。

| 场景 | 故障触发与前提 | 要证明的行为 |
| --- | --- | --- |
| Worker 在客户端在线时停止 | 首条结果已经应用、输入尚未结束，强制停止测试 gRPC 服务 | 原场非完整完成；关闭源；清理 Gateway 注册与名额；不另选 Worker 来续原场 |
| Worker 在客户端离线保留期内停止 | 代理截断连接并暂扣恢复请求；确认原场 detached，再停止 Worker | 原场终止，不继续等待完整保留窗口；放行恢复后明确拒绝；不自动 start |
| Gateway 受控停止 | 已建立原场；调用停止接入和取消会话，观察实际任务退出 | 客户端未误报 completed；原 Worker RPC 退出；停止中的入口拒绝恢复 |
| 同一客户端入口转到全新 Gateway 对象 | 原场失败后，将测试入口指向新注册表的 Gateway，保持客户端原凭据 | 返回 `resume_unavailable`；新 Gateway 不为旧恢复请求选择或建立 Worker；显式新场可以正常完成 |
| Worker / Gateway 独立进程强制退出并重启 | 仅操作测试创建的子进程，记录 PID、退出事实与启动完成；保持客户端 URL 不变 | 检验真实连接断开、旧场失败及新场可用；不要求被杀进程运行 defer，不把新进程空注册表称为旧进程主动清理 |

受控测试需要覆盖“已收到 audio_ack，但 Worker 尚未处理”的屏障场景，避免所有故障都发生在音频已完全处理之后。故障触发依赖明确事件，不能通过随意 sleep 猜测位置。

新场探测在后端已经确认恢复可用后独立调用客户端 Run，使用独立输入和结果记录。旧场与新场分别记账；新场成功不能覆盖旧场失败。在恢复握手和最终清理竞争时允许出现暂时的 `session_busy`，但必须受原有次数与时间预算约束。

## 测试实现安排与事实记录

第一实现步由助手完成测试夹具及前两个 Worker 场景，不要求开发者先增加业务结构体。测试文件建议为 `internal/gateway/client_v2_instance_failure_test.go`。夹具负责明确持有 gRPC Server、监听器和退出通知；注入关闭操作只作用于测试拥有的服务，生产代码继续使用原有生命周期机制。

每场独立记录：故障触发事件、客户端返回状态和错误类别、恢复尝试次数、旧场 Worker 选择/RPC 数、已应用结果序号、源关闭次数、资源实际退出以及独立新场结果。原场启动只有一次，恢复不能产生替代 RPC；不强行断言只有一次恢复尝试，因为清理竞争可能产生暂时拒绝。

客户端返回与服务端自然清理使用独立观察条件，测试兜底取消不能算自然清理成功。对强制退出的进程只检查操作系统退出事实及存活对端的资源；不声称读取到了已死亡进程的最终内部状态。测试超时是防止验收挂住的保护，不作为产品延迟承诺。

本轮先验证行为。若进一步量化，再预先固定输入、故障位置、重复次数、期限及运行环境，分别记录故障到客户端停止、存活端清理和服务可用后的新场完成时间；失败样本保留。race 测试耗时不作恢复性能数据。

若测试暴露核心清理或完成判定错误，先向开发者解释触发条件与修改点，再按协作分工修正。故障原因可观测性的已知缺口单独保留：可以后续增加安全的会话退出分类记录，但不把新增诊断日志当作跨实例恢复，也不让诊断写出阻塞清理。

## 后续顺序

1. Worker 在线/离线故障的完整客户端集成验收；发现问题再调整核心逻辑。
2. Gateway 退出、新实例拒绝旧恢复及独立新场验证，随后独立进程退出/重启验收。
3. 汇总实例边界证据，再设计群体重连与随机弱网条件；第六阶段继续保留真实模型、长时恢复等证据缺口。

## Worker 受控故障验收（2026-10-11）

助手完成 [client_v2_instance_failure_test.go](../../internal/gateway/client_v2_instance_failure_test.go)，包含在线 attached 和离线 detached 两个完整客户端场景。生产 v2 客户端 → 故障代理 → 公开 v2 Gateway → 本机 TCP gRPC Worker；测试不直接修改会话状态。每场先应用第一块音频的结果，再等待第二块被 Worker 收到并暂停处理，同时观察 Gateway 的八字节 audio_ack 已成功转发。代理转发成功不等于客户端已处理音频 ACK，测试未读取客户端内部缓存状态。

在线场景通过协调者查询确认原场仍附着、接纳位置为 8 且尚未 end；离线场景关闭原连接后通过同一命令查询确认 detached。随后调用测试拥有的 `grpc.Server.Stop()`，等待原 handler 和 Serve 实际退出。Gateway 的恢复保留与上传预算均为 30 秒，测试独立保护期限为 5 秒：验收期间没有取消 Gateway 或调用 StopAccepting，普通恢复到期不能解释观察到的自然清理。

恢复请求由代理暂扣，直到 Gateway 原场实际清理，再放行。因此本轮固定结果为一次恢复、`resume_unavailable`、`Completed=false`、`AppliedSeq=1`、零成功恢复连接；这个固定尝试次数来自测试屏障，不能外推到没有屏障的在线故障竞争。原场只有一次 start、Worker 选择和 RPC；音频源关闭恰好一次、代理任务自然退出，Gateway 活跃会话、握手数、注册表和 Worker 活动流均归零。

Worker 观测事实：原流逐字收到 `aabbccdd` 共 8 字节，但仅处理前 4 字节，没有接纳输入结束，客户端只应用第一条结果。后 4 字节未被测试 Worker 处理；这证明已接纳音频不能被误报为整场完整完成，不代表当前产品已经能为真实崩溃精确诊断缺失范围。

随后在原 TCP 地址启动新的 gRPC Server，复用 Gateway 的原 ClientConn 并等待其重新可用。由测试显式发起独立新 Run，发送不同的 `11223344` 共 8 字节；新流从自己的零处理位置开始，完整收到并处理 8 字节，结果序号 1、2、3 各应用一次，尾部结果和 completed/completed_ack 闭环完成。Gateway 仍限制 MaxSessions=1，能够接纳新场也验证了旧场名额已归还。新场证据和旧场失败分别保留，未把新连接视为旧 RPC 恢复。

### 验证记录与范围

- 首次非 race 定向运行两个场景均通过，没有首次失败样本。
- 初版 race 重复十轮、20 个场景通过；随后补充初始 ready 与代理自然退出检查。
- 最终版相关两包 race 回归：Gateway 1010、wsclient 305 个叶级检查通过，合计 1315 通过、0 失败；Gateway 13 个显式实验默认跳过。
- 最终版两个场景 race 重复十轮：20 个场景通过、0 失败、0 跳过，每个场景均保留旧场失败并验收独立新场完成。测试耗时只表示验证耗时，本轮没有性能测量。

可复现命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway -run '^TestV2ClientWorkerInstanceFailure$' -count=10 -timeout=90s

GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./internal/gateway ./internal/wsclient -count=1 -timeout=5m
```

本轮未修改生产代码。Worker 替身、Gateway 和客户端同处一个测试进程，gRPC Server 重启与新流处理已验证，操作系统进程强制退出尚未验证；多会话、真实 ASR、Gateway 状态丢失及故障耗时分布均不在这些测试的证据范围。退出原因与客户端音频范围诊断缺口继续保留。下一步验证 Gateway 受控停止、全新 Gateway 拒绝旧恢复及新场完成。

## Gateway 受控停止与全新实例验收（2026-10-11）

助手完成 [client_v2_gateway_failure_test.go](../../internal/gateway/client_v2_gateway_failure_test.go)，包含 `stopping_entry` 和 `fresh_instance` 两个完整客户端场景。采用同一真实 HTTP/WebSocket 入口和 URL，每次新 HTTP 请求进入时选择目标 Gateway；已有连接继续由原 Gateway 管理。与 WebSocket 代理转发错误相比，本轮让真实客户端直接处理停止入口的 HTTP 503，以及全新实例握手中的 `resume_unavailable`，不在夹具里制造错误码。

首场仍使用八字节 `aabbccdd` 输入，第一条结果已应用、Worker 收到第二块后暂停，协调者查询确认 Gateway 连续接纳到 8 字节且没有 end。本轮直接查询接纳位置，没有观测客户端处理 audio_ack 的状态。测试调用原 Gateway 的 `StopAccepting()`、取消其独立会话 context 并等待 `Wait(ctx)`；Worker Server 保持运行，原 RPC 的阻塞由会话取消解除。随后确认 Worker handler 退出、原 Gateway 会话/握手/注册归零，且停止状态为 true。共享客户端连接的关闭留在夹具最终清理，不能解释本轮原 RPC 退出。

第二次 HTTP 请求暂扣在入口、尚未进入 Gateway，使旧实例退出和目标切换时点确定；普通保留及发送预算为 30 秒，独立测试保护期限为 5 秒。暂扣屏障导致本轮一次恢复尝试，不外推到实际请求竞争或部署重启耗时。

| 场景 | 恢复请求到达的位置 | 观测到的原场结果 | 独立新场 |
| --- | --- | --- | --- |
| stopping_entry | 保留在停止中的原 Gateway | HTTP 503 对应 `service_stopping`，Completed=false、AppliedSeq=1、一次恢复、零接回；停止入口也拒绝显式新场 | 切换到新 Gateway 后，在相同 URL 完整完成 |
| fresh_instance | 全新的 Gateway 对象 | `resume_unavailable`，Completed=false、AppliedSeq=1、一次恢复、零接回；新 Gateway 的 Worker 选择/RPC 均没有因恢复请求增加 | 调用方在相同 URL 显式新建后完整完成 |

新 Gateway 使用独立注册表、生命周期和 gRPC ClientConn，不共享旧会话状态。旧场音频源恰好关闭一次，第一条结果保留且没有额外应用；原 Gateway 只有一次 Worker 选择，旧流收到 8 字节、仅处理 4 字节，没有正常输入结束。拒绝旧恢复及停止入口的新场探测均未建立替代 RPC。

显式新场发送不同的 `11223344`，实际处理 8 字节，结果序号 1、2、3 及对应文本独立、按序应用；尾部结果与 completed/completed_ack 闭环完成，源关闭一次。每个场景最终只观察到旧/新各一条 Worker RPC、原/新 Gateway 各一次 Worker 选择。两个 Gateway 的会话/握手/注册、Worker 活动流及固定入口 handler 均已归零；这些观测发生在测试兜底清理之前。

### 验证记录与边界

- 首次非 race 定向运行两个场景均通过，无首次失败样本。
- 两个场景 race 各重复十轮，共 20 个场景通过、0 失败、0 跳过。
- 相关两包最终 race 回归：Gateway 1012、wsclient 305 个叶级检查通过，合计 1317 通过、0 失败；Gateway 13 个显式实验默认跳过。本轮没有性能测量。

可复现命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway -run '^TestV2ClientGatewayInstanceFailure$' -count=10 -timeout=90s

GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./internal/gateway ./internal/wsclient -count=1 -timeout=5m
```

生产代码未修改。这里验证的是 Gateway 对象的受控停止和固定入口切换到新对象；测试 HTTP 监听器一直存活，以便直接观察停止入口拒绝和新实例接入。没有在本轮执行 `cmd/gateway` 的 HTTP Shutdown、真实信号退出、监听器重启或进程强制终止，不能据此声称实际部署崩溃已经验收。下一步独立验证测试子进程被强制退出后的连接断开、存活对端收尾以及重启后新场完成，继续保留退出原因和音频范围诊断缺口。

## 独立进程 SIGKILL 与原地址重启验收（2026-10-11）

助手完成 [client_v2_process_failure_test.go](../../internal/gateway/client_v2_process_failure_test.go)。Unix 测试二进制通过私有 helper 分别运行独立 Worker 和 Gateway 子进程；父测试只终止自己创建的进程，唯一 Cmd.Wait 任务取得退出状态，明确检查 Signaled/SIGKILL。子进程使用临时监听端口，重启进程重新绑定原地址，并检查其 PID 与原进程不同。

选择私有 helper，是为了使用生产 Gateway、真实 TCP gRPC/WebSocket，同时固定 Worker 的处理位置和观察屏障；不要求为了测试修改 Gateway 命令的监听配置。相较直接启动现有命令，本轮不覆盖命令行解析或真实模型，但能验证真实进程退出对连接和原场状态的影响。父/子进程使用同一编译版本，race 运行时子进程也使用 race 二进制。子进程输出在唯一 Wait 结束后额外检查竞态/崩溃日志，避免 SIGKILL 的退出码掩盖已输出的错误。

原场输入仍为 `aabbccdd` 八字节；首条结果已应用，代理观察到八字节 audio_ack 已成功转发，Worker 在第二块收到后暂停，实际只处理前四字节。子进程就绪和 Worker 故障前事实通过私有目录中的完整 JSON 记录发布，Gateway 存活状态通过测试专用 HTTP 查询观察；记录不含会话凭据。文件 rename 仅用于避免读到半条测试记录，不是持久恢复机制，也未测量记录写出成本。

| 被终止的进程 | 存活对端的观察 | 重启后的行为 |
| --- | --- | --- |
| Worker | 原 Gateway 在未取消自身生命周期、未 StopAccepting 的情况下，关闭原场并撤销注册、归还名额，保持服务可用 | 新 Worker 进程在原地址就绪，共享 ClientConn 重新可用；原 Gateway 显式新场完整完成 |
| Gateway | 原 Worker 服务保持运行，原 RPC 因远端进程消失解除阻塞并实际退出，活动流归零 | 新 Gateway 进程初始注册/名额/选择均为零；旧恢复被拒绝且选择仍为零，独立新场随后完整完成 |

被杀 Worker 的 `worker-held.json` 只表示死亡前的八字节接收、四字节处理和一条活动 RPC，未把它当作死亡后的快照；测试确认其没有写出正常退出记录。Gateway 死亡后的清理只在存活 Worker 上观察，新 Gateway 的空注册表属于新进程初始状态，不能称为旧进程执行了清理。

原场客户端在固定代理 URL 上发起一次原身份 resume；代理暂扣请求，直到存活端收尾或新 Gateway 已就绪后再放行。两类故障均得到 `resume_unavailable`，Completed=false、AppliedSeq=1、零成功接回，源关闭一次，没有额外结果应用或回退 start。一次尝试来自受控屏障，不表示真实部署中不会遇到拨号失败和多次重试。测试保护期限为 20 秒，客户端恢复窗口为 10 秒、服务端保留为 30 秒；这些是测试参数，不是恢复性能或生产承诺。

独立新 Run 直连原业务地址：Worker 场景使用一直存活的 Gateway 入口，Gateway 场景使用原端口的新进程。发送不同的 `11223344` 八字节，实际处理全部输入，三条结果序号 1/2/3 按序独立应用，尾部结果及 completed/completed_ack 闭环完成，源关闭一次。原场的代理 URL 和后端目标地址在恢复尝试期间均未改变；显式新场不被统计为原场恢复。

每场的新旧 RPC 分开计量。Worker 场景原 Gateway 总选择次数为二，新 Worker 的唯一 RPC 从零开始、实际处理八字节后退出；Gateway 场景新 Gateway 只有一次新场选择，存活 Worker 总 RPC 数为二，新旧音频与处理位置相互隔离。自然收尾在测试最终取消/关闭夹具前观察；替代子进程在验收完成后由自有清理任务终止并等待，不遗留后台进程。

### 验证记录与边界

- 首次非 race 运行 Worker/Gateway 两个进程场景均通过，没有首次失败样本。
- 加入子进程错误日志检查后，两个场景 race 各重复五轮：10 个场景通过、0 失败、0 跳过；10 条旧 PID/SIGKILL/新 PID/原地址相同的观测与断言一致。
- 相关两包 race 回归：Gateway 1014、wsclient 305 个叶级检查通过，共 1319 通过、0 失败；默认跳过 13 个显式实验及 1 个私有进程入口。该入口在父测试启动子进程时执行，不是未验证的业务场景。

可复现命令：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway -run '^TestV2ClientInstanceProcessFailure$' -count=5 -timeout=2m

GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race -p 1 ./internal/gateway ./internal/wsclient -count=1 -timeout=5m
```

运行环境为当前 macOS 本机，单会话、位置可控的测试 Worker，没有真实 ASR、群体负载、网络黑洞、跨实例迁移或故障耗时分布证据；命令启动、部署探针、发布滚动策略也不在 helper 的验证范围。生产逻辑未修改，本轮完成的是“原场明确失败、存活端收尾、原地址重启后独立新场可用”的进程异常边界，不是进程崩溃后的原场无损恢复。退出原因分类与客户端精确音频范围诊断仍保留缺口。下一步汇总当前恢复能力与失败边界，确定群体重连实验的范围、指标及退避策略。
