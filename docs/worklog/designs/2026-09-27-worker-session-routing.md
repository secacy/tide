# 第五阶段：多 Worker 新会话分配

方案日期：2026-09-27；最后核对：2026-09-28。状态：独立轮询选择器已接入 Gateway 并通过双 Worker 集成及全项目 race 回归；生产入口已支持 -workers 有序地址列表及统一连接回收。

## 背景与目标

多个 Mock Worker 已能独立启动，具备实例共享的处理名额；接入前 Gateway 只使用一个 ASRServiceClient。现已在合法 start 校验后、StreamingRecognize 创建前为会话选择一个 Worker，后续音频、进度和识别结果始终使用同一条 stream。不能逐块换 Worker，否则有状态 ASR 的上下文和结果归属会被打散。

目标是建立可解释的新会话分配基线，并保留 Worker ID 供后续会话归属及指标使用；此时不预填容量提升或延迟改善数字。

## 策略选择与取舍

| 策略 | 优点 | 代价或限制 |
| --- | --- | --- |
| 轮询 | 状态少，固定列表下选择次数差至多 1，可建立可重复基线 | 不感知会话时长、Worker 速度及健康状态；不保证活跃数或计算负载均衡 |
| 随机 | 简单，无需轮询游标 | 小样本可能分配不均，实验需处理随机性 |
| 最少活跃会话 | 能随会话退出调整选择 | 必须准确登记、释放与处理建流失败；活跃数仍不是实际计算负载 |
| 加权轮询 | 可表达已知容量差异 | 权重需要依据，不能自动反映实时抖动 |

先实现轮询，后续在同等总处理名额和输入条件下比较更复杂策略。先不抽象通用调度接口：当前轮询不需要释放通知，未来最少活跃会话需要选择与登记的一致性，届时再设计完整获取/释放契约。

## 接入与资源责任（后续小步）

- cmd/gateway 组装固定 Worker 列表，每个 Worker 使用可复用的 grpc.ClientConn 和 ASRServiceClient。启动过程中失败关闭已创建连接；退出时先结束会话，再关闭连接。每个会话只新建 RPC stream，不新建 ClientConn。
- 网关准入与合法 start 校验成功后选一次 Worker。超额请求、非法 start 不消耗轮询选择。选择对象保留稳定 ID，便于后续日志和指标归因。
- 轮询锁只保护选择和游标更新；网络建流、Send、Recv 和会话清理均不在锁内。
- 选中后建流失败按现有 Worker 失败路径收尾，不回退轮询游标、不主动在列表中重试；后续新会话继续轮询。选中并不代表 Worker 健康，建流调用返回成功也不能代替整个流成功。
- 已建立会话固定使用选中 Worker 的 stream；不做会话迁移或音频重放。健康剔除/重试规则另行设计。
- 不增加会话 ID 注册表，已有 sessionTracker 仍负责 Gateway 总准入和清理等待；选择器不承担资源保护或会话生命周期职责。

## 独立轮询选择器（已验收）

开发者已新增 internal/workerpool/round_robin.go，本步未修改 Gateway、session 或启动配置。这里 workerpool 保存 Worker 客户端列表，不创建连接池、不创建 goroutine、不执行 RPC、不关闭外部连接。

类型：

```go
// Worker 描述可被选中的计算后端；Client 由外部创建并复用。
// ID 用于标识日志和指标中的归属，本身不参与网络寻址。
type Worker struct {
    ID string // 列表内唯一、非空白的稳定标识。
    Client asrv1.ASRServiceClient // 发起 RPC 的客户端，由外部管理连接生命周期。
}

// RoundRobin 在固定 Worker 列表上循环选择，支持并发调用。
// 必须由 NewRoundRobin 构造，使用后不得复制。
type RoundRobin struct {
    mu sync.Mutex // 只保护 next，持锁期间不执行网络操作。
    workers []Worker // 构造时复制的列表，之后不再修改。
    next int // 下一次选择下标，始终处于 [0, len(workers))。
}

// NewRoundRobin 校验并复制列表；保留调用方提供的顺序。
// 空列表、空白 ID、重复 ID 或 nil Client 返回错误；不连接或探测 Worker。
func NewRoundRobin(workers []Worker) (*RoundRobin, error)

// Pick 返回本次选中的 Worker，并推进下次选择位置。
// 每次调用算一次选择，即使后续建流失败也不回退；不判断健康或可用容量。
func (r *RoundRobin) Pick() Worker
```

构造使用 strings.TrimSpace 检查空白 ID，重复检测按原始 ID 精确匹配；浅复制切片及 Worker 值，Client 保持共享引用。Client 不得为 nil，调用者也不得传入包含 nil 指针的接口。成功构造保证列表非空且固定，Pick 不需要返回错误；未来动态摘除时需重新设计无可选 Worker 的错误契约。

Pick 在锁内取 workers[next]，推进并回绕下标，返回 Worker 值副本。优先选择 Mutex 而非原子自增取模：当前临界区仅几个内存操作，有界下标无累计计数溢出，便于后续理解状态一致性；不宣称此处已测得高并发性能。

## 验收与数字化依据

助手补：构造拒绝非法列表；单 Worker 恒定选择；A/B/C 连续选择顺序；修改原切片或返回的 Worker 值不改变内部列表；并发选择无数据竞争，固定 N 次调用的总数准确且各 Worker 被选择次数差至多 1。N 为测试条件，不是会话吞吐。

后续接线另验：非法 start 不分配、同会话多块音频只到同一后端、并发会话无串流、建流失败不回退或意外重试、停止时全部会话及连接清理。负载实验再记录各 Worker 选择数、活跃会话、失败数与延迟；不能仅凭轮询次数均匀断言稳定容量提高。

## 2026-09-28 初稿检查与测试（历史记录）

开发者已实现 Worker、RoundRobin、构造校验与切片复制，以及短锁内的 Pick 和下标回绕。构造检查重复 ID 时漏写已见 ID 的 map，因此重复 ID 不会被拒绝。修正时按原约定使用原始 w.ID 作为查重和插入键；strings.TrimSpace 仅用于判定是否全空白，不作为 ID 规范化步骤。

助手新增 [round_robin_test.go](../../../internal/workerpool/round_robin_test.go)，执行：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race ./internal/workerpool -count=1 -timeout=30s -v
```

共五个顶层测试、13 项叶级检查，11 项通过、2 项失败：相邻与非相邻重复 ID 均未被拒绝。单 Worker/多 Worker 轮询、原始 ID 保留、原切片与返回值修改隔离通过；32 个 goroutine 共选择 3200 次，三个 Worker 计数为 1067/1067/1066，后续选择位置正确，未报告数据竞争。测试客户端在发生 RPC 调用时 panic，验证选择器不主动建流。

上述计数只验证选择规则和并发安全，不是会话吞吐或调度收益。尚未接入 Gateway，因此本次仅运行独立包测试。业务代码留开发者修正，当前不标记验收完成、不提交未通过的代码。

## 2026-09-28 修正后验收

构造函数已在每个 Worker 校验成功后执行 m[w.ID] = struct{}{}，查询和登记均使用原始 ID，TrimSpace 仅检查空白。助手未修改业务逻辑。

再次执行上述独立包 race 命令：五个顶层测试、13 项叶级检查全部通过，包耗时 1.557s，未报告数据竞争。此前失败的相邻与非相邻重复 ID 均被拒绝；32 个 goroutine 共 3200 次选择仍为 1067/1067/1066，下一次选择位置正确。单 Worker、多 Worker 回绕、原始 ID 保留、列表及返回值隔离均通过。

源码格式和 git diff --check 通过。本步尚未接入其他包，因此仅复验 workerpool 包，未重复网络回归。选择次数的均匀性不代表活跃会话、计算负载或稳定容量均衡。下一步接入 Gateway：合法 start 后选择一次，整场复用选中的 stream，再验证协议失败不分配、会话归属和异常清理。

## Gateway 与 session 接入轮询（2026-09-28，已验收）

目标：让现有选择器进入真实会话路径，并保持第三、四阶段的收尾与保护行为。生产入口本步仍组装一个 Worker，测试使用两个 Worker；多地址命令行配置留后续独立小步。

### 接入方式与取舍

- 在 ServeHTTP 接纳连接时选择：改动较少，但非法/超时 start 也推进轮询，不符合当前分配语义。
- 将选择器包装为 ASRServiceClient：可以保持 Gateway 构造签名，但 Worker 归属容易隐藏在代理内部，后续按 Worker 归因需额外传递。
- session 在合法 start 后显式 Pick，再使用选中 Client 建流：选择时机、Worker ID 和 stream 归属直接可见；选择此方案，代价是 Gateway/newSession 构造调用需要适配。

当前直接依赖 *workerpool.RoundRobin，先不增加只有 Pick 的通用调度接口。未来最少活跃会话还需要原子登记和可靠释放，不应假定替换 Pick 就足够。生产依赖需通过 NewRoundRobin 构造，禁止传入零值 RoundRobin。

### 开发者修改范围

1. internal/gateway/handler.go：Gateway.worker 替换为 pool *workerpool.RoundRobin；New(ctx,pool,cfg) 校验非 nil，保存传入池，保留所有配置默认值与准入流程。ServeHTTP 传同一个池给 newSession，既不 Pick，也不逐会话创建池。
2. internal/gateway/session.go：session 保存同一 pool 指针及 workerID string（合法 start 后选中，表示后端归属，不是会话 ID）；newSession 第二参数改为 pool。run 保留 start 校验和 RPC/WS context 构造，紧接建流前执行 selected := s.pool.Pick()，保存 selected.ID，再 selected.Client.StreamingRecognize(rpcCtx)。建流错误包含 Worker ID 并用 %w 包装，其余 finish/取消/等待逻辑保持现有行为。upload/download 继续只接收同一 stream，不能再次 Pick。
3. cmd/gateway/main.go：复用现有 grpcConn/workerClient，以 workerAddr 为 ID 构造仅含一项的 RoundRobin，处理构造错误，再传给 gateway.New。原连接关闭和会话退出顺序保持不变，不新增多地址参数或每会话连接。

本步不新增选择失败分支或 release：已构造池的列表固定非空，轮询不占用 Worker 会话名额。建流失败照常收尾，已推进游标不回退，不对其他 Worker 重试；已有 sessionTracker 仍在 handler 清理后减少总会话计数。

### 助手后续测试任务

适配所有旧测试里的 Gateway.New/newSession 调用，使用单 Worker 选择器保留旧测试意图；开发者无需修改测试。补验证 nil pool 拒绝、未发/非法/超时 start 不推进轮询、停止/准入拒绝不选择、合法会话只 Pick 一次、同会话多块音频和结果固定归属、两个 Worker 并发无串流、选中 Worker 建流失败后下个会话仍分到下一个 Worker且无隐式重试、异常后清理及名额复用。

运行相应集成测试和完整 race 回归；只记录实际执行的会话数、分配数与清理结果，不将短测试写成容量或吞吐结论。实际多 Worker 启动配置尚未完成，Gateway 默认仍使用 localhost:50051。

## 2026-09-28 Gateway 接入验收

开发者已完成三个生产文件：Gateway/session 共享一个 RoundRobin；start 校验成功后保存选中 Worker ID 并创建一次 stream；建流错误保留 ID 与错误链；生产入口复用原 grpcConn，以 localhost:50051 组装单项池。现有 finish、取消、goroutine 等待及 tracker 清理逻辑保持不变。

助手适配旧测试的构造参数及测试 session 字段，新增单后端组装帮助函数 [worker_pool_test.go](../../../internal/gateway/worker_pool_test.go) 和七个顶层路由测试 [worker_routing_test.go](../../../internal/gateway/worker_routing_test.go)。测试使用真实 WebSocket 与两个独立临时 TCP gRPC 后端；建流失败在客户端方法处可控注入。

验证结果：

- 6 个顺序会话轮流分配为 A/B 各 3；12 个已建立且同时保持的会话分配为各 6，确认 Gateway 活跃数为 12 后并行完成剩余传输。每个正常路由会话三块音频及尾部结果均保持后端/会话归属，正常关闭后 Worker 退出、handler 返回、活跃数归零。会话建立顺序由测试控制，未将此写成并发到达负载。
- 非法 start、等待 start 超时、start 前断连、start 前停服、停止接入后拒绝、WebSocket 升级失败六类场景均没有后端调用，清理后轮询仍从 A 开始。
- MaxSessions=1 时额外连接被 503 拒绝，原会话在 A 正常完成后，新会话到 B，拒绝不消耗轮询且名额可复用。
- A 建流失败 → B 正常完成 → A 再次建流失败：失败没有隐式重试或回退；每次 handler 返回后活跃数为 0，失败 RPC context 已取消。另直接检查 session.run 返回值的 Worker ID、%w 错误链和 session.workerID。
- 两个后端都有活动会话时，A 客户端断开后只清理 A，B 继续完整结束；服务停止时两个后端 RPC 均取消，handler 和总会话计数完成清理。
- Gateway 构造拒绝 nil pool；此前单后端生命周期、准入和背压测试继续通过。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./... -count=1 -timeout=180s` 全部通过，未报告数据竞争：Gateway 13.674s、cmd/gateway 1.820s、Mock 1.777s、workerpool 2.815s、cmd/asr-worker 4.257s。未开启额外可选负载实验。

这些是分配与清理的正确性证据，不是稳定容量、吞吐或延迟改善结论。Gateway 库已支持传入多 Worker 池，命令入口仍只组装 localhost:50051；下一步补多地址启动配置与连接组装/回收，之后再推进负载工具和指标。

## Gateway 多地址配置与连接组装（2026-09-28，已验收）

目标：通过同一 Gateway 可执行程序配置多个后端，不再修改源码中的单个 workerAddr。选择单个 -workers 逗号分隔列表，默认 localhost:50051，保留输入顺序。相比重复 -worker 参数，无需自定义 flag.Value；相比配置文件，当前两个配置项无需引入文件加载和覆盖规则。重复指定 -workers 沿用标准 flag 字符串参数的后值覆盖前值行为，不累计。

采用入口级 gatewayConfig{WorkerAddrs []string; Gateway gateway.Config}，parseGatewayConfig 返回该类型；后端地址属于依赖组装，不加入内部 gateway.Config。负音频预算仍由 gateway.New 校验。run 改为接收 gatewayConfig，使用 cfg.Gateway 构造 Gateway，原 HTTP 地址及退出协调保持不变。

开发者修改 cmd/gateway/config.go 与 main.go，可新增同包 worker_addresses.go 存放纯函数 parseWorkerAddresses(raw string) ([]string,error)。助手适配旧测试并补参数与生命周期验证。

地址规则：

- 使用 strings.Split 按逗号拆分，逐项 TrimSpace；空列表、空项（包括中间/尾部逗号）报错，不静默过滤。
- 本步支持 host:port：域名/IPv4 或带方括号的 IPv6。使用 net.SplitHostPort 校验拆分，host 非空且不包含空白、正斜杠或反斜杠；port 为十进制数字且范围 1..65535。解析器 URI（如 dns:///...）不在本步配置范围；这是项目限定，不是 gRPC 能力限制。
- TrimSpace 后的完整地址字符串用于查重，重复报错；保留顺序、大小写及其余原始表示，不做 DNS 查询、域名/IP 别名归并或端口写法归一化。因此不是物理后端去重，也不支持用重复地址表达权重。
- 解析失败返回零配置；帮助原样返回 flag.ErrHelp；保留 max-pending-audio-bytes 的默认值、最大消息上限及既有语义。

连接组装规则：run 在循环前声明 []*grpc.ClientConn 并登记统一 defer 清理，每个 NewClient 成功立即追加，然后构造 Worker{ID: addr, Client: asrv1.NewASRServiceClient(conn)}；列表全部组装后创建 RoundRobin，再创建 Gateway 与 HTTP 服务。中途任意错误均通过该 defer 关闭此前已创建的客户端对象，不在单次会话结束时关闭共享 ClientConn。正常退出先由 serve 取消/等待会话，run 返回时再关连接；退出等待超时则记录原有错误并继续兜底回收，不能声称超时等于清理成功。

已核对本地 grpc-go v1.83.2 clientconn.go：NewClient 不执行网络 I/O，创建客户端成功不意味着 Worker 在线；本步不增加启动探活、健康剔除或自动换 Worker 重试。连接到不可用后端的失败仍由会话处理。创建错误应注明对应地址并保留 %w 错误链；日志记录 Worker 地址列表/顺序与 round_robin 策略，不宣称列表中后端已健康。

后续助手验收：保留单地址默认值，双地址/IPv6/空白修剪/顺序；空项、重复及非法端口/主机形式拒绝；旧预算与帮助规则保持；完整链路轮询及服务退出后的连接关闭。可触发错误路径与仅代码审查的清理分支分别记录，不把 NewClient 构造成功或静态配置检查当成在线探测。此步实现前 docs/command.md 不提前发布可用多地址命令。

### 多地址初稿检查（2026-09-28）

入口级 gatewayConfig、-workers 参数和 run 的有序连接组装已实现；run 在循环前登记 defer，每个成功创建的 ClientConn 立即追加，后续返回时统一关闭，代码结构符合清理责任约定。当前发现两项解析问题：

1. parseGatewayConfig 仍直接 strings.Split，未调用已新增的 parseWorkerAddresses，导致去空白、空项、重复及格式校验被命令行入口绕过。应调用辅助函数，失败返回零配置，成功保存校验后的列表。
2. parseWorkerAddresses 使用 strconv.Atoi，接受 +50051，与本步无符号十进制端口规则不一致。可改用 strconv.ParseUint(port,10,16)，处理错误并拒绝 0，保留地址原始表示。

助手已适配旧配置测试，并新增 worker_addresses_test.go。有效地址与非法地址同时经过 helper 和完整 CLI 两条路径验证；保留默认值、预算语义、帮助、重复参数后值覆盖和多次解析隔离检查。

执行 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race ./cmd/gateway -count=1 -timeout=30s -v`：77 项叶级检查中 54 通过、23 失败，未报告数据竞争。旧 20 项配置检查通过；新增 57 项中 23 项失败，分别是 CLI 首尾空白未去除、21 类非法 CLI 地址被接受，以及 helper 接受带加号端口。

业务代码留开发者修正；连接回收目前只有代码审查结论，尚未执行本步的多地址入口链路/退出回收验证。本次未运行全项目重复回归，不标记完成、不提交未通过的代码，也不发布已可用的多地址命令。

### 地址修正复验及入口日志问题（2026-09-28）

parseGatewayConfig 已调用 parseWorkerAddresses 并传播错误，端口已改为 ParseUint(port,10,16) 且拒绝 0。当前 main 的配置日志改成 slog.Info，但仍使用 Printf 格式串和单个值参数，普通包测试被 vet 拒绝：slog.Info 的 cfg.WorkerAddrs 参数应对应字符串键或 slog.Attr。应改为消息加键值对，例如 slog.Info("gateway backend configuration", "strategy", "round_robin", "workers", cfg.WorkerAddrs)。

为独立确认解析修正，在 cmd/gateway 目录执行：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race config.go worker_addresses.go config_test.go worker_addresses_test.go -count=1 -timeout=30s -v
```

全部 77 项配置检查通过（1.635s），未报告数据竞争。该命令只编译列出的配置源码及测试，不包含 main.go，不能视为 Gateway 包或完整启动验收；没有关闭 vet。修正启动日志后，继续多地址完整链路和连接回收验证。当前不提交未通过的整体实现。

### 多地址入口修正后验收（2026-09-28）

地址校验已接入，端口 ParseUint 与 slog 键值对写法均已修正。助手新增 [startup_test.go](../../../cmd/gateway/startup_test.go)，没有修改生产逻辑。

执行：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
go test -race ./cmd/gateway -count=1 -timeout=120s -v
```

整包测试及默认 vet 通过（3.188s），共 86 项叶级检查：原配置和地址 helper/CLI 共 77 项、真实可执行程序退出检查 8 项、完整多地址启动链路 1 项。未报告数据竞争。

真实可执行程序验证两种帮助退出 0；空列表、重复、带加号端口、尾部空项、未知参数以及负预算均退出 1，均未启动 HTTP 监听。负预算在地址解析成功后由 Gateway 构造拒绝，其配置日志按键值对记录策略和有序地址，无 !BADKEY。子程序通过普通 go build 构建，未声称对子程序开启 race。

网络集成直接调用当前 parseGatewayConfig → run，使用两个仍在运行的真实 TCP gRPC 测试后端。以含首尾空白的 B,A 地址顺序配置，6 个真实 WebSocket 会话依次到 B,A,B,A,B,A；前 4 个发送两块音频并正常收到尾部结果与 1000 关闭，后 2 个保持活动并在应用 context 取消后收到 1001，Worker RPC 返回 Canceled，run 返回 nil。

通过服务端 gRPC stats.ConnBegin/ConnEnd 观测：首次 RPC 前没有建立后端连接；每个 Worker 的 3 条 stream 共用 1 条连接（opened=1、closed=0），run 退出后两者均 opened=1、closed=1。断言时测试后端尚未 Stop，测试进程仍在运行，因而此结果不依赖服务端或进程退出帮忙关闭连接。两个仍活动的 RPC 和共享连接均完成清理。

该测试因当前 HTTP 地址固定 :8080，会在端口已占用时明确跳过；本次实际执行并通过，没有跳过。中途 NewClient 构造失败后的统一 defer 清理只完成代码审查，未通过故障注入验证；不混同为实测证据。

本步只改变 cmd/gateway 的启动组装，定向包测试已覆盖受影响入口及真实链路，未重复其他包的网络回归。更新 [运行文档](../../command.md) 发布 -workers 与预算组合命令。这些属于配置、分配与资源回收正确性证据，不是容量或吞吐结论；后续进入并发负载工具、指标口径和容量实验。
