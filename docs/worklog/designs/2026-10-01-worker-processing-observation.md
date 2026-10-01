# 第五阶段：Worker 处理占用与等待观测

日期：2026-10-01
状态：名额池快照、Worker 层 ProcessingSnapshot 及 HTTP 查询响应与路由均已验收。第四步之一明确可选 debug-listen 参数，等待开发者实现纯配置解析；正式监听和双服务协调尚未接入，进程外采样尚未接入，没有新增性能或容量结果。
相关：[共享处理名额](2026-09-27-worker-processing-capacity.md)、[Gateway 联合观测基线](../../experiments/gateway-observation-baseline.md)

## 为什么继续补这项

EXP-005-03 已用独立采样确认九批计划 1/2/4 场对应的 Gateway 活动峰值和前后零活动状态，但活动会话不等于正在处理的音频块。Worker 可能在接收音频、等待处理名额、模拟处理或发送结果；若只看 Gateway active，负载变慢后仍无法判断是否在争用共享处理能力。

本轮先让已有 processingSlots 提供一致的名额状态快照，随后再设计 Worker 访问方法、进程外查询和采样。仅计数不改变处理延迟、名额上限和音频流程，也不增加任务执行池或待处理音频队列。首步不增加等待时长分布、CPU/GPU 使用率、累计吞吐或容量结论。

## 方案比较与选择

| 方案 | 好处 | 代价与本次选择 |
| --- | --- | --- |
| 仅读取 len(tokens) | 不增加额外计数 | 只能看到物理预占，缺少等待者信息 |
| len(tokens) 加独立 atomic 等待计数 | 更新简单 | 获取/归还与计数交接跨多个操作，读取可能混合时刻；不适合声称一份一致状态 |
| 保留 channel，用短锁维护逻辑占用/等待并返回快照 | 复用既有阻塞唤醒和取消路径，快照字段统一读取 | 每块处理增加短临界区，需要明确交接中的等待口径；本次采用 |
| 改成互斥锁加通知 channel 或显式等待队列 | 状态与调度可以完全统一，能进一步做公平性 | 同时改变调度实现；广播可能造成集中唤醒，显式队列需要取消移除与交接协议；本轮不采用 |

采样一致性不等于无成本；不能预先宣称新增锁无性能影响。后续观测基线会包含这一实现版本，只有另做 A/B 才能量化它的开销。保留 channel 也不承诺严格 FIFO 或无饥饿。

## 指标定义

```go
// processingSlotsSnapshot 是一个处理名额池的值快照。
// 三个字段由同一临界区读取，不包含音频或会话对象。
type processingSlotsSnapshot struct {
    Limit int // 配置的处理名额上限，始终大于零，不是实测稳定容量。
    InUse int // 已登记成功获取、尚未执行有效归还的名额数。
    Waiting int // 因满额登记、尚未完成成功获取或取消收尾的请求数。
}
```

Waiting 只在非阻塞尝试发现满额后登记；容量内立即获取不增加它。等待者已经被唤醒但还未取得记账锁时，或已收到取消但还未完成收尾时，仍在 Waiting 中。因此它描述逻辑等待请求，不是运行时当前阻塞的 goroutine 数，也不是已接收音频字节数。获取成功时在同一锁内执行 Waiting--、InUse++，快照不会看到该登记请求同时属于两个逻辑状态。

InUse 表示名额所有权，包含从成功登记到实际模拟处理开始、以及处理结束到归还登记之间的很短交接区间，不是 CPU 正在执行的线程数。当前 Worker 在 processChunk 内获取并 defer 归还，每条流串行处理，所以这里对应处理音频块的获取请求；不能当成完整 Worker 活跃流计数。

例如 Gateway active=4、Limit=1 时，InUse=1、Waiting=3 是四个块同时争用名额的一种可能状态；InUse=1、Waiting=0 也合法，其他会话可能还在接收音频或发送结果。跨进程采样也不是同一时刻，不能要求 Gateway active 等于 InUse+Waiting。

必须满足：0 <= InUse <= Limit、Waiting >= 0；所有获取请求完成或取消、所有持有者归还后，两项都归零。Waiting 没有由 Limit 给出的上限，仍不构成完整排队或内存保护。

## 本小步代码范围

只修改 internal/mockasr/processing_slots.go，保留 newProcessingSlots(limit)、acquire(ctx) 的已有签名与成功/取消语义。

```go
// processingSlots 保留 channel 名额控制，并维护逻辑占用与等待状态。
// 必须通过 newProcessingSlots 创建，创建后不得复制。
type processingSlots struct {
    tokens chan struct{} // 物理名额，容量固定；只在本类型的方法中操作。
    mu sync.Mutex        // 保护 inUse/waiting 及逻辑状态转换；不跨容量等待持有。
    inUse int            // 已成功登记且尚未归还的名额数。
    waiting int          // 因满额登记、尚未完成获取或取消的请求数。
}

// snapshot 返回同一临界区内的值快照，不暴露内部 channel 或锁。
// 接收者必须是构造成功的非 nil 名额池；Limit 来自固定的 cap(tokens)。
func (s *processingSlots) snapshot() processingSlotsSnapshot

// newRelease 为一次已成功登记的名额创建幂等归还函数。
// 仅在本次 acquire 已增加 inUse 后调用一次；本方法自身不获取或归还名额。
// 返回函数可重复或并发调用，实际归还一次，不得归还后续持有者的名额。
func (s *processingSlots) newRelease() func()
```

newProcessingSlots 继续拒绝 limit<=0，只需创建固定容量 tokens，锁和计数使用零值。ctx 非 nil 仍为 acquire 调用前提。snapshot 不支持 nil 接收者；Worker 关闭限制时 slots=nil，后续 Worker 层必须明确表示“未启用限制/未采集”，不能伪造 Limit=0、InUse=0 表示空闲。本步不改 Worker 或对外接口。

## acquire 的实现顺序

### 立即获取路径

1. 检查 ctx.Err()，已经结束则原样返回 nil,error，不登记计数。
2. 加锁，再检查 ctx.Err()；若在等锁期间取消，解锁返回。
3. 在锁内做一次非阻塞发送：`select { case s.tokens <- struct{}{}: ...; default: ... }`。这里的 default 是关键，满额不能持锁阻塞，否则归还和 snapshot 都会受阻。
4. 发送成功后再次检查 ctx.Err()。若观察到取消，取回刚预占的一个 token、解锁并返回原错误，InUse/Waiting 不变。
5. 未取消则 InUse++，解锁，返回 s.newRelease(),nil。不要增加 Waiting。

### 满额等待路径

1. 上述 default 分支仍在锁内：Waiting++，保存该次调用已经登记等待的事实，然后解锁。
2. 使用原来的可取消 select，在向 tokens 发送与 ctx.Done() 之间等待，期间不持有 mu，不新增 goroutine、轮询或 timer。
3. 若取消分支胜出，加锁 Waiting--，解锁，返回 nil,ctx.Err()；本分支没有取得 token，不能接收 token，也不能减少 InUse。
4. 若发送分支胜出，加锁 Waiting--，再检查 ctx.Err()。
5. 已取消则取回一个刚预占的 token，解锁返回错误，InUse 不增加；未取消则 InUse++，解锁返回 s.newRelease(),nil。

每次因满额登记 Waiting 都必须且只能扣减一次。不要用循环重试时反复递增，也不要在已经扣减的分支另加 defer 再扣一次。取消与名额同时可用时，保留取得后复查取消的行为；成功返回后再发生取消仍由调用方完成处理退出并显式归还。

## 归还与快照

newRelease 每次创建独立的 sync.Once。返回闭包的 once.Do 内：加锁、InUse--、从 tokens 接收一个元素、解锁。这几个操作必须都在同一临界区内，防止新持有者先增加 InUse 而旧持有者还未扣减，出现超过 Limit 的逻辑计数。

此处接收 token 不承担容量等待：调用者已经成功持有一个名额，且所有有效归还/取消回滚都按所有权成对执行，因此池中必有可归还的 token。旧闭包重复调用被 sync.Once 拦截，不能拿走他人的 token。只有“获取名额”的发送可能因为满额而阻塞，必须在锁外执行。

snapshot 加锁读取 cap(tokens)、inUse、waiting 后返回值副本。不要用 len(tokens) 替代 InUse：慢路径中可能有已经物理取得 token、尚未完成逻辑登记的请求，此时仍记在 Waiting。Limit 取 cap(tokens) 无需再维护第二份可变上限。

## 验收计划与下一步

开发者实现，助手补测试：容量内快照、满额后的单个/多个等待者、归还后的等待转占用、等待取消/到期、成功后取消不自动归还、重复与并发归还、取消/归还竞争、并发快照边界和最后归零；继续运行已有名额池与 Worker 测试，验证网络发送/暂停/停读仍不占名额。

同步次序优先使用 testing/synctest 或明确的同步点，不用 sleep 猜测等待状态。计数测试不是吞吐实验；本步没有新增对外查询、采样文件或容量结论。验收后再设计 Worker 层的开关语义与访问入口，随后接外部采样和等待时长观测。


## 首步实现与验收（2026-10-01）

开发者实现 [processing_slots.go](../../../internal/mockasr/processing_slots.go)，保留 channel 名额控制，增加 InUse/Waiting、立即获取与满额等待两条路径、值快照及独立幂等归还闭包。初次审查发现持锁复查 ctx.Err 的返回分支缺少 Unlock；复查时开发者已补齐，助手未修改生产逻辑。新增确定性回归检查确保该路径返回取消错误后，锁已释放、快照可读且名额仍可复用。

助手新增 [processing_snapshot_test.go](../../../internal/mockasr/processing_snapshot_test.go)，7 个顶层测试、9 项叶子检查：

- 容量内获取只增加 InUse，历史快照和值副本不随内部变化而改变，归还后归零。
- 容量为 1 时登记 3 个等待者，每次归还只完成一个占用交接，Waiting 依次减少，最后两项归零；不要求等待顺序。
- 等待取消和期限到期两个用例，只撤销对应请求的 Waiting，其他持有者与等待者不受影响。
- 持有者 context 取消不提前减少 InUse；旧归还闭包被 32 个 goroutine 重复调用，不扣减新持有者，当前持有者归还后归零。
- 首次取消检查读取后、持锁复查前取消：获取返回错误后验证锁可取得，计数未增加、名额可复用。同步点控制顺序，未用 sleep 猜测。
- 快路径和慢路径各一次取得物理 token 后取消：计数和 token 都回滚，随后可重新取得名额。
- 24 个调用者各获取/归还 25 次，共 600 次，同时读取快照；占用不超过 3、计数非负、逻辑请求数不超过本测试调用者数量，所有操作结束后归零。该操作数是并发正确性检查，不是吞吐数据。

定向 `go test -race ./internal/mockasr -run '^TestProcessingSnapshot' -count=1 -timeout=30s -json` 的 9 项检查通过。随后运行 `go test -race ./... -count=1 -timeout=120s -json`，全项目 946 项叶子检查通过，12 个需显式开启的实验默认跳过，无数据竞争报告。Mock 包 34 个顶层测试、83 项叶子检查通过（5.155s）；包含原有名额竞争、取消、Worker 跨流处理与故障边界回归。

本步只增加包内名额状态观测，没有修改 Worker 的开关配置或处理区间，没有新增 HTTP/gRPC 查询，也没有重跑正式负载实验。旧 EXP-005-03 仍对应其记录的源码版本，不能当作新增短锁之后的性能测量。下一步定义 Worker 层的可访问快照，明确 slots=nil 时表示限制未启用，不伪造零占用结论，再考虑对外查询与采样。


## 第二步：Worker 层的处理快照访问（2026-10-01，已验收）

### 问题与选择

名额池已经能记录状态，后续命令/查询入口不应访问 Worker 的私有 slots，也不能把 slots=nil 时的零值解释为空闲。ProcessingConcurrency=0 表示不施加这个共享处理限制，processChunk 仍然会执行；只是当前没有名额池来统计这段占用与等待。关闭限制不等于计算能力无限，也不等于没有处理任务。

| 方案 | 好处 | 代价与选择 |
| --- | --- | --- |
| 关闭限制时直接返回三个零 | 返回类型简单 | 与已启用但空闲的状态混淆，不采用 |
| 返回 *ProcessingSnapshot，nil 表示关闭 | 可自然映射缺失值 | 暴露可选指针和解引用，本步只需要一份值与是否启用，暂不采用 |
| 返回 (ProcessingSnapshot, enabled bool) | 明确区分有效数据与未启用，不引入指针或错误语义 | 调用方必须先检查 enabled；本步采用 |
| 返回错误表示未启用 | 强制调用方处理 | 未启用是合法配置，不应作为运行失败，不采用 |

公开类型与私有 processingSlotsSnapshot 分开，Worker 负责映射实际池快照；不导出内部类型别名或名额池指针。未来 JSON 是否用 null、哪些字段展示，由传输层单独定义，不在本步添加 JSON tags。

### 实现范围

只新增 internal/mockasr/snapshot.go，package mockasr。生产代码由开发者实现。

```go
// ProcessingSnapshot 描述本 Worker 已启用的共享处理名额状态。
// 仅在 Worker.ProcessingSnapshot 返回 enabled=true 时具有观测含义。
// 它是值副本，不引用内部可变状态，也不是 Worker 活跃会话或 CPU 使用率。
type ProcessingSnapshot struct {
    Limit int // 实际名额池上限，大于零，不是实测容量。
    InUse int // 已登记成功获取、尚未有效归还的名额数。
    Waiting int // 因满额登记、尚未完成获取或取消收尾的请求数。
}

// ProcessingSnapshot 返回处理名额的值快照及共享限制是否启用。
// 未启用时返回零值和 false，零字段是占位值，不能解释为没有任务。
// 启用时三个字段来自名额池同一次 snapshot，可与处理流程并发调用。
// w 必须由 New 成功创建；方法不改变状态，不等待处理结束，不执行 I/O。
func (w *Worker) ProcessingSnapshot() (snapshot ProcessingSnapshot, enabled bool)
```

执行顺序：

1. w.slots==nil 时返回 ProcessingSnapshot{},false。不为关闭模式临时创建池，也不改变 Worker 配置。
2. 启用时调用一次 w.slots.snapshot()，取得三个字段来自同一临界区的内部快照。
3. 将 Limit/InUse/Waiting 映射为公开的 ProcessingSnapshot，返回该值和 true。

不分别调用三次 snapshot 拼接字段，不重新从 cfg 读取 Limit，不复制额外计数器。Worker 的 slots 在 New 中设置后不会被运行逻辑替换，读取该指针不需要增加 Worker 级锁；可变计数已由池内锁保护。本方法可能短暂等待记账锁，但不会等整个处理过程完成。若未来增加动态替换池或开关功能，必须重新设计指针同步与快照语义。

不为 nil Worker 返回 false 来掩盖构造/调用错误；与 Gateway.Snapshot 一样，成功构造是调用前提。未启用是合法 Worker 状态，nil Worker 不是。

### 示例与验收

启用、空闲：`ProcessingSnapshot{Limit:2, InUse:0, Waiting:0}, true`。
关闭限制：`ProcessingSnapshot{}, false`，即使此时 processChunk 正在运行也仍为 false，当前没有该名额计数。

助手会用实际 processChunk 竞争和取消场景验证公开访问反映占用/等待与最终归零，并检查关闭限制时正在处理也返回未启用、启用空闲可区分、历史值不会跟随内部状态变化。测试重点是业务状态与有效性，不重复每个内部赋值的机械断言。

本步验收后再设计进程外查询接口与传输格式；没有新增 HTTP 服务、采样接入、等待时长或性能结果。

### 第二步实现与验收

开发者实现 [snapshot.go](../../../internal/mockasr/snapshot.go)，使用独立公开值类型与 enabled，启用时映射一次池快照，未启用时返回零值与 false。审查未发现需要修改的生产逻辑；未增加 Worker 级锁或改变处理名额的获取、归还区间。

助手新增 [snapshot_test.go](../../../internal/mockasr/snapshot_test.go)，两个顶层测试、四项叶子检查，使用实际 processChunk 和虚拟时间控制状态：

- 关闭限制时，同时运行两个处理请求，处理前、处理中与取消后均返回零值和 false，避免将未采集解释为空闲。
- 启用一个名额时，正常完成、等待者取消、持有者取消三种路径均验证占用与等待的转换，以及最后归零；未取消的请求正常完成。
- 在上述生命周期用例中同时验证历史快照不随状态变化、修改返回副本不影响池内状态，以及不同 Worker 的计数互不影响。

执行 `go test -race ./internal/mockasr -count=1 -timeout=60s -json`，36 个顶层测试、87 项叶子检查全部通过，无跳过、失败或数据竞争报告。本次只运行相关 Mock 包回归；上一小步的全项目检查结果保留原记录，不冒充本次执行。

本步完成的是进程内公开访问，尚不能通过网络采集 Worker 状态。下一步设计 HTTP 查询接口、未启用时的 JSON 表达及命令中的服务接入；没有新增负载实验或性能结论。

## 第三步：HTTP 查询响应与路由（2026-10-01，已验收）

### 价值与方案选择

进程内的公开方法还不能被外部负载工具调用。需要一个只读查询入口，保留未启用语义，使后续采样可以区分没有计数与有效零占用。Gateway 活动会话数和 Worker 处理名额计数衡量不同阶段，不能互相代替。

| 方案 | 好处 | 代价与本次选择 |
| --- | --- | --- |
| 定期输出日志 | 不需要新增监听器 | 采样方需要解析日志、处理交错并统一采样时间，暂不采用 |
| 新增 gRPC 状态 RPC | 复用现有服务监听和客户端技术 | 需要修改 proto、生成代码和查询客户端；可行，但本项目已有 HTTP 观测模式，暂不采用 |
| HTTP JSON 查询 | 可以单独请求，沿用 Gateway 的查询和采样思路 | Worker 要新增 HTTP 服务及退出协调；采用，分步接入 |
| Prometheus 指标 | 便于后续监控、时序查询和看板 | 本轮只需要明确的小型状态快照，暂不引入完整指标接入 |

本步只实现 Handler 和私有路由，不创建监听器、不修改 run 或配置。之后单独设计 HTTP 监听地址、启动失败回滚及 HTTP/gRPC 退出协调。使用显式私有 ServeMux，不修改全局 DefaultServeMux。

### 响应契约

路由为 `GET /debug/worker`，成功状态 200，`Content-Type: application/json`，`Cache-Control: no-store`。与现有 Gateway 路由一致，GET 模式同时允许 HEAD，实际 HTTP 服务不发送 HEAD 响应体；其他方法由 mux 返回 405，未知路径返回 404。

启用限制的响应示例：

```json
{"schema_version":1,"processing_limit_enabled":true,"processing":{"limit":2,"in_use":1,"waiting":3}}
```

未启用限制的响应：

```json
{"schema_version":1,"processing_limit_enabled":false,"processing":null}
```

不用全零对象表示未启用，也不省略 processing 字段。布尔字段明确说明共享限制开关，null 表示没有该计数；它们必须成对一致。采用嵌套可空对象，是因为三个字段的有效性相同，无需分别维护三个可空数字。字段名不使用 processing_enabled，以免误解为关闭限制后不处理音频。有效空闲仍为 enabled=true、limit>0、in_use=0、waiting=0。

此接口 schema_version 独立于 Gateway 与采样文件版本。响应不加入 Worker ID、时间戳、会话数或时长；后续采样方记录查询地址与请求起止时间。本接口只描述一个 Worker 的瞬时逻辑名额状态，不提供跨实例同时刻快照或 CPU 使用率。

### 开发者实现范围

新增 `cmd/asr-worker/snapshot.go`，package main，定义以下类型与函数，并为每个类型、字段和函数补上说明：

```go
// workerProcessingJSON 是共享处理限制启用时的名额状态，不代表活跃会话数。
type workerProcessingJSON struct {
    Limit int `json:"limit"` // 实际名额上限，不是实测稳定容量。
    InUse int `json:"in_use"` // 已登记持有、尚未归还的名额数。
    Waiting int `json:"waiting"` // 已登记、尚未完成获取或取消收尾的等待请求数。
}

// workerSnapshotJSON 定义 Worker 状态查询 v1；与内部快照类型分开维护。
type workerSnapshotJSON struct {
    SchemaVersion int `json:"schema_version"` // 本接口版本，固定为 1。
    ProcessingLimitEnabled bool `json:"processing_limit_enabled"` // 共享处理限制是否启用。
    Processing *workerProcessingJSON `json:"processing"` // 未启用时为 null，不使用 omitempty。
}

// workerSnapshotHandler 创建只读查询处理函数；worker 必须由 mockasr.New 成功创建。
// 每个请求读取一次快照，不占处理名额；编码和写回发生在快照返回后。
func workerSnapshotHandler(worker *mockasr.Worker) http.HandlerFunc

// routes 返回本 Worker 的私有状态查询路由，不监听端口，不注册 gRPC 服务。
func routes(worker *mockasr.Worker) http.Handler
```

Handler 实现顺序：

1. 在请求处理函数内部调用一次 worker.ProcessingSnapshot()，不能在创建 Handler 时缓存快照。
2. 构造 schema_version=1、processing_limit_enabled=enabled 的响应；仅 enabled=true 时创建并映射 processing 对象，否则保持 nil。
3. 先 json.Marshal；若失败，记录日志并返回 500。固定整数/布尔响应正常不会编码失败，保留错误处理即可，不为这一分支新增生产注入点。
4. 编码成功后设置上述两个响应头，写 200，再写编码后的数据。写入失败记录日志，不在已经开始的 JSON 响应后追加第二个错误响应。
5. 不重新获取处理名额、不增加 Worker 锁，不持有池锁进行编码或网络写回。复用已有 Gateway Handler 的结构，但使用独立 DTO，不给 mockasr 类型增加 JSON 标签。

routes 创建 http.NewServeMux()，注册 `GET /debug/worker` 到该 Handler 后返回；本步不增加 healthz 或新 goroutine。当前命令没有监听这一路由，因此本步完成后还不能直接用 curl 查询运行中的 Worker。

### 验收安排

实现由开发者完成，助手补测试：未启用 null、启用空闲与忙碌/等待、完成或取消后的新请求状态、版本与响应头、GET/HEAD/不支持的方法/错误路径、写回失败与慢写回不阻塞处理名额释放。忙碌/等待通过实际 Worker 流程构造，不导出私有名额池来满足测试。路由可以用 httptest 临时 HTTP 服务验收，无需提前改造命令启动。

这些检查验证接口语义与隔离性，不作为吞吐、稳定容量或观测开销实验。验收后再接入命令服务生命周期，然后设计外部 Worker 查询与采样。

### 第三步实现与验收

开发者实现 [cmd/asr-worker/snapshot.go](../../../cmd/asr-worker/snapshot.go)：独立 DTO、每请求一次快照、未启用时明确 null、编码后设置响应头与写回、私有 GET 路由均符合约定。助手未修改生产代码。

新增 [snapshot_test.go](../../../cmd/asr-worker/snapshot_test.go)，六个顶层测试、14 项叶子检查：

- 未启用和启用空闲两种响应，独立解码核对所有字段、零值、null、版本、响应头与没有多余响应；重复查询不改变名额状态。
- 真实临时 HTTP 服务验证 GET、HEAD 无正文、POST/DELETE 返回 405，以及未知路径和子路径返回 404。
- 正常完成、等待者取消、持有者取消三个场景，通过公开 StreamingRecognize 执行 Worker 处理逻辑，复用同一路由验证空闲、占用、等待、交接和最终归零；正常流完整返回处理进度和 final，取消流不确认未处理音频。
- 限制关闭但仍在处理音频时，HTTP 响应仍明确未采集，处理完成后也不会伪造有效零计数。
- 注入部分写入失败，确认不追加第二份响应，随后查询仍成功。
- 阻塞 HTTP Write 时，音频处理仍完成并归还名额，另一次查询可读取空闲状态；解除阻塞后，旧响应仍保留原先忙碌快照。

执行 `go test -race ./cmd/asr-worker ./internal/mockasr -count=1 -timeout=90s -json`，命令包 48 项、Mock 包 87 项，共 135 项叶子检查通过，无失败、跳过或数据竞争报告。命令包回归包含原有真实 Worker 子进程与 RPC 检查；新增状态转换用例使用内存流替身和虚拟时间，不声称进行了 HTTP 与真实 gRPC 网络的联合负载实验。慢写回/写入失败也使用受控 ResponseWriter，不作为实际弱网测量。

本步没有修改 main、run 或启动配置，正式 Worker 命令仍只监听 gRPC，不能直接 curl 查询。下一步设计 HTTP 监听及 HTTP/gRPC 启动失败回滚与退出协调；本次没有新增性能或容量数据。

## 第四步之一：HTTP 监听配置（2026-10-01，待实现）

### 接入方向与选择

已有 Handler 需要监听入口才能被外部查询。当前 Worker 的 run 只运行一个 gRPC Serve；新增 HTTP 后，要同时管理两个监听器及服务退出，不能只启动一个不受管理的后台 goroutine。

| 方案 | 好处 | 代价与选择 |
| --- | --- | --- |
| gRPC 与 HTTP 共用端口 | 部署少一个端口 | 需要协议分流或改造服务入口；当前观测场景没有必要引入，暂不采用 |
| HTTP 独立固定端口且默认开启 | 启动后即可查询 | 同机多 Worker 会争用默认端口，改变已有命令和实验启动行为，暂不采用 |
| 独立、显式配置的可选 HTTP 地址 | 保持既有启动方式，多实例分别指定地址，也支持系统分配端口 | 实验脚本必须明确开启查询并记录地址；采用 |

增加 -debug-listen，空字符串表示关闭，非空表示显式启用 HTTP 状态查询。这个开关和 -processing-concurrency 相互独立：前者控制能否从网络查询，后者控制是否施加共享处理限制。HTTP 启用而共享处理限制关闭时，应正常返回既有 enabled=false、processing=null 响应。

后续服务接入遵循以下契约，本次不实现：

1. 先完成配置与 Worker 构造校验，再取得所需的全部监听器；全部成功后才启动 Serve。请求启用的 HTTP 端口绑定失败，启动整体失败并关闭此前取得的 gRPC 监听器，不默默降级成无法观测的实验进程。
2. HTTP 和 gRPC 必须使用同一个 Worker 实例，否则查询看到的是另一份空闲名额池。
3. 显式启用时，任一服务意外退出，协调停止另一个服务并报告原始错误，避免留下只有半套功能的进程。默认关闭 HTTP 时仍只启动 gRPC。
4. 进程停止信号触发统一清理，等待启动的运行 goroutine 返回；长流可能一直不结束，因此退出必须具有有界等待与强制停止策略。具体 Stop/GracefulStop/Shutdown 的选择及错误归类在下一小步说明，不在本步套用未经设计的等待逻辑。
5. 启动日志记录两个实际绑定地址；配置端口为 0 时记录系统分配值。下一小步还需明确 HTTP 请求期限和清理期限，不把它们当作稳定容量或恢复 SLA。

单独拆出参数解析，是为了先固定兼容行为及开关语义，再修改持有资源的运行逻辑。此中间步骤完成后 run 仍未消费新字段，不能宣称正式命令已经提供 HTTP 查询；命令使用文档与 curl 示例在实际接线验收后再发布。

### 本步实现

只修改 cmd/asr-worker/config.go，由开发者完成：

```go
// workerConfig 描述 Worker 的启动配置；监听地址由命令运行层使用。
type workerConfig struct {
    ListenAddr string // gRPC TCP 监听地址，不能为空。
    DebugListenAddr string // HTTP 状态查询监听地址；空字符串表示关闭。
    Mock mockasr.Config // 语音处理与响应行为配置。
}
```

保留 ListenAddr 的字段名及所有旧默认值，新增 DebugListenAddr 默认为空。通过现有 FlagSet.StringVar 绑定 -debug-listen，帮助文案明确它是 HTTP 状态查询地址、空值关闭，并给出本机地址示例 127.0.0.1:50081。

校验规则：

- 未传入和显式 -debug-listen= 均合法，保留空字符串。
- 非空但 strings.TrimSpace 后为空，如一个空格或制表符，返回包含 debug-listen 的错误和 workerConfig{}。
- 非空且含有效字符时保留原值，不 TrimSpace 后写回，不在解析阶段 net.Listen、解析主机或主动试探端口。
- 127.0.0.1:50081、127.0.0.1:0、[::1]:50081、:50081 都由运行阶段 net.Listen 判定可绑定性。:50081 表示通配地址范围，/debug 路径不代表仅本机可达；本地实验显式使用 127.0.0.1。
- 不从 gRPC 端口自动推算 HTTP 端口，不强制两个地址字符串不同（两个 :0 可以取得不同实际端口）；实际占用冲突交给后续监听错误与回滚处理。

新校验放在 fs.Parse 成功和位置参数校验之后，与现有 listen 校验并列。现有帮助、未知参数、无效时长及返回零配置约定不变。不修改 main/run/snapshot.go，不增加 goroutine、监听器、地址解析工具或新依赖。

### 验收与下一步

用户实现后，助手补测试：默认与显式空值关闭、等号/分离参数语法、多个地址原值保留、空白拒绝、缺少参数值、帮助文案、与处理限制开关独立，以及旧参数默认值和重复解析不受影响。语法不合法但非空的地址应仍能解析，绑定失败属于下一步运行层。

这一步只验证配置契约；之后再设计并接入同一 Worker 的双服务运行、监听失败回滚、信号与退出，最后才可从外部查询。没有新增负载、吞吐或资源消耗结果。
