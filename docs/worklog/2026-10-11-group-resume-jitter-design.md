# 群体恢复：jitter 设计与第一步实现指引

日期：2026-10-11。关联 [次数对照结果](2026-10-11-group-resume-budget-results.md)、[EXP-006-04 预登记](../experiments/group-resume-jitter-comparison.md)。状态：客户端可选 jitter 已实现并验收；群体比较工具及正式测量待完成。以下实现指引保留设计过程，实际实现与验证见文末。

## 问题与目标

EXP-006-03 中，2s 占位的八次配置全部完成，但 24 场用了 154 次恢复请求，其中 130 次被拒绝；入口释放后还有十次拒绝，成功场最大相邻转录间隔为 2.63–4.63s。次数增加让客户端等到了入口开放，接下来要检验能否减少开放后的争抢和结果等待。500ms 组的八次配置全部在前五次成功，不能把它和五次组的差异归功于额外次数，也不能据此宣称 jitter 已有收益。

八场几乎同时断线、立即尝试，再按相同的 100、200、400、800、1000ms 等待，后续请求容易继续成批到达。入口只有两个握手名额，短暂争抢失败后，客户端又可能一起等待下一轮。这是本轮待验证的机制解释；真实请求、名额占用和拒绝事件才是证据，固定时间桶不等于精确同时。

目标是提供可选的随机退避，并在固定八次/5s 窗口下比较完成完整性、请求及等待成本。当前八次只是实验配置，生产默认仍是五次；默认关闭 jitter。

## 备选方案与取舍

设 B 为当前失败次数对应的确定性退避间隔，已按 MaxBackoff 封顶。

| 方案 | 等待分布 / 理论均值 | 优点 | 代价与选择 |
| --- | --- | --- | --- |
| 确定性退避 | B / B | 简单、排期易解释 | 同时失败后可能继续同步；作为同版本对照 |
| Full jitter | U(0,B) / B/2 | 随机范围大，可更早再试 | 同参数下均值减半，有限次数可能更早用完；后续可单独比较 |
| Equal jitter | B/2 + U(0,B/2) / 3B/4 | 保留最短等待 | 仍改变均值；名称中的 equal 不表示与原退避均值相等 |
| 对称 jitter | U(B/2,3B/2) / B | 保留每个失败档位的理论均值，同时分散时点 | 单次最大等待变大；本轮选择固定 ±50% |
| 仅首次随机延迟 | 首次错开，后续仍为 B | 改动小，分散第一波 | 增加立即可恢复场景的首个机会等待，后续仍可能重新聚集；另作策略 |
| 服务端排队、Retry-After 或扩容 | 由入口协调或增加容量 | 可能更直接控制竞争 | 新增服务端资源、协议或改变容量，本轮证据不足以决定这些改造 |

[AWS 的 backoff/jitter 说明](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) 提供了请求聚集及 full/equal/decorrelated jitter 的参考，其数据库竞争模拟结论不直接作为 Tide 的结果。本项目选择对称分布，是为了保留各失败档位的期望等待，便于解释此次有限预算对照；不据此声称它普遍优于 full jitter。

这个选择不能同时保留原来的单次硬上限：随机等待若永远不大于 B，且均值仍为 B，就只能每次都等于 B。这里明确接受 MaxBackoff=1s 表示**基础间隔上限**，开启 jitter 后实际抽样上限为 1.5s；关闭时行为完全保持原样。不在抽样后再次 min(delay, MaxBackoff)，否则上半区集中到上限且均值下降。

理论均值相同也不代表实测平均相同。成功会提前停止，5s 窗口会截断长等待；随机分布同时改变了某次机会赶上开放、用完次数或触及期限的概率。因此结论应是“该随机退避策略的对照表现”，不能声称只改变同步性而所有时间性质相同。

## 固定语义

1. 首次 start 不参与退避。每轮断线在旧连接实际退出后立即尝试第一次 resume，不抽随机数。
2. 可重试的 resume 失败后，先检查取消、次数与期限；仍有预算才计算下一次等待。失败计数 k=1 对应 InitialBackoff；B 沿用现有 v2BackoffDelay。下一档由失败次数决定，不能把上次随机值翻倍。
3. 用整数纳秒计算 h=floor(B/2)，在闭区间 `[B-h, B+h]` 均匀抽样：`B-h + Int64N(2*h+1)`。均值恰为 B；B=1ns 时退化为 1ns，不产生零等待。
4. 每次进入新的退避只抽一次，保存绝对 backoffUntil。源事件或定时器唤醒不重新抽样，不调用 time.Sleep 阻塞协调器。等待期间继续采集、处理取消及预算。
5. 不按剩余窗口缩短随机间隔以强行挤入下一次请求。现有 earliest-deadline 定时器在窗口先到期时结束恢复；到期后不得再 Dial 或接受迟到 ready。抽样不是一次尝试，实际启动 resume Dial 才累计次数。
6. MaxAttempts 仍是整场累计，成功不清零；Window 仍是每轮固定预算。成功 ready 后重置当前轮失败档位，但整场随机源继续向前，不重新播种。
7. 随机源属于每个 Run，只由协调 goroutine 使用，不放进可并发复用的 V2Client，不与音频或握手 goroutine 共享。

## 由开发者实现：可选 jitter 接入客户端

这一步形成一个完整的小能力：配置可开关，运行时在原恢复循环内使用随机等待。负载工具和测试随后由助手完成。

### 1. 配置与校验：client_v2.go

在 V2ResumeConfig 原有字段后增加以下字段，并更新 InitialBackoff / MaxBackoff 注释，说明它们是随机化之前的基础间隔：

```go
Jitter     bool   // 默认 false；true 时在基础间隔上下各 50% 的范围内均匀抽样。
JitterSeed uint64 // 仅 Jitter=true 有效；0 表示每个 Run 自动选种子，非零用于可复现实验。
```

保持 NewV2 已有的独立配置副本。在默认值填充和原有校验后补两项：关闭 jitter 却指定非零种子返回配置错误；启用时确认 `MaxBackoff + MaxBackoff/2` 能用 time.Duration 表示。先比较 `MaxBackoff > maxDuration-MaxBackoff/2`，不要先做可能溢出的加法。关闭 jitter 时保留原有大 duration 配置的支持范围。

固定非零种子会让相同配置的不同 Run 生成相同序列，这是复现功能，不是自动分散功能；实验要按会话赋不同种子。日常启用使用零值自动种子。无需增加模式枚举、任意比例或策略接口，目前只支持关闭与这一种分布。

### 2. 随机源所有权：client_v2_run.go

给 v2ClientRun 增加一个私有字段：

```go
// backoffRand 仅开启 jitter 时创建，由本 Run 的协调 goroutine 独占。
// 生命周期覆盖整场；一次恢复成功后不重新初始化。
backoffRand *rand.Rand
```

使用标准库 math/rand/v2。在 newV2Run 内仅当 Resume 非 nil 且 Jitter=true 时创建：非零配置种子直接使用；零值调用 rand.Uint64 选取一次种子。固定构造为 `rand.New(rand.NewPCG(seed, seed ^ 0x9e3779b97f4a7c15))`。该源用于调度，不用于身份或恢复凭据。这里创建本地随机源的代价是每个启用的 Run 多一个小状态，收益是并发顺序不会争用或改变别场的随机序列。

### 3. 分离计算与调度：建议 client_v2_backoff.go

保留现有 v2BackoffDelay 作为基础指数计算。新增两个私有函数，函数体由开发者实现：

```go
// v2JitterDelay 对正基础间隔 base 做整数纳秒的 ±50% 均匀抽样。
// 调用前已校验 base+base/2 不溢出；sample(n) 必须返回 [0,n) 的整数。
// 函数只计算时长，不睡眠、不修改次数或期限；sample 用于确定性边界测试。
func v2JitterDelay(base time.Duration, sample func(int64) int64) time.Duration

// resumeBackoffDelay 计算当前轮 failures 次失败后的下一次等待。
// failures 从 1 开始；关闭 jitter 时返回旧基础值，开启时只抽样一次。
func (r *v2ClientRun) resumeBackoffDelay(failures int) time.Duration
```

前者实现上面的整数公式。后者从 r.client.cfg.Resume 取得配置，调用 v2BackoffDelay 得到 B；关闭时直接返回 B，开启时传入 r.backoffRand.Int64N。sample 是内部受约束依赖，不作为任意用户回调公开。

### 4. 接入唯一等待点

在 recover 中当前 `r.current == nil` 的分支，保留 failures++，把 `v2BackoffDelay(*cfg, r.recovery.failures)` 替换为 `r.resumeBackoffDelay(r.recovery.failures)`，仍由 `time.Now().Add(delay)` 设置 backoffUntil。

coordinate 的旧连接退出分支仍设置 backoffUntil=time.Now()，保留首次立即恢复。现有 checkBudget、earlierV2、清理等待和 Dial 计数继续负责退出与边界；无需另开 goroutine 或新增睡眠循环。

## 助手后续验证与工具工作

用户完成上述接入后，先评审实际代码，再由助手补测试：关闭时原序列；抽样端点/中点、奇数纳秒/1ns、溢出校验；固定种子复现、独立 Run 随机状态；首次立即且不消耗随机数；每次失败只抽一次；恢复成功后档位重置而随机源不重置；取消、窗口先到、次数先耗尽、等待中继续采集及自然清理。使用可控抽样/虚拟时间验证边界，不以偶然分布阈值作为正确性断言。

随后助手扩展 EXP-006-04 独立入口、采集和分析，保留 EXP-006-02/03 的确定性契约。工具保存每场实际配置、非零固定种子、算法标识、候选等待序列和真实代理请求；候选序列是代码推导，不标成实际等待或实际请求时间。代理事件仍是请求聚集、拒绝及接回的实测依据。

最后按预登记运行同版本四条件各四批，再更新 Result。四轮是为让每个场景的先后顺序各占两轮、覆盖四组预定种子，不是根据有利结果补样本；不承诺完成率或延迟一定改善。本步只核对代码接入点、算术约束及文档链接，没有修改业务代码、运行新实验或产生 jitter 收益数据。

## 客户端接入与验收（2026-10-11）

开发者明确委托助手完成本步实现。`V2ResumeConfig` 新增 Jitter/JitterSeed、非法种子与时长溢出校验；`newV2Run` 创建独占 PCG；`client_v2_backoff.go` 分离基础指数计算和整数随机计算；recover 仅在进入下一次退避时抽样。原有首次立即、次数累计、固定窗口、采集和清理路径继续生效。默认五次及关闭 jitter 不变。

兼容性检查发现原实验分析器严格比较实际 JSON 配置。新增字段使用 omitempty，关闭时保持原四字段格式，开启时显式输出 Jitter=true 和非零固定种子。未放宽旧分析规则；正式随机实验必须由新契约识别。自动种子仅属于运行时随机源，不宣称普通自动模式的报告包含可复现种子。

验证基于 `143d1a4` 加本步实现，Go 1.26.5，命令使用 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off`：

```sh
go test -race ./internal/wsclient -run '^TestV2Jitter' -count=3 -json
go test -race ./internal/gateway \
  -run '^TestV2ClientGatewayRecovery$/^jitter_resume_ready_lost$' -count=3 -json
go test -race ./internal/wsclient ./internal/gateway -count=1 -timeout=4m -json
```

- 客户端新增 22 个叶级用例，各三轮共 66 通过。覆盖零值/配置副本/最大安全间隔、抽样两端及中点/奇数纳秒/1ns、确定性序列、固定 PCG 序列、并发 Run 独立状态、首次不抽样、采集不重抽/不延长等待、取消、窗口先到与恰好同刻、次数先耗尽、恢复成功后的档位重置及随机序列继续。
- 新增真实 WebSocket/Gateway/TCP gRPC 场景三轮通过：丢失第一份 resume ready，迫使客户端再次恢复；各轮原 Worker RPC 和 Pick 均为一，10 字节音频逐字完整、四条结果连续、完成确认写出，source 关闭一次且注册/会话自然空闲。这是正确性验收，测试使用十次/2s 预算，不是正式八次/5s 对照数据。
- 最终两包 race 回归客户端 327、Gateway 1057，共 1384 叶级通过，16 默认跳过（15 显式实验及一个私有 helper），无失败或 race 报告。
- 从最终回归日志重组实际五组默认关闭报告，未经修改的 EXP-006-02 分析器全部验证通过；分别在总配置和单场实际配置伪加 Jitter=true 的十个反例全部拒绝。没有运行业务性能对照或全项目测试。

首次定向测试发现测试自身竞态：预算用例读取 backoffUntil 后等待下一次 Dial，协调器会在开始握手时清空该字段，虚拟时间同步不能代替未来写入的同步。修正为窗口退出后检查留存等待时间，次数场景通过实际 Dial 时刻及终态核对；生产逻辑未因此更改。首次失败日志保留。初次两包回归也通过，补 JSON 兼容性测试后对最终版本重新完成定向和两包回归。

[压缩原始日志与摘要](../experiments/results/jitter-client-validation-2026-10-11/README.md) 包含首次失败、最终客户端/真实网络/两包回归，均解压读回并核对 SHA-256。它是测试证据子集，不是源码/二进制齐全的性能实验归档。

当前可选策略已经接通。下一步由助手扩展 EXP-006-04 群体入口、逐场种子与候选序列报告、采集和独立分析，补工具反例及缩短 probe；工具验收后再执行预登记正式 16 批。暂无随机策略降低拒绝或改善转录等待的量化结论。
