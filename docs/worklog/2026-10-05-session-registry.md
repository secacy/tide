# 第六阶段：逻辑会话容器与并发注册表

日期：2026-10-05。状态：实现指导，等待开发者编写；尚未接入恢复链路。依据 `6fddf74` 和[身份与注册表职责](2026-10-05-session-identity-registry.md)。

## 目标与价值

身份部件可以生成 ID 和校验凭据，resumeState 可以判断附着与过期。现在需要将二者归属于同一场逻辑会话，并允许新连接通过 ID 找到它。本步实现进程内的登记、查找与条件删除；后续协调者才处理恢复请求及网络资源。

当前 [session](../../internal/gateway/session.go) 表示一个 WebSocket 与 Worker 流的桥接关系。独立逻辑会话的寿命需要跨越多次 WebSocket 连接，因此本轮先建立 resumableSession 容器，不将旧 session 指针解释为已经支持恢复的对象。

## 值类型的选择

| 方案 | 收益 | 代价与选择 |
| --- | --- | --- |
| map 保存现有 *session | 复用已有类型 | 身份和保留状态未归属其中，且当前 run/finish 与单连接绑定，需要同时改造生命周期 |
| 泛型注册表或 any 值 | 可独立于具体会话编写 | 泛型增加类型参数，any 需要断言；当前只有一种明确的逻辑会话用途 |
| 保存 *resumableSession | 静态类型明确，组合已完成的身份和恢复状态，未来可加入协调者与流所有权 | 新增一个领域对象；本轮采用，逐步接入其生命周期 |

这里的容器已有身份与保留状态职责，但尚无运行中的 Worker 或协调者；不能把构造成功描述为问诊已经建立。后续接入时再调整旧桥接路径与逻辑会话之间的关系，避免长期维持两套完整生命周期。

## 同步与删除方案

注册表继续采用 map + sync.Mutex。所有映射操作使用同一把锁，使“检查是否存在再插入”和“比较对象再删除”成为不可分割的一步。锁内没有网络、等待会话事件或清理操作。

RWMutex 可允许并行读取，但需要读写负载与争用数据支撑选择；sync.Map 也提供并发操作，仍须明确复合操作语义。当前选择 Mutex 保持规则集中，后续测到锁竞争再评估。注册表仅在建立、恢复查找、结束时访问，不放在逐块音频转发路径上；当前没有它的吞吐测量。

删除有两种选择：只按 ID delete 最简单，但迟到的旧清理可能删除同 ID 下的新对象；采用 `remove(id, expected)` 同时比较对象指针后删除，多一个参数，但保留清理操作的对象身份。举例：A 移除后同 ID 登记了 B，A 的重复清理必须返回 false，B 继续可查。

同一个对象只能经历一次登记生命周期，移除后不得重新登记该指针。未来重新创建必须生成新对象和身份；条件删除不能区分同一指针被违反约定反复移除、重插的情况。随机 ID 的低碰撞概率不替代插入时的冲突检查。

## 本步文件一：resumable_session.go

```go
// resumableSession 保存一场可恢复逻辑会话的身份与连接保留状态。
// identity 和 resume 指针在构造后保持不变；resume 指向的状态由协调者串行修改。
// 当前容器不创建 WebSocket、Worker 流、goroutine 或计时器。
// 必须通过 newResumableSession 构造，并使用指针传递。
type resumableSession struct {
    identity sessionIdentity // 稳定 ID 和恢复凭据，禁止记录整个身份对象。
    resume   *resumeState    // 可变保留状态；注册表与查找调用方不得直接读写。
}

// newResumableSession 组合独立身份和恢复状态。
// window 必须为正；生产 random 使用 crypto/rand.Reader。
// 先校验并创建恢复状态，再生成身份；失败返回 nil 和可追溯的错误。
// 不占用准入名额、不登记、不选择 Worker，也不启动会话。
func newResumableSession(random io.Reader, window time.Duration) (*resumableSession, error)
```

构造顺序为 `newResumeState(window)` → `newSessionIdentity(random)` → 组合对象。错误使用 `%w` 补充上下文。无效 window 应在读取随机源之前返回。resume 的初始 attached/代次 1 沿用既有部件规则，只是对象的初始状态；实际初始连接由后续创建流程关联。

身份不再单独生成后传入，避免新增一套对任意外部身份值的校验规则。测试可用相同的固定随机材料构造同 ID 的不同对象，确定性检查注册冲突。

## 本步文件二：session_registry.go

```go
// sessionRegistry 按 ID 保存逻辑会话引用，支持并发调用。
// 必须通过 newSessionRegistry 创建，使用后不能复制。
// 锁仅保护映射，不保护对象内部状态，也不负责准入、过期和资源清理。
type sessionRegistry struct {
    mu       sync.Mutex                  // 保护 entries 及复合操作。
    entries  map[string]*resumableSession // 仅存放非 nil、已构造的会话对象。
}

// newSessionRegistry 创建空注册表，不启动后台任务。
func newSessionRegistry() *sessionRegistry

// add 登记一个新逻辑会话。
// nil 或缺少构造所需字段的对象返回 errInvalidResumableSession。
// ID 已存在时返回 errSessionAlreadyRegistered，保留原记录，即使传入同一指针。
// 调用方负责保证对象已初始化、身份不再改变，且只登记一次生命周期。
func (r *sessionRegistry) add(s *resumableSession) error

// lookup 按原始 ID 精确查找，返回共享引用及是否找到。
// 缺失时返回 nil, false；不校验凭据、不检查期限、不变更恢复状态。
// 方法返回时已释放注册表锁；找到引用不表示会话仍允许恢复。
func (r *sessionRegistry) lookup(id string) (*resumableSession, bool)

// remove 仅在 id 当前对应 expected 对象时删除，并返回 true。
// expected 为 nil、ID 不存在或对象不同时返回 false。
// 允许重复调用；不会关闭会话、取消 RPC 或归还准入名额。
func (r *sessionRegistry) remove(id string, expected *resumableSession) bool
```

新增两个内部哨兵错误：`errInvalidResumableSession` 和 `errSessionAlreadyRegistered`，便于 errors.Is 判断。错误文本不包含 token 或整个会话对象。

实现规则：

1. 构造器初始化 map。零值注册表与 nil 接收者不属于有效使用契约，不要求提供额外兼容逻辑。
2. add 在锁外检查对象非 nil、resume 指针非 nil、ID 长度为 sessionIDLength、凭据长度为 resumeTokenLength；这只是内部构造完整性检查，不验证随机质量。随后持锁检查 ID 冲突并插入。只读取不变字段，不检查 `resume.phase` 等可变状态。
3. lookup 持锁取值并释放锁，返回原对象指针。不拷贝 resumableSession，不对 ID 做 TrimSpace 或大小写转换；空 ID 自然查找失败。
4. remove 在同一个临界区中读取当前值、比较 `current == expected`、再 delete。不能将比较与删除分成两次加锁。不存在及 nil expected 均不得报告成功。
5. 本步不增加 len、遍历、TTL 扫描、自动取消、容量计数或停止标志。准入继续由独立机制管理；查找表数量不能直接代替“尚未清理完成的会话数”。

`resumeState` 仍由未来会话协调者串行操作。注册表加锁不会让 `lookup(...).resume.resume(...)` 变成安全调用。后续连接 handler 可以读取不可变 identity 校验凭据，再通过会话命令入口请求恢复；协调者处理请求时再次判断当前期限、状态和停服条件。该命令入口与并发停止语义在下一小步设计，本步不用空 channel 或假处理器占位。

## 正确性边界与后续验收

注册表提供映射操作的原子性，不提供跨 lookup 与恢复判定的事务。查到对象后它可能已关闭或已移除，已有 Go 引用仍然存在，恢复请求必须由协调者拒绝。移除映射不能让其他 goroutine 已取得的指针自动失效。

助手在实现后补确定性及 race 测试：构造失败与窗口优先校验；空表查找；非法对象拒绝；不同 ID 独立；同 ID 同/不同指针拒绝覆盖；删除后缺失；错误对象、nil 与重复删除无效；旧对象删除不能影响后登记对象；并发同 ID 插入恰好一个成功；并发登记/查找/删除不破坏其他键。

这些测试只验证进程内对象组合与注册表行为。Gateway 仍未调用它们，真实重连、名额转移、Worker 保留、音频和结果恢复另行接入验证。本步核心由开发者实现，助手负责测试和记录。
