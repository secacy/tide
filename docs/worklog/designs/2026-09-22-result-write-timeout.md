# 单条结果写回期限

Date: 2026-09-22
Status: 2026-09-22 构造传参修正后集成完成；定向三轮与全量 race 回归通过，三次真实 TCP 改造后验收完成。

## 问题与范围

当前 download 顺序执行 Worker Recv 与 WebSocket Write，writeJSON 使用会话级 wsCtx，没有单次写回期限。若客户端继续发送音频但不读取响应，在输出侧缓冲耗尽后 Write 可能持续等待。输入仍活跃，不会触发输入空闲期限；尚无合法 end，尾部期限也不生效。其他已实现保护可能在后续间接触发，但不能把它们当作这次写回的直接等待上限。

本步约束单条结果的 WebSocket Write 等待，超时结束当前会话，不尝试在相同连接重发。Write 返回只说明本次库写入完成，不代表客户端 UI 已显示或应用已消费结果。

## 方案比较

| 方案 | 优点 | 代价 / 决策 |
| --- | --- | --- |
| 依赖已有发送、空闲或尾部期限 | 不增加逻辑 | 各自约束不同阶段，不能直接限制当前写回等待 |
| 结果队列分离 Worker 读取与客户端写入 | 暂时允许读取继续推进 | 客户端持续不读时仍需容量和满载策略，并不能代替写回期限 |
| 合并或丢弃旧 partial | 可减少部分输出量 | 依赖片段修订语义，不能随意丢弃 final；也不能中断已阻塞的写入 |
| 同步 Write 配置子 context 期限 | 复用库的取消机制，不新增业务写入 goroutine | 到期可能使连接失效，会终止问诊连接；本步采用 |

实现不采用 goroutine 包裹 Write 后让外层 select 单独返回；必须让实际 Write 退出，不能遗留持续写入的操作。

## 已核对的库行为及并发影响

本项目使用 github.com/coder/websocket v1.8.15。其 [Conn 文档](https://pkg.go.dev/github.com/coder/websocket@v1.8.15#Conn) 明确说明连接在操作错误（包括 context 到期）后关闭。本地依赖 conn.go 的 setupWriteTimeout 也会在 context 完成时关闭连接。

因此，结果写入超时可能先让 upload 的 Read 返回错误，再由 download 返回写回超时事件。沿用 inputReadCtx 的思路：在 Write 前公开本次写入 context，协调者在结束前检查期限状态。只在保护 context 指针时持锁，不把 JSON 编码或网络 Write 放在锁里。

## 行为约定

- 计划配置 ResultWriteTimeout：负值无效，0 使用开发默认值 2 秒，正值自定义。2 秒不是稳定容量或业务 SLA 结论。
- JSON 编码在计时前完成；期限覆盖这次 WebSocket Write 调用（含库内可能的等待），不覆盖 Worker Recv 或 JSON 编码。
- 每条结果独立创建子 context；保存状态后立即调用同步 Write。读取 child.Err 后取消计时，正常取消不应归类为期限到期。
- 返回原因依次考虑父 context 已取消、本次子 context DeadlineExceeded、其他写入错误。仅自身期限到期返回 ErrResultWriteTimeout；参数无效返回普通配置错误。
- 保存最近一次写入 context，写入结束后不立即置 nil，以便协调者恢复超时原因。成功后主动 cancel 得到 Canceled，不是 DeadlineExceeded。单个 session 只允许一个结果写入者串行调用该方法。
- 后续集成增加 resultResultWriteTimeout。协调者保留原有服务停止、Worker Send 超时和输入空闲超时优先级，再检查结果写回期限，最后使用等待到的事件；不承诺识别两个近同时发生的错误在物理时间上的绝对先后。
- 写回超时的 finish 取消 RPC、取消 WebSocket I/O 并 CloseNow 兜底；不承诺客户端收到某个业务关闭码，也不等待正常关闭握手。原因需在服务端分类和测试中验证。
- 不增加音频或结果队列，不改变协议，不做自动重试；这一小步不证明总缓冲有界，也不解决输入过程中的识别进度落后。

## 分步实现

1. 开发者在 session 新增 resultWriteTimeout、resultWriteMu、resultWriteCtx；新增 result_write.go 的 ErrResultWriteTimeout、writeResult(ctx, wsprotocol.ResultMessage) 与 resultWriteExpired()。先不改 download，不接入 Config 或构造参数，不改旧 writeJSON。
2. 助手补正常写入、参数边界、父取消、自身超时与状态保留测试，并记录旧链路在慢客户端条件下的对照。
3. 开发者接入配置、构造传参、download、协调者分类和 finish；助手同步测试与真实链路验收。旧 writeJSON 无调用后再移除。

## 首步函数约定

writeResult 的步骤：校验 resultWriteTimeout > 0；父 context 已结束则直接返回；编码结果；创建子 context；在短锁内存入 resultWriteCtx；调用 s.ws.Write；读取子 context 状态并 cancel；按上述优先级返回错误或 nil。函数不自行取消 RPC或减少会话计数。

resultWriteExpired 在短锁内取 context 指针，释放锁后检查非 nil 且 Err 为 DeadlineExceeded。没有写入、成功写入后的主动取消都应返回 false。

## 预先定义的验收与量化口径

- 正常接收端：完整有序返回结果；单次成功后重建下一次期限；不影响正常尾部与其他会话。
- 慢接收端：保持客户端上传或给足输入期限，保持未 end，避免其他期限先触发；客户端停止读取，使用明确限制总量/持续时间的输出负载填充缓冲。记录单条结果大小、发送频率及实际开始阻塞的 Write，不能从停止读取时刻直接推断写入阻塞时刻。
- 小结果可能长期被底层缓冲接纳，因此不能以“客户端不读”就宣称已经复现。测试可先用确定性的受控传输验证取消；真实 TCP 实验只有观察到阻塞才能作为前后对照。
- 改造前后各三次，保留正常/写回超时/其他原因、是否需要外部兜底、Worker 与 handler 退出、active 归零及重新接入结果。测试预算可缩短，但与默认 2 秒分开记录。
- 对照中的加速输出用于复现故障，不能当作临床真实输出分布或模型性能结果。
- 使用确定的期限状态与事件顺序验证 upload 断开事件先到时仍可分类为写回超时；同时运行既有尾部和生命周期回归。

## 2026-09-22 首步复验

- writeResult 已在 Write 前公开 context，并在 Write 返回后先读取期限状态再 cancel；父 context 错误优先于本次写回超时，符合约定。
- 新增 [result_write_test.go](../../../internal/gateway/result_write_test.go)，使用真实 WebSocket 与可被 Close 唤醒的受控底层 Write，验证取消实际解除写入，而不是仅让外层等待返回。
- `go test -race ./internal/gateway -run '^TestResultWrite' -count=1 -timeout=30s` 未通过：0 和负值配置错误返回 ErrResultWriteTimeout，两个边界用例失败。应改为普通参数错误，例如 fmt.Errorf("result write timeout must be positive: %s", s.resultWriteTimeout)。
- 本次正常连续写入、每次新期限、父 context 已结束、阻塞中父取消、自身期限到期、状态保留及已关闭连接写入的用例通过，未报告数据竞争；整组仍因参数分类失败而失败。
- 此处受控底层等待是行为测试，不是停止读取后填满真实 TCP 缓冲的性能实验。辅助方法仍未接入 download，没有新增链路收益。

## 修正复验与完整链路集成任务

无效期限已改为普通参数错误，定向测试连续三轮通过 race 检查。真实 TCP 三轮[慢客户端基线](../../experiments/slow-reader-baseline.md)均观察到同一次底层 Write 持续等待约 804–808ms、active=1，需要外部取消。外部取消至 handler 返回约 5.001–5.002 秒，包含现有服务停止关闭流程的等待；这不是写回期限保护的结果。

下一步由开发者完成：

1. Config.ResultWriteTimeout 与默认 2 秒、负值校验、零值默认处理；newSession 参数和字段赋值；ServeHTTP 传参。
2. 声明 resultResultWriteTimeout 会话事件；download 调用 s.writeResult，按 errors.Is 区分独立的写回超时和其他错误。原 Recv / EOF 处理保留；旧 writeJSON 无调用后移除及清理无用 import。
3. waitSessionResult 在原有服务停止、发送超时、输入空闲超时检查之后，检查 s.resultWriteExpired 并恢复写回超时原因；之后保留 completed 合法 end 校验。
4. finish 的写回超时分支取消 RPC、取消 WebSocket I/O、CloseNow 并返回 nil。原始 ErrResultWriteTimeout 由 run 返回，不在此处归还会话名额，也不承诺发送业务关闭码。

助手同步测试构造调用，验证 upload 的读取失败先到仍保留写回超时原因；在集成后再运行对照和全量回归。

## 集成初稿复验

- New 的默认值和负值校验、download 调用与分类、协调者的 resultWriteExpired 检查、finish 直接清理均已接入。
- newSession 未增加 resultWriteTimeout 参数和赋值，ServeHTTP 未传入 g.cfg.ResultWriteTimeout，导致 session 字段为零。首次结果写入立即返回参数错误，随后按普通写入失败关闭连接。
- 助手新增 result_write_integration_test.go 的配置与优先级测试。`go test -race ./internal/gateway -run '^(TestResultWrite|TestGatewayNormalEndPreservesTail)' -count=1 -timeout=30s` 中，配置、辅助方法和优先级用例通过；正常尾部的两种场景都在第一条结果读取时收到 EOF，整组失败。未报告数据竞争。
- 先由开发者补齐上述构造传参；测试调用由助手同步。当前失败不能视为写回期限生效，暂不运行改造后性能对照。

## 集成修正后验收

构造参数、字段赋值和 ServeHTTP 传递已补齐；助手同步了测试构造调用。另发现 ServeHTTP 构造后重复赋同一值，可由开发者删除，不影响本次行为。

定向测试连续三轮通过 race 检查，全量 race 回归通过。完整 run 的受控传输测试直接验证 ErrResultWriteTimeout；优先级测试验证读取失败事件先到仍保留写回超时原因。

真实 TCP 三轮实验使用 200ms 测试预算，自主清理 3/3，外部兜底由 3/3 降为 0/3，名额归零后重新接入 3/3 成功。从已观测阻塞的底层 Write 开始到 handler 返回为 201.18–201.41ms；与基线的外部取消起点不同，不计算加速倍数。详见[结果写回期限验收](../../experiments/result-write-timeout.md)。
