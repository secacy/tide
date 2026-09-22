# 输入结束后的完成等待期限

Date: 2026-09-22
Status: 2026-09-22 配置和会话集成修正完成；定向及全量 race 回归通过，三次改造后对照完成。

## 问题与范围

Gateway 收到合法 end 后不再使用输入空闲超时，Worker Send 期限也不约束响应流结束。若客户端保持连接、Worker 不结束响应流，当前会话会一直等待，并继续占用准入名额。这是代码路径分析，不是已完成的停滞实验结论。

增加 TailTimeout：协调者观察到合法 end 通知后，对等待 download 完成设置一次性的期限。download 完成意味着响应流 EOF 且此前结果已完成 WebSocket 写入；因此预算覆盖尾部处理、传输与写回，不是纯模型推理耗时，也不证明客户端应用已消费结果。

## 方案与权衡

| 方案 | 优点 | 代价与选择 |
| --- | --- | --- |
| 限制整个 RPC 时长 | 期限设置简单 | 会限制正常长时问诊，不对应当前 end 后等待的问题 |
| 每次收到结果重新开始倒计时 | 适合检测持续无输出 | Worker 持续发少量结果也能一直延长占位，且结果频率依模型而变 |
| 合法 end 后一次性完成预算 | 不限制 end 前问诊时长，也不被少量输出无限续期 | 尾部积压过大时会明确失败，可能收不到完整尾部；本步采用 |

实现采用协调者 select 循环内的 timer，不增加独立看门狗或取消回调。原因是本来就由协调者决定会话结果，timer 可直接产生退出原因，然后交现有 finish 取消 RPC、关闭 WebSocket 和等待内部操作退出。

准入限制同时有多少会话，尾部期限限制一个已结束输入的会话继续等待多久；二者互补。本步仍不解决输入过程中的识别落后或所有缓冲边界。

## 精确约定

- 计划配置 TailTimeout：负值无效，0 使用开发默认值 15 秒，正值自定义。15 秒是初始策略，不是已验证的业务 SLA；后续结合允许的尾部等待及实测调整。
- inputEnded 仍表示 upload 已读取并校验合法 end；当前直接转发路径在此前已经逐次调用完音频 Send，但这不表示 Worker 已完成处理。
- 首步沿用现有关闭通知，由协调者收到通知时启动计时，因此不声称从客户端发送 end 或网关校验 end 的精确瞬间计时。调度延迟不属于该 timer 的计量；如以后需要严格绝对期限，可传递 end 时间戳。
- 期限到达前可以继续转发尾部结果；不会因 partial/final 消息重新计时。is_final 是片段定稿，只有 download 结束事件才满足等待完成的候选条件。
- end 通知是中间事件，不直接返回会话成功。单独使用局部接收变量，处理后置 nil，防止已关闭 channel 不断被选中；保留原始 inputEnded 给现有正常完成合法性检查。
- 首步函数只等待并返回事件，不操作 RPC、WebSocket 或 tracker。timeout 必须为正值，后续由 Gateway.New 校验后传入。
- 事件、停止和 timeout 同时就绪时，select 可能选择任一分支。后续仍由 waitSessionResult 统一检查服务停止、既有发送超时和输入超时优先级；不承诺完成与尾部超时在临界时刻绝对谁先胜出。
- 集成后增加 resultTailTimeout 和 ErrTailTimeout，finish 取消 RPC，尝试发送 1011 / tail timeout，取消 WebSocket I/O，并等待内部 goroutine 退出。名额仍在 handler 完成清理后归还。
- 15 秒是完成等待预算，不是包括关闭握手、调度和所有清理工作的硬性释放上限。尾部阶段的慢客户端写回也可能耗尽此预算，不将超时归因于模型故障。

## 分步实现

1. 开发者新增 tail.go 的 ErrTailTimeout 与 waitSessionEvent(ctx, events, inputEnded, tailTimeout)，在 sessionResultKind 中声明 resultTailTimeout。首步先不替换 waitSessionResult，不增加配置，不修改 upload/download。
2. 助手以虚拟时间验证 end 前不启用期限、end 后一次性计时、正常事件与服务取消及时返回、关闭通知不会引起循环重启等行为。
3. 开发者接入配置、session 参数和 waitSessionResult/finish。助手同步测试构造调用并增加真实链路验证。

## 预先定义的验收与量化口径

- 正常对照：预算内返回完整尾部结果并正常关闭；end 前运行超过尾部期限仍不因此失败。
- 卡住场景：可控 Worker 已读到输入 EOF，但等待测试指令才返回尾部；客户端维持连接并持续读取。
- 改造前在固定观察窗口内记录是否仍持有名额、是否需要服务取消兜底；不能把窗口结束当作自动退出。
- 改造后配置较短测试预算，记录正常/超时/其他错误、Worker 与 handler 退出、active 归零、是否需要兜底、清理后重新接入是否成功；前后各重复三次。
- 时间记录区分 Worker 观察到 EOF、客户端看到超时关闭、handler 清理完成；不把 Worker 的 EOF 时刻冒充协调者计时起点。
- 增加阶段事件、服务取消与超时竞争的回归，保留正常尾部和准入测试；未测量前不填写提升百分比或固定退出耗时。

## 2026-09-22 首步检查

- waitSessionEvent 正确地在观察到 end 后创建计时器，并通过局部 endSignal=nil 禁用已关闭的通知通道；退出事件、服务取消及尾部超时的返回语义符合约定。
- 新增 [tail_test.go](../../../internal/gateway/tail_test.go)，使用虚拟时间验证 end 前一小时不触发尾部超时、end 后完整预算、end 前后退出事件和取消，以及调用前 end 已发生的情况。
- `go test -race ./internal/gateway -run '^TestTailWait' -count=3 -timeout=30s` 通过。
- 代码检查发现提前返回时没有显式停止已创建的 timer。应在变量声明后注册 defer 闭包，在函数退出时检查 timer 非 nil 再 Stop。此项是资源收尾约定，以上行为测试通过不代表已经验证 Stop 调用；不将此问题描述为已实测的持续内存泄漏。
- 当前辅助函数尚未被 waitSessionResult 调用；后续先完成该收尾，再记录接入前尾部停滞基线并接入配置与 finish。

## 2026-09-22 复验及集成任务

- defer 闭包和括号已修正，定向测试连续三轮通过；已创建的 timer 在退出时显式停止。
- 已记录[尾部停滞基线](../../experiments/tail-stall-baseline.md)：三次均在约 500ms 观察窗口后继续占用唯一名额，三次新接入均返回 503，需要外部服务取消才能退出；另有一轮 race 验证通过。这不是改造后结果。
- 下一步由开发者增加 Config.TailTimeout（0→15s、负值报错）、session.tailTimeout 和 newSession 构造传参。
- waitSessionResult 仅将开头的等待 select 替换为 waitSessionEvent(ctx, events, inputEnded, s.tailTimeout)，保留服务停止、Worker Send 超时、输入空闲超时的已有优先级检查，以及 completed 必须有合法 end 的检查。
- finish 增加 resultTailTimeout 分支：取消 RPC，尝试 1011 / tail timeout 关闭，取消 WebSocket I/O，返回关闭错误（若有）；run 继续等待 goroutine，handler 完成后归还名额。这里的 timeout 由协调者直接返回，不需要另外启动回调或仿照发送期限再次取消 RPC 写入 cause。
- 两个测试中的 newSession 构造调用（read_input_test.go、start_timeout_test.go）以及直接构造 session 的测试参数，由助手在集成后同步；不要求开发者修改测试。

## 集成初稿检查

- waitSessionResult 已调用等待辅助函数，并保留原有结果优先级和合法 end 校验；finish 的取消 RPC、关闭 WebSocket、取消 WebSocket I/O 顺序正确。
- resultTailTimeout 分支正常关闭路径缺少 return nil，`go test ./internal/gateway -run '^TestTailWait' -count=1` 在编译阶段报告 missing return，测试未执行。
- Config 已声明 TailTimeout 和 15s 默认常量，但 New 尚未执行负值校验和零值默认处理。
- newSession 尚未增加参数和字段赋值，ServeHTTP 也未传入 g.cfg.TailTimeout。因此当前 session.tailTimeout 保持零值；若仅修复编译，可能在 end 后立即选中超时事件。
- 助手补充 tail_config_test.go 覆盖配置边界，待开发者补齐后运行，并继续同步构造测试及完整链路验收。

## 集成修正后验收

New 的默认值与负值校验、newSession 参数与字段赋值、ServeHTTP 传参和 finish 正常返回已补齐。助手同步了旧测试构造调用，增加真实链路与协调者优先级测试。

定向测试连续三轮通过 race 检查；随后新增的协调者测试随全量 race 回归通过。正常尾部不被误伤，end 前超过尾部预算仍可继续输入。正式三次停滞实验配置 200ms，全部自主以 1011 / tail timeout 退出，外部兜底从 3/3 降为 0/3，清理后重新接入 3/3 成功。相对测试观察到 Worker EOF，handler 返回为 201.45–202.65ms；这不是 timer 精确起点到完成的测量。完整条件和限制见[尾部期限验收](../../experiments/tail-timeout.md)。
