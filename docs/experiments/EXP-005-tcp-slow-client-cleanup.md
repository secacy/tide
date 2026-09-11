# EXP-005: 真实 TCP 慢读与异常清理

Date: 2026-09-11
Commit: e9a7743e178c96a72d940877948a0ba762f3d283
Related: [ADR-002](../adr/ADR-002-streaming-io-backpressure.md), [ADR-003](../adr/ADR-003-processing-progress-deadline.md), [EXP-004](EXP-004-processing-progress-deadline.md)

## Question

真实 TCP socket 写入阻塞时，当前结果写入、处理进度和 End 期限能否触发退出？RPC 取消与会话注销之间是否存在额外等待？客户端恢复读取、发送 RST 或服务显式 Abort 时，能否解除写入并完成清理？

## Hypothesis

- 默认结果写入期限可以结束持续无法发送结果的会话，短暂慢读恢复后可以正常完成。
- 扩大结果写入期限后，处理或 End 期限可能先取消 RPC，但尽力发送关闭帧的过程仍受阻塞写入影响，资源释放可能晚于 RPC 取消。
- RST 与显式 Abort 可以关闭底层连接，解除真实 socket 的读写阻塞。

## Setup

生产代码与参数取上述版本，无 Go overlay，不改生产逻辑。本轮验证已有行为，不预先引入异常收尾预算。

Client：每轮一个真实 TCP 客户端，连接 `127.0.0.1` 随机端口。完成 HTTP/WebSocket 升级，发送 Start 和一个 2 字节音频块；除 processing_timeout 外都发送 End。读取结果帧的前两个字节后停止读取，bufio 可能预读少量数据。客户端请求接收缓冲 1024 字节，服务端请求发送缓冲 4096 字节；实际内核缓冲和 TCP 窗口可能由系统调整，没有将请求值当作实际窗口大小。

Worker：真实 gRPC，经 bufconn 连接，正常确认音频后发送一个包含 1 MiB 文本的结果；processing_timeout 场景不确认音频。正常收到半关闭后再发送 final 并返回 EOF；processing_timeout 发送大结果后等待取消。该大结果用于稳定复现 socket 堵塞，不代表真实 ASR 结果大小或故障发生频率。

正式场景（每种三次）：

| 场景 | ResultWriteTimeout | 动作与隔离目的 |
| --- | ---: | --- |
| write_timeout | 默认 2 秒 | 一直不读取，观察默认写入期限 |
| processing_timeout | 8 秒 | 不发 End、不确认处理，隔离 3 秒处理期限与收尾 |
| end_timeout | 8 秒 | 全部音频已确认且发 End，隔离 5 秒 End 期限与收尾 |
| client_reset | 默认 2 秒 | 确认 socket 阻塞后，客户端 SetLinger(0) 并关闭以产生 RST |
| abort | 8 秒 | 确认 socket 阻塞后，调用 Gateway.Abort |
| resume | 默认 2 秒 | 确认阻塞后再暂停 400 ms，然后读取全部结果并响应关闭握手 |

所有场景首先要求真实底层 Write（至少 4096 字节）持续未返回达 100 ms，再记录“阻塞已建立”及执行对应动作。仅收到帧头不作为 socket 已经阻塞的证据。三个 8 秒配置用于检验合法配置之间的交互，未修改产品默认值。

Environment：Go 1.26.5，darwin/arm64，机器报告 12 个逻辑 CPU；具体 CPU 型号、物理内存与内核 TCP 参数未测量。测试在允许本地 bind 的环境执行，仅 loopback 通信，无外部服务。正式场景顺序运行，关闭 race detector。

## Metrics

- `socket_write_max_ms`：包装真实 net.Conn.Write 测量调用耗时；锁不跨越 I/O。
- `socket_block_confirmed_ms`：满足上述持续阻塞条件的观测时刻。
- `rpc_cancel_ms`：通过 Context AfterFunc 观测 Session 的 RPC Context 取消，包含回调调度延迟；不是远程 Worker 的退出时刻。
- `rpc_cancel_to_cleanup_ms`：从取消回调被观察到，到 Gateway handler 与 Worker handler 均退出的时间。
- `worker_exit_ms`：Worker handler 实际退出时刻，可能早于 RPC Context 取消，因为结果可能已进入传输缓冲。
- `action_to_rpc_cancel_ms`：RST/Abort/恢复读取动作到取消回调；resume 中它是正常完成后的取消，不是故障恢复失败。
- Gateway 的结束错误日志、已读完整帧和字节数、客户端实际观察到的关闭码、最终注册表数量。关闭码 0 表示未读取到关闭帧，不能由服务日志推断客户端收到了通知。

时间除另有说明外均相对发送 Start 前的同进程单调时间。所有清理完成后才报告结果；设置 20 秒驱动兜底期限，超出会使实验失败。日志墙钟跨度与测试报告耗时相差超过 1 秒时拒绝作为连续样本。

## Procedure

检出上述 commit，在允许绑定 loopback 端口的环境执行：

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
python3 scripts/run_tcp_slow_client_experiment.py --count 3 \
  --output /tmp/tide-exp005.jsonl

PYTHONDONTWRITEBYTECODE=1 python3 scripts/summarize_tcp_slow_client_experiment.py \
  /tmp/tide-exp005.jsonl --output /tmp/tide-exp005-summary.json
```

运行器保存版本、源码摘要、环境、实际命令与退出码，拒绝覆盖旧样本。`--case processing_timeout` 可单独复现；`--race` 用于正确性检查，不用于正式耗时比较。

[驱动](../../internal/gateway/tcp_slow_client_experiment_test.go)、[运行器](../../scripts/run_tcp_slow_client_experiment.py)、[汇总器](../../scripts/summarize_tcp_slow_client_experiment.py)。

## Results

18 次正式运行（六场景各三次）全部建立了真实 socket 写入阻塞，并最终完成 Gateway 与 Worker handler 清理，注册表归零。源码摘要、时间连续性及阻塞验证通过，没有排除或补跑正式样本。六场景先行 race 检查和带 tide_tcp 构建的 vet 通过；检查数据不并入正式统计。

| 场景 | RPC 取消时刻 ms | 取消到全部清理 ms | 启动到全部清理 ms |
| --- | ---: | ---: | ---: |
| write_timeout | 2003.010–2008.283 | 0.200–0.403 | 2003.210–2008.487 |
| processing_timeout（写入期限 8 秒） | 3001.377–3001.816 | 5000.564–5000.778 | 8002.155–8002.380 |
| end_timeout（写入期限 8 秒） | 5000.881–5002.599 | 3002.453–3002.746 | 8003.349–8005.052 |
| client_reset | 107.463–109.724 | 0.117–0.126 | 107.579–109.843 |
| abort | 104.554–107.730 | 0.133–0.399 | 104.953–107.863 |
| resume | 518.784–521.404 | 0.004–0.248 | 518.788–521.651 |

时刻相对发送 Start 前的实验起点，3/5 秒生产期限的精确起点分别是音频成功入队和 End 被 Gateway 接收，所以这些列不是直接的期限超出量。

RST 动作到取消回调为 0.446–2.782 ms；Abort 为 0.102–0.165 ms。resume 在确认阻塞后再暂停 400 ms，约第 0.5 秒恢复读取，全部读完两个结果帧及关闭帧，观察到 1000 正常关闭；其他场景不继续读取，不能声明客户端收到了某个关闭码。

真实 socket Write 最大耗时：默认写入超时组约 1999 ms，处理/End 优先组约 7996–7999 ms，RST/Abort 组约 100–104 ms，resume 组约 513–515 ms。没有用 goroutine 模拟阻塞替代 socket 阻塞。

processing_timeout 中 Worker 在约 3001.49–3001.89 ms 退出，与 RPC 取消接近；其他场景的 Worker 大多在起点后 1–4 ms 已退出，结果和 final 已进入传输缓冲。这进一步说明，RPC Context 取消、Worker 退出和 Gateway 会话注销是不同事件。

结束原因分别为结果写入超时、processing_timeout、end_timeout、客户端 connection reset、session aborted 和正常完成。原始日志保留本机临时端口信息；它们不是生产用户数据。

数据：[18 次原始运行](results/EXP-005-tcp-slow-client.jsonl)、[源码与运行元数据](results/EXP-005-tcp-slow-client.jsonl.meta.json)、[机器可读汇总](results/EXP-005-summary.json)。

## Conclusion

三项假设在本轮条件下成立：

- 默认 2 秒结果写入期限能解除真实 TCP 慢写；恢复读取后正常完成，RST 和显式 Abort 也能中止 socket 等待并释放会话。
- 扩大写入期限后，处理/End 期限仍按预期取消 RPC，但取消与会话注销之间出现约 5 秒 / 3 秒间隔。不能将 EXP-004 内存传输下很短的清理时间推广到慢客户端。
- 本轮没有发现清理后残留注册记录，但证明当前没有独立于写入和关闭握手的异常收尾预算。

代码核对与结果一致：[Session.finish](../../internal/gateway/session.go) 在处理/End 失败时先取消 RPC，再调用 WebSocket Close，之后才取消 WS I/O。固定依赖 github.com/coder/websocket v1.8.15 的 close.go 中，Close 为发送关闭帧设置 5 秒等待，成功后还可能等待对端关闭响应。被大结果占据的写入路径会影响关闭帧推进。processing_timeout 组在 RPC 取消后等待约 5 秒；end_timeout 组先碰到原结果的 8 秒写入期限，额外等待约 3 秒。这是结合依赖实现对观测的解释，不是对所有网络条件的精确时限证明。

该现象发生在合法的 8 秒写入配置下，默认 2 秒配置在本轮先由写入期限退出。它不等于“3/5 秒取消目标失效”，也不能据此声称默认部署都会额外等 5 秒。当前 ADR 明确未承诺清理的无条件硬上界；是否需要更早强制断开，应由资源释放目标和错误通知取舍决定。

证据边界：单会话、本机 TCP、极大合成结果、固定操作系统及请求缓冲配置；不包含 TLS、WAN 丢包、静默黑洞、真实 ASR 或并发容量测试。三次重复用于复现路径，不能估计故障概率或高分位生产 SLO。

## Follow-up

在对话中确认异常收尾是否需要独立预算：预算内尽力发送关闭通知，耗尽后强制关闭底层连接，再等待执行流退出；代价是部分客户端可能只能观察到异常断开，拿不到具体关闭原因。新增期限需要实验选值，不能从这三次样本推导最优参数。

本轮保持生产实现与默认参数不变，不创建未确认的 ADR。确认新的收尾语义后再记录最终决策、实现，并用本实验验证 RPC 取消时机、强制断开时机与最终资源释放。
