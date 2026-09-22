# EXP-004-08：慢客户端写回阻塞基线

Date: 2026-09-22
Status: 历史改造前基线已完成；该次测量时 writeResult 尚未接入 download。后续结果见[写回期限验收](result-write-timeout.md)。
Related: [结果写回期限设计](../worklog/designs/2026-09-22-result-write-timeout.md)

## 问题与实验条件

客户端不再读取结果时，输出可先被底层缓冲接纳；只有观察到实际写入持续等待，才说明复现了写回阻塞。本实验使用真实本机 TCP，不通过人为阻塞 Write 制造等待。

- 单会话，MaxSessions=1，输入空闲期限 30 秒；客户端握手后发送 start 和一条 2 字节音频，不发送 end，不调用 Read。
- Worker 读取请求后，最多尝试输出 64 条结果，每条含 256KiB ASCII 文本，无额外发送间隔；达到上限后仅等待取消。实际发送会受到 gRPC 流控限制。
- 此负载专用于加速填满缓冲，文本长度和频率不代表临床转录。没有测量真实 ASR、吞吐或稳定容量。
- 不调整 TCP 缓冲大小，WebSocket 使用默认不压缩设置。客户端、网关和 Worker 同进程、本机 TCP；Go 1.26.5、macOS 26.6.2，硬件参照同日准入实验。
- 在握手后记录服务端 net.Conn.Write 起止；一次 WebSocket Write 可能包含多次底层 Write，不能把底层调用数当作结果数量。
- 当某次底层 Write 连续至少 300ms 未返回，再观察 500ms，确认同一次调用仍未返回且 active=1，之后才主动取消服务作为兜底。发现阻塞最多等待 4 秒；单轮实验最多 15 秒。

本步特意给足输入空闲预算，未发送 end，因此观察窗口内输入空闲和尾部期限不负责退出。实验不是证明所有已有超时永远不会触发，而是隔离缺少单次写回期限的路径。

## Results

未启用 race 的正式实验重复三次：

| 指标 | 实测 |
| --- | --- |
| 同一次 TCP Write 在追加观察后仍未返回 | 3/3 |
| 该次 Write 的累计等待 | 803.67–808.25ms |
| 观察窗口末 active | 三次均为 1 |
| 需要外部服务取消兜底 | 3/3 |
| 外部取消发起至 Worker 返回 | 0.197–1.041ms |
| 外部取消发起至 handler 返回 | 5001.32–5002.24ms |
| 最终 active | 三次均为 0 |

Worker 均返回取消错误。客户端在上述清理完成前仍未读取结果，也未由测试主动关闭。约 5 秒的 handler 等待与当前服务停止使用 WebSocket Close 的握手路径相符；源码核对表明该库的关闭流程有写入关闭帧的等待预算，参见 [v1.8.15 Close 文档](https://pkg.go.dev/github.com/coder/websocket@v1.8.15#Conn.Close)。这不是网络抓包所得的完整归因，也不是任意故障下的清理期限。

这里记录的是观察窗口内的阻塞和占位，没有测量每层缓冲占用了多少内存，更不能从少量发送计数推导 TCP 或 gRPC 的固定容量。

## 验证与后续对照

参数错误分类修正后，`TestResultWrite*` 连续三轮通过 race 检查。本基线另以 race 运行一轮通过。受控底层写入的行为测试与本实验分开，前者验证取消机制，后者验证真实 TCP 缓冲耗尽路径。

后续将 ResultWriteTimeout 接入配置、download、协调者分类和 finish。写回期限到期可能已经关闭 WebSocket，采用取消 RPC / WebSocket I/O 和 CloseNow 兜底，不再等待正常关闭握手；不承诺客户端收到指定关闭码。

改造后沿用负载，记录阻塞后是否由写回期限自主退出、服务端退出分类、Worker / handler 退出、active 归零与重新接入。真实 TCP 计时明确区分底层 Write 起点和整条结果写入起点，不使用“停止读取至退出”冒充单次写入耗时。

## 复现与证据

```sh
TIDE_RUN_SLOW_READER_BASELINE=1 go test ./internal/gateway \
  -run '^TestSlowReaderBaselineExperiment$' -count=3 -timeout=45s -v
```

历史基线对应 manifest 的源码版本。接入写回期限后，旧基线的“持续占位”断言可能不再成立，应运行改造后实验而非覆盖历史数据。

- [实验代码](../../internal/gateway/slow_reader_experiment_test.go)
- [原始日志](results/slow-reader-baseline-2026-09-22.log)、[汇总](results/slow-reader-baseline-2026-09-22-summary.json)
- [环境、配置、源码摘要与命令](results/slow-reader-baseline-2026-09-22-manifest.json)、[源码修改参照](results/slow-reader-baseline-2026-09-22-source.patch)
