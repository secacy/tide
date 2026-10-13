# 滑动恢复预算短场网络验收原始记录

2026-10-14，Asia/Taipei。[结果与范围](../../../worklog/2026-10-14-recovery-attempt-window-network.md)、[网络测试](../../../../internal/gateway/client_v2_attempt_window_test.go)。真实 WebSocket/Gateway/TCP gRPC，控制输入与故障时序；不作为性能负载实验。

- `validation.json`：受测基线 16e93d3、最终两份测试源码 SHA256、命令、退出码、父/子节点计数、逐包结果、21 条结构化业务记录与三个原日志摘要。
- `first-network.jsonl.gz`：初轮三次重复的九场均通过；计划字段含四字节备用输入，写成 36B。此日志仅保留修正历史，最终源码摘要不代表初轮源码。
- `network.jsonl.gz`：输入字面量修正为 32B 后，三次重复九场符合预期，12 父/子节点通过。
- `regression.jsonl.gz`：Gateway/wsclient 完整 race 回归，1745 节点通过、19 默认跳过，含另外三场新场景。Gateway vet 通过。

三个 gzip 日志均解压逐字节核对，`validation.json` 同时保存原始与压缩 SHA256。最终源码只缩短备用输入，没有改变任何场景的实际发送步骤或每场预期；业务代码未修改。可从 `ATTEMPT_WINDOW_NETWORK ` 后的 JSON 复核每场原 RPC/Worker pick、输入/处理/结果、尝试/接纳、完成/耗尽及自然空闲/TCP 拆除事实；日志不含恢复身份或凭据。

分散组完整结束，另外两组应该明确失败。九场 9/9 是预期行为核实，不能写成恢复成功率 100%；未采集后缀与已采集未处理缺口分开，包耗时不是恢复延迟。初轮、最终重复和回归不拼接为生产可靠率或容量样本。
