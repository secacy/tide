# 客户端滑动尝试窗口验证记录

2026-10-14，Asia/Taipei。[实施说明](../../../worklog/2026-10-14-recovery-attempt-window.md)与[新增测试](../../../../internal/wsclient/client_v2_attempt_budget_test.go)。此目录记录正确性/回归验收，不是性能实验。

- `validation.json`：受测基线、三份改动源码 SHA256、命令、退出码、全项目父/子测试节点数、逐包终态、默认跳过列表、新增节点及日志摘要。
- `race.jsonl.gz`：全项目 `go test -race -json ./... -count=1 -timeout=5m` 原始输出，gzip 只是无损压缩，归档后已解压逐字节核对。

完整回归 3330 个测试节点通过、19 个实验/辅助测试默认跳过；12 个有测试包通过、另两个包无测试。新增 14 节点为可控时间/连接替身下的协调者验证；既有真实网络测试继续回归，但不冒充新滑动策略的 Gateway/TCP 组合验收。定向 race 和 wsclient/gateway vet 均通过。

基线 dc55f11 加 manifest 中改动源码就是受测实现，测试后仅补文档与归档。记录中的包耗时不是恢复延迟，节点数包含父测试与子测试，不是独立业务会话数量。新策略真实网络完整性、资源回落及长时表现仍待后续验证。
