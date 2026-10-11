# EXP-006-04 工具验收证据子集

2026-10-12，基于 `a921557` 加本步工具。**probe 为 800ms 输入、40ms/300ms 占位，不能作为正式 500ms/2s 收益。**详见[实现与验收](../../../worklog/2026-10-12-group-resume-jitter-tools.md)。

- manifest.json、preregistration.md、referenced-conditions.md：四轮计划、实际命令/环境、317 个源文件及产物摘要、条件快照。
- go-events.jsonl.xz / reports.json.xz：完整原始事件与重组报告，128 场策略/种子/候选序列和真实请求；长 Output 需按 Test 拼接。
- summary.json、sessions.csv、attempt-bins.csv、release-bins.csv、analysis-manifest.json：固定分母、两个原点的请求桶及六个分析依赖摘要。
- go-race.jsonl.xz / related-race.jsonl.xz：定向 21 通过、三次正式跳过；两包 1391 通过、17 默认跳过。正确性覆盖不计恢复性能。
- python-validation.log、python-initial.log、collection.log、reanalysis.log、old-*-compatibility.log：测试、采集、独立及历史比较复算日志。
- evidence-index.json：外层及压缩前内容 SHA-256，独立/搬移五类输出一致、旧正式四类输出一致记录。

本目录是可读证据子集，**不含测试二进制和全部源码**，不能直接用完整 manifest 校验采集目录。完整目录保留于 `/private/tmp/tide-group-jitter-tools-2026-10-12-run-1`，整体复制与独立复算验证通过。

使用 Python 标准库 lzma 解压读取；四份压缩日志均读回并核对原始内容。仓库 test_group_resume_jitter.py 的真实归档用例能核对摘要、四轮顺序、逐场种子、Python PCG 序列及原 RPC/音频/结果完整性。
