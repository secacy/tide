# 客户端 jitter 实现验证

2026-10-11，基于提交 `143d1a4` 加本步实现，Go 1.26.5。完整实现、命令和首次问题见[工程记录](../../../worklog/2026-10-11-group-resume-jitter-design.md#客户端接入与验收2026-10-11)。

| 文件 | 范围 |
| --- | --- |
| first-client-race.jsonl.xz | 首次三轮定向测试：62 叶级通过、1 失败；测试读取等待字段与后续握手清空字段竞争 |
| client-race.jsonl.xz | 最终客户端 22 个叶级用例 × 3，66 通过 |
| network-race.jsonl.xz | 新增真实丢失恢复 ready 场景三轮通过 |
| related-race.jsonl.xz | 最终 wsclient/gateway 两包 race，1384 叶级通过、16 默认跳过 |
| validation.json | 原始/压缩 SHA-256、叶级计数、包结果及旧分析器兼容检查 |

原始文件来自 Go test -json，长行报告需先按 Test 拼接 Output；不把父测试和子测试重复计数。四份压缩文件均已解压读回，与原文件逐字节一致。Python 标准库 lzma 可读取，无需第三方依赖。

兼容检查从最终两包日志提取五组实际默认关闭报告，使用未经修改的 summarize_group_resume.py 的 extract / verify_report 验证；分别向报告总配置和单场实际配置添加 Jitter=true，十个反例均被拒绝。该检查不修改归档报告或旧分析器。

本目录为测试证据，未包含全部源码快照、二进制或中间复测日志，不是完整性能归档。网络验收为单场十次/2s 预算，不能作为 EXP-006-04 八次/5s 群体对照结果。
