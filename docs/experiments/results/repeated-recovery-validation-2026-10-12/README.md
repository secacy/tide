# 同一会话多次断线：真实网络验收

对应[实现与验收记录](../../../worklog/2026-10-12-repeated-recovery-validation.md)。真实本机 WebSocket / TCP gRPC、公开 v2 Gateway、测试位置 Worker；同进程、小音频及事件屏障，是行为测试。

- `network-first.jsonl`：新增三个场景首次实际网络运行，3/3 通过，退出码 0。
- `network-race-five-rounds.jsonl`：三个场景各五轮 race，15/15 通过，零失败/跳过，退出码 0。
- `related-packages-race.jsonl.xz`：Gateway/wsclient 两包串行 race 回归的完整原始日志。
- `validation-manifest.json`：命令、叶级检查统计、归档文件和新测试源码 SHA256；源码工作区尚未提交时运行，不能称为既有提交版本的验收。

两类完成场各两次恢复，一次 Worker 选择/原 RPC，24 字节逐字处理且七条结果连续应用、完成确认写出。第三轮断线场用完累计两次预算后不再 Dial，24 字节采集、20 字节处理、五条结果，明确失败；原流按服务端保留期限自然退出。每场源关闭一次、资源在兜底清理前归零。

测试耗时不表示恢复延迟；尚无固定采集节奏、真实 ASR、长时或群体竞争证据。此前沙箱及登录阻塞保留在[历史检查目录](../repeated-recovery-validation-pending-2026-10-12/README.md)，未删除或算成业务测试失败。这里也不将预期明确失败的场写成业务完整完成。
