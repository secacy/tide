# EXP-006-07 工具 probe 验收归档

2026-10-14，Asia/Taipei。[验收及限制](../../../worklog/2026-10-14-recovery-budget-comparison-tools.md)。四场为十二秒工具检查，S 回看 1s，不是正式十分钟/60s 性能结果。

- `collection.tar.xz`：完整 `collection/`，364 文件，含单次构建二进制、339 冻结源码/计划、四场原事件/终态/标准流、manifest 和四份分析输出。
- `archive-index.json`：全部文件及压缩包 SHA256；已读回逐文件核对。
- `manifest.json` / `preregistration.md`：测量时原始副本，保留当时版本，不随当前文档更新。
- `probe-summary.json.xz`：原四场独立分析，含固定窗口、逐轮及位置序列。
- `validation.json`：命令、退出状态、Go/Python 检查、首次错误、复算及日志摘要。
- `collection.log.gz`、`go-race.log.gz`、`python-first.log.gz`、`python-final.log.gz`：采集、两包回归及首次/最终 Python 日志；`archive-fixture-tests.log` 为最终测试从本归档读取 fixture 的验证。

L 两场各五次接纳，第六轮明确耗尽，采集/处理 275200/272000B；S 两场各六次恢复，384000B 与 121 结果完整。四场原 RPC/Worker pick 一次、源关闭一次、自然空闲，无兜底。probe 同期运行 Go 回归，时间质量通过也不作为正式性能证据。

首次新反例有迭代器切片 TypeError；probe 后只修正这个测试，测量实现不变。冻结原源码保留修正前测试，清单另记录最终 SHA；没有改写归档或替换场次。新/旧配置与损坏证据检查最终 206 Python 方法通过，两包 race 1746 父/子节点通过、20 默认跳过。

解压后，在任意目录用冻结分析器独立复算：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 collection/source/docs/experiments/scripts/summarize_attempt_window_resume.py \
  collection --output 一个尚不存在的分析目录
```

原目录和搬移目录的冻结四输出均与原分析逐字节一致，旧 probe 以当前默认分析器复算四输出亦一致。二进制不需执行即可复算；CSV 保留原行尾，压缩日志仅做无损存储。基线是 0a59312 加记录的未提交冻结实现，最终验收提交还包含测试修正与文档更新。Formal 尚未执行。
