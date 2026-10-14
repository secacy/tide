# EXP-006-07 正式四场原始归档

2026-10-14，Asia/Taipei。[正式结果与限制](../../../worklog/2026-10-14-recovery-budget-comparison-results.md)。同一受测 ae6f06c、五次额度，十分钟/六轮 500ms 断线，L 整场累计、S 每 60s，顺序 L→S→S→L。

- `collection.tar.xz`：完整 `collection/`，364 文件，含 339 冻结源码/计划、单次构建二进制、配置/环境、四场原事件/终态/标准流及全部分析输出。
- `archive-index.json`：压缩包和全部读回文件 SHA256。
- `manifest.json` / `preregistration.md`：正式测量时原始副本，未随 Results 更新而改变。
- `summary.json.xz`：冻结独立分析器完整 JSON 的无损副本，保留所有位置、逐轮及资源窗口，包含质量降级场原值。
- `sessions.csv`：四场关键原值的 LF 表；完整 CSV 原行尾保存在二进制归档，不受 Git 文本规范化影响。
- `collection.log` / `validation.json`：采集终态、命令、运行与归档核对、源码/独立副本摘要、降级说明。

L 0/2 完成、两场第六轮明确耗尽，S 2/2 完成且各六次恢复/19.2MB/6001 结果。完整性及预期行为 4/4，时间/资源质量 3/4；槽位 2 在首轮故障前的音频计划落后 185.781ms 超过 100ms，七窗降级，原记录保留，无补场。自然清理及唯一原 RPC/Worker pick/source Close 四场核实。

原目录冻结复算四文件、搬移后冻结复算四文件均与原分析逐字节一致；364 文件 tar 读回摘要一致。任意路径解压后复算，无需执行二进制：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 collection/source/docs/experiments/scripts/summarize_attempt_window_resume.py \
  collection --output 一个尚不存在的分析目录
```

固定四场、每组两场，只能描述本机 Mock 与给定故障计划的效果。完整完成与保护规则达标分别统计；降级场不作延迟/资源性能结论；小幅堆增长、真实 ASR、群体长期弱网与生产参数仍有边界。没有把 null 变成 0、没有拼入旧样本、没有比较不同在线时长的总分配量。正式测量后仅更新结果文档及归档，业务与工具代码未变。
