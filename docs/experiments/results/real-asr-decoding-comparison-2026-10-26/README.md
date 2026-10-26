# Greedy / beam 四路径：六段配对证据

2026-10-26，各策略 6/6 完整完成；CER 为 G 35%（70/200）、B 42.5%（85/200）。B 多错 15 字、增加 7.5 个百分点，体温错误仍在。本轮决定保持生产 greedy；条件、计算口径和边界见[结果记录](../../../worklog/2026-10-26-real-asr-decoding-results.md)。

- `collection.tar.xz`：68824B、81 文件，摘要见 `archive-index.json`。
- `summary.json`：配对汇总与原始文本；归档 `comparison.json` 另含完整逐字对齐。
- 归档包含执行前计划/十九份源码快照、冻结参考/旧基线摘要、基础输入幅度检查、程序/模型/SDK 库摘要、十二场逐调用事件与退出/计时、测试日志、冻结复核及七类反例。
- 没有录音、WAV、权重、程序；重放推理仍需同一份本地音频及依赖。81 文件读回/搬移后 SHA 均一致，搬移后冻结复核结果和七个反例一致。

不加载模型也可重新计算评分：

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/source/experiments/asrquality/compare_decoding.py collection
PYTHONDONTWRITEBYTECODE=1 python3 collection/counterchecks.py
```

`run.py` 保存实际固定路径与执行方式，异机重新推理需另建证据目录并明确素材/依赖；它不是直接覆盖旧结果的命令。每格一次，六段均已参与诊断；结果不能作为独立新样本质量、临床可用性或并发容量结论。关键事实的正式人工审核仍待完成。
