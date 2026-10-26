# 模型及配套特征对照：六段配对证据

2026-10-26，两组各 6/6 完整完成、6/6 输入期间出字；CER 为 A 35%（70/200）、B 2%（4/200），下降 33 个百分点。B 仍误识别体温，并新增摔倒史疑点；本轮不替换生产 Worker。条件和限制见[结果记录](../../../worklog/2026-10-26-real-asr-model-results.md)。

- `collection.tar.xz`：102808B、103 文件，摘要见 `archive-index.json`。
- `summary.json`：十二场汇总、原始文本及指标；归档 `comparison.json` 另含完整逐字对齐。
- 包含执行前计划/三十份源快照、两组模型及程序/SDK 摘要、参考与旧基线、十二场原始事件和报告、命令与退出状态、模型元数据/使用条件缺口、时钟采样、助手事实初评和复核脚本。
- `relocation-check.json`：全部 103 文件搬移读回校验、冻结重算、十二类反例，以及 Python 23 项质量测试和 1 项资产测试的搬移执行结果。

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/recheck.py
PYTHONDONTWRITEBYTECODE=1 python3 collection/counterchecks.py
```

六段均为已见素材、每格一次；助手事实判断不代替人工审核。B 自动采用 Whisper 特征，本轮不是只换权重的实验；权重使用条件仍未确认，结果不构成部署或分发授权。归档不含音频、权重、原生库或程序；上述复算不运行推理，异机重放需另行取得同一份本地素材和依赖。
