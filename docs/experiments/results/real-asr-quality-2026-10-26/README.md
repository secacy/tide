# 六段自录模拟问诊质量基线：本地证据

2026-10-26 首轮六段，6/6 完整完成，参考共 200 字，S=7、D=0、I=63，CER=35%。完整条件与限制见[结果报告](../../../worklog/2026-10-26-real-asr-quality-results.md)。关键事实仅助手初评，人工审核待完成。

- `collection.tar.xz`：47468B、73 份文件；完整摘要见 `archive-index.json`。保留执行前输入/参考和 20 份源快照、转换信息、六场原始消息和摘要、原生生命周期日志、冻结评分和助手事实初评。
- `summary.json`：评分汇总；逐段完整对齐在归档 `run-1/score.json`。不平均逐段 CER，也不将完成率当作识别准确率。
- 归档不含录音、WAV、权重、程序或 ready 凭据。原始素材在本地 `testdata/m4a`，派生输入在 `testdata/asr-quality/self-recorded-2026-10-26`。异机重放模型需另行取得同一素材及依赖。
- 73 文件全部读回 SHA 一致；复制到另一目录后再核对全部文件，并使用冻结评分工具重算，结果相同。没有在搬移验证中重新推理。

解压后可独立重算评分（不需要模型或音频）：

```sh
tar -xf collection.tar.xz
PYTHONDONTWRITEBYTECODE=1 python3 collection/before/source/experiments/asrquality/score.py collection/run-1
```

准备阶段首次父目录缺失的错误以摘要保留，修复后才冻结并运行；原始终端 traceback 未存为文件。正式六段均保留，无补场。
