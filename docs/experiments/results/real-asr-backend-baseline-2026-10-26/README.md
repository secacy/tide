# 真实 Worker 连续会话运行证据

2026-10-26。[结果与解释](../../../worklog/2026-10-26-real-asr-backend-baseline-results.md)记录五轮正常/取消、逐场等待和资源边界。[实验计划](../../real-asr-backend-baseline.md)在复测前固定输入、条件、预算与分母。

`collection.tar.xz` 包含首轮 `initial/`、完整复测 `final/`，以及硬件查询导致冻结中断但未运行的 `preparation-incomplete/`。每次运行保存源码/依赖快照、资产/二进制摘要、预登记槽位、逐场回调与 RPC 快照、客户端返回、自然清理、原生日志及连续资源采样。模型、原始 WAV、动态库和二进制不包含在归档中。`archive-index.json` 给出归档及全部 905 份文件摘要，真正解包后已逐份读回核对；`reconciled.json` 是最终复核结果副本。

真正解包后可独立运行冻结脚本，不需要 Go、模型或原仓库：

```sh
tar -xf collection.tar.xz
python3 collection/final/source/experiments/asrbackend/verify.py \
  collection/final --output final-reconciled.json
python3 collection/initial/source/experiments/asrbackend/verify.py \
  collection/initial --output initial-reconciled.json
```

最终正常完成 5/5、取消符合预期 5/5；预热单列，十一场原生创建/释放完整。292 条资源采样无超过三秒缺口或 RSS 缺失。驱动空闲 goroutine 为 11；Worker 空闲 RSS 没回到预热水平，原值与解释均保留。首轮模型摘要一次性读取干扰驱动堆的记录也保留；它与最终运行各自独立，不合并成十轮或包装为优化收益。
