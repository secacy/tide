# 两个真实 Worker 准入对照证据

2026-10-27。[预登记条件](../../real-asr-admission-comparison.md)、[完整结果及边界](../../../worklog/2026-10-27-real-asr-admission-results.md)。

`collection.tar.xz` 包含运行前冻结源码/依赖、输入/模型/程序/原生库摘要、29 次预登记尝试、每场回调/返回/RPC 观察、两进程原生日志、触发快照、263 条资源采样及原始测试日志。模型、WAV、动态库和二进制不包含在归档中。`archive-index.json` 记录归档和全部文件摘要；`reconciled.json` 为独立复核结果副本。

首版冻结复核器在合法的 Go nil 切片 JSON null 上失败，原脚本和 `verification.log` 保留。`revised-tools/` 仅修正复核兼容性并新增测试/反例；`revision.json` 及 `verify.diff` 说明差异。原实验数据与运行源码没有改动，也没有重跑识别。解包后使用明确的修订脚本：

```sh
tar -xf collection.tar.xz
python3 collection/revised-tools/verify.py collection --output checked.json
python3 collection/revised-tools/counterchecks.py collection
```

归档真正解包后，全部 392 份文件已逐份读回核对，修订复核与六类反例结果均和原件一致。源快照身份仍按原 manifest 检查，修订工具身份另由 revision 与归档摘要固定。

主要结果为错峰新场完整完成 **0/3 → 3/3**，两组原 L/S 各 **6/6**；超额第三场 **3/3** 入口拒绝、无额外 RPC，六个已接纳场全部完成。23 份模型均有对应释放，分开记录暖机、忙拒绝及入口拒绝。不能据此推断稳定容量、长期无泄漏或一般问诊完成率。
