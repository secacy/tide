# 十分钟固定分段观察证据

2026-10-27 按[预登记计划](../../real-asr-segmented-600s.md)完成一次正式运行；[结果与限制](../../../worklog/2026-10-27-segmented-600s-results.md)保留二十段时间点、五个窗口及空闲状态。

正式场 19200000B、每段 960000B，完成 1/1；20 段成功提交的 float32 样本摘要和整场传输 SHA 核实，796 条快照及 completed/ACK 一致。三场合计三个模型、22 份流，创建/释放配对；1267 条采样质量通过。RSS 未回预热值，结果不证明长期无泄漏或稳定并发容量。

- collection.tar.xz：运行前 manifest、284 份冻结源码、base/input PCM、完整 data、驱动与复核输出、七类损坏副本检查及运行后身份核对。
- archive-index.json：压缩包和每份文件的摘要与字节数；归档后真实解包逐文件核对。
- reconciled.json：冻结复核器产生的结果副本，便于直接读取。

无需加载模型即可复核；请使用归档中的冻结工具：

```sh
tar -xf collection.tar.xz
python3 collection/source/experiments/asrsegment/verify.py collection --output checked.json
python3 collection/source/experiments/asrsegment/counterchecks.py collection
```

Worker 与驱动程序、原生动态库及模型不收进压缩包；启动前/后的摘要分别保存在 manifest 与 post-run-identity。运行复现需要计划指定的同一 Worker 程序和固定模型，见[工具说明](../../../../experiments/asrsegment/README.md)。

冻结计划保存运行前版本；主仓库的计划在运行后回写结果链接。正式运行和复核均首次通过，未修改原始数据、冻结工具或重跑补场。
