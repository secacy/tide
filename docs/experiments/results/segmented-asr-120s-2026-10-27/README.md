# 120 秒固定分段基线证据

2026-10-27 按[预登记计划](../../real-asr-segmented-120s.md)完成一次正式运行；[结果和限制](../../../worklog/2026-10-27-segmented-120s-results.md)区分正式场、预热和新场。

正式场 3840000B、四段各 960000B，完成 1/1；四段成功提交的 float32 样本摘要与源 PCM 转换一致，四个 final、completed/ACK 和清理已核实。共三场、三个模型、六份流，创建/释放配对；307 条采样资源质量通过。RSS 没有回到预热水平，无长期无泄漏或稳定容量结论。

- collection.tar.xz：运行前的 manifest、284 份冻结源码、base/input PCM、完整 data、驱动/复核输出、五类损坏检查结果、运行后身份核对及准备失败说明。
- archive-index.json：压缩包和每份原始文件的摘要及字节数；归档后真实解包逐文件核对。
- reconciled.json：独立复核结果副本，方便直接查看。
- source-format-note.json：运行后对一个驱动文件的 gofmt 等价性记录；压缩包保留实际执行前冻结的源码。

无需模型也可解包复核，运行冻结的原脚本：

```sh
tar -xf collection.tar.xz
python3 collection/source/experiments/asrsegment/verify.py collection --output checked.json
python3 collection/source/experiments/asrsegment/counterchecks.py collection
```

原生库和实际程序不收进压缩包，其运行前/后摘要保留在 manifest 与 post-run-identity 中。复现识别仍需相同模型、SDK、本机程序和运行环境，见[工具说明](../../../../experiments/asrsegment/README.md)。

source 中的计划保留运行前版本；主仓库计划仅在运行后回写结果链接，未改变冻结判据。原始数据和冻结工具未修订，正式识别没有重跑。
