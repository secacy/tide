# 真实 ASR 经 Gateway/v2 的接通证据

日期：2026-10-26。[验收记录](../../../worklog/2026-10-26-real-asr-gateway-v2.md)列出固定条件、口径修正及最终结果。范围为生产 Gateway 处理器/V2Client、真实 WebSocket/TCP gRPC、独立原生 Worker 进程；不包含 cmd/gateway 及演示客户端启动命令验收。

[collection.tar.xz](collection.tar.xz)含 204 文件：首次/最终各五场原始应用消息、gRPC 观察、客户端返回事实、运行条件、摘要和进程日志；每轮 77 份源码/依赖文件摘要及源快照；同一非 race Worker 的构建信息、二进制与原生库摘要；冻结复核及反例脚本。模型、WAV、动态库和可执行文件不包含在归档中，其固定资源准备见 [Worker 说明](../../../../workers/sherpa/README.md)。

```sh
tar -xf collection.tar.xz
python3 collection/verify.py --gateway \
  --manifest collection/final/manifest.json collection/final/data
python3 collection/counterchecks.py \
  collection/final/data collection/final/manifest.json
```

[archive-index.json](archive-index.json)记录归档及各文件 SHA256，全部文件已读回校验；搬移后冻结复核仍与 [reconciled.json](reconciled.json)一致，四个反例复跑也通过。反例分别伪造处理进度、完成确认、转发音频摘要及回调文本，均被拒绝。复核脚本运行后编写，源文件与预先记录的摘要分别核对。

最终四场完整输入均 completed/确认写入，原始 PCM 与网关成功 gRPC.Send 的负载哈希一致；另一场预期取消，未 completed，恢复窗口后自然清理。五次原生模型创建均有对应释放完成记录。Source Close、Client Completed、确认 Write、Gateway 活动数及原生释放分别保留，不合成一个“全部成功率”。

首轮同样通过功能判据，但清理时间字段从等待循环开始计时，漏掉 Run 返回后保存中间记录的开销；完整原值和源快照保留。最终改为从已记录 Run 返回点计时，并将正常场景等待明确限制为预定五秒，再用同一 Worker 二进制完整复测，两轮不合并分母。首轮清理时间不作最终指标。

取消由首个非空回调触发，该回调已产生示例文本，但客户端随后因取消未提交 AppliedSeq；回调一次与 AppliedSeq=0 均为实际记录。约十秒等待包含默认恢复窗口，不是模型取消速度。没有临床质量、硬取消、生产容量或无泄漏结论，处理前缀/CER 保持未知。
