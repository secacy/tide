# 真实 sherpa Worker 接通证据

日期：2026-10-26。范围为固定模型、本机单场、直接 gRPC Worker；尚未经过 Gateway。运行条件与结果见[组装验收](../../../worklog/2026-10-26-sherpa-engine-assembly.md)。

[collection.tar.xz](collection.tar.xz)含 40 份文件：原始逐条结果、六次 RPC 摘要、模型创建/释放日志、CLI 帮助/拒绝输出、真实模型/进程测试日志、构建前源码摘要与源快照、Go 构建信息、原生库/二进制摘要、冻结复核脚本及三个反例结果。模型权重、WAV、动态库和可执行文件不包含在归档中；固定下载 revision/摘要及构建说明保留，可用[运行说明](../../../../workers/sherpa/README.md)准备依赖。

从本目录解压后核对：

```sh
tar -xf collection.tar.xz
python3 collection/verify.py collection/data
```

[archive-index.json](archive-index.json)记录归档及各文件 SHA256，已逐份读回一致；搬移后冻结脚本复核仍与[reconciled.json](reconciled.json)一致。复核脚本在运行后编写，并拒绝伪造处理进度、修改末尾文本及错误输入量三个反例；不将它描述为预先固定的性能判据。

两场语音和一场静音正常完成；一场预期取消无 final；取消后第一次探测忙、下一次完成，分开保留，不能合并成“六场全部完成”。五次模型创建都有对应释放调用返回，空闲 SIGTERM 退出 0。客户端 Send 成功字节不是 Worker 接纳/模型处理确认；忙时探测的 3200B 没有因此被解释为模型输入。

首字等待从 RPC 发起计时，包含加载、客户端排期与网络等待；尾部从 CloseSend 调用开始到 EOF。无独立人工参考，CER=null；无可信处理前缀，processed_audio_bytes=null。没有容量、临床质量、硬取消或无泄漏结论。
