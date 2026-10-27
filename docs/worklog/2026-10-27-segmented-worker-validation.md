# 分段 Worker 接入验收

日期：2026-10-27。前置版本 `5af8c8c`。经开发者明确委托，助手完成[接入设计](2026-10-27-segmented-worker-integration.md)的核心实现、测试及运行验证。本文记录接入时的短验证；后续已完成[120 秒基线](2026-10-27-segmented-120s-results.md)，长期资源趋势仍待验证。

## 实现结果

- [分段接口](../../internal/realasr/engine.go)使用独立 SegmentedEngine/工厂与 NewSegmented；原 New 继续使用单段配置。共享配置校验和尾部刷新，不使用运行时能力断言。
- [Worker 分段循环](../../internal/realasr/worker_segmented.go)在提交前校验整个请求的总预算，按原始样本边界拆分，Accept 成功才 commit。满段立即 Finish、drain、final，再同步释放流；下一段非空输入到来才建流、推进编号、建立新 mapper。边界 EOF 不多建流或重复 final。
- 整场准入留在最外层，工厂每场调用一次；分支实际 Close 完成后才释放 Worker 名额。片段 final 不结束 RPC，也不归还 Gateway Lease。异常不重试、不补造成功终态。
- [原生适配器](../../workers/sherpa/internal/native/engine.go)逐段删除/创建 online stream，整场复用 recognizer；每段重置 padding/Finish 状态。无流调用受保护，取消后的 ReleaseSegment/Close 仍清理；创建返回后观察到取消时清理局部流。
- [启动组装](../../workers/sherpa/cmd/asr-worker/worker_setup.go)增加显式分段分支及保留完整方法集合的日志包装；CLI 增加 max-audio-bytes、segment-audio-bytes，默认仍为 960000B 整场、关闭分段。

分段输入数量不转成模型处理确认；AudioProgress 仍未提供。日志中的 model_created/engine_closed 是整场模型边界，没有用它们冒充逐段流事件。

## 可控模型与真实网络验证

[分段 Worker 测试](../../internal/realasr/worker_segmented_test.go)验证请求跨多段、跨请求边界、短尾段、静音、空输入/空块、总/单块上限、递增 ID 和尾部快照；包含模型各调用、发送与换流失败、取消优先级、创建成功后取消、第二段失败保留第一段 final，以及实际 ReleaseSegment/Close 返回前不放名额。复用的旧模式测试同时回归公共准入与尾部流程。

TCP gRPC 测试先读取第 1/2 段 final，再继续发送音频，最后半关闭并收到第 3 段 final 和正常 EOF，证明没有把片段完成当作整场完成。

[Gateway/v2 组合测试](../../internal/gateway/segmented_worker_integration_test.go)使用生产 Worker、客户端、占用池和真实 TCP/WebSocket，正常路径的 10B 输入按 4/4/2B 分段，原始样本重组一致，结果 Seq 连续，三个唯一 final、completed 和 completed_ack 闭环成立；模型与 Lease 各申请一次。换流失败的独立路径返回失败，不冒充 completed，两条路径均自然清理并归还本地名额。这里的微型输入只验证机制，不代表实际问诊质量或性能。

## 实际原生验证

使用本机既有的原始中英 Zipformer int8、`sherpa-onnx-go-macos v1.13.8`、CPU 两线程，没有替换模型或调整识别参数。

1. 旧 TestRealModelLifecycle 回归通过。新增 TestRealModelSegmentedLifecycle 在同一个 recognizer 内完成三个一秒静音段，逐段释放流并检查模型引用保持；第四流输入 100ms 后取消，流与模型实际清理、重复 Close 安全。
2. 编译新的非 race Worker，通过新增 CLI 启动独立进程，整场上限 128000B、单段 32000B。真实 gRPC 两场分别输入 64000B 和 67200B 静音，**2/2 正常完成**，分别得到 `1/2` 与 `1/2/3` 的唯一 final；满段 final 在半关闭之前收到，EOF 后没有额外结果。两个模型创建均有实际释放日志，空闲 SIGTERM 正常退出。

输入未按真实采集节奏发送，只有两秒/两秒一百毫秒的素材长度；没有测量或声称吞吐、延迟优化、准确率、RSS 稳定或 120 秒能力。直接原生对象测试与独立进程测试是不同夹具，不合并计成容量样本。

独立进程的原始 cases.json、server.log 及运行后收集的代码/模型/二进制摘要已[归档](../experiments/results/segmented-worker-smoke-2026-10-27/README.md)。身份摘要是在运行后采集，明确区别于预登记并冻结条件的正式性能实验。

## 检查与复现

```sh
# 根模块受影响包的完整回归
go test -race ./internal/realasr ./internal/gateway ./internal/workerpool ./internal/wsclient -count=1 -timeout=3m
go vet ./...
go build ./...

# 独立模块普通回归（默认跳过显式模型实验）
cd workers/sherpa
go test -race ./... -count=1 -timeout=3m
go vet ./...
go build -o /tmp/tide-segmented-worker ./cmd/asr-worker

# 本次用既有固定模型显式执行的原生短验证
TIDE_SHERPA_MODEL_DIR=/path/to/fixed-model \
  go test -v ./internal/native -run '^TestRealModel(Lifecycle|SegmentedLifecycle)$' -count=1 -timeout=90s
```

上述检查通过，新增第二段失败场景也通过定向 race；根模块完整全仓 race 未在本步执行。网络检查使用已获许可的本机监听环境。没有放宽测试预算或修改失败判据。

## 仍需验证

当前已接通固定分段的运行机制。下一步预登记 120 秒、每段 30 秒的真实链路验证，并增加逐段流创建/释放观察点，记录源边界至 final、发送落后、换流时间、资源趋势和批后状态。既有 model 日志无法单独证明这套逐段计数，不能跳过新观察点。

固定边界可能截断词句和丢失跨段声学上下文；自然端点策略仍待比较。Recognizer 内部缓存、分配器与传输层内存不由单段字节上限严格限定；原生硬中断、可信处理前缀、稳定容量及完整录音保存继续保留为独立边界。
