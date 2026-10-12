# EXP-006-05：有界记录器与多轮恢复夹具验收

日期：2026-10-12 开始；相关回归于 2026-10-13 00:00:01（UTC+8）完成。关联[预登记](../experiments/repeated-resume-extended.md)、[设计](2026-10-12-repeated-resume-extended-design.md)。本步实现专用 Go 测试工具，完成十二秒三组 probe；**十分钟正式六场、独立 Python 采集/分析与资源窗口统计尚未完成。**

## 实现与取舍

新增 [事件写入器](../../internal/gateway/extended_resume_log_test.go)、[专用夹具](../../internal/gateway/extended_resume_harness_test.go)、[验收测试](../../internal/gateway/extended_resume_test.go)，均为测试代码。采用流式事件文件与固定大小终态摘要，旧恢复工具和生产逻辑没有改动。

1. 源按预登记块号算法生成 3200 字节，Worker 逐字核对连续块并报告位置结果；双方保存当前块、计数及增量 SHA256，不保存整场音频。五个由 Python 标准库独立计算的已知向量覆盖首块、中间块与正式尾块，真实 TCP Worker 拒绝内容损坏、错块和短块。
2. 多轮代理每个计划 offset 只触发一次；同一块重放不再次切断。每轮独立计时开放，新连接在代理暂扣期间持续监测前端退出；源仍按生产客户端实时排期读取。身份在内存中核对，日志没有 ID/token。
3. 记录器先编码独占字节副本，再非阻塞进入 1024 项队列，每条上限 8KiB，文件缓冲 64KiB。五个固定组件各自递增序号，共享单调时间原点；调用方改动原 payload 不改变已交付事件。首错通知与队列独立，队列满/编码/超限/写入/flush/close 失败明确取消测量，不在磁盘等待期间阻塞业务生产者。
4. 采样每 100ms 串行执行，保存过程资源、GC、队列采样峰值和缓冲接纳字节量；位置事件供后续分析外部积压线索。队列峰值是投递时观测值，不宣称捕获所有瞬时变化；buffer 接纳字节量不等于 flush 前已持久化。
5. 客户端返回后使用独立窗口观察服务自然空闲；随后关闭监听器、等待代理/计时器/Worker/采样任务实际退出，再关闭事件队列、flush/close 文件，保存小型终态。服务停止仅在自然空闲已观察到后用于正常销毁，或在超出自然观察预算时明确标为 forced_cleanup。磁盘永久卡住仍需外部进程保护，context 不承诺强制中断任意文件 I/O。

没有为本轮新增客户端内部缓存快照。过程堆包含夹具与客户端，代理位置差不等于实际缓冲占用；资源窗口分析与独立进程隔离在下一工具单元落实。

## 首次失败与修正

首次组件 race 运行在 `short_write` 场景超时：故障 Writer 返回 `(0, nil)`，Go bufio 的直接写路径反复尝试、没有进展。新增 `extendedCheckedWriter` 将短写及非法写入长度转换为明确错误，再交给 bufio；不改变真实音频和恢复业务规则。原始超时日志保留。

原始三组网络 race probe 同期全部通过；在最终短写处理、事件读者及终态格式完成后，另行运行完整非 race 三组 probe，分别保留目录，没有拼接事件。组件反例验证包括：不可变副本、非阻塞队列满、写/短写/flush/close、事件超限/编码错误、固定组件与时间、并发投递/关闭、配置、坏音频、强制收尾，以及完整行/连续序号/终态缺失等反例。读取器只核对 Go probe 证据，并不替代下一步独立 Python 分析器。

## 最终 probe 结果

十二秒、120 块、每块 100ms；六轮断线位于块号 10/25/40/55/70/85，每轮阻断 500ms。保留正式客户端/服务器预算、缓存与处理参数；C 失败后原场仍按 10s 服务器期限自然退出。

| 条件 | 完整完成 | 故障轮数 | 恢复尝试 / 接纳 | 实际采集 / 处理字节 | 成功应用结果 | 原 RPC |
| --- | --- | ---: | --- | --- | ---: | ---: |
| A 正常 | 是 | 0 | 0 / 0 | 384000 / 384000 | 121 | 1 |
| B 八次预算 | 是 | 6 | 6 / 6 | 384000 / 384000 | 121 | 1 |
| C 五次预算 | 否，recovery_exhausted | 6 | 5 / 5 | 275200 / 272000 | 85 | 1 |

完成场输入逐字/摘要匹配、结果连续且完成确认写出；C 已采集未处理 `[272000,275200)` 3200 字节，尚未采集 `[275200,384000)` 108800 字节分别保留，第六轮没有第六次 Dial，也没有回退 start。三组各只选择 Worker 一次、源 Close 一次，自然清理且没有兜底。每个成功恢复轮次都观察到阻断期间的新源读取。

最终分别保存 1703、1744、1335 条完整事件，观测队列峰值分别 2、10、13，低于 1024 项预算；这是一次 probe 的工具事实，不能据此推导正式资源上界、恢复延迟或生产完成率。最终快照三份 Go 文件与工作区摘要一致，事件及摘要原样归档。

## 检查与可复现入口

最终定向组件及网络故障三轮 race 共 78 个叶级场景通过，零失败/跳过；普通最终三组 probe 通过；Gateway `go vet` 通过。相关两包串行 race：Gateway 1102、wsclient 327 叶级检查通过，共 1429 通过、零失败；17 既有显式实验/私有入口默认跳过，无竞态报告。首次超时不从历史记录删除。

```sh
GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test -race ./internal/gateway \
  -run '^TestExtendedResume(Log|PatternAndConfiguration|EvidenceReader|WorkerRejectsBadInput|ForcedCleanup)' \
  -count=3 -timeout=30s

# 目录必须没有已有的逐场 events.jsonl；不会覆盖原文件。
TIDE_EXTENDED_PROBE_OUTPUT=/private/tmp/tide-extended-new-probe \
  GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off \
  go test ./internal/gateway -run '^TestExtendedResumeProbe$' -count=1 -timeout=90s
```

默认 Go 回归会执行十二秒 probe；正式十分钟入口尚未启用。probe 允许 race，只作正确性与工具接线验证。观察时长、资源分组、独立分析和正式运行不能由测试数量或测试耗时替代。

## 下一步

实现独立 Python 采集/分析：逐场独立进程、小型清单加原始事件文件、共享单调口径的逐故障关联、固定窗口资源统计与数据质量分类。补充缺轮、改配置、位置/摘要矛盾、工具失败和产物搬移等拒绝反例；通过后冻结版本再执行正式六场。已有正式预登记不因 probe 全部按预期而宣称完成。

[完整工具验收归档](../experiments/results/extended-resume-harness-2026-10-12/README.md)含 22 文件，解压读回摘要一致；最终三份新增 Go 工具源码与测量前副本一致。初始原型没有冻结源码，正式全量源码/二进制及独立复算仍待下一工具单元完成。
