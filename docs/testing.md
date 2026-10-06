# 测试与实验运行

从仓库根目录执行。需要项目 go.mod 指定的 Go 工具链；Python 工具测试使用 Python 3 标准库。首次运行需能取得 Go 依赖，离线环境应提前准备缓存。Makefile 沿用调用方的 Go 环境，不固定机器上的缓存路径。

## 日常入口

| 命令 | 范围与用途 |
| --- | --- |
| `make test-fast` | audio 与 workerpool 两包，快速检查排期和选择算法；覆盖范围有限 |
| `make test` | 全项目默认 Go 测试，包括真实本地 WebSocket/gRPC、进程启动、信号与清理检查 |
| `make test-race` | 全项目默认 Go 测试加竞态检测，适用于并发逻辑或公共夹具变更 |
| `make test-tools` | Python 实验编排和分析工具的正确性测试，使用合成输入及模拟依赖；不运行正式容量实验 |

Go 入口使用 `-count=1`，避免测试结果缓存；`-timeout` 是每个包的测试预算。网络和进程测试需允许本机监听及启动子进程。测试里的进程可能自行构建程序，第一次运行会更慢。

`make test-race` 使用 `-p=1` 串行运行包，减少多个包同时构建和运行 race 子进程的资源争用，代价是全量回归更慢。包内的并发场景与 `t.Parallel()` 仍按原测试运行；测试中的操作期限和断言保持原值。需要默认包并行方式时可直接执行 `go test -race ./... -count=1 -timeout=5m`。

修改单个包时可以直接限定范围，例如：

```sh
go test -race ./internal/gateway -count=1 -timeout=5m
go test ./internal/gateway -run '^TestGatewaySelectorNilContract$' -count=1
```

当前没有基于 build tags 或 `testing.Short()` 的完整单元/集成分层。`make test-fast` 只是两个明确的包，`go test -short ./...` 不会自动跳过所有网络和进程测试。默认测试也包含短时保护探测，不能仅凭文件名中的 `experiment` 判断它是否运行。

## 恢复内部组件与协调验收

第六阶段内部恢复状态、身份、注册表、控制/期限、音频输入与上传协调，以及 Worker 接收任务可以单独验证：

```sh
go test -race ./internal/gateway \
  -run '^Test(WorkerReceiver|DecodeWorkerResponse|ResultBuffer|SessionWorker|WorkerUploader|SendWithTimeout|AudioInputBuffer|AudioInputState|SessionControl|ResumableSession|SessionRegistry|ResumeState|SessionIdentity)' \
  -count=1 -timeout=45s
```

`session_worker_test.go` 主要运行实际 runWithWorker/runCoordinator，以可控 Worker I/O 验证阻塞期间控制响应、代次隔离、输入预算和清理等待。待交付与损坏结果场景直接运行协调循环；提交后回复契约使用一次性接收夹具。testing/synctest 推进虚拟时间，channel 建立事件同步，不能用虚拟时间替代可变字段的同步。

`session_worker_duplex_test.go` 运行实际上传/接收任务，验证断开期间结果保存、处理进度边界、两种完成观察顺序、发送 EOF 的最终状态、固定期限、正常完成后的结果保留与双向清理。首轮发现的取消、期限选择及输入拒绝问题已修正；相同期限与 Recv EOF 先到而 CloseSend 随后失败也有覆盖，见[双向协调验收](worklog/2026-10-06-worker-coordination.md#双向协调验收2026-10-06)。

这是内部正确性验收，没有真实 WebSocket 连接恢复或性能测量；全链路需要后续网络/故障实验。当前证据与范围见[协调验收记录](worklog/2026-10-06-session-upload-coordination.md#内部协调组合验收2026-10-06)。

`result_buffer_test.go` 串行验证结果序号、字符串副本、累计确认、读取游标和双预算；环形复用与线性队列对照，确认清理/槽位复用不改变已借出的只读值。它没有 Worker 或客户端确认接入，证据见[结果缓冲验收](worklog/2026-10-06-result-retention.md#结果缓冲验收2026-10-06)。

`worker_receiver_test.go` 验证 Worker 响应解析、唯一读取的事件顺序、无缓冲交接、取消及终态发布，并用真实接收任务和 `resultBuffer` 组成受控夹具；没有接入生产协调者或真实 WebSocket 恢复，见[接收任务验收](worklog/2026-10-06-worker-receiver.md#接收任务验收2026-10-06)。

## 显式实验

以下十二个实验使用环境变量开启，变量等于 `1` 时才运行。默认回归前应确保这些变量未启用；`make test` 和 `make test-race` 沿用调用方环境。

| 测试名（均在 internal/gateway） | 环境变量 |
| --- | --- |
| TestSlowWorkerBaselineExperiment | TIDE_RUN_SLOW_WORKER_EXPERIMENT |
| TestStalledWorkerExperiment | TIDE_RUN_STALLED_WORKER_EXPERIMENT |
| TestWorkerSendTimeoutExperiment | TIDE_RUN_WORKER_SEND_TIMEOUT_EXPERIMENT |
| TestWorkerPauseExperiment | TIDE_RUN_WORKER_PAUSE_EXPERIMENT |
| TestSlowReaderBaselineExperiment | TIDE_RUN_SLOW_READER_BASELINE |
| TestTailStallBaselineExperiment | TIDE_RUN_TAIL_STALL_BASELINE |
| TestTailTimeoutExperiment | TIDE_RUN_TAIL_TIMEOUT_EXPERIMENT |
| TestSessionAdmissionExperiment | TIDE_RUN_SESSION_ADMISSION_EXPERIMENT |
| TestProgressBacklogBaselineExperiment | TIDE_RUN_PROGRESS_BACKLOG_EXPERIMENT |
| TestAudioBacklogComparisonExperiment | TIDE_RUN_BACKLOG_COMPARISON |
| TestResultWriteTimeoutExperiment | TIDE_RUN_RESULT_WRITE_TIMEOUT_EXPERIMENT |
| TestProtectionCleanupExperiment | TIDE_RUN_PROTECTION_CLEANUP |

例如只执行尾部超时实验：

```sh
TIDE_RUN_TAIL_TIMEOUT_EXPERIMENT=1 go test ./internal/gateway \
  -run '^TestTailTimeoutExperiment$' -count=1 -timeout=5m -v
```

正式性能测量的构建方式、输入、重复次数与判据以对应[实验报告](experiments/)为准；race 检查的耗时不作为性能对照。第五阶段独立进程负载实验使用 Python 编排，入口见[运行命令](command.md)。原始结果保存到新目录，已有证据按原样保留。

## Gateway 测试组织

| 文件 | 职责 |
| --- | --- |
| `internal/gateway/session_fixture_test.go` | 真实 WebSocket 单会话入口、完整结果断言、检查意外建流的 recordingWorker |
| `internal/gateway/network_fixture_test.go` | 本机 TCP gRPC Worker 的启动/回收，以及报告毫秒换算 |
| `internal/gateway/routing_fixture_test.go` | 两种策略共用的路由后端、Gateway 夹具、音频/尾部操作、start 前退出场景 |
| 各场景 `*_test.go` | 保留输入条件及行为断言；普通轮询与加权各自检查分配顺序、次数和失败后行为 |

公共夹具留在 gateway 测试包内，可直接检查协调者与 tracker 的行为。复用连接搭建和操作步骤时保留场景断言，避免把预期结果藏进通用框架。只在故障条件与断言相同时合并重复测试；单元、真实网络、进程测试可能保护同一契约的不同层次。

当前 `TestGatewaySelectorNilContract/nil_interface` 覆盖原 `TestGatewayNilWorkerPool` 的 nil 构造行为；旧测试名已合并。加权 start 前场景的 `disconnect`、`service_shutdown` 统一命名为 `disconnect_before_start`、`shutdown_before_start`，行为和策略断言保留。

文件归类尚未构成完整的实验编译隔离，一些专用观测夹具仍由多个场景共享。后续若需要 build tags，应先检查这些依赖。历史实验源码哈希按当时版本解释，重构不改写原始归档。
