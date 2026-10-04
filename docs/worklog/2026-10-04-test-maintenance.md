# 第六阶段前：测试组织与运行入口整理

状态：公共夹具和重复操作整理完成，验证结果见下。

## 目标与选择

恢复功能将修改连接与会话的关系。现有测试需要便于复用连接搭建、尾部结果核对和退出清理，同时保留不同故障与策略的断言。此次优先整理测试依赖，核心会话架构仍由后续恢复契约驱动。

公共夹具可以留在各场景文件、集中到包内测试文件，或建立跨包测试库。当前选择包内三个 `*_fixture_test.go`：这些夹具直接观测 Gateway 内部协调者和 tracker，跨包库会增加接口和导出需求；分开保存单会话、TCP 后端及路由夹具，更便于查找。专用 Worker 和故障控制仍留在相应场景中。

## 完成内容

- 将 recordingWorker、单会话 WebSocket 入口和完整结果断言移到 [session_fixture_test.go](../../internal/gateway/session_fixture_test.go)。
- 将本机 TCP gRPC 后端及报告毫秒换算移到 [network_fixture_test.go](../../internal/gateway/network_fixture_test.go)，解除普通测试对 slow_worker 实验文件中这两个函数的依赖。
- 将共享路由后端、Gateway 夹具及音频/尾部操作移到 [routing_fixture_test.go](../../internal/gateway/routing_fixture_test.go)。普通轮询与加权复用六种合法 start 前退出操作，选择顺序、次数及后端未调用断言各自保留。
- 将完全重复的 `TestGatewayNilWorkerPool` 合并到 `TestGatewaySelectorNilContract/nil_interface`。Gateway 顶层测试清单从 98 个变为 97 个，唯一移除的名称为上述重复测试。
- 加权的两个子场景名称统一为 `disconnect_before_start`、`shutdown_before_start`。条件和断言保留，便于与普通轮询对应。
- 新增 [Makefile](../../Makefile) 的快速、全量、race 和 Python 工具测试入口，以及 [testing.md](../testing.md) 的范围与十二个显式实验开关说明；文档索引与运行命令已链接。

本次主要收益是明确夹具位置、复用操作并保留覆盖。测试拆文件后的 imports 和注释抵消了部分合并行数，全 Gateway 测试源码为 8699 → 8700 行，不能描述为显著缩减代码量。

## 验证与发现

Go 验证使用 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off`，命令从仓库根目录运行。

1. 整理前后 `go test ./internal/gateway -list '^Test'` 编译通过；清单差异只有被合并的 nil 构造测试，未新增顶层用例。
2. `make test-tools`：103 个 Python 工具正确性测试通过。没有执行正式容量实验。
3. 首轮 `make test-race` 使用默认包并行运行：internal/gateway 等包通过，cmd/gateway 的 `TestGatewayStartupCLI/help` 在 5 秒期限内未退出，输出中已有帮助文本。该次完整回归失败，保留此结果。
4. `go test -race ./cmd/gateway -run '^TestGatewayStartupCLI$' -count=3 -timeout=2m`：三次完整 CLI 检查通过，未复现上述超时。当前不能确定首次超时原因。
5. 后续 `make test-race` 改为 `go test -race -p=1 ./... -count=1 -timeout=5m`。按包串行减少同时构建/运行 race 子进程的资源争用，代价是整体更慢；包内并发行为、子进程期限与断言沿用原值。全项目 11 个有测试的包全部通过，3 个包无测试文件，未报告竞态。
6. `make test-fast`：audio 与 workerpool 两包通过；范围不包含 Gateway 网络测试。
7. 十二个显式实验开关均已对应文档，新增/修改文档的本地链接及 `git diff --check` 通过。

首轮包并行超时与后续串行通过只能说明两次运行的结果，尚未证明资源争用是失败根因。未来复现时应采集子进程状态与机器负载，再评估期限或测试进程调度。

## 后续

测试夹具仍在同一个 Go 测试包，文件归类尚未构成完整的实验编译隔离；更改 build tags 前须继续检查专用观测夹具依赖。历史实验归档保持原始内容和当时源码哈希。本次整理没有对总内存、容量或恢复能力提供新结论。

会话构造配置收拢已按用户委托由助手完成，见下。下一步先确定第六阶段恢复语义，再调整连接与逻辑会话的生命周期。

## 会话构造配置收拢（2026-10-04）

按用户明确委托，由助手完成本步核心重构。原 `newSession` 连续接收五个 `time.Duration` 和一个预算值，改为 `newSession(ws, pool, cfg sessionConfig)`，通过字段名表达每个参数用途。连接与共享选择器仍是独立依赖，构造只复制配置，不执行 I/O 或启动 goroutine。

可直接传入整个 Gateway Config，改动更少，但会让构造函数接触不使用的准入和消息大小配置；也可以采用逐项 option，但当前六项固定值没有动态组合需求。本步选择内部 sessionConfig，包含五种期限和 uint64 音频积压预算。保留已有 session 字段与运行逻辑，避免同时修改所有读写路径。

Config.sessionConfig 仅从 Gateway.New 已完成校验和默认值填充的配置提取字段；负积压预算仍由 New 拒绝，转换时已有非负前提。生产入口和两个实验观测入口复用该转换，三个直接构造会话的测试使用具名字段，并沿用原参数值。字段和函数注释说明用途及前置条件。

验证：`GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off go test -race -p=1 ./internal/gateway ./cmd/gateway -count=1 -timeout=5m` 两包通过，耗时分别 13.163s、6.075s，未报告竞态。沿用正常尾部、期限/取消、积压预算、后端路由和真实启动的既有测试；未新增只检查字段复制的测试。格式和 diff 检查通过。此处耗时为正确性回归耗时，不用于性能结论。

本步没有重跑历史容量实验；历史源码哈希和结果保留其当时含义。恢复能力仍须由下一阶段的协议和生命周期方案实现并验证。
