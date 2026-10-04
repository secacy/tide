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

下一步可由开发者将 `newSession` 的多个连续期限参数收拢为具名内部配置，并保持现有校验/默认值契约；随后先确定第六阶段恢复语义，再调整连接与逻辑会话的生命周期。
