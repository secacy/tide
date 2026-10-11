# 群体恢复：随机退避比较工具与验收

日期：2026-10-12。关联 [EXP-006-04](../experiments/group-resume-jitter-comparison.md)、[客户端设计与验收](2026-10-11-group-resume-jitter-design.md)。状态：群体比较工具已接通，正式 16 批测量待执行。

## 目标与选择

客户端已支持 ±50% jitter。需要把计划种子、生效种子及实际请求联系起来，才能回答请求是否分散、拒绝是否减少、用户等待是否改善。候选序列只能说明算法的可能排期，不能说明实际用了几次或恢复更快。

复用共享 Gateway/位置 Worker 夹具、事件重组及字节/结果/清理校验，新增独立实验身份、四轮順序及逐场预期。完全复制校验容易分叉，放宽旧契约则会混淆参数。公用校验增加显式逐场 resume 预期，旧入口仍要求原配置；EXP-006-02/03 条件和默认轮数保持原语义。

## 实现

1. `group_resume_jitter_test.go` 提供独立正式/缩短入口，固定四轮相邻顺序，正式默认跳过且拒绝 race。报告保存实验、round、逐场 seed/算法/七项候选等待/总和，失败仍输出观察。共享夹具只在启动前向独立客户端配置注入种子，实际配置同时记录。
2. Python 固定契约独立实现 Go 64 位 PCG DXSM 和 Int64N 拒绝采样映射，核对候选序列；不从 Go 报告借用结果当预期。支持 amd64/arm64，种子仅复现抽样，不复现系统调度。
3. 采集器一次构建、test.count=1，内部四轮，独占目录；完整快照含 EXP-006-04 及引用的 EXP-006-03 条件。通用采集器增加显式 formal_rounds/reference_plans 参数，旧入口仍默认三轮。
4. 分析器核对逐场生效配置、计划序列、唯一槽位、分母、顺序、不重叠、原 RPC/音频/连续结果和自然清理；复用原 RPC 保留及逐块等待计算。输出 none/jitter 总差和八对相邻差；不合格、缺失或重复槽位保留未知，抑制相应比较。
5. T0 与实际占位 handler 归还名额两个原点分别保存固定 50ms 请求桶，含首波/后续/空桶和 handshake_limit。开放后拒绝按请求到达分类；开放前到达、开放后响应不算开放后竞争。桶峰值与完整性、用户等待和远端保留同时呈现。

候选等待只描述连续失败假设，没有增加生产客户端观测回调，候选和不作为实测耗时。窗口/次数的证据不足时，不猜测唯一耗尽原因。

## 正确性与兼容性

基于 `a921557` 加本步未提交工具，Go 1.26.5。Go 命令使用 `GOCACHE=/private/tmp/tide-review-gocache GOPROXY=off GOSUMDB=off`，Python 使用 PYTHONDONTWRITEBYTECODE=1：

```sh
go test -race ./internal/gateway -run '^TestGroupResumeJitter' -count=3 -timeout=2m -json
go test -race ./internal/wsclient ./internal/gateway -count=1 -timeout=4m -json
python3 -m unittest discover -s docs/experiments/scripts -p 'test_*.py'
```

- 新 Go 七个叶级终点三轮共 21 通过，正式入口跳过三次。覆盖四条件 probe、条件差只来自策略、32 个种子唯一且 D/E 复用、随机后次数耗尽/窗口先到的明确失败、生效种子及自然清理。
- 两包 race：wsclient 327、gateway 1064，共 1391 叶级通过，17 默认跳过（16 显式实验与一个私有 helper），无失败或 race 报告。
- 最终 Python 159 个方法通过，本步新增 17 个。覆盖 22 种报告反例、10 种清单反例，丢批/重复/错路径/换序、失败和未知分母、到达与响应口径、搬移、凭据碎片、构建失败、随机归约拒绝采样和真实归档。测试数量是覆盖规模。
- EXP-006-02/03 两份旧正式归档用当前分析器复算，reports/summary/sessions/attempt-bins 四类输出逐字节一致。旧入口继续拒绝新策略；历史归档未修改。

首次 14 项 Python 定向验收通过；加入换序/未知分母后全工具 157 项通过；归档后加入真实报告及随机归约检查，最终 159 项通过。初轮 Go 四条件验收通过，补耗尽边界后完成三轮与两包回归。本步没有测试失败被删去或通过放宽规则变为合格。

## 真实缩短采集与复核

目录 `/private/tmp/tide-group-jitter-tools-2026-10-12-run-1`；2026-10-12 00:36:59–00:37:15（UTC+8）。一次非 race 构建，317 个源码/工具/两份条件文件采集前后摘要一致。

```sh
PYTHONDONTWRITEBYTECODE=1 GOCACHE=/private/tmp/tide-review-gocache \
  GOPROXY=off GOSUMDB=off \
  python3 docs/experiments/scripts/run_group_resume_jitter.py \
  --mode probe --count 4 \
  --output /private/tmp/tide-group-jitter-tools-2026-10-12-run-1
```

采集前登记短条件：800ms 输入、40ms/300ms 占位、1ms/块 Worker、20ms→80ms 基础退避、八次/2s、服务端保留 1s。16/16 批、128/128 场证据合格且完整完成，未知为零；start/Pick/原 RPC 一一对应，无串场/替代 RPC，音频逐字完整、结果连续、完成确认写出。16 批自然空闲，无强制收尾；6530 资源样本，最大间隔 13.521ms。

| probe 条件 | 完整完成 | 恢复 Dial | 实际开放后 handshake_limit |
| --- | ---: | ---: | ---: |
| D none | 32/32 | 112 | 16 |
| D jitter | 32/32 | 96 | 0 |
| E none | 32/32 | 203 | 11 |
| E jitter | 32/32 | 202 | 0 |

这是短条件工具事实，不作为正式 500ms/2s 收益。失败记录能力由上述真实耗尽场景与合成反例覆盖；probe 全部完成不是更换种子或新增正式条件的理由。

首次分析、新目录 reanalysis 及完整目录搬到 `/private/tmp/tide-group-jitter-tools-relocated-2026-10-12-run-1` 后复算，reports/summary/sessions/attempt-bins/release-bins 五类输出逐字节一致。分析摘要记录六个依赖脚本及输入/输出哈希。

[仓库证据子集](../experiments/results/group-resume-jitter-tools-2026-10-12/README.md) 包含完整压缩原始事件、报告、清单、预登记、CSV、分析及测试日志，压缩文件读回核对。它不含二进制和全部源码，不能称为完整可运行归档；完整临时目录保留这些文件。测量后才添加的真实归档测试不属于该次 317 文件快照，正式测量将重新冻结最终版本。

## 下一步

客户端策略和比较工具已接通。接下来按预登记正式四条件各四批、128 场，同版本测量并独立复算、完整归档。继续并列报告完成、请求、开放后拒绝、转录等待与原 RPC 保留；无改善或退化也是结果。当前暂无正式 jitter 性能结论或生产参数推荐。
