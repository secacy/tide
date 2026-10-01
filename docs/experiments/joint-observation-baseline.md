# EXP-005-04：Gateway 与 Worker 联合观测基线

日期：2026-10-02
状态：实验条件与分析检查已固定，正式数据待执行。
相关：[Gateway 观测基线](gateway-observation-baseline.md)、[Worker 采样工具](../worklog/designs/2026-10-02-worker-recording-tool.md)。

## 问题与方案选择

Gateway 活动会话数说明接入规模，但不能说明 Worker 正在处理多少音频块、多少流正在等待处理名额。本轮把客户端完成和延迟、Gateway 活动数、Worker 占用与等待放进同一负载窗口，验证能否获得相互对应的证据。

可以直接增大负载寻找失败点，也可以先在已知的小规模条件下建立联合观测基线。本轮选择后者：保持 Worker/Gateway 和输入负载参数，用观测覆盖、状态变化及结果完整性先验证测量链路。两路采样使用独立进程和每批十秒文件，容易独立核对与保留失败；代价是多一个进程、每批需要等观察窗口结束。整轮长采样的开销更低，但共享文件切窗和异常归属更复杂。

Worker 每次查询及交付完成后等待 20ms，Gateway 等待 100ms。Worker 的逐块模拟处理为 10ms，缩短间隔增加捕捉占用及短暂等待的机会；固定节奏仍可能与音频块周期形成偏差，所以只报告样本中占用/等待的比例，不解释为真实时间利用率，也不假设采样开销为零。当前不改成事件级追踪，避免在建立基线时同时改变处理链路和数据规模。

## 执行前固定的条件与判据

- 同机五个独立进程：Mock Worker、Gateway、loadgen、gateway-sampler、worker-sampler。正式二进制不启用 race。
- Worker 一份共享处理名额，逐块处理 10ms、文本响应等待 5ms；HTTP 调试和 gRPC 均使用系统分配的本机端口。Gateway 最大会话数 64、未确认音频预算 32000 字节，其余沿用 EXP-005-03 参数。
- 单场 2 秒预热单独保存。正式每轮依次 1/2/4 场，重复三轮，共九批 21 场；每场 5 秒静音 PCM、160000 字节、50 块，实时发送，单场期限 20 秒。
- 两路采样每批预算 10 秒，请求期限 1 秒；各自等待最多 2 秒直到落盘有效空闲样本，才启动负载。Worker 要求 limit=1、in_use=0、waiting=0，Gateway 要求 active=0、max=64、stopping=false。
- 客户端检查完整完成、字节/块/结果数、首条结果早于 end、发送排期样本数和摘要复算；负载和两个采样命令均应退出 0。
- 清单与 JSONL 格式、来源、配置、计数、序号和时间顺序一致。每路在负载前后均须有完整的有效查询，并观测到空闲状态；窗口两侧最近有效样本之间，最大相邻有效查询完成间隔不超过 500ms。负载窗口不得与失败查询重叠。500ms 是采集覆盖标准，不是业务 SLA。
- Gateway 窗口内观测峰值应为计划数 N，所有成功状态 max=64、stopping=false、active≤N。Worker 始终启用限制，limit=1、0≤in_use≤1、waiting≥0，in_use+waiting≤N。
- 不把“必须出现等待”作为验收标准，也不要求每批必然采到短暂占用。峰值与样本比例按实际记录；没有观测到不代表从未发生。
- 只管理脚本启动的进程。8080 被占用则中止，不接管其他服务；失败目录保留、不自动重试挑选结果；先停 Gateway，再停 Worker，均需正常退出。归档源码、脚本、二进制哈希、环境、命令和全部退出结果。

## 指标口径与边界

负载窗口是脚本启动 loadgen 前、等待返回后记录的 UTC 时间。只有查询起止都在窗口内的有效快照进入状态直方图；两路查询不是同一瞬间，不做逐行强行配对。Worker 忙碌样本比例为窗口内 in_use>0 的条数除以窗口内有效计数条数，等待样本比例同理；未启用和失败不补零，并分别统计。

waiting 是登记等待处理名额的流数量，不是等待时长、音频积压字节或 CPU/GPU 排队量。离散峰值不是连续峰值，查询耗时不等于锁开销。首个负载退出后空闲查询的完成延迟包含采样等待和 HTTP 耗时，不是精确资源回收时间。跨进程使用同机墙钟，时钟倒退拒绝分析。

这轮新增了 Worker 快照、HTTP 和采样进程，不能直接把与历史延迟的差异归为某一改动。未隔离本机其他负载，未做采样开关 A/B；五秒有限批次不证明长时稳定容量。

## 复现与校验

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s docs/experiments/scripts -p 'test_*observations.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/test_loadgen_reports.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/experiments/scripts/run_joint_observation_baseline.py --output /tmp/tide-joint-observation-new
```

输出目录必须不存在。正常结束生成分组统计；复算用 `summarize_joint_observations.py <目录>`，会校验原始产物哈希并重写两个派生文件。归档结果需要先复制再复算，避免改变分析生成时间。

本轮新增八个 Python 测试方法，覆盖状态三分、比例分母、窗口边界、缺口、格式损坏、无等待及就绪条件；加上既有 Gateway 的八个方法共十六个通过。Go 代码未改动，沿用上一提交的全项目 race 验证。正式结果待运行后追加，不能把这些检查数量作为性能成果。
