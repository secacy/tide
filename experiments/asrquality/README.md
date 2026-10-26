# 本地短音频质量基线工具

用于[六段模拟问诊计划](../../docs/experiments/real-asr-quality-baseline.md)，不负责通用模型部署。Python 标准库处理清单和评分；本机 `ffmpeg`/`ffprobe` 转换音频，已有 Go 夹具运行真实 Gateway/v2 和独立 sherpa Worker。

`cases.json` 保存本轮提示、参考及事实检查项。只有录制者回听确认一致后才可用它冻结参考；有口误应先修改本轮 case 文件，不能以模型输出反推参考。2026-10-26 六段录音已经录制者确认一致，按单人参考记录。

## 准备

在根目录运行，输出目录必须不存在：

```sh
python3 experiments/asrquality/prepare.py \
  testdata/m4a testdata/asr-quality/self-recorded-2026-10-26 \
  --reviewer '录制者姓名、回听日期及确认依据'
```

准备保留原文件，转换生成 `pcm/q01.wav`～`q06.wav`，不裁切、不增益修正、不做识别；输出 `references`、源格式/转换日志及 manifest。容器 duration 与实际解码样本时长分别保留，评分及排期使用转换后的实际 PCM。`testdata/.gitignore` 仅排除本轮录音和派生目录，其他测试素材仍可跟踪。

## 测试与执行

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s experiments/asrquality -p 'test_*.py' -v
```

在 `workers/sherpa` 模块中指定已构建的固定 Worker、固定模型、本地输入绝对路径和全新的证据目录：

```sh
TIDE_SHERPA_WORKER_BIN=/tmp/tide-sherpa-worker \
TIDE_SHERPA_MODEL_DIR=/tmp/tide-sherpa-model \
TIDE_SHERPA_QUALITY_INPUT_DIR=/absolute/path/to/testdata/asr-quality/self-recorded-2026-10-26 \
TIDE_SHERPA_QUALITY_EVIDENCE_DIR=/tmp/tide-quality-evidence-new \
  go test -v -count=1 -timeout=6m ./cmd/asr-worker -run '^TestRealWorkerQualityV2$'
```

本轮六段顺序各运行一次；预检核对全部 WAV、PCM、参考摘要后才创建连接与模型。复用原闭环的 `runGatewayCase` 和正常完成检查，保留所有计划槽位及中间结果。普通测试自动跳过真实模型入口。运行前保存代码/二进制/模型/参考的版本或摘要；性能观察使用非 race 驱动与 Worker，race 回归独立执行。

## 评分

回到根目录运行（输出文件必须不存在）：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/asrquality/score.py \
  /tmp/tide-quality-evidence-new --output /tmp/tide-quality-evidence-new/score.json
```

评分先核对原始消息、成功转发摘要、定稿与完成确认；证据不足的声称成功项改列 unknown。六个槽位不能删减。只对验证通过的 final 快照计算 CER，输出逐字符对齐和 S/D/I/N，汇总采用总错误数/总参考字符数。归一化与平局处理固定在输出版本中；不转换数字写法，不删除小数点。

事实检查输出待人工审核项目，工具不自动认定医学语义正确。可以另外保存带原文依据、注明审核者的判断；助手的文字对照必须注明为助手初评，不能标成人工或临床标注。

原始音频和 WAV 保留在本地；提交代码、条件、摘要及结果文本时，不包含音频或模型权重。仅有哈希无法恢复音频，异机重放需要另行取得同一份本地素材。

## 直接探针对照

针对首轮 q04/q05 的错误，另用既有 `experiments/asrprobe` 独立调用 SDK，不传其 `-reference` 参数。固定模型、同一 WAV/PCM、100ms 块、padding 和解码配置，只移除生产适配与网络链路；完整步骤与结果见[定位记录](../../docs/worklog/2026-10-26-real-asr-direct-diagnostic.md)。

`compare_probe.py` 是该固定两段诊断的复核入口，复用探针 `verify.py` 及当前 `score.py`，核对原始消息、输入和模型身份，再原样比较变化全文与最后全文。它读取诊断目录的执行前清单、基线副本、执行记录及两个探针目录，不是通用语料采集器。

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/asrquality/compare_probe.py \
  /tmp/tide-quality-direct-evidence --output /tmp/tide-quality-direct-evidence/comparison.json
```

冻结目录结构与可搬移复算命令随[诊断证据](../../docs/experiments/results/real-asr-direct-diagnostic-2026-10-26/README.md)提供。相邻相同快照可忽略末尾重复 final，文本内部任何字符均不去重；本轮不比较两路延迟。

## 解码策略对照

[六段配对计划](../../docs/experiments/real-asr-decoding-comparison.md)固定 G=greedy、B=beam/4 路，交替顺序，各素材每策略一次。探针显式记录 SDK 解码配置；`compare_decoding.py` 根据预登记十二个槽位、实际配置、原始调用/结果和同一参考评分，分别列失败/未知，只有六对完整时才计算同素材总体 CER 差值。

```sh
PYTHONDONTWRITEBYTECODE=1 python3 experiments/asrquality/compare_decoding.py \
  /tmp/tide-decoding-evidence --output /tmp/tide-decoding-evidence/comparison.json
```

目录需包含执行前 `before-run.json`、冻结基线、十二个原始探针目录及 `execution.json`；实际运行器、固定条件与源快照随结果归档。计算代价采用探针既有包围段总时间，不是完整 CPU 时间或端到端容量。旧六段结果继续保留；本轮的同素材比较也不代表新场景质量。
