#!/usr/bin/env python3
"""固定加权16场短时过载候选，三批后分别检验两场新会话可用性。"""
import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest
from run_weighted_extended import (CONDITION_KEYS, EXPERIMENT as SOURCE_EXPERIMENT,
                                   validate_selection as validate_short_reference)
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize

EXPERIMENT = "dual_worker_weighted_overload"
DEFAULT_SOURCE = Path(__file__).resolve().parent.parent/"results/weighted-extended-2026-10-03"


def overload_decision(groups):
    """12场两分钟两批须已完整达标；固定16作为过载候选，不预设实测一定失败。"""
    require(isinstance(groups, list) and len(groups) == 1, "incomplete extended groups")
    g = groups[0]
    require(type(g.get("planned_sessions")) is int and g["planned_sessions"] == 12
            and type(g.get("attempts")) is int and g["attempts"] == 24
            and g.get("outcomes") == {"completed": 24} and g.get("completion_rate") == 1
            and type(g.get("batches_meeting_criteria")) is int and g["batches_meeting_criteria"] == 2
            and g.get("all_repetitions_meet_criteria") is True, "12-session extended point not repeatedly complete/pass")
    require(g.get("tail_completed_only", {}).get("p95_ns", float("inf")) <= 250_000_000
            and type(g.get("max_audio_schedule_lag_ns")) is int and 0 <= g["max_audio_schedule_lag_ns"] <= 100_000_000,
            "extended metrics not within original budgets")
    return dict(rule_version=1, sessions=16, audio_duration_s=20, repetitions=3,
                recovery_sessions=2, recovery_audio_duration_s=2,
                reason="fixed_input_160_chunks_per_second_above_optimistic_mock_150")


def validate_selection(selection):
    """核对两分钟来源与其短时证据链；16是预定义观察点，不是已测容量边界。"""
    require(isinstance(selection, dict), "missing overload selection")
    decision = overload_decision(selection.get("source_groups"))
    require(all(selection.get(k) == v for k, v in decision.items()), "overload selection differs")
    require(selection.get("source_experiment") == SOURCE_EXPERIMENT, "wrong extended source")
    reference = selection.get("short_reference")
    validate_short_reference(reference)
    conditions = selection.get("source_conditions", {})
    require(set(conditions) == set(CONDITION_KEYS) and conditions == reference["source_conditions"], "overload source conditions differ")
    hashes = selection.get("source_artifact_sha256", {})
    required = {"manifest.json", "groups.json", "analysis-manifest.json", "gateway.log", "asr-worker-1.log", "asr-worker-2.log"}
    required.update(f"r{r}-n12.json" for r in (1, 2))
    required.update(f"r{r}-n12-{key}/samples.jsonl" for r in (1, 2) for key in ("gateway", "worker1", "worker2"))
    require(required <= set(hashes) and all(isinstance(v, str) and len(v) == 64 and all(c in "0123456789abcdef" for c in v) for v in hashes.values()), "missing/invalid extended evidence hashes")
    return decision


def validate_overload_conditions(manifest):
    """只改变并发、时长与对应客户端/采样预算；源码和所有业务条件保持来源值。"""
    selection = manifest.get("selection")
    validate_selection(selection)
    for key in CONDITION_KEYS:
        require(manifest.get(key) == selection["source_conditions"][key], "overload condition differs: "+key)


def selection_from_source(source):
    """副本重算两分钟来源并记录其哈希，保持已有归档不变。"""
    source = source.resolve()
    m = json.loads((source/"manifest.json").read_text())
    require(m.get("experiment") == SOURCE_EXPERIMENT and m.get("status") == "passed", "extended source incomplete")
    saved = json.loads((source/"groups.json").read_text())
    analysis = json.loads((source/"analysis-manifest.json").read_text())
    require(analysis["input_sha256"]["manifest.json"] == digest(source/"manifest.json")
            and analysis["groups_sha256"] == digest(source/"groups.json"), "extended analysis reference changed")
    with tempfile.TemporaryDirectory(prefix="tide-weighted-overload-reference-") as temp:
        target = Path(temp)/"source"
        shutil.copytree(source, target)
        with contextlib.redirect_stdout(io.StringIO()):
            recomputed = summarize(target)
    require(recomputed == saved, "extended analysis differs from evidence")
    selection = dict(**overload_decision(recomputed["groups"]), source_path=str(source), source_experiment=SOURCE_EXPERIMENT,
                     source_groups=recomputed["groups"], short_reference=m["selection"],
                     source_conditions={k: m[k] for k in CONDITION_KEYS},
                     source_artifact_sha256={**m["artifact_sha256"],
                       **{name: digest(source/name) for name in ("manifest.json", "groups.json", "analysis-manifest.json")}})
    validate_selection(selection)
    return selection


def run_overload(directory, source=DEFAULT_SOURCE):
    """按固定顺序保留过载及新会话探测；采集失败不替换，业务失败也可正常分析。"""
    selection = selection_from_source(source)
    run(directory, pressure=True, profile=pressure_profile(EXPERIMENT), worker_count=2,
        policy="weighted_round_robin", selection=selection)
    return summarize(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; preserve all attempts")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="complete 12-session extended observation")
    parser.add_argument("--analyze-only", action="store_true", help="recheck evidence without producing load")
    args = parser.parse_args()
    if args.analyze_only:
        require(json.loads((args.output/"manifest.json").read_text()).get("experiment") == EXPERIMENT, "not weighted overload observation")
        summarize(args.output.resolve())
    else:
        run_overload(args.output.resolve(), args.source.resolve())
