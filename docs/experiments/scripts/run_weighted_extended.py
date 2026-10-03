#!/usr/bin/env python3
"""复核同条件短时策略证据，固定加权12并发做两批120秒观察。"""
import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest
from run_strategy_comparison import EXPERIMENT as SOURCE_EXPERIMENT, ORDER, compare
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize
from worker_comparison_contract import CRITERIA, GATEWAY, worker_configs

EXPERIMENT = "dual_worker_weighted_extended"
DEFAULT_SOURCE = Path(__file__).resolve().parent.parent/"results/strategy-comparison-2026-10-03-retest-1"
CONDITION_KEYS = ("source_sha256", "worker_count", "worker_configs", "gateway", "criteria", "policy", "weights",
                  "go", "os", "cpu", "logical_cpus", "memory_bytes", "environment", "race")


def extended_decision(groups):
    """只选择已测8/10/12中最高且三批全达标的12场；拒绝缺批或矛盾完成记录。"""
    require(isinstance(groups, list) and [g.get("planned_sessions") for g in groups] == [8, 10, 12], "incomplete weighted short groups")
    for g in groups:
        n = g["planned_sessions"]
        passed = g.get("batches_meeting_criteria")
        require(type(passed) is int and 0 <= passed <= 3 and type(g.get("attempts")) is int and g["attempts"] == 3*n, "short repetition/count differs")
        require(type(g.get("all_repetitions_meet_criteria")) is bool and g["all_repetitions_meet_criteria"] == (passed == 3), "short criteria inconsistent")
        outcomes = g.get("outcomes", {})
        require(isinstance(outcomes, dict) and set(outcomes) <= {"completed", "failed", "canceled", "timed_out"}
                and all(type(v) is int and v >= 0 for v in outcomes.values()) and sum(outcomes.values()) == 3*n, "short outcome denominator differs")
        if passed == 3:
            require(outcomes.get("completed") == 3*n and g.get("completion_rate") == 1, "short pass has failures")
    require(all(g["batches_meeting_criteria"] == 3 for g in groups), "short evidence does not justify fixed 12 observation")
    return dict(rule_version=1, sessions=12, reason="highest_measured_weighted_point_with_three_short_passes",
                audio_duration_s=120, repetitions=2)


def validate_selection(selection):
    """复算已存选点依据；短时来源身份、哈希与固定Worker/策略条件必须齐全。"""
    require(isinstance(selection, dict), "missing weighted selection")
    decision = extended_decision(selection.get("source_groups"))
    require(all(selection.get(k) == v for k, v in decision.items()), "extended selection differs")
    require(selection.get("source_experiment") == SOURCE_EXPERIMENT, "wrong short experiment")
    hashes = selection.get("source_artifact_sha256", {})
    required = {"manifest.json", "comparison.json", "analysis-manifest.json"}
    required.update(item["directory"]+"/manifest.json" for item in ORDER)
    required.update(item["directory"]+"/groups.json" for item in ORDER)
    require(required <= set(hashes) and all(isinstance(v, str) and len(v) == 64 and all(c in "0123456789abcdef" for c in v) for v in hashes.values()), "missing/invalid short evidence hashes")
    conditions = selection.get("source_conditions", {})
    require(set(conditions) == set(CONDITION_KEYS) and bool(conditions["source_sha256"]), "missing source conditions")
    require(conditions["worker_count"] == 2 and conditions["worker_configs"] == worker_configs(EXPERIMENT, 2)
            and conditions["gateway"] == GATEWAY and conditions["criteria"] == CRITERIA
            and conditions["policy"] == "weighted_round_robin" and conditions["weights"] == [2, 1]
            and conditions["race"] is False, "short conditions differ")
    return decision


def validate_extended_conditions(manifest):
    """长观察只改变时长及相应预算；源码、环境、策略和业务判据与短时来源一致。"""
    selection = manifest.get("selection")
    validate_selection(selection)
    for key in CONDITION_KEYS:
        require(manifest.get(key) == selection["source_conditions"][key], "extended condition differs: "+key)


def selection_from_source(source):
    """在副本重算全部六组来源，不修改归档；记录根及原始产物哈希以便追溯。"""
    source = source.resolve()
    m = json.loads((source/"manifest.json").read_text())
    require(m.get("experiment") == SOURCE_EXPERIMENT and m.get("status") == "passed", "short comparison incomplete")
    saved = json.loads((source/"comparison.json").read_text())
    analysis = json.loads((source/"analysis-manifest.json").read_text())
    require(analysis["input_sha256"]["manifest.json"] == digest(source/"manifest.json")
            and analysis["comparison_sha256"] == digest(source/"comparison.json"), "short analysis reference changed")
    with tempfile.TemporaryDirectory(prefix="tide-weighted-extended-reference-") as temp:
        target = Path(temp)/"source"
        shutil.copytree(source, target)
        with contextlib.redirect_stdout(io.StringIO()):
            recomputed = compare(target)
    require(recomputed == saved, "short comparison differs from raw evidence")
    groups = [g["weighted_round_robin"] for g in recomputed["groups"]]
    conditions = json.loads((source/"pair-1-weighted_round_robin/manifest.json").read_text())
    selection = dict(**extended_decision(groups), source_path=str(source), source_experiment=SOURCE_EXPERIMENT,
                     source_groups=groups, source_conditions={k: conditions[k] for k in CONDITION_KEYS},
                     source_artifact_sha256={**m["artifact_sha256"],
                       **{name: digest(source/name) for name in ("manifest.json", "comparison.json", "analysis-manifest.json")}})
    validate_selection(selection)
    return selection


def run_extended(directory, source=DEFAULT_SOURCE):
    """先复核来源再执行固定两批，业务失败照常保存；不自动替换批次。"""
    selection = selection_from_source(source)
    run(directory, pressure=True, profile=pressure_profile(EXPERIMENT), worker_count=2,
        policy="weighted_round_robin", selection=selection)
    return summarize(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; preserve all attempts")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="completed six-cohort short strategy comparison")
    parser.add_argument("--analyze-only", action="store_true", help="recheck completed long observation without load")
    args = parser.parse_args()
    if args.analyze_only:
        require(json.loads((args.output/"manifest.json").read_text()).get("experiment") == EXPERIMENT, "not weighted extended observation")
        summarize(args.output.resolve())
    else:
        run_extended(args.output.resolve(), args.source.resolve())
