#!/usr/bin/env python3
"""先固定 9/10/11 场短时细测，再按预定义规则选择两分钟观察点。"""
import argparse
import json
from pathlib import Path

from run_loadgen_baseline import digest
from run_joint_observation_baseline import run
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize


def choose_points(groups):
    """只按短时各档全部重复是否达标选点；不隐藏波动和全部未达标的情况。"""
    require(isinstance(groups, list) and [g["planned_sessions"] for g in groups] == [9, 10, 11], "selection requires complete 9/10/11 groups")
    for g in groups:
        n, count = g["planned_sessions"], g["batches_meeting_criteria"]
        require(type(count) is int and 0 <= count <= 3 and g["attempts"] == n*3, "invalid short repetitions")
        require(type(g["all_repetitions_meet_criteria"]) is bool and g["all_repetitions_meet_criteria"] == (count == 3), "inconsistent short criteria")
    passed = [g["planned_sessions"] for g in groups if g["all_repetitions_meet_criteria"]]
    unmet = [g["planned_sessions"] for g in groups if g["batches_meeting_criteria"] == 0]
    return dict(rule_version=1, normal=8, near_boundary=max(passed) if passed else 9,
                near_boundary_short_passed=bool(passed), overload_candidate=min(unmet) if unmet else 12,
                selection_reason="highest_repeated_short_pass" if passed else "lowest_tested_point_no_repeated_pass")


def selection_from_directory(source):
    """读取已结束、哈希匹配的短时证据；重新计算分析，拒绝伪造派生选点。"""
    import contextlib
    import io
    import shutil
    import tempfile
    m = json.loads((source/"manifest.json").read_text())
    require(m.get("experiment") == "boundary_short" and m["status"] == "passed", "source is not completed short experiment")
    analysis = json.loads((source/"analysis-manifest.json").read_text())
    require(digest(source/"manifest.json") == analysis["input_sha256"]["manifest.json"], "source manifest changed")
    require(digest(source/"groups.json") == analysis["groups_sha256"], "source groups changed")
    # 临时副本复算，不重写历史分析时间或原始文件。
    with tempfile.TemporaryDirectory(prefix="tide-selection-check-") as tmp:
        copy = Path(tmp)/"source"
        shutil.copytree(source, copy)
        with contextlib.redirect_stdout(io.StringIO()):
            result = summarize(copy)
    require(result == json.loads((source/"groups.json").read_text()), "source analysis differs from raw evidence")
    groups = result["groups"]
    return dict(**choose_points(groups), source_directory=str(source.resolve()), source_groups=groups,
                source_manifest_sha256=digest(source/"manifest.json"), source_groups_sha256=digest(source/"groups.json"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("short", "extended"), required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory; preserve all attempts")
    parser.add_argument("--source", type=Path, help="completed short result directory, required for extended phase")
    args = parser.parse_args()
    if (args.phase == "extended") != (args.source is not None):
        parser.error("--source is required only for extended phase")
    selection = selection_from_directory(args.source.resolve()) if args.source else None
    profile = pressure_profile("boundary_"+args.phase, selection["near_boundary"] if selection else None)
    if selection:
        print(json.dumps(selection, ensure_ascii=False, indent=2), flush=True)
    directory = args.output.resolve()
    run(directory, pressure=True, profile=profile, selection=selection)
    summarize(directory)
