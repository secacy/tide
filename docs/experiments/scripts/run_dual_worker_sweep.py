#!/usr/bin/env python3
"""固定两个同配置 Worker，对 16/18/20 场各做三次 20 秒筛查。"""
import argparse
from pathlib import Path

from run_joint_observation_baseline import run
from summarize_pressure_sweep import pressure_profile, summarize


def run_sweep(directory):
    """复用多实例编排与完整证据检查；过载业务失败保留并继续后续批次。"""
    run(directory, pressure=True, profile=pressure_profile("dual_worker_sweep"), worker_count=2)
    return summarize(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; all attempts retained")
    parser.add_argument("--analyze-only", action="store_true", help="recheck a completed sweep without generating load")
    args = parser.parse_args()
    directory = args.output.resolve()
    if args.analyze_only:
        import json
        from summarize_gateway_observations import require
        require(json.loads((directory/"manifest.json").read_text()).get("experiment") == "dual_worker_sweep", "not a dual-worker sweep")
        summarize(directory)
    else:
        run_sweep(directory)
