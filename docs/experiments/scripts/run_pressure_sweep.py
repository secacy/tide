#!/usr/bin/env python3
"""固定 4/8/12 场、20 秒音频、各三批；有效失败结果不触发丢弃或重跑。"""
import argparse
from pathlib import Path

from run_joint_observation_baseline import run
from summarize_pressure_sweep import summarize

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; preserve every attempt")
    directory = parser.parse_args().output.resolve()
    run(directory, pressure=True)
    summarize(directory)
