#!/usr/bin/env python3
"""Worker 1 每块 10ms、Worker 2 每块 20ms，以轮询运行 8/10/12 场三批。"""
import argparse
import json
from pathlib import Path

from run_joint_observation_baseline import run
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize


def run_heterogeneous(directory):
    """固定静态速度差异；共享采样、全尝试保留和进程回收逻辑。"""
    run(directory,pressure=True,profile=pressure_profile('dual_worker_heterogeneous'),worker_count=2)
    return summarize(directory)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='new directory; preserve all attempts')
    parser.add_argument('--analyze-only',action='store_true',help='recheck completed evidence without generating load')
    args=parser.parse_args();directory=args.output.resolve()
    if args.analyze_only:
        require(json.loads((directory/'manifest.json').read_text()).get('experiment')=='dual_worker_heterogeneous','not heterogeneous observation')
        summarize(directory)
    else:
        run_heterogeneous(directory)
