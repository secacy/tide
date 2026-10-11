#!/usr/bin/env python3
"""采集 EXP-006-04；四轮在 Go 程序内执行，一次构建，test.count 固定为一。"""
import argparse
from pathlib import Path

from group_resume_jitter_contract import EXPERIMENT, PLAN, REFERENCE_PLAN, ROOT_TEST, environment, order
from run_group_resume import REPO, collect_profile
from summarize_group_resume_jitter import summarize


def collect(output, mode='formal', rounds=4, repo=REPO):
    return collect_profile(output, mode, rounds, repo, plan=PLAN, order=order(rounds),
                           root_test=ROOT_TEST.get(mode), test_count=1, planned_sessions=32 * rounds,
                           env_overrides=environment(mode, rounds), analyze=summarize, experiment=EXPERIMENT,
                           formal_rounds=4, reference_plans=(REFERENCE_PLAN,))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['formal', 'probe'], default='formal')
    parser.add_argument('--count', type=int, default=4, help='logical rounds; formal requires 4')
    args = parser.parse_args()
    raise SystemExit(collect(args.output, args.mode, args.count))
