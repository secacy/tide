#!/usr/bin/env python3
"""采集 EXP-006-03；三轮由 Go 入口内部执行，不能再用 test.count 重复三轮。"""
import argparse
from pathlib import Path

from group_resume_budget_contract import EXPERIMENT, PLAN, ROOT_TEST, environment, order
from run_group_resume import REPO, collect_profile
from summarize_group_resume_budget import summarize


def collect(output, mode='formal', rounds=3, repo=REPO):
    return collect_profile(output, mode, rounds, repo, plan=PLAN, order=order(rounds),
                           root_test=ROOT_TEST.get(mode), test_count=1, planned_sessions=32 * rounds,
                           env_overrides=environment(mode, rounds), analyze=summarize, experiment=EXPERIMENT)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=['formal', 'probe'], default='formal')
    parser.add_argument('--count', type=int, default=3, help='logical rounds; formal requires 3')
    args = parser.parse_args()
    raise SystemExit(collect(args.output, args.mode, args.count))
