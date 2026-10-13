#!/usr/bin/env python3
"""Collect EXP-006-07, defaulting to four short probe processes; formal is explicit."""
import argparse
from pathlib import Path
import attempt_window_resume_contract as profile
from run_extended_resume import collect

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mode', choices=('probe', 'formal'), default='probe')
    args = parser.parse_args()
    raise SystemExit(collect(args.output, args.mode, rounds=2, profile=profile))
