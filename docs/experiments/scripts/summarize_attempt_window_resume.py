#!/usr/bin/env python3
"""Recompute EXP-006-07 with explicit fixed strategy profiles and portable artifact paths."""
import argparse
from pathlib import Path
import attempt_window_resume_contract as profile
from summarize_extended_resume import summarize

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('collection', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(0 if summarize(args.collection, args.output, profile=profile)['evidence_valid'] else 1)
