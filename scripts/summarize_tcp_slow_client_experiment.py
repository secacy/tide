#!/usr/bin/env python3
"""Summarize EXP-005 socket blockage, RPC cancellation, and resource cleanup."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
from summarize_streaming_load import records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    groups = defaultdict(list)
    for path in args.inputs:
        for r in records(path, strict_timing=True):
            if r['experiment'] != 'EXP-005' or r['registered_after_cleanup']:
                raise ValueError('invalid experiment or retained sessions')
            if r['socket_write_max_ms'] < 100 or r['socket_block_confirmed_ms'] > r['rpc_cancel_ms']:
                raise ValueError('socket blockage not established before cancellation')
            groups[r['case']].append(r)
    summary = {}
    for case, runs in sorted(groups.items()):
        group = {'runs': len(runs), 'session_errors': [r['session_error'] for r in runs],
                 'observed_close_codes': [r['observed_close_code'] for r in runs]}
        for key in ('elapsed_ms', 'socket_block_confirmed_ms', 'rpc_cancel_ms', 'rpc_cancel_to_cleanup_ms',
                    'worker_exit_ms', 'action_to_rpc_cancel_ms', 'socket_write_max_ms',
                    'frames_read', 'payload_bytes_read'):
            values = [r[key] for r in runs if r[key] is not None]
            group[key] = {'min': min(values), 'max': max(values)} if values else None
        summary[case] = group
    with args.output.open('x') as stream:
        json.dump(summary, stream, indent=2)
        stream.write('\n')
    print(f'{sum(g["runs"] for g in summary.values())} trials; clock/blockage/cleanup checks passed')


if __name__ == '__main__':
    main()
