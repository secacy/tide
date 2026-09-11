#!/usr/bin/env python3
"""Summarize EXP-004, requiring complete passing logs and continuous run clocks."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from summarize_streaming_load import records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    groups = defaultdict(list)
    for path in args.inputs:
        for row in records(path, strict_timing=True):
            if row.get('experiment') != 'EXP-004':
                raise ValueError(f'wrong experiment: {path}')
            if row['registered_after_cleanup'] != 0 or row['client_error']:
                raise ValueError(f'cleanup or driver error: {path}')
            if not 0 <= row['acknowledged'] <= row['processed'] <= row['dequeued'] <= row['admitted'] <= row['sent']:
                raise ValueError(f'audio accounting violated: {row}')
            if row['unacknowledged_tail'] != row['admitted'] - row['acknowledged']:
                raise ValueError('tail accounting violated')
            if row['queue_peak_bytes'] > 64000:
                raise ValueError('queue capacity violated')
            groups[(row['scenario'], row['policy'])].append(row)
    def span(values):
        values = [value for value in values if value is not None]
        return {'min': min(values), 'max': max(values)} if values else None
    summary = {}
    for (scenario, policy), rows in sorted(groups.items()):
        group = {'runs': len(rows), 'outcomes': dict(Counter(r['outcome'] for r in rows)),
                 'close_codes': dict(Counter(str(r['close_code']) for r in rows)),
                 'age_breach_runs': sum(r['first_age_breach_ms'] is not None for r in rows),
                 'censored_runs': sum(r['outcome'] == 'observation_end' for r in rows)}
        for key in ('elapsed_ms', 'trigger_ms', 'first_age_breach_ms', 'end_to_cleanup_ms',
                    'trigger_to_cleanup_ms', 'oldest_unacknowledged_peak_ms', 'queue_age_peak_ms',
                    'send_max_ms', 'queue_peak_bytes', 'sent', 'admitted', 'processed',
                    'acknowledged', 'results', 'unacknowledged_tail'):
            group[key] = span([r[key] for r in rows])
        group['result_p95_ms'] = span([r['result_latency']['p95_ms'] if r['result_latency']['count'] else None for r in rows])
        summary[f'{scenario}/{policy}'] = group
    with args.output.open('x') as stream:
        json.dump(summary, stream, indent=2)
        stream.write('\n')
    print(f'{sum(g["runs"] for g in summary.values())} trials, {len(summary)} groups; timing/accounting checks passed')


if __name__ == '__main__':
    main()
