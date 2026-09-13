#!/usr/bin/env python3
"""Validate EXP-014 raw evidence and produce a compact semantic summary."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

CASES = {
    'normal': True, 'worker_before_checkpoint': True, 'worker_after_checkpoint': True,
    'websocket_checkpoint_lost': True, 'end_closure_lost': True,
    'buffer_gap_continue': False, 'end_tail_budget': False, 'fallback_budget': False,
    'unsupported_worker': False, 'invalid_checkpoint': False,
}


def summarize(raw):
    meta = json.loads(raw.with_suffix(raw.suffix + '.meta.json').read_text())
    if meta['experiment'] != 'EXP-014' or meta['exit_code'] != 0:
        raise ValueError('experiment run did not succeed')
    if hashlib.sha256(raw.read_bytes()).hexdigest() != meta['raw_sha256']:
        raise ValueError('raw evidence hash mismatch')
    samples = []
    for line in raw.read_text().splitlines():
        event = json.loads(line)
        if event.get('Action') == 'fail':
            raise ValueError('raw test failure; preserve and investigate')
        output = event.get('Output', '')
        if 'RECOVERY_SAMPLE ' in output:
            samples.append(json.loads(output.split('RECOVERY_SAMPLE ', 1)[1]))
    if Counter(s['case'] for s in samples) != Counter({c: meta['count'] for c in CASES}):
        raise ValueError('missing, extra or duplicated case samples')
    groups = defaultdict(list)
    for s in samples:
        if s['experiment'] != 'EXP-014' or not s['quality_pass'] or s['complete'] != CASES[s['case']]:
            raise ValueError('unexpected semantic outcome')
        if not s['gateway_drained'] or s['worker_active_after_cleanup'] != 0:
            raise ValueError('resource cleanup failure')
        if s['cache_peak_bytes'] > s['cache_limit_bytes']:
            raise ValueError('PCM bound exceeded')
        spans = [(a['fromSample'], a['throughSample']) for a in s['segments'] or []]
        spans += [(a['FromSample'], a['ThroughSample']) for a in s['gaps'] or []]
        frontier = 0
        for start, end in sorted(spans):
            if start != frontier or end <= start:
                raise ValueError('coverage gap or overlap not explicitly represented')
            frontier = end
        if frontier != s['captured_through'] or (s['complete'] and (s['gaps'] or not s['ended'])):
            raise ValueError('invalid consultation completion')
        if s['case'] == 'websocket_checkpoint_lost' and s['retry_from_samples'][:2] != [0, 160]:
            raise ValueError('lost delivery did not replay from applied checkpoint')
        if s['case'] == 'end_tail_budget' and any(s['retry_from_samples']):
            raise ValueError('End skipped history')
        if s['case'] in ('end_tail_budget', 'fallback_budget') and 'recovery budget exhausted' not in s['error']:
            raise ValueError('automatic recovery did not stop on budget')
        if s['case'] in ('unsupported_worker', 'invalid_checkpoint') and s['attempts'] != 1:
            raise ValueError('fatal protocol error retried')
        groups[s['case']].append(s)
    lines = ['# EXP-014 production recovery summary', '',
             f"Source commit: `{meta['commit']}`; race: `{meta['race']}`; samples: {len(samples)}; excluded: 0.", '',
             'Run elapsed includes initial capture/admission and cleanup; it is not fault-to-recovery latency or an ASR benchmark.', '',
             '| Case | n | Complete | Attempts min–max | Run ms min–max | PCM peak bytes max |',
             '| --- | ---: | --- | ---: | ---: | ---: |']
    for case, group in groups.items():
        attempts = [s['attempts'] for s in group]
        elapsed = [s['run_elapsed_ms'] for s in group]
        lines.append(f"| {case} | {len(group)} | {CASES[case]} | {min(attempts)}–{max(attempts)} | {min(elapsed):.1f}–{max(elapsed):.1f} | {max(s['cache_peak_bytes'] for s in group)} |")
    lines += ['', 'All returned audio is partitioned into committed results and explicit gaps; all Gateways drained and Worker RPC counts reached zero.']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('raw', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = summarize(args.raw)
    if args.output:
        if args.output.exists():
            parser.error('preserve existing summary; choose another path')
        args.output.write_text(result)
    else:
        print(result, end='')


if __name__ == '__main__':
    main()
