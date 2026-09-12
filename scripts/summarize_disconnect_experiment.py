#!/usr/bin/env python3
"""Validate EXP-010 provenance/cleanup, preserving missing detections and deadline misses."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import subprocess

from summarize_streaming_load import records


def digest(data):
    return hashlib.sha256(data).hexdigest()


def summarize(record):
    before = record['events_before_cleanup']
    after = record['events']
    for name, event in before.items():
        if after.get(name) != event:
            raise ValueError(f'observation snapshot was changed: {name}')
    if record['registered_after_cleanup'] or record['reserved_after_cleanup']:
        raise ValueError('resources retained after cleanup')
    for name in ('worker_exit', 'rpc_cancel', 'handler_exit', 'client_read_exit'):
        if name not in after:
            raise ValueError(f'cleanup observation missing: {name}')
    end = before['observation_end']['ms']
    fault = before.get('fault')
    if any(e['ms'] > end for e in before.values()):
        raise ValueError('observation includes a later teardown event')
    if fault and end - fault['ms'] < 5900:
        raise ValueError('observation window truncated')
    if 'watchdog_cleanup' in before:
        raise ValueError('watchdog contamination')
    if record['scenario'].startswith('send_stall'):
        if not fault or before['send_stall']['ms'] > fault['ms']:
            raise ValueError('Send was not stalled before network fault')
    healthy = record['scenario'] in ('idle', 'acked_idle', 'pause')
    detections = ('gateway_decision', 'gateway_heartbeat_failure', 'client_heartbeat_failure', 'client_read_exit')
    false_positive = healthy and any(k in before for k in detections)
    if healthy and not false_positive and record['mode'] == 'heartbeat' and min(record['successful_pings'].get(k, 0) for k in ('client', 'gateway')) < 2:
        raise ValueError('control probes did not run')
    if record['scenario'] == 'pause':
        paused_ms = before['resumed']['ms'] - fault['ms']
        if not 490 <= paused_ms <= 1000:
            raise ValueError('pause timing outside experiment definition')
        if record['mode'] == 'heartbeat' and not (record['upstream_blocked_reads'] and record['downstream_blocked_reads']):
            raise ValueError('pause missed all probe traffic')

    def since_fault(name):
        return before[name]['ms'] - fault['ms'] if fault and name in before else None

    def delta(left, right):
        return before[right]['ms'] - before[left]['ms'] if left in before and right in before else None

    client_candidates = [before[k]['ms'] for k in ('client_read_exit', 'client_heartbeat_failure') if k in before]
    client_detect = min(client_candidates) - fault['ms'] if fault and client_candidates else None
    gateway_detect = since_fault('gateway_decision')
    return {
        'healthy_control': healthy,
        'false_positive': false_positive,
        'gateway_detect_ms': gateway_detect,
        'client_detect_ms': client_detect,
        'gateway_heartbeat_failure_ms': since_fault('gateway_heartbeat_failure'),
        'client_heartbeat_failure_ms': since_fault('client_heartbeat_failure'),
        'rpc_cancel_ms': since_fault('rpc_cancel'),
        'handler_exit_ms': since_fault('handler_exit'),
        'worker_exit_ms': since_fault('worker_exit'),
        'decision_to_rpc_cancel_ms': delta('gateway_decision', 'rpc_cancel'),
        'rpc_cancel_to_handler_exit_ms': delta('rpc_cancel', 'handler_exit'),
        'gateway_decision_detail': before.get('gateway_decision', {}).get('detail'),
        'gateway_within_target': gateway_detect is not None and 0 <= gateway_detect <= record['detection_target_ms'] if not healthy else None,
        'client_within_target': client_detect is not None and 0 <= client_detect <= record['detection_target_ms'] if not healthy else None,
        'registered_before_cleanup': record['registered_before_cleanup'],
        'reserved_before_cleanup': record['reserved_before_cleanup'],
        'successful_pings': record['successful_pings'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output exists; preserve prior summary')
    root = Path(__file__).resolve().parents[1]
    groups, provenance = defaultdict(list), []
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + '.meta.json').read_text())
        if meta['race'] or meta['exit_code'] or meta['experiment'] != 'EXP-010':
            raise ValueError('not a formal successful experiment run')
        if digest(path.read_bytes()) != meta['raw_sha256']:
            raise ValueError('raw data hash mismatch')
        for name, expected in meta['source_sha256'].items():
            original = subprocess.check_output(['git', 'show', f'{meta["commit"]}:{name}'], cwd=root)
            if digest(original) != expected:
                raise ValueError(f'source differs from recorded commit: {name}')
        original = subprocess.check_output(['git', 'show', f'{meta["commit"]}:internal/gateway/session.go'], cwd=root).decode()
        site = '\tresult := waitSessionResult(ctx, events, requestClosing, progress)\n'
        if original.count(site) != 1:
            raise ValueError('unexpected overlay site')
        modified = original.replace(site, site + '\tdisconnectDecision(s, result)\n')
        modified += '\nfunc init() { disconnectOverlayEnabled = true }\n'
        if meta['overlay_sha256'] != {'internal/gateway/session.go': digest(modified.encode())}:
            raise ValueError('overlay is not the recorded decision-only observation')
        rows = records(path, strict_timing=True)
        per_case = defaultdict(int)
        for row in rows:
            if row['experiment'] != 'EXP-010':
                raise ValueError('unexpected experiment')
            per_case[row['case']] += 1
            groups[row['case']].append({'source': str(path), 'round': per_case[row['case']], **summarize(row)})
        if any(n != meta['count'] for n in per_case.values()):
            raise ValueError('missing or repeated scenario samples')
        provenance.append({'path': str(path), 'raw_sha256': meta['raw_sha256'], 'commit': meta['commit'], 'samples': len(rows), 'timing_continuity': 'passed'})
    summary = {'experiment': 'EXP-010', 'samples': sum(map(len, groups.values())), 'sources': provenance,
               'excluded': [], 'groups': dict(sorted(groups.items()))}
    with args.output.open('x') as stream:
        json.dump(summary, stream, indent=2)
        stream.write('\n')
    print(f'{summary["samples"]} samples; provenance, timing and cleanup validated; detection misses retained')


if __name__ == '__main__':
    main()
