#!/usr/bin/env python3
"""Validate EXP-011 and compare measured detection/cost; replay quantities are analytical only."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import subprocess

from summarize_disconnect_experiment import summarize
from summarize_streaming_load import records


def digest(data):
    return hashlib.sha256(data).hexdigest()


def overhead(row):
    n = row['connections']
    if row['window_ms'] != 21000 or n not in (64, 256):
        raise ValueError('smoke workload is not formal overhead evidence')
    if row['observed_ms'] < row['window_ms'] - 10:
        raise ValueError('observation window truncated')
    if any(row[k] for k in ('registered_after', 'reserved_after', 'active_rpc_after')):
        raise ValueError('retained overhead resources')
    pings = sum(row['successful_pings'].values())
    if row['interval_ms'] == 0 and pings:
        raise ValueError('baseline unexpectedly sent periodic pings')
    if row['interval_ms'] and pings == 0 and row['heartbeat_failures'] == 0:
        raise ValueError('periodic probe driver did not run')
    elapsed = row['observed_ms'] / 1000
    return {**row,
            'pings_per_second': pings / elapsed,
            'websocket_bytes_per_second': (row['upstream_bytes'] + row['downstream_bytes']) / elapsed,
            'allocated_bytes_per_second': row['allocated_bytes'] / elapsed,
            'extra_probe_goroutines': row['goroutines_at_window_end'] - row['goroutines_before_probes'],
            'all_healthy': row['heartbeat_failures'] == 0 and row['early_client_exits'] == 0 and row['normal_complete'] == n
                and all(row[k] == n for k in ('registered_at_window_end', 'reserved_at_window_end', 'active_rpc_at_window_end'))}


def detection(row):
    if row['interval_ms'] not in (2000, 5000) or row['ping_timeout_ms'] != 3000:
        raise ValueError('unexpected interval candidate')
    out = {**summarize(row), 'interval_ms': row['interval_ms']}
    # Do not mislabel processing failure as link detection. These cases have no
    # unconfirmed audio at injection, and each endpoint's own probe is reported.
    delay = out['client_heartbeat_failure_ms']
    out['analytical_audio_seconds_until_client_detection'] = None if delay is None else delay / 1000
    out['analytical_pcm_bytes_until_client_detection'] = None if delay is None else delay / 1000 * 32000
    # Illustrative constant-rate fluid model: replay and ongoing recording share
    # one recognizer. The assumed rates are NOT measured model capacities. Start
    # from detection with instantaneous admission/connect and no earlier backlog.
    out['idealized_catchup_seconds_after_detection'] = None if delay is None else {
        str(speed): delay / 1000 / (speed - 1) for speed in (1.2, 1.5, 2.0)}
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('inputs', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output exists; preserve prior summary')
    root = Path(__file__).resolve().parents[1]
    groups, sources = defaultdict(list), []
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + '.meta.json').read_text())
        if meta['experiment'] != 'EXP-011' or meta['race'] or meta['exit_code'] or meta['connections_override'] or meta['window_override_ms']:
            raise ValueError('not a successful formal run')
        if digest(path.read_bytes()) != meta['raw_sha256']:
            raise ValueError('raw hash mismatch')
        for name, expected in meta['source_sha256'].items():
            original = subprocess.check_output(['git', 'show', f'{meta["commit"]}:{name}'], cwd=root)
            if digest(original) != expected:
                raise ValueError(f'source mismatch: {name}')
        original = subprocess.check_output(['git', 'show', f'{meta["commit"]}:internal/gateway/session.go'], cwd=root).decode()
        site = '\tresult := waitSessionResult(ctx, events, requestClosing, progress)\n'
        if original.count(site) != 1:
            raise ValueError('overlay site mismatch')
        modified = original.replace(site, site + '\tdisconnectDecision(s, result)\n')
        modified += '\nfunc init() { disconnectOverlayEnabled = true }\n'
        if meta['overlay_sha256'] != {'internal/gateway/session.go': digest(modified.encode())}:
            raise ValueError('overlay changed behavior')
        rows = records(path, strict_timing=True)
        counts = defaultdict(int)
        for row in rows:
            if row['experiment'] != 'EXP-011':
                raise ValueError('wrong record experiment')
            counts[row['case']] += 1
            result = overhead(row) if meta['kind'] == 'overhead' else detection(row)
            groups[row['case']].append({'source': str(path), 'round': counts[row['case']], **result})
        if any(n != meta['count'] for n in counts.values()):
            raise ValueError('sample count mismatch')
        if meta['case'] == '.*' and len(counts) != {'interval': 12, 'overhead': 6}[meta['kind']]:
            raise ValueError('missing scenario')
        sources.append({'path': str(path), 'kind': meta['kind'], 'commit': meta['commit'], 'raw_sha256': meta['raw_sha256'], 'samples': len(rows), 'timing_continuity': 'passed'})
    result = {'experiment': 'EXP-011', 'samples': sum(map(len, groups.values())), 'sources': sources,
              'excluded': [], 'groups': dict(sorted(groups.items())),
              'analytical_model_boundary': 'No replay or audio collection in fault cases. 32000 B/s and 1.2/1.5/2x speed are assumed; instantaneous network recovery and admission at detection. Does not prove actual catch-up or lost audio.'}
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(f'{result["samples"]} samples validated; deadline misses and unhealthy cases retained; replay metrics explicitly analytical')


if __name__ == '__main__':
    main()
