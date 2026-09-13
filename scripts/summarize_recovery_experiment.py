#!/usr/bin/env python3
"""Validate EXP-013 evidence; retain expected semantic failures instead of hiding them."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import subprocess


def require(condition, message):
    if not condition:
        raise ValueError(message)


def summarize(path):
    root = Path(__file__).resolve().parents[1]
    meta = json.loads(path.with_suffix(path.suffix + '.meta.json').read_text())
    require(meta['experiment'] == 'EXP-013' and meta['exit_code'] == 0, 'incomplete/failed driver')
    require(hashlib.sha256(path.read_bytes()).hexdigest() == meta['raw_sha256'], 'raw hash mismatch')
    for relative, expected in meta['source_sha256'].items():
        data = subprocess.check_output(['git', 'show', meta['commit'] + ':' + relative], cwd=root)
        require(hashlib.sha256(data).hexdigest() == expected, f'source mismatch: {relative}')
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    require(not any(row['Action'] == 'fail' for row in rows), 'Go test failed')
    require(sum(row['Action'] == 'pass' and 'Test' not in row for row in rows) == 1, 'missing package completion')
    passes = Counter(row['Test'] for row in rows if row['Action'] == 'pass' and 'Test' in row)
    groups = defaultdict(list)
    for row in rows:
        line = row.get('Output', '')
        marker = 'EXPERIMENT_RESULT '
        if marker in line:
            record = json.loads(line.split(marker, 1)[1])
            require(record['experiment'] == 'EXP-013', 'wrong experiment')
            prefix = 'TestExperimentRecoveryAdmission/' if record['case'] in ('released_then_admitted', 'admission_budget_exhausted') else 'TestExperimentRecoveryProtocol/'
            require(row.get('Test') == prefix + record['case'], 'record belongs to wrong test')
            groups[record['case']].append(record)
    expected = {f'{scenario}/{policy}' for scenario in ('result_lost', 'result_applied') for policy in ('processed', 'checkpoint')}
    expected |= {f'{scenario}/{policy}' for scenario in ('late_old_result', 'duplicate_delivery') for policy in ('unfenced', 'checkpoint')}
    expected |= {'checkpoint_stalled', 'silence_checkpoints', 'invalid_checkpoint', 'released_then_admitted', 'admission_budget_exhausted'}
    require(set(groups) == expected, 'missing/unexpected cases')
    for case, records in groups.items():
        require(len(records) == meta['count'], f'wrong repetition count: {case}')
        prefix = 'TestExperimentRecoveryAdmission/' if case in ('released_then_admitted', 'admission_budget_exhausted') else 'TestExperimentRecoveryProtocol/'
        require(passes[prefix + case] == meta['count'], f'case did not pass: {case}')
        for r in records:
            if '/' in case:
                missing = 5 if case == 'result_lost/processed' else 0
                duplicate = {'late_old_result/unfenced': 5, 'duplicate_delivery/unfenced': 10}.get(case, 0)
                require((r['missing_frames'], r['duplicate_frames']) == (missing, duplicate), 'coverage counterexample changed')
                require(r['semantic_ok'] == (missing == duplicate == 0), 'incorrect semantic success')
                require(r['worker_active_after'] == 0 and r['cache_after'] == 0, 'RPC/cache cleanup incomplete')
                require(r['processed_before_failure'] == 10, 'fault did not follow processing')
                require(r['checkpoint_before_failure'] == (10 if r['scenario'] == 'result_applied' else 5), 'wrong result application boundary')
                require(r['replay_from'] == (11 if r['policy'] == 'processed' or r['scenario'] == 'result_applied' else 6), 'wrong replay origin')
            elif case == 'checkpoint_stalled':
                require(r['explicit_cache_exhausted'] and r['capture_through'] == 151 and r['cache_frames'] == 150 and r['checkpoint'] == 0 and r['cache_peak_bytes'] == 480000 and not r['semantic_ok'], 'exhaustion not explicit/bounded')
                require(r['worker_active_after'] == 0, 'exhausted RPC leaked')
            elif case == 'silence_checkpoints':
                require(not r['explicit_cache_exhausted'] and r['checkpoint'] == 200 and r['cache_frames'] == 0 and r['semantic_ok'] and r['worker_active_after'] == 0, 'silence coverage failed')
            elif case == 'invalid_checkpoint':
                require(r['rejected'] == 3 and r['checkpoint'] == 0 and r['cache_frames'] == 1 and r['semantic_ok'], 'invalid checkpoint changed state')
            else:
                exhausted = case == 'admission_budget_exhausted'
                require(r['rejections'] > 0 and r['reserved_before_release'] == 1, 'old reservation bypassed')
                require(r['budget_ms'] == 500 and r['budget_exhausted'] == exhausted and r['replacement_completed'] != exhausted, 'budget/replacement mismatch')
                require(r['worker_open_calls'] == (1 if exhausted else 2) and r['reserved_after'] == r['registered_after'] == 0, 'admission accounting mismatch')
                require(r['cleanup_release_is_injected'], 'cleanup timing misrepresented')
            if 'cache_peak_bytes' in r:
                require(0 <= r['cache_peak_bytes'] <= 480000, 'audio memory bound exceeded')
    return {'experiment': 'EXP-013', 'commit': meta['commit'], 'race': meta['race'], 'raw_sha256': meta['raw_sha256'],
            'cases': len(groups), 'samples': sum(map(len, groups.values())), 'rounds': meta['count'],
            'semantic_success_samples': sum(r['semantic_ok'] for records in groups.values() for r in records),
            'expected_semantic_failure_samples': sum(not r['semantic_ok'] for records in groups.values() for r in records),
            'excluded': [], 'groups': dict(sorted(groups.items())),
            'boundary': 'Synthetic restartable segments and client state model; actual Gateway only in admission trials. No speech quality, production replay, wall-clock recovery latency or model capacity claim.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.input.resolve())
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    print(f"validated {result['samples']} samples; expected semantic failures retained: {result['expected_semantic_failure_samples']}")


if __name__ == '__main__':
    main()
