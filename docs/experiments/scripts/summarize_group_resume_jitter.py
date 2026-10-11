#!/usr/bin/env python3
"""独立复核 EXP-006-04，比较固定预算下的随机策略与请求/等待成本。"""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path

from group_resume_jitter_contract import CASES, EXPERIMENT, MARKER, PLAN, REFERENCE_PLAN, ROOT_TEST, configured, environment, order, retry_plan, session_resume
from summarize_group_resume import (digest, distribution, duration, extract_reports, require,
                                    stamp_ns, verify_archive_files, verify_report)
from summarize_group_resume_budget import enrich_original_rpc, metrics as budget_metrics, difference as budget_difference


def verify_manifest(root, m):
    mode, rounds = m['mode'], m['planned_rounds']
    require(mode in ROOT_TEST and type(rounds) is int and 1 <= rounds <= 4 and
            (mode != 'formal' or rounds == 4), 'invalid jitter mode or rounds')
    require(m['version'] == 2 and m['experiment'] == EXPERIMENT and m['plan'] == PLAN and
            m['root_test'] == ROOT_TEST[mode] and m['test_count'] == 1, 'jitter experiment identity differs')
    require(m['order'] == order(rounds) and m['planned_sessions'] == 32 * rounds, 'jitter order or denominator differs')
    require(m['experiment_environment'] == environment(mode, rounds), 'jitter environment differs')
    require(Path(m['collection_directory']).is_absolute(), 'original collection directory missing')
    # 命令记录核对原采集路径；产物摘要从当前目录读，允许归档搬移后复算。
    binary = str(Path(m['collection_directory']) / 'group-resume.test')
    require(m['build_argv'] == ['go', 'test', '-c', '-o', binary, './internal/gateway'], 'build command differs')
    require(m['run_argv'] == ['go', 'tool', 'test2json', '-t', '-p', 'github.com/secacy/tide-artisan/internal/gateway',
                            binary, '-test.run=^' + ROOT_TEST[mode] + '$', '-test.count=1',
                            '-test.timeout=5m', '-test.v=test2json'], 'jitter run command differs')
    require('-race' not in m['go_env']['GOFLAGS'].split(), 'race build flags in collection')
    verify_archive_files(root, m)
    require(REFERENCE_PLAN in m['source_sha256'], 'referenced fixed conditions snapshot missing')
    require(PLAN in m['source_sha256'] and digest(root / 'source' / PLAN) == digest(root / 'preregistration.md'),
            'preregistration differs from snapshot')


def extract(events, mode):
    def validate_test(test, r):
        require(r.get('experiment') == EXPERIMENT and type(r.get('round')) is int and 1 <= r['round'] <= 4,
                'report jitter identity or round differs')
        require(test == ROOT_TEST[mode] + '/round_' + str(r['round']) + '/' + r['config']['case'],
                'report test path differs from round/case')
    return extract_reports(events, ROOT_TEST[mode], MARKER[mode], validate_test=validate_test)



def verify_jitter_report(r, mode):
    cfg = configured(r['config']['case'], mode)
    require(r.get('experiment') == EXPERIMENT and type(r.get('round')) is int and 1 <= r['round'] <= 4,
            'report jitter identity or round differs')
    plans = [retry_plan(cfg, r['round'], identity, mode) for identity in range(1, 9)]
    require(r['retry_plans'] == plans, 'seed, algorithm or candidate sequence differs')
    require(all(type(p['id']) is int and type(p['seed']) is int and type(p['candidate_sum_ns']) is int and
                all(type(d) is int for d in p['candidate_delays_ns']) for p in r['retry_plans']), 'candidate integer types differ')
    require(r['environment']['goarch'] in ('amd64', 'arm64'), 'PCG reproduction requires a supported 64-bit target')
    resumes = {p['id']: session_resume(cfg, p) for p in plans}
    row = verify_report(r, mode, expected_config=cfg, expected_resumes=resumes)
    require(not row['environment']['race'], 'collected jitter reports require a non-race build')
    row = enrich_original_rpc(r, row)
    for s, p in zip(row['sessions'], plans):
        s.update(strategy=p['strategy'], seed=p['seed'], candidate_sum_ns=p['candidate_sum_ns'])
    release = stamp_ns(r['released_at'])
    arrivals = [a for a in r['attempts'] if a['connection'] > 1]
    require(all(stamp_ns(a['arrived_at']) <= stamp_ns(r['returned_at']) for a in arrivals), 'request arrival after whole-batch return')
    # 分类取实际请求到达；响应在释放后写出，不代表请求在开放后到达。
    later = [a for a in arrivals if stamp_ns(a['arrived_at']) >= release]
    row['post_release_arrivals'] = len(later)
    row['post_release_handshake_limit'] = sum(a['code'] == 'handshake_limit' for a in later)
    row['pre_release_handshake_limit'] = sum(a['code'] == 'handshake_limit' for a in arrivals if stamp_ns(a['arrived_at']) < release)
    fault_rejected = Counter((stamp_ns(a['arrived_at']) - stamp_ns(r['fault_at'])) // 50_000_000
                             for a in arrivals if a['code'] == 'handshake_limit')
    for b in row['attempt_bins']:
        b['handshake_limit'] = fault_rejected[b['begin_ns']//50_000_000]
    end = max(0, stamp_ns(r['returned_at']) - release)
    require(end <= cfg['run_timeout_ns'], 'release observation exceeds run bound')
    counts = Counter((stamp_ns(a['arrived_at']) - release) // 50_000_000 for a in later)
    first = Counter((stamp_ns(a['arrived_at']) - release) // 50_000_000 for a in later if a['connection'] == 2)
    rejected = Counter((stamp_ns(a['arrived_at']) - release) // 50_000_000 for a in later if a['code'] == 'handshake_limit')
    require(not counts or max(counts) <= end // 50_000_000, 'request arrival after whole-batch return')
    row['release_bins'] = [dict(begin_ns=i*50_000_000, arrivals=counts[i], first=first[i], retries=counts[i]-first[i],
                               handshake_limit=rejected[i]) for i in range(end//50_000_000+1)]
    row['max_retry_bin'] = max((b['retries'] for b in row['attempt_bins']), default=0)
    row['max_release_bin'] = max((b['arrivals'] for b in row['release_bins']), default=0)
    return row


def metrics(batches):
    result = budget_metrics(batches)
    for key in ('post_release_arrivals', 'post_release_handshake_limit', 'pre_release_handshake_limit'):
        result[key] = sum(b[key] for b in batches)
    for key in ('max_retry_bin', 'max_release_bin'):
        result[key] = distribution([b[key] for b in batches])
    return result


def difference(control, candidate, planned):
    result = budget_difference(control, candidate, planned)
    for key in ('post_release_arrivals', 'post_release_handshake_limit', 'pre_release_handshake_limit'):
        result[key] = candidate[key] - control[key]
    return result


def write_csv(path, rows, fields):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fields)
        writer.writeheader(); writer.writerows(rows)


def summarize(root, output):
    root, output = Path(root), Path(output)
    m = json.loads((root / 'manifest.json').read_text())
    mode, rounds = m['mode'], m['planned_rounds']
    require(mode in ROOT_TEST and type(rounds) is int and 1 <= rounds <= 4, 'cannot interpret jitter denominator')
    output.mkdir(parents=True, exist_ok=False)
    errors, reports, verified = [], [], []
    try:
        verify_manifest(root, m)
    except (ValueError, OSError, KeyError, TypeError) as exc:
        errors.append('manifest: ' + str(exc))
    archive_valid = not errors
    events_valid = False
    try:
        events = [json.loads(line) for line in (root / 'go-events.jsonl').read_text().splitlines()]
        reports = extract(events, mode)
        require(any(e.get('Action') == 'pass' and e.get('Test') == ROOT_TEST[mode] for e in events), 'jitter root test did not pass')
        events_valid = True
    except (ValueError, OSError, KeyError, TypeError) as exc:
        errors.append('events: ' + str(exc))
    expected = [(n + 1, case) for n in range(rounds) for case in order(4)[n*4:n*4+4]]
    slots = Counter((r.get('round'), r.get('config', {}).get('case')) for r in reports)
    actual = [(r.get('round'), r.get('config', {}).get('case')) for r in reports]
    ordered = actual == expected
    if not ordered:
        errors.append('jitter reports missing, duplicated, unexpected, or out of order')
    previous_idle = None
    for i, r in enumerate(reports):
        try:
            row = verify_jitter_report(r, mode)
            env = row['environment']
            require(all(env[k] == m['go_env'][v] for k,v in [('go_version','GOVERSION'), ('goos','GOOS'), ('goarch','GOARCH')]),
                    'build/run environment differs')
            if previous_idle:
                require(stamp_ns(r['started_at']) >= stamp_ns(previous_idle), 'batches overlap before natural idle')
            previous_idle = r['natural_idle_at']
            row['report_index'] = i
            verified.append(row)
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            errors.append(f'report {i}: {exc}')
    # 只有唯一、属于预登记轮次的槽位才能进入已核实业务计数；重复不多算成功。
    unique = {(b['round'], b['case']): b for b in verified if (b['round'], b['case']) in expected and slots[(b['round'], b['case'])] == 1}
    groups = []
    for case in CASES:
        batches = [unique[(n,case)] for n in range(1,rounds+1) if (n,case) in unique]
        sessions = [s for b in batches for s in b['sessions']]
        completed = sum(s['completed'] for s in sessions)
        unknown = 8 * rounds - len(sessions)
        valid = archive_valid and events_valid and ordered and not unknown
        groups.append(dict(case=case, planned_sessions=8*rounds, verified_completed=completed,
                           verified_failed=len(sessions)-completed, unknown_sessions=unknown,
                           all_batches_verified=valid, completion_rate=completed/(8*rounds) if valid else None,
                           completion_rate_bounds=[completed/(8*rounds), (completed+unknown)/(8*rounds)] if archive_valid else None,
                           metrics=metrics(batches) if valid else None))
    comparisons, pairs = [], []
    for prefix in ('D_busy_500ms', 'E_busy_2s'):
        control, candidate = [next(g for g in groups if g['case'] == prefix + '_' + a) for a in ('none','jitter')]
        valid = control['all_batches_verified'] and candidate['all_batches_verified']
        comparisons.append(dict(scenario=prefix, planned_per_strategy=8*rounds, control=control['case'], candidate=candidate['case'],
                                valid=valid, difference=difference(control['metrics'], candidate['metrics'], 8*rounds) if valid else None))
        for n in range(1,rounds+1):
            a, b = unique.get((n,control['case'])), unique.get((n,candidate['case']))
            valid = archive_valid and events_valid and ordered and a is not None and b is not None
            pairs.append(dict(round=n, scenario=prefix, valid=valid,
                              difference=difference(metrics([a]), metrics([b]), 8) if valid else None))
    result = dict(version=1, experiment=EXPERIMENT, mode=mode, evidence_valid=not errors, errors=errors,
                  planned_batches=4*rounds, planned_sessions=32*rounds, collected_batches=len(reports), verified_batches=len(unique),
                  groups=groups, comparisons=comparisons, pairs=pairs, batches=verified,
                  scope='controlled same-process loopback; probe is not formal performance',
                  metric_definitions=dict(rpc_after_fault_ms='T0 to this original Worker RPC exit, not Gateway session exit',
                    rpc_after_return_ms='nonnegative Worker RPC exit minus its own client return; later failure can shorten this without earlier release',
                    rpc_seconds_after_fault='sum of original RPC lifetimes after T0; not CPU seconds',
                    latency_population='complete sessions only; independent batches are not paired patients',
                    remote_idle_after_return_ms='whole-batch natural idle minus this client return',
                    completion_rate_bounds='verified complete/planned to (verified complete+unknown)/planned; unavailable for corrupt archive',
                    candidate_sum_ns='hypothetical seven waits if all retries fail; not measured elapsed time',
                    post_release_handshake_limit='actual resume arrivals at or after holder handler release, rejected with handshake_limit',
                    max_retry_bin='maximum T0-relative 50ms bucket count excluding the first resume wave',
                    max_release_bin='maximum release-relative 50ms bucket arrival count'))
    (output / 'reports.json').write_text(json.dumps(reports, separators=(',', ':')) + '\n')
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    fields = ['round', 'case', 'report_index', 'id', 'completed', 'run_error_class', 'last_entry_code', 'outer_deadline',
              'strategy', 'seed', 'candidate_sum_ns', 'resume_attempts', 'proxy_arrivals', 'parsed_resumes', 'backend_requests', 'ready_after_fault_ms', 'ready_after_release_ms',
              'return_after_fault_ms', 'rpc_after_fault_ms', 'rpc_after_return_ms', 'remote_idle_after_return_ms',
              'captured_bytes', 'worker_bytes', 'processed_bytes', 'missing_captured_bytes', 'unprocessed_captured_bytes',
              'uncaptured_bytes', 'repeated_transfer_bytes', 'max_capture_lag_ms', 'max_partial_gap_ms']
    rows = [dict(round=b['round'], case=b['case'], report_index=b['report_index'], **{k:s[k] for k in fields[3:]}) for b in verified for s in b['sessions']]
    write_csv(output / 'sessions.csv', rows, fields)
    write_csv(output / 'attempt-bins.csv', [dict(round=b['round'],case=b['case'],report_index=b['report_index'],**x)
              for b in verified for x in b['attempt_bins']], ['round','case','report_index','begin_ns','arrivals','first','retries','handshake_limit'])
    write_csv(output / 'release-bins.csv', [dict(round=b['round'],case=b['case'],report_index=b['report_index'],**x)
              for b in verified for x in b['release_bins']], ['round','case','report_index','begin_ns','arrivals','first','retries','handshake_limit'])
    paths = ['release-bins.csv', 'reports.json', 'summary.json', 'sessions.csv', 'attempt-bins.csv']
    scripts = ['summarize_group_resume_jitter.py', 'group_resume_jitter_contract.py', 'summarize_group_resume_budget.py', 'group_resume_budget_contract.py', 'summarize_group_resume.py', 'summarize_client_resume.py']
    audit = dict(version=1, source_sha256={name:digest(Path(__file__).parent/name) for name in scripts},
                 input_sha256={name:digest(root/name) for name in ('manifest.json','go-events.jsonl') if (root/name).exists()},
                 output_sha256={name:digest(output/name) for name in paths})
    (output / 'analysis-manifest.json').write_text(json.dumps(audit, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.directory, args.output)
    print(json.dumps({k:result[k] for k in ('evidence_valid','planned_sessions','collected_batches','verified_batches','errors')}))
    raise SystemExit(0 if result['evidence_valid'] else 1)
