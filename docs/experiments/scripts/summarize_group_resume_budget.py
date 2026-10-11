#!/usr/bin/env python3
"""独立复核 EXP-006-03，按固定分母比较次数预算，不改变旧基线契约。"""
import argparse
from collections import Counter
import csv
import json
from pathlib import Path

from group_resume_budget_contract import CASES, EXPERIMENT, MARKER, PLAN, ROOT_TEST, configured, environment, order
from summarize_group_resume import (digest, distribution, duration, extract_reports, require,
                                    stamp_ns, verify_archive_files, verify_report)


def verify_manifest(root, m):
    mode, rounds = m['mode'], m['planned_rounds']
    require(mode in ROOT_TEST and type(rounds) is int and 1 <= rounds <= 3 and
            (mode != 'formal' or rounds == 3), 'invalid budget mode or rounds')
    require(m['version'] == 2 and m['experiment'] == EXPERIMENT and m['plan'] == PLAN and
            m['root_test'] == ROOT_TEST[mode] and m['test_count'] == 1, 'budget experiment identity differs')
    require(m['order'] == order(rounds) and m['planned_sessions'] == 32 * rounds, 'budget order or denominator differs')
    require(m['experiment_environment'] == environment(mode, rounds), 'budget environment differs')
    require(Path(m['collection_directory']).is_absolute(), 'original collection directory missing')
    # 命令记录核对原采集路径；产物摘要从当前目录读，允许归档搬移后复算。
    binary = str(Path(m['collection_directory']) / 'group-resume.test')
    require(m['build_argv'] == ['go', 'test', '-c', '-o', binary, './internal/gateway'], 'build command differs')
    require(m['run_argv'] == ['go', 'tool', 'test2json', '-t', '-p', 'github.com/secacy/tide-artisan/internal/gateway',
                            binary, '-test.run=^' + ROOT_TEST[mode] + '$', '-test.count=1',
                            '-test.timeout=5m', '-test.v=test2json'], 'budget run command differs')
    require('-race' not in m['go_env']['GOFLAGS'].split(), 'race build flags in collection')
    verify_archive_files(root, m)
    require(PLAN in m['source_sha256'] and digest(root / 'source' / PLAN) == digest(root / 'preregistration.md'),
            'preregistration differs from snapshot')


def extract(events, mode):
    def validate_test(test, r):
        require(r.get('experiment') == EXPERIMENT and type(r.get('round')) is int and 1 <= r['round'] <= 3,
                'report budget identity or round differs')
        require(test == ROOT_TEST[mode] + '/round_' + str(r['round']) + '/' + r['config']['case'],
                'report test path differs from round/case')
    return extract_reports(events, ROOT_TEST[mode], MARKER[mode], validate_test=validate_test)


def verify_budget_report(r, mode):
    cfg = configured(r['config']['case'], mode)
    require(r.get('experiment') == EXPERIMENT and type(r.get('round')) is int and 1 <= r['round'] <= 3,
            'report budget identity or round differs')
    row = verify_report(r, mode, expected_config=cfg)
    require(not row['environment']['race'], 'collected budget reports require a non-race build')
    originals = {s['id']: s['observation'] for s in r['sessions']}
    for s in row['sessions']:
        obs = originals[s['id']]
        require(not s['completed'] or obs['run_result']['CompletionAckWritten'], 'completed without written completion ack')
        worker = obs['worker']
        require(stamp_ns(worker['started_at']) <= stamp_ns(r['fault_at']) <= stamp_ns(worker['exited_at']) <= stamp_ns(r['natural_idle_at']),
                'original RPC not alive at fault or exit after idle')
        s['rpc_after_fault_ms'] = duration(worker['exited_at'], r['fault_at'])
        s['rpc_after_return_ms'] = max(0, duration(worker['exited_at'], obs['returned_at']))
        reads = [x for x in obs['source_reads'] if x['end'] > x['begin']]
        partials = [x for x in obs['applications'] if not x['final']]
        s['result_actual_wait_samples_ms'] = [duration(app['applied_at'], reads[i]['started_at']) for i, app in enumerate(partials)]
        s['result_planned_wait_samples_ms'] = [(stamp_ns(app['applied_at']) - stamp_ns(reads[0]['started_at']) - i*100_000_000)/1e6
                                              for i, app in enumerate(partials)]
        s['rejection_codes'] = dict(Counter(a['code'] for a in r['attempts'] if a['id'] == s['id'] and a['connection'] > 1 and a['code']))
    row.update(round=r['round'], natural_idle_after_fault_ms=duration(r['natural_idle_at'], r['fault_at']),
               rpc_seconds_after_fault=sum(s['rpc_after_fault_ms'] for s in row['sessions']) / 1000)
    return row


def metrics(batches):
    """有效批次合并原始场样本；成功时延与失败分母同时输出。"""
    sessions = [s for b in batches for s in b['sessions']]
    success = [s for s in sessions if s['completed']]
    result = dict(completed=len(success), failed=len(sessions) - len(success), latency_population=len(success),
                  rejection_codes=dict(sum((Counter(b['rejection_codes']) for b in batches), Counter())),
                  failure_classes=dict(Counter(s['run_error_class'] for s in sessions if not s['completed'])),
                  outer_deadlines=sum(s['outer_deadline'] for s in sessions),
                  rpc_seconds_after_fault=sum(b['rpc_seconds_after_fault'] for b in batches))
    for origin in ('actual', 'planned'):
        result['result_' + origin + '_wait_ms'] = distribution([v for s in success for v in s['result_' + origin + '_wait_samples_ms']])
    for key in ('resume_attempts', 'proxy_arrivals', 'parsed_resumes', 'backend_requests', 'repeated_transfer_bytes',
                'captured_bytes', 'worker_bytes', 'processed_bytes', 'missing_captured_bytes',
                'unprocessed_captured_bytes', 'uncaptured_bytes'):
        result[key] = sum(s[key] for s in sessions)
        if key in ('resume_attempts', 'proxy_arrivals', 'parsed_resumes', 'backend_requests'):
            result[key + '_per_planned_session'] = result[key] / len(sessions)
    for key in ('ready_after_fault_ms', 'ready_after_release_ms', 'max_partial_gap_ms'):
        result[key] = distribution([s[key] for s in success if s[key] is not None])
    for anchor in ('fault', 'release'):
        result['fresh_after_' + anchor + '_ms'] = distribution([s['fresh_after_' + anchor]['after_anchor_ms']
                                                             for s in success if s['fresh_after_' + anchor]])
    for key in ('return_after_fault_ms', 'rpc_after_fault_ms', 'rpc_after_return_ms', 'max_capture_lag_ms'):
        result[key] = distribution([s[key] for s in sessions if s[key] is not None])
    result['failed_return_after_fault_ms'] = distribution([s['return_after_fault_ms'] for s in sessions if not s['completed']])
    for key in ('natural_idle_after_fault_ms', 'natural_cleanup_ms', 'max_sample_gap_ms', 'max_sample_call_ms'):
        result[key] = distribution([b[key] for b in batches if b[key] is not None])
    return result


def difference(control, candidate, planned):
    return dict(completed=candidate['completed'] - control['completed'],
                completion_percentage_points=100 * (candidate['completed'] - control['completed']) / planned,
                rejection_codes={key: candidate['rejection_codes'].get(key,0) - control['rejection_codes'].get(key,0)
                                 for key in sorted(control['rejection_codes'].keys() | candidate['rejection_codes'].keys())},
                **{key: candidate[key] - control[key] for key in ('resume_attempts', 'proxy_arrivals', 'parsed_resumes',
                   'backend_requests', 'rpc_seconds_after_fault', 'repeated_transfer_bytes')})


def write_csv(path, rows, fields):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fields)
        writer.writeheader(); writer.writerows(rows)


def summarize(root, output):
    root, output = Path(root), Path(output)
    m = json.loads((root / 'manifest.json').read_text())
    mode, rounds = m['mode'], m['planned_rounds']
    require(mode in ROOT_TEST and type(rounds) is int and 1 <= rounds <= 3, 'cannot interpret budget denominator')
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
        require(any(e.get('Action') == 'pass' and e.get('Test') == ROOT_TEST[mode] for e in events), 'budget root test did not pass')
        events_valid = True
    except (ValueError, OSError, KeyError, TypeError) as exc:
        errors.append('events: ' + str(exc))
    expected = [(n + 1, case) for n in range(rounds) for case in order(3)[n*4:n*4+4]]
    slots = Counter((r.get('round'), r.get('config', {}).get('case')) for r in reports)
    actual = [(r.get('round'), r.get('config', {}).get('case')) for r in reports]
    ordered = actual == expected
    if not ordered:
        errors.append('budget reports missing, duplicated, unexpected, or out of order')
    previous_idle = None
    for i, r in enumerate(reports):
        try:
            row = verify_budget_report(r, mode)
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
        control, candidate = [next(g for g in groups if g['case'] == prefix + '_a' + str(a)) for a in (5,8)]
        valid = control['all_batches_verified'] and candidate['all_batches_verified']
        comparisons.append(dict(scenario=prefix, planned_per_budget=8*rounds, control=control['case'], candidate=candidate['case'],
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
                    completion_rate_bounds='verified complete/planned to (verified complete+unknown)/planned; unavailable for corrupt archive'))
    (output / 'reports.json').write_text(json.dumps(reports, separators=(',', ':')) + '\n')
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    fields = ['round', 'case', 'report_index', 'id', 'completed', 'run_error_class', 'last_entry_code', 'outer_deadline',
              'resume_attempts', 'proxy_arrivals', 'parsed_resumes', 'backend_requests', 'ready_after_fault_ms', 'ready_after_release_ms',
              'return_after_fault_ms', 'rpc_after_fault_ms', 'rpc_after_return_ms', 'remote_idle_after_return_ms',
              'captured_bytes', 'worker_bytes', 'processed_bytes', 'missing_captured_bytes', 'unprocessed_captured_bytes',
              'uncaptured_bytes', 'repeated_transfer_bytes', 'max_capture_lag_ms', 'max_partial_gap_ms']
    rows = [dict(round=b['round'], case=b['case'], report_index=b['report_index'], **{k:s[k] for k in fields[3:]}) for b in verified for s in b['sessions']]
    write_csv(output / 'sessions.csv', rows, fields)
    write_csv(output / 'attempt-bins.csv', [dict(round=b['round'],case=b['case'],report_index=b['report_index'],**x)
              for b in verified for x in b['attempt_bins']], ['round','case','report_index','begin_ns','arrivals','first','retries'])
    paths = ['reports.json', 'summary.json', 'sessions.csv', 'attempt-bins.csv']
    scripts = ['summarize_group_resume_budget.py', 'group_resume_budget_contract.py', 'summarize_group_resume.py', 'summarize_client_resume.py']
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
