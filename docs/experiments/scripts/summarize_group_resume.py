#!/usr/bin/env python3
"""EXP-006-02：独立复核字节/位置/事件，保留失败及固定分母，重新计算群体恢复指标。"""
import argparse
import base64
from collections import Counter, defaultdict
import csv
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import statistics

from summarize_client_resume import digest, pattern, require, stamp_ns

FORMAL = ['A_normal', 'B_resume_h8', 'C_resume_h2', 'D_busy_500ms', 'E_busy_2s']
PROBE = ['A_normal', 'B_resume_h8', 'C_resume_h2', 'D_busy_short', 'E_attempt_exhausted']
ROOT_TEST = {'formal': 'TestGroupResumeBaselineExperiment', 'probe': 'TestGroupResumeSharedGatewayProbe'}
MARKER = {'formal': 'GROUP_RESUME_JSON ', 'probe': 'GROUP_RESUME_PROBE_JSON '}
BIN_NS = 50_000_000


def configured(case, mode):
    """独立固定 Go 条件，不只相信组名；probe 不能被当作正式样本。"""
    order = FORMAL if mode == 'formal' else PROBE
    index = order.index(case)
    formal = mode == 'formal'
    return dict(case=case, clients=8, max_handshakes=8 if index == 1 else 2,
                chunks=60 if formal else 8, chunk_bytes=3200, barrier_chunk=20 if formal else 3,
                fault=index != 0, busy_ns=([0, 0, 0, 500_000_000, 2_000_000_000] if formal else
                                         [0, 0, 0, 40_000_000, 200_000_000])[index],
                processing_ns=10_000_000 if formal else 1_000_000,
                server_window_ns=10_000_000_000 if formal else 1_000_000_000,
                run_timeout_ns=15_000_000_000 if formal else 5_000_000_000,
                cleanup_timeout_ns=15_000_000_000 if formal else 2_000_000_000,
                sample_period_ns=10_000_000 if formal else 2_000_000,
                sync_limit_ns=50_000_000 if formal else 500_000_000,
                resume=dict(MaxAttempts=2 if not formal and index == 4 else 5,
                            Window=5_000_000_000 if formal else 2_000_000_000,
                            InitialBackoff=100_000_000 if formal else 20_000_000,
                            MaxBackoff=1_000_000_000 if formal else 80_000_000))


@lru_cache(maxsize=16)
def group_pattern(identity, size):
    data = bytearray(x ^ ((identity * 37) & 255) for x in pattern(size))
    data[:8] = identity.to_bytes(8, 'little')
    return bytes(data)


def extract(events, mode):
    """按 Test 重组碎片，允许不同测试日志交错；只解析本模式的报告。"""
    buffers = defaultdict(str)
    for event in events:
        if event.get('Action') == 'output':
            text = event.get('Output', '')
            require('resume_token' not in text and 'session_id' not in text, 'credentials in output')
            buffers[event.get('Test', '')] += text
    require(all('resume_token' not in value and 'session_id' not in value for value in buffers.values()), 'credentials in output')
    decoder = json.JSONDecoder()
    reports = []
    for test, text in buffers.items():
        cursor = 0
        while True:
            index = text.find(MARKER[mode], cursor)
            if index < 0:
                break
            report, size = decoder.raw_decode(text[index + len(MARKER[mode]):])
            require(test.startswith(ROOT_TEST[mode] + '/'), 'report belongs to another test')
            reports.append(report)
            cursor = index + len(MARKER[mode]) + size
    return sorted(reports, key=lambda r: stamp_ns(r['started_at']))


def duration(end, begin):
    return None if end is None or begin is None else (stamp_ns(end) - stamp_ns(begin)) / 1e6


def distribution(values):
    return None if not values else dict(n=len(values), min=min(values), median=statistics.median(values), max=max(values))


def verify_settings(r, mode):
    cfg = configured(r['config']['case'], mode)
    require(r['version'] == 2 and r['config'] == cfg, 'report version or configured conditions differ')
    env = r['environment']
    require(env['transport'] == 'loopback_tcp' and env['topology'] == 'same_process', 'topology differs')
    require(isinstance(env['go_version'], str) and env['go_version'].startswith('go') and
            env['goos'] and env['goarch'] and type(env['cpus']) is int and env['cpus'] > 0 and
            type(env['gomaxprocs']) is int and env['gomaxprocs'] > 0 and type(env['race']) is bool, 'environment missing')
    require(mode != 'formal' or not env['race'], 'race data cannot be formal measurements')
    gateway = dict(MaxSessions=8, MaxMessageBytes=1048576, StartTimeout=10_000_000_000, InputIdleTimeout=30_000_000_000,
                   WorkerSendTimeout=2_000_000_000, TailTimeout=15_000_000_000, ResultWriteTimeout=2_000_000_000,
                   MaxPendingAudioBytes=0, V2=dict(MaxHandshakes=cfg['max_handshakes'], EntryTimeout=10_000_000_000,
                   InputProgressTimeout=30_000_000_000, ResumeWindow=cfg['server_window_ns'],
                   ResultRetentionTimeout=cfg['server_window_ns'], WorkerStatusTimeout=2_000_000_000,
                   MaxAudioBytes=1048576, MaxAudioChunks=256, MaxResultBytes=1048576, MaxResults=256))
    require(r['gateway_config'] == gateway, 'effective Gateway settings differ')
    return cfg


def verify_session(session, cfg, r):
    identity, s = session['id'], session['observation']
    chunk, total = cfg['chunk_bytes'], cfg['chunks'] * cfg['chunk_bytes']
    data = group_pattern(identity, total)
    expected_client = dict(chunk_bytes=chunk, realtime=True, read_limit=65536, max_replay_bytes=320000, max_replay_chunks=128,
                           handshake_timeout_ns=10_000_000_000, write_timeout_ns=5_000_000_000,
                           apply_timeout_ns=5_000_000_000, completion_timeout_ns=30_000_000_000, resume=cfg['resume'])
    require(s['client_config'] == expected_client, 'effective client settings differ')
    require(session['started'] and not s['tool_errors'] and s['source_closes'] == 1, 'session ownership or tool errors')
    require(s['input_sha256'] == hashlib.sha256(data).hexdigest(), 'input hash differs')
    require(stamp_ns(s['returned_at']) >= stamp_ns(s['started_at']), 'return precedes start')
    reads = s['source_reads'] or []
    require(reads, 'no source reads')
    captured = 0
    nonempty = []
    for read in reads:
        require(read['begin'] == captured and read['end'] >= captured and read['end'] <= total, 'source range gap')
        require(stamp_ns(read['finished_at']) >= stamp_ns(read['started_at']), 'source timestamp reversed')
        if read['end'] > read['begin']:
            require(read['end'] - read['begin'] == chunk and not read['eof'], 'source chunk differs')
            nonempty.append(read)
        else:
            require(read['eof'] and captured == total, 'invalid source EOF')
        captured = read['end']
    actual = base64.b64decode(s['worker']['audio'] or '', validate=True)
    require(len(actual) <= captured and actual == data[:len(actual)], 'Worker byte order/content differs')
    worker = s['worker']
    require(worker['calls'] == 1 and stamp_ns(worker['exited_at']) >= stamp_ns(worker['started_at']), 'Worker lifecycle differs')
    require(len(worker['chunks'] or []) * chunk == len(actual), 'Worker chunk count differs')
    for i, item in enumerate(worker['chunks'] or []):
        require((item['begin'], item['end']) == (i * chunk, (i + 1) * chunk), 'Worker chunk range gap')
        times = [item[k] for k in ('received_at', 'processed_at', 'progress_sent_at', 'result_sent_at') if item[k] is not None]
        require(all(stamp_ns(b) >= stamp_ns(a) for a, b in zip(times, times[1:])), 'Worker timestamps reversed')
    frames = s['frames'] or []
    dropped = []
    union, end = 0, 0
    for f in sorted(frames, key=lambda x: (x['begin'], x['end'])):
        require(type(f['connection']) is int and 1 <= f['connection'] <= cfg['resume']['MaxAttempts'] + 1, 'frame connection differs')
        require(0 <= f['begin'] < f['end'] <= captured and f['end'] - f['begin'] == chunk, 'frame outside captured input')
        require(f['sha256'] == hashlib.sha256(data[f['begin']:f['end']]).hexdigest(), 'frame hash differs')
        if f['forwarded']:
            require(f['forward_started_at'] and f['forward_returned_at'], 'forward timestamps missing')
        if f['forward_started_at']:
            require(stamp_ns(f['forward_started_at']) >= stamp_ns(f['read_at']), 'forward precedes read')
        if f['forward_returned_at']:
            require(f['forward_started_at'] and stamp_ns(f['forward_returned_at']) >= stamp_ns(f['forward_started_at']), 'forward times reversed')
        if f['dropped']:
            dropped.append(f)
            require(not f['forwarded'] and f['forward_started_at'] is None and f['connection'] == 1 and
                    f['begin'] == (cfg['barrier_chunk'] - 1) * chunk, 'fault boundary differs')
        if f['end'] > end:
            union += f['end'] - max(end, f['begin'])
            end = f['end']
    require(len(dropped) == (1 if cfg['fault'] else 0), 'fault frame count differs')
    apps = s['applications'] or []
    require([a['seq'] for a in apps] == list(range(1, len(apps) + 1)), 'application duplicate/gap')
    actual_waits, planned_waits, lags, partial_at = [], [], [], []
    origin = stamp_ns(nonempty[0]['started_at'])
    for i, read in enumerate(nonempty):
        lags.append((stamp_ns(read['started_at']) - origin - i * 100_000_000) / 1e6)
    for i, a in enumerate(apps):
        require(a['audio_end'] == min(i + 1, cfg['chunks']) * chunk and a['final'] == (i == cfg['chunks']), 'application audio position differs')
        require(stamp_ns(a['applied_at']) >= stamp_ns(a['received_at']), 'application precedes receive')
        if i:
            require(stamp_ns(a['applied_at']) >= stamp_ns(apps[i - 1]['applied_at']), 'application time reversed')
        if not a['final']:
            require(i < len(worker['chunks']) and worker['chunks'][i]['processed_at'] is not None and
                    stamp_ns(a['received_at']) >= stamp_ns(worker['chunks'][i]['processed_at']), 'result lacks prior processing')
            require(i < len(nonempty) and stamp_ns(a['received_at']) >= stamp_ns(nonempty[i]['started_at']), 'result precedes capture')
            actual_waits.append(duration(a['applied_at'], nonempty[i]['started_at']))
            planned_waits.append((stamp_ns(a['applied_at']) - origin - i * 100_000_000) / 1e6)
            partial_at.append(stamp_ns(a['applied_at']))
    run = s['run_result']
    require(type(run['Completed']) is bool and type(run['CompletionAckWritten']) is bool, 'completion facts must be boolean')
    require(run['AppliedSeq'] == len(apps), 'application count differs from RunResult')
    require(type(run['ResumeAttempts']) is int and 0 <= run['ResumeAttempts'] <= cfg['resume']['MaxAttempts'], 'attempt budget exceeded')
    require(0 <= run['ResumedConnections'] <= run['ResumeAttempts'], 'accepted resume count differs')
    attempts = [a for a in r['attempts'] if a['id'] == identity]
    require(sum(a['kind'] == 'start' for a in attempts) == 1 and all(a['kind'] != 'start' for a in attempts if a['connection'] > 1), 'fallback start')
    require(len(attempts) - 1 <= run['ResumeAttempts'], 'more arrivals than Dial attempts')
    require(sum(a['kind'] == 'resume' for a in attempts) <= run['ResumeAttempts'], 'more parsed resumes than Dial attempts')
    ready_rows = s['readies'] or []
    require(ready_rows and ready_rows[0]['connection'] == 1 and ready_rows[0]['next_offset'] == 0, 'initial ready missing')
    require(all(b['generation'] > a['generation'] for a, b in zip(ready_rows, ready_rows[1:])), 'ready generation reversed')
    require(all(x['next_offset'] <= captured and x['acked_seq'] <= run['AppliedSeq'] for x in ready_rows), 'ready progress ahead')
    for ready_row in ready_rows:
        attempt = next((a for a in attempts if a['connection'] == ready_row['connection']), None)
        require(attempt is not None and attempt['backend_at'] and attempt['http_status'] == 101 and not attempt['code_written'], 'ready lacks successful backend upgrade')
    require(run['ResumedConnections'] <= sum(x['connection'] > 1 for x in ready_rows), 'accepted resume lacks ready observation')
    if run['Completed']:
        require(actual == data and captured == total and len(apps) == cfg['chunks'] + 1 and
                run['FinalOffset'] == total and worker['tail_sent_at'], 'false completed')
    elif not s['run_error_class']:
        raise ValueError('incomplete run has no failure')
    if not cfg['fault']:
        require(run['ResumeAttempts'] == 0, 'normal session resumed')
    partials = [a for a in apps if not a['final']]
    def fresh(anchor):
        if anchor is None:
            return None
        read = next((x for x in nonempty if stamp_ns(x['started_at']) > stamp_ns(anchor)), None)
        app = next((a for a in partials if read and a['audio_end'] >= read['end']), None)
        return None if app is None else dict(audio_end=read['end'], capture_at=read['started_at'], applied_at=app['applied_at'],
                                             after_anchor_ms=duration(app['applied_at'], anchor), actual_wait_ms=duration(app['applied_at'], read['started_at']))
    ready = next((x['written_at'] for x in ready_rows if x['connection'] > 1), None)
    return dict(id=identity, completed=run['Completed'], run_error_class=s['run_error_class'], last_entry_code=session['last_entry_code'],
                outer_deadline=s['outer_deadline'], captured_bytes=captured, worker_bytes=len(actual),
                processed_bytes=max([0] + [x['end'] for x in worker['chunks'] if x['processed_at']]),
                missing_captured_bytes=captured - len(actual),
                unprocessed_captured_bytes=captured - max([0] + [x['end'] for x in worker['chunks'] if x['processed_at']]),
                uncaptured_bytes=total - captured,
                repeated_transfer_bytes=sum(f['end'] - f['begin'] for f in frames) - union,
                applied_results=len(apps), resume_attempts=run['ResumeAttempts'], resumed_connections=run['ResumedConnections'],
                proxy_arrivals=len(attempts) - 1, parsed_resumes=sum(a['kind'] == 'resume' for a in attempts),
                backend_requests=sum(a['backend_at'] is not None for a in attempts if a['connection'] > 1),
                ready_after_fault_ms=duration(ready, r['fault_at']), ready_after_release_ms=duration(ready, r['released_at']),
                fresh_after_fault=fresh(r['fault_at']), fresh_after_release=fresh(r['released_at']),
                result_actual_wait_ms=distribution(actual_waits), result_planned_wait_ms=distribution(planned_waits),
                max_capture_lag_ms=max([0] + lags), max_partial_gap_ms=max([(b - a) / 1e6 for a, b in zip(partial_at, partial_at[1:])], default=None),
                return_after_fault_ms=duration(s['returned_at'], r['fault_at']),
                remote_idle_after_return_ms=duration(r['natural_idle_at'], s['returned_at']))


def verify_report(r, mode='formal'):
    cfg = verify_settings(r, mode)
    require(not r['tool_errors'] and not r['evidence_errors'] and not r['forced_cleanup'], 'report has invalid evidence or forced cleanup')
    require(r['picks'] == r['worker_calls'] == cfg['clients'], 'replacement RPC or Pick')
    require(len(r['sessions']) == cfg['clients'] and sorted(s['id'] for s in r['sessions']) == list(range(1, cfg['clients'] + 1)), 'planned session denominator or identity differs')
    require(all(len(r[k]) == cfg['clients'] for k in ('barrier_at', 'close_started_at', 'close_finished_at')), 'observation slots differ')
    require(stamp_ns(r['returned_at']) >= stamp_ns(r['started_at']) and stamp_ns(r['natural_idle_at']) >= stamp_ns(r['returned_at']), 'batch cleanup time reversed')
    barrier = [stamp_ns(x) for x in r['barrier_at']]
    require(all(t >= stamp_ns(r['started_at']) for t in barrier), 'barrier precedes start')
    if cfg['fault']:
        cut = stamp_ns(r['fault_at'])
        require(cut >= max(barrier), 'fault precedes full barrier')
        begins, ends = [stamp_ns(x) for x in r['close_started_at']], [stamp_ns(x) for x in r['close_finished_at']]
        require(all(e >= b >= cut for b, e in zip(begins, ends)), 'cut ordering differs')
        require(max(ends) - min(begins) <= cfg['sync_limit_ns'], 'cut spread exceeded')
    else:
        require(r['fault_at'] is None and all(x is None for x in r['close_started_at'] + r['close_finished_at']), 'unexpected fault')
    if cfg['busy_ns']:
        require(stamp_ns(r['occupied_at']) <= stamp_ns(r['fault_at']) and
                stamp_ns(r['released_at']) >= stamp_ns(r['fault_at']) + cfg['busy_ns'], 'occupancy timing differs')
        require(any(a['code'] == 'handshake_limit' and a['http_status'] == 503 and a['code_written'] for a in r['attempts']), 'real handshake rejection missing')
    else:
        require(r['occupied_at'] is None and r['released_at'] is None, 'unexpected occupancy')
    pairs = set()
    for a in r['attempts']:
        pair = (a['id'], a['connection'])
        require(pair not in pairs and 1 <= a['id'] <= cfg['clients'] and 1 <= a['connection'] <= cfg['resume']['MaxAttempts'] + 1, 'duplicate or invalid attempt')
        pairs.add(pair)
        require(stamp_ns(a['exited_at']) >= stamp_ns(a['arrived_at']), 'attempt did not exit')
        if a['backend_at']:
            require(stamp_ns(a['exited_at']) >= stamp_ns(a['backend_at']) >= stamp_ns(a['arrived_at']), 'backend request ordering')
        if a['code_written']:
            require(a['code'] and a['backend_at'], 'fabricated entry rejection')
    sessions = [verify_session(s, cfg, r) for s in sorted(r['sessions'], key=lambda x: x['id'])]
    origins = [stamp_ns(s['observation']['source_reads'][0]['started_at']) for s in r['sessions']]
    require(max(origins) - min(origins) <= cfg['sync_limit_ns'], 'startup spread exceeded')
    samples = r['samples']
    require(samples, 'resource samples missing')
    core = ['sessions', 'handshakes', 'registry', 'worker_active', 'gateway_handlers', 'proxy_handlers', 'fault_timers']
    for s in samples:
        require(stamp_ns(s['at']) >= stamp_ns(s['begin_at']) and all(type(s[k]) is int and s[k] >= 0 for k in core), 'invalid resource sample')
        require(s['sessions'] <= cfg['clients'] and s['registry'] <= cfg['clients'] and s['worker_active'] <= cfg['clients'] and s['handshakes'] <= cfg['max_handshakes'], 'sample admission bound exceeded')
        require(type(s['goroutines']) is int and s['goroutines'] > 0 and type(s['heap_alloc_bytes']) is int and s['heap_alloc_bytes'] > 0 and type(s['heap_inuse_bytes']) is int and s['heap_inuse_bytes'] > 0, 'runtime resource observation missing')
    require(all(stamp_ns(b['at']) >= stamp_ns(a['at']) for a, b in zip(samples, samples[1:])), 'sample timestamp reversed')
    idle = next((s['at'] for s in samples if stamp_ns(s['begin_at']) >= stamp_ns(r['returned_at']) and all(s[k] == 0 for k in core)), None)
    require(idle and stamp_ns(idle) == stamp_ns(r['natural_idle_at']), 'natural cleanup lacks zero-count observation')
    if cfg['busy_ns']:
        require(any(s['sessions'] == cfg['clients'] and s['handshakes'] == cfg['max_handshakes'] and
                    stamp_ns(s['at']) >= stamp_ns(r['occupied_at']) and stamp_ns(s['at']) < stamp_ns(r['released_at']) for s in samples), 'actual occupancy sample missing')
    arrivals = [a for a in r['attempts'] if a['connection'] > 1]
    bins = []
    if cfg['fault']:
        counts = Counter((stamp_ns(a['arrived_at']) - stamp_ns(r['fault_at'])) // BIN_NS for a in arrivals)
        require(not counts or min(counts) >= 0, 'recovery precedes fault')
        # 保存 T0 到整批客户端返回的完整观察区间，包括最后一次请求之后的空桶。
        last = max(max(counts, default=0), (stamp_ns(r['returned_at']) - stamp_ns(r['fault_at'])) // BIN_NS)
        require(last <= cfg['run_timeout_ns'] // BIN_NS + 1, 'arrival outside observation')
        first = Counter((stamp_ns(a['arrived_at']) - stamp_ns(r['fault_at'])) // BIN_NS for a in arrivals if a['connection'] == 2)
        bins = [dict(begin_ns=i * BIN_NS, arrivals=counts[i], first=first[i], retries=counts[i] - first[i]) for i in range(last + 1)]
    codes = Counter(a['code'] for a in arrivals if a['code'])
    return dict(case=cfg['case'], environment=r['environment'], planned=cfg['clients'], completed=sum(s['completed'] for s in sessions),
                failed=sum(not s['completed'] for s in sessions), sessions=sessions, attempt_bins=bins, rejection_codes=dict(codes),
                resume_attempts=sum(s['resume_attempts'] for s in sessions), recovery_arrivals=len(arrivals),
                parsed_resumes=sum(s['parsed_resumes'] for s in sessions), backend_requests=sum(s['backend_requests'] for s in sessions),
                startup_spread_ms=(max(origins) - min(origins)) / 1e6,
                barrier_spread_ms=(max(barrier) - min(barrier)) / 1e6,
                cut_spread_ms=(max(ends) - min(begins)) / 1e6 if cfg['fault'] else None,
                occupancy_after_fault_ms=duration(r['released_at'], r['fault_at']),
                resource_samples=len(samples), max_sample_gap_ms=max((duration(b['at'], a['at']) for a, b in zip(samples, samples[1:])), default=None),
                max_sample_call_ms=max(duration(s['at'], s['begin_at']) for s in samples),
                sampled_peak={k: max(s[k] for s in samples) for k in core + ['goroutines', 'heap_alloc_bytes', 'heap_inuse_bytes']},
                natural_cleanup_ms=duration(r['natural_idle_at'], r['returned_at']))


def verify_artifacts(root, manifest):
    require(manifest['version'] == 1 and manifest['mode'] in ('formal', 'probe') and
            manifest['planned_rounds'] in (1, 2, 3) and (manifest['mode'] != 'formal' or manifest['planned_rounds'] == 3), 'invalid collection manifest')
    require(manifest['order'] == (FORMAL if manifest['mode'] == 'formal' else PROBE) and
            manifest['planned_sessions'] == 8 * 5 * manifest['planned_rounds'], 'manifest denominator differs')
    required = {'preregistration.md', 'build.log', 'go-events.jsonl', 'go-stderr.log', 'group-resume.test', 'git-status.txt'}
    require(required <= manifest['artifacts_sha256'].keys() and manifest['source_sha256'], 'required artifacts or source snapshot missing')
    for name, value in manifest['artifacts_sha256'].items():
        path = Path(name)
        require(not path.is_absolute() and '..' not in path.parts and digest(root / path) == value, 'artifact hash differs: ' + name)
    for name, value in manifest['source_sha256'].items():
        path = Path(name)
        require(not path.is_absolute() and '..' not in path.parts and digest(root / 'source' / path) == value, 'source snapshot hash differs')
    require(manifest['status'] == 'collected' and manifest['build_exit'] == 0 and manifest['run_exit'] == 0 and manifest['source_unchanged'], 'collection failed or source changed')
    if manifest['mode'] == 'formal' and manifest.get('host', {}).get('system') == 'Darwin':
        require(manifest.get('awake_guard') == 'owned_caffeinate' and manifest.get('awake_guard_exit_before_cleanup') is None, 'wake guard exited during collection')


def summarize(root, output):
    """输出目录独占创建，错误分析也保存固定分母；无效样本不提供时延结论。"""
    root, output = Path(root), Path(output)
    output.mkdir(parents=True, exist_ok=False)
    errors, reports, verified = [], [], []
    manifest = json.loads((root / 'manifest.json').read_text())
    try:
        verify_artifacts(root, manifest)
    except (ValueError, OSError, KeyError) as exc:
        errors.append(str(exc))
    archive_valid = not errors
    mode, rounds = manifest['mode'], manifest['planned_rounds']
    require(mode in ('formal', 'probe') and type(rounds) is int and 1 <= rounds <= 3, 'cannot interpret manifest denominator')
    order = FORMAL if mode == 'formal' else PROBE
    try:
        events = [json.loads(line) for line in (root / 'go-events.jsonl').read_text().splitlines()]
        reports = extract(events, mode)
        require(any(e.get('Action') == 'pass' and e.get('Test') == ROOT_TEST[mode] for e in events), 'root test did not pass')
    except (ValueError, OSError, KeyError) as exc:
        errors.append('event extraction: ' + str(exc))
    counts = Counter(r.get('config', {}).get('case') for r in reports)
    if counts != Counter({case: rounds for case in order}):
        errors.append('reports missing, duplicated, or unexpected: ' + str(dict(counts)))
    if [r.get('config', {}).get('case') for r in reports] != order * rounds:
        errors.append('collection order differs from fixed A/B/C/D/E rounds')
    for i, report in enumerate(reports):
        try:
            row = verify_report(report, mode)
            env = row['environment']
            require(env['go_version'] == manifest['go_env']['GOVERSION'] and env['goos'] == manifest['go_env']['GOOS'] and env['goarch'] == manifest['go_env']['GOARCH'], 'build/run environment differs')
            row['report_index'] = i
            verified.append(row)
        except (ValueError, KeyError, TypeError, IndexError) as exc:
            errors.append(f'report {i}: {exc}')
    groups = []
    for case in order:
        batches = [r for r in verified if r['case'] == case]
        valid = archive_valid and len(batches) == rounds and counts[case] == rounds
        sessions = [s for b in batches for s in b['sessions']]
        success = [s for s in sessions if s['completed']]
        completed = sum(s['completed'] for s in sessions)
        groups.append(dict(case=case, planned_sessions=8 * rounds, collected_batches=counts[case], verified_batches=len(batches),
                           all_batches_verified=valid, verified_completed=completed, verified_failed=len(sessions) - completed,
                           unknown_sessions=max(0, 8 * rounds - len(sessions)), completion_rate=completed / (8 * rounds) if valid else None,
                           completed_latency_population=len(success),
                           ready_after_fault_ms=distribution([s['ready_after_fault_ms'] for s in success if s['ready_after_fault_ms'] is not None]) if valid else None,
                           fresh_after_fault_ms=distribution([s['fresh_after_fault']['after_anchor_ms'] for s in success if s['fresh_after_fault']]) if valid else None,
                           max_partial_gap_ms=distribution([s['max_partial_gap_ms'] for s in success if s['max_partial_gap_ms'] is not None]) if valid else None,
                           rejection_codes=dict(sum((Counter(b['rejection_codes']) for b in batches), Counter()))))
    result = dict(version=1, mode=mode, evidence_valid=not errors, errors=errors, planned_batches=5 * rounds,
                  planned_sessions=40 * rounds, collected_batches=len(reports), verified_batches=len(verified),
                  groups=groups, batches=verified, scope='same-process loopback, controlled shared-gateway occupancy; probe is not formal performance data',
                  metric_definitions=dict(missing_captured_bytes='captured minus Worker received; not the processing gap',
                     unprocessed_captured_bytes='captured minus contiguous Worker processed position',
                     result_actual_wait_ms='source Read start to application, each observed partial',
                     result_planned_wait_ms='source first Read plus chunk index*100ms to application',
                     fresh_after_fault='first nonempty Read starting strictly after T0, then its partial application',
                     ready_after_fault_ms='proxy ready written, not client acceptance',
                     attempt_bins='50ms T0 bins through all clients returned, including empty bins; first means connection 2',
                     sampled_peak='non-atomic whole-process/runtime and component observations, not strict bounds'))
    (output / 'reports.json').write_text(json.dumps(reports, separators=(',', ':')) + '\n')
    (output / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
    session_rows = [dict(case=b['case'], report_index=b['report_index'], **{k: s[k] for k in ('id', 'completed', 'run_error_class', 'resume_attempts', 'ready_after_fault_ms', 'captured_bytes', 'worker_bytes', 'missing_captured_bytes', 'unprocessed_captured_bytes', 'uncaptured_bytes', 'max_capture_lag_ms', 'max_partial_gap_ms', 'remote_idle_after_return_ms')}) for b in verified for s in b['sessions']]
    with (output / 'sessions.csv').open('w', newline='') as stream:
        fields = list(session_rows[0]) if session_rows else ['case', 'report_index', 'id']
        writer = csv.DictWriter(stream, fields); writer.writeheader(); writer.writerows(session_rows)
    with (output / 'attempt-bins.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, ['case', 'report_index', 'begin_ns', 'arrivals', 'first', 'retries']); writer.writeheader()
        writer.writerows(dict(case=b['case'], report_index=b['report_index'], **x) for b in verified for x in b['attempt_bins'])
    paths = ['reports.json', 'summary.json', 'sessions.csv', 'attempt-bins.csv']
    audit = dict(version=1, source_sha256=digest(__file__), input_sha256={name: digest(root / name) for name in ('manifest.json', 'go-events.jsonl') if (root / name).exists()},
                 output_sha256={name: digest(output / name) for name in paths})
    (output / 'analysis-manifest.json').write_text(json.dumps(audit, indent=2) + '\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = summarize(args.directory, args.output)
    print(json.dumps({k: result[k] for k in ('evidence_valid', 'planned_sessions', 'collected_batches', 'verified_batches', 'errors')}))
    raise SystemExit(0 if result['evidence_valid'] else 1)
