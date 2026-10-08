#!/usr/bin/env python3
"""复核 EXP-006-01 原始事实与派生指标；失败保留在固定分母，不用通过计数代替业务完成。"""
import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import re

ORDER = ['A_normal', 'B_disabled_500ms', 'C_resume_500ms', 'D_resume_2s', 'E_over_window_6s']
ROOT_TEST = 'TestClientResumeBaselineExperiment'
MARKER = 'CLIENT_RESUME_JSON '
# Go Time.Sub 使用单调时钟，JSON 丢弃此部分。1ms 仅为双时钟证据一致性上界，
# 不是放宽 200ms 追赶判据；所有实际差值保留，超界不得继续解释。
CLOCK_AGREEMENT_MS = 1.0
STAMP = re.compile(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stamp_ns(value):
    """精确保留 Go RFC3339Nano 的纳秒，避免 datetime 的微秒截断改变短延迟。"""
    require(isinstance(value, str), 'missing timestamp')
    match = STAMP.fullmatch(value)
    require(match is not None, 'invalid RFC3339Nano timestamp')
    whole, fraction, zone = match.groups()
    dt = datetime.fromisoformat(whole + ('+00:00' if zone == 'Z' else zone))
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 10**9 + int((fraction or '').ljust(9, '0'))


def ms(end, begin):
    return None if end is None or begin is None else (stamp_ns(end) - stamp_ns(begin)) / 10**6


@lru_cache(maxsize=2)
def pattern(size):
    """复现实验固定种子 xorshift32 输入；逐字节核对，不只相信 Go 的 WorkerExact。"""
    state = 0x1ac3e591
    data = bytearray()
    for _ in range(size):
        state ^= (state << 13) & 0xffffffff
        state ^= state >> 17
        state ^= (state << 5) & 0xffffffff
        data.append(state & 255)
    return bytes(data)


def extract(events):
    """test2json 会将长日志拆成约 1024 字节事件，先拼接输出再解析 JSON 标记。"""
    text = ''.join(e.get('Output', '') for e in events if e.get('Action') == 'output')
    require('resume_token' not in text and 'session_id' not in text, 'credentials in log')
    decoder = json.JSONDecoder()
    reports, cursor = [], 0
    while True:
        offset = text.find(MARKER, cursor)
        if offset < 0:
            break
        report, length = decoder.raw_decode(text[offset + len(MARKER):])
        reports.append(report)
        cursor = offset + len(MARKER) + length
    return reports


def formal_config(case):
    index = ORDER.index(case)
    return dict(case=case, chunks=60, chunk_bytes=3200, cut_chunk=0 if index == 0 else 20,
                resume=index >= 2, outage_ns=[0, 500_000_000, 500_000_000, 2_000_000_000, 6_000_000_000][index],
                processing_ns=10_000_000, client_window_ns=5_000_000_000, server_window_ns=10_000_000_000,
                run_timeout_ns=15_000_000_000, cleanup_timeout_ns=15_000_000_000,
                sample_period_ns=10_000_000, catchup_threshold_ns=200_000_000)


def verify_settings(r):
    """核对每场真实客户端和 Gateway 配置，没有只检查场景名称。"""
    require(r['version'] == 1 and r['config'] == formal_config(r['config']['case']), 'formal conditions differ')
    client = dict(chunk_bytes=3200, realtime=True, read_limit=65536, max_replay_bytes=320000, max_replay_chunks=128,
                  handshake_timeout_ns=10_000_000_000, write_timeout_ns=5_000_000_000,
                  apply_timeout_ns=5_000_000_000, completion_timeout_ns=30_000_000_000,
                  resume=dict(MaxAttempts=5, Window=5_000_000_000, InitialBackoff=100_000_000, MaxBackoff=1_000_000_000)
                  if r['config']['resume'] else None)
    require(r['client_config'] == client, 'effective client settings differ')
    gateway = dict(MaxSessions=1, MaxMessageBytes=1048576, StartTimeout=10_000_000_000, InputIdleTimeout=30_000_000_000,
                   WorkerSendTimeout=2_000_000_000, TailTimeout=15_000_000_000, ResultWriteTimeout=2_000_000_000,
                   MaxPendingAudioBytes=0, V2=dict(MaxHandshakes=4, EntryTimeout=10_000_000_000,
                   InputProgressTimeout=30_000_000_000, ResumeWindow=10_000_000_000, ResultRetentionTimeout=10_000_000_000,
                   WorkerStatusTimeout=2_000_000_000, MaxAudioBytes=1048576, MaxAudioChunks=256,
                   MaxResultBytes=1048576, MaxResults=256))
    require(r['gateway_config'] == gateway, 'effective Gateway settings differ')


def verify_report(r):
    """从源、范围、实际 Worker 字节和结果/资源时间重新计算，不直接汇总已填指标。"""
    verify_settings(r)
    cfg, saved = r['config'], r['metrics']
    clock_differences = []
    def check_duration(name, observed, wall):
        require(type(observed) in (int, float) and math.isfinite(observed), 'invalid duration: ' + name)
        delta = observed - wall
        require(abs(delta) <= CLOCK_AGREEMENT_MS, 'monotonic/wall disagreement exceeds 1ms: ' + name)
        clock_differences.append(dict(metric=name, monotonic_ms=observed, wall_ms=wall, difference_ms=delta))
    total, chunk = cfg['chunks'] * cfg['chunk_bytes'], cfg['chunk_bytes']
    data = pattern(total)
    require(r['input_sha256'] == hashlib.sha256(data).hexdigest(), 'input hash differs')
    require(not r['tool_errors'] and not r['evidence_errors'], 'report has invalid evidence')
    require(r['source_closes'] == 1 and r['worker_picks'] == r['worker']['calls'] == 1, 'ownership or Worker count differs')
    require(stamp_ns(r['returned_at']) >= stamp_ns(r['started_at']), 'return precedes start')
    reads = [x for x in r['source_reads'] if x['end'] > x['begin']]
    captured, fresh = 0, None
    for read in reads:
        require(read['begin'] == captured and read['end'] - read['begin'] == chunk and read['end'] <= total, 'source range gap')
        require(stamp_ns(read['finished_at']) >= stamp_ns(read['started_at']), 'source time reversed')
        captured = read['end']
        if fresh is None and r['cut_at'] is not None and stamp_ns(read['started_at']) > stamp_ns(r['cut_at']):
            fresh = captured
    require(reads, 'no source capture')
    origin = stamp_ns(reads[0]['started_at'])
    actual = base64.b64decode(r['worker']['audio'], validate=True)
    require(len(actual) <= captured and actual == data[:len(actual)], 'Worker byte order/content differs')
    require(len(r['worker']['chunks']) * chunk == len(actual), 'Worker chunk count differs')
    require(stamp_ns(r['worker']['exited_at']) >= stamp_ns(r['worker']['started_at']), 'Worker exit missing')
    for i, item in enumerate(r['worker']['chunks']):
        require((item['begin'], item['end']) == (i * chunk, (i + 1) * chunk), 'Worker range gap')
        times = [item[k] for k in ['received_at', 'processed_at', 'progress_sent_at', 'result_sent_at'] if item[k] is not None]
        require(all(stamp_ns(b) >= stamp_ns(a) for a, b in zip(times, times[1:])), 'Worker processing order differs')
    frames = r['frames']
    for f in frames:
        require(0 <= f['begin'] < f['end'] <= captured, 'frame outside captured range')
        require(f['sha256'] == hashlib.sha256(data[f['begin']:f['end']]).hexdigest(), 'frame hash differs')
        if f['forwarded']:
            require(f['forward_started_at'] and f['forward_returned_at'], 'forward timestamps missing')
        if f['forward_started_at'] is not None:
            require(stamp_ns(f['forward_started_at']) >= stamp_ns(f['read_at']), 'forward before read')
        if f['forward_returned_at'] is not None:
            require(stamp_ns(f['forward_returned_at']) >= stamp_ns(f['forward_started_at']), 'forward time reversed')
    transmitted = sum(f['end'] - f['begin'] for f in frames)
    union, end = 0, 0
    for f in sorted(frames, key=lambda f: f['begin']):
        if f['end'] > end:
            union += f['end'] - max(end, f['begin'])
            end = f['end']
    dropped = [f for f in frames if f['dropped']]
    if cfg['cut_chunk']:
        require(len(dropped) == 1 and dropped[0]['begin'] == 60800 and dropped[0]['end'] == 64000
                and dropped[0]['connection'] == 1 and not dropped[0]['forwarded']
                and dropped[0]['forward_started_at'] is None, 'fault boundary differs')
        require(r['cut_at'] and r['opened_at'] and stamp_ns(r['opened_at']) >= stamp_ns(r['cut_at']) + cfg['outage_ns'], 'fault duration shorter than configured')
    else:
        require(not dropped and r['cut_at'] is None and r['opened_at'] is None, 'unexpected control fault')
    require(sum(h['kind'] == 'start' for h in r['handshakes']) == 1, 'session restarted')
    require(sum(h['kind'] == 'resume' for h in r['handshakes']) <= r['run_result']['ResumeAttempts'], 'resume count differs')
    seqs = [a['seq'] for a in r['applications']]
    require(seqs == list(range(1, len(seqs) + 1)), 'application duplicate/gap')
    partials, final = [a for a in r['applications'] if not a['final']], [a for a in r['applications'] if a['final']]
    for i, a in enumerate(r['applications']):
        require(stamp_ns(a['applied_at']) >= stamp_ns(a['received_at']), 'application before receive')
        if i:
            require(stamp_ns(a['received_at']) >= stamp_ns(r['applications'][i-1]['received_at']), 'result time reversed')
    waits = []
    for i, a in enumerate(partials):
        require(a['audio_end'] == (i + 1) * chunk and a['audio_end'] <= len(actual), 'partial audio position differs')
        require(stamp_ns(a['received_at']) >= stamp_ns(r['worker']['chunks'][i]['processed_at']), 'result before processing')
        planned = origin + i * 100_000_000
        waits.append((stamp_ns(a['received_at']) - planned) / 10**6)
        row = saved['waits'][i]
        require(row['seq'] == a['seq'] and row['audio_end'] == a['audio_end'] and stamp_ns(row['planned_at']) == planned
                and stamp_ns(row['received_at']) == stamp_ns(a['received_at']), 'saved result wait position differs')
        check_duration('result_wait_seq_' + str(a['seq']), row['wait_ms'], waits[-1])
        require((0 <= row['wait_ms'] <= 200) == (0 <= waits[-1] <= 200), 'clock difference changes catch-up classification')
    require(len(saved['waits']) == len(partials), 'result wait count differs')
    first_fresh = next((a['received_at'] for a in partials if fresh is not None and a['audio_end'] >= fresh), None)
    ready = next((x['written_at'] for x in r['readies'] if x['connection'] > 1), None)
    catchup, streak = None, 0
    for a, wait in zip(partials, waits):
        if r['opened_at'] is not None and stamp_ns(a['received_at']) >= stamp_ns(r['opened_at']):
            streak = streak + 1 if 0 <= wait <= 200 else 0
            if streak == 3 and catchup is None:
                catchup = ms(a['received_at'], r['opened_at'])
    samples = r['samples']
    require(samples, 'resource samples missing')
    counts = ['sessions', 'handshakes', 'registry', 'worker_active']
    tasks = ['gateway_handlers', 'proxy_handlers', 'fault_timers']
    for s in samples:
        require(stamp_ns(s['at']) >= stamp_ns(s['begin_at']) and all(type(s[k]) is int and s[k] >= 0 for k in counts + tasks), 'invalid resource sample')
    require(all(stamp_ns(b['at']) >= stamp_ns(a['at']) for a, b in zip(samples, samples[1:])), 'sample clock reversed')
    idle = next((s['at'] for s in samples if stamp_ns(s['begin_at']) >= stamp_ns(r['returned_at']) and all(s[k] == 0 for k in counts)), None)
    derived = dict(captured_bytes=captured, worker_bytes=len(actual), worker_prefix_bytes=len(actual), worker_exact=actual == data,
                   worker_is_prefix=True, missing_captured=dict(begin=len(actual), end=captured) if len(actual) < captured else None,
                   uncaptured=dict(begin=captured, end=total) if captured < total else None, repeated_transfer_bytes=transmitted-union,
                   duplicate_applications=len(seqs)-len(set(seqs)), fresh_audio_end=fresh,
                   ready_after_cut_ms=ms(ready, r['cut_at']), ready_after_open_ms=ms(ready, r['opened_at']),
                   fresh_after_cut_ms=ms(first_fresh, r['cut_at']), fresh_after_open_ms=ms(first_fresh, r['opened_at']),
                   catchup_after_open_ms=catchup,
                   max_partial_gap_ms=max((ms(b['received_at'], a['received_at']) for a, b in zip(partials, partials[1:])), default=None),
                   max_sample_gap_ms=max((ms(b['at'], a['at']) for a, b in zip(samples, samples[1:])), default=None),
                   max_sample_call_ms=max(ms(s['at'], s['begin_at']) for s in samples), natural_idle_at=idle,
                   cleanup_wait_ms=ms(idle, r['returned_at']))
    for k, value in derived.items():
        got = saved[k]
        if isinstance(value, float):
            check_duration(k, got, value)
        elif k == 'natural_idle_at' and value is not None:
            require(stamp_ns(got) == stamp_ns(value), 'derived idle time differs')
        else:
            require(got == value, 'derived metric differs: ' + k)
    run = r['run_result']
    completed = (r['run_error_class'] == '' and run['Completed'] and run['CompletionAckWritten'] and run['FinalOffset'] == total
                 and actual == data and len(seqs) == 61 and len(final) == 1 and final[0]['audio_end'] == total and run['AppliedSeq'] == 61)
    require(type(r['business_completed']) is bool and r['business_completed'] == completed, 'business completion differs')
    require(r['natural_tasks_exited'] and not r['forced_cleanup'] and idle is not None
            and all(samples[-1][k] == 0 for k in counts + tasks), 'natural cleanup not complete')
    require(not r['outer_deadline'] and ms(r['returned_at'], r['started_at']) < 15000, 'outer deadline reached')
    expected_failure = cfg['case'] in (ORDER[1], ORDER[4])
    if cfg['case'] == ORDER[1]:
        require(not completed and not run['Completed'] and run['ResumeAttempts'] == run['ResumedConnections'] == 0, 'disabled control resumed/completed')
    elif cfg['case'] == ORDER[4]:
        require(not completed and not run['Completed'] and r['run_error_class'] == 'recovery_exhausted' and 1 <= run['ResumeAttempts'] <= 5, 'over-window behavior differs')
        require(all(h['forwarded_at'] is None for h in r['handshakes'] if h['kind'] == 'resume'), 'canceled handshake forwarded after reopening')
    else:
        # 业务判据失败仍保留在统计中，不借分析器删除失败样本。
        pass
    acceptance = not r['acceptance_errors'] and (expected_failure or completed)
    return dict(case=cfg['case'], business_completed=completed, criteria_met=acceptance, error_class=r['run_error_class'],
                clock_differences=clock_differences, max_clock_difference_ms=max(abs(d['difference_ms']) for d in clock_differences),
                timing_basis='wall-clock recomputed from JSON; original monotonic durations preserved in reports and clock_differences',
                audio_bytes=captured, worker_sha256=hashlib.sha256(actual).hexdigest(),
                result_applications=len(seqs), resume_attempts=run['ResumeAttempts'], resumed_connections=run['ResumedConnections'],
                run_elapsed_ms=ms(r['returned_at'], r['started_at']), actual_outage_ms=ms(r['opened_at'], r['cut_at']),
                return_after_cut_ms=ms(r['returned_at'], r['cut_at']),
                source_max_schedule_lag_ms=max((stamp_ns(x['started_at'])-origin-i*100_000_000)/10**6 for i,x in enumerate(reads)),
                capture_blocks_during_outage=sum(r['cut_at'] is not None and r['opened_at'] is not None
                    and stamp_ns(r['cut_at']) < stamp_ns(x['started_at']) < stamp_ns(r['opened_at']) for x in reads),
                max_result_wait_ms=max(waits, default=None), resource_samples=len(samples), **derived)


def range_summary(values):
    """三次样本展示原值和 min–max，不做可靠尾部分位数推断。"""
    clean = [v for v in values if v is not None]
    return dict(samples=len(clean), values=values, min=min(clean) if clean else None, max=max(clean) if clean else None)


def summarize(directory):
    """从完整 Go JSON 复算并归档；配置/证据错误保留原始目录并中止解释。"""
    root = Path(directory)
    manifest = json.loads((root/'manifest.json').read_text())
    require(manifest['experiment'] == 'EXP-006-01' and manifest['race'] is False and manifest['planned_sessions'] == 15
            and manifest['planned_rounds'] == 3 and manifest['order'] == ORDER, 'experiment identity differs')
    require(manifest.get('status') in ('collected', 'collected_with_failure') and manifest.get('wake_active_until_go_return'), 'collection incomplete')
    require(digest(root/manifest['preregistration_snapshot']) == manifest['preregistration_sha256'], 'preregistration snapshot changed')
    events = [json.loads(line) for line in (root/'go-events.jsonl').read_text().splitlines()]
    reports = extract(events)
    require([r['config']['case'] for r in reports] == ORDER * 3, 'reports missing or order differs; do not replace attempts')
    root_starts = [e for e in events if e.get('Test') == ROOT_TEST and e['Action'] == 'run']
    root_ends = [e for e in events if e.get('Test') == ROOT_TEST and e['Action'] in ('pass', 'fail')]
    package_ends = [e for e in events if 'Test' not in e and e['Action'] in ('pass', 'fail', 'skip')]
    require(len(root_starts) == len(root_ends) == 3 and len(package_ends) == 1
            and package_ends[0]['Action'] == ('pass' if manifest['go_exit_code'] == 0 else 'fail'), 'Go process terminal states differ')
    environments = [e['Output'].strip() for e in events if 'environment go=' in e.get('Output', '')]
    require(len(environments) == 3 and len(set(environments)) == 1 and 'GOMAXPROCS=12' in environments[0], 'runtime environment differs')
    terminal = [e for e in events if '/' in e.get('Test', '') and e['Action'] in ('pass','fail','skip')]
    require([e['Test'].split('/',1)[1] for e in terminal] == ORDER*3 and all(e['Action'] != 'skip' for e in terminal), 'case terminal states incomplete')
    verified = []
    for i, report in enumerate(reports):
        row = verify_report(report)
        row['round'] = i//5+1
        row['go_case_state'] = terminal[i]['Action']
        verified.append(row)
    metrics = ['ready_after_cut_ms','ready_after_open_ms','fresh_after_cut_ms','fresh_after_open_ms',
               'catchup_after_open_ms','max_result_wait_ms','max_partial_gap_ms','repeated_transfer_bytes','cleanup_wait_ms',
               'run_elapsed_ms','return_after_cut_ms','actual_outage_ms','source_max_schedule_lag_ms','max_sample_gap_ms','max_sample_call_ms']
    groups = []
    for case in ORDER:
        rows = [r for r in verified if r['case'] == case]
        successful = [r for r in rows if r['business_completed']]
        groups.append(dict(case=case, planned=3, completed=len(successful), completion_rate=len(successful)/3,
                           criteria_met=sum(r['criteria_met'] for r in rows), outcomes=dict(Counter(r['error_class'] or 'completed' for r in rows)),
                           all_attempt_metrics={k:range_summary([r[k] for r in rows]) for k in metrics},
                           successful_recovery_metrics={k:range_summary([r[k] for r in successful]) for k in metrics[:5]}))
    result=dict(experiment='EXP-006-01',planned_sessions=15,reports_verified=len(verified),groups=groups,runs=verified,
                all_behavior_criteria_met=all(r['criteria_met'] for r in verified), go_exit_code=manifest['go_exit_code'],
                max_resource_sample_gap_ms=max(r['max_sample_gap_ms'] for r in verified),
                max_source_schedule_lag_ms=max(r['source_max_schedule_lag_ms'] for r in verified),
                clock_agreement_limit_ms=CLOCK_AGREEMENT_MS,
                max_observed_clock_difference_ms=max(r['max_clock_difference_ms'] for r in verified),
                actual_resource_samples=sum(r['resource_samples'] for r in verified), runtime_environment=environments[0],
                same_fault_comparison=dict(disabled_completed=groups[1]['completed'], enabled_completed=groups[2]['completed'],
                    planned_per_group=3, completion_rate_delta_pp=(groups[2]['completion_rate']-groups[1]['completion_rate'])*100),
                scope=manifest['scope'],limits=['three repetitions per group','same-process loopback','fixed deterministic fault','single session','not real ASR or production recovery probability'])
    (root/'reports.json').write_text(json.dumps(reports,ensure_ascii=False,separators=(',',':'))+'\n')
    (root/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    files=['manifest.json','preregistration.md','go-events.jsonl','go-stderr.log','wake.log','reports.json','summary.json',
           'analysis-first.py','analysis-first-stdout.log','analysis-first-failure.log','analysis-second-failure.log',
           'collection-runner.py','analysis-validation.log']
    analysis=dict(version=1,source_sha256=digest(__file__),input_and_output_sha256={p:digest(root/p) for p in files},reports_verified=15,
                  test_source_sha256=digest(Path(__file__).with_name('test_client_resume.py')),
                  note='Raw report verification, exact RFC3339Nano timing and independent byte/range recomputation; business and criteria counts separated')
    (root/'analysis-manifest.json').write_text(json.dumps(analysis,ensure_ascii=False,indent=2)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args = parser.parse_args()
    result = summarize(args.directory)
    print(json.dumps({k:result[k] for k in ['reports_verified','all_behavior_criteria_met','max_resource_sample_gap_ms','max_source_schedule_lag_ms']}))
