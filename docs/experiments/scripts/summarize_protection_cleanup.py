#!/usr/bin/env python3
"""汇总反复保护退出的资源记录；只做描述性统计，不自动宣判不存在泄漏。"""
import collections
import csv
import json
from pathlib import Path
import sys


def summarize(path):
    raw = path.read_text()
    rows = [json.loads(line.split('PROTECTION_CLEANUP_JSON ', 1)[1])
            for line in raw.splitlines() if 'PROTECTION_CLEANUP_JSON ' in line]
    if len(rows) != 6 or '\nFAIL' in raw or '\nPASS\n' not in raw:
        raise ValueError('expected six group records and a passing formal run')
    groups = collections.Counter(r['group'] for r in rows)
    if groups != {'normal_control': 3, 'mixed_faults': 3}:
        raise ValueError('expected three repetitions of each group')
    summary = []
    samples = []
    totals = collections.Counter()
    for number, r in enumerate(rows, 1):
        if not r['validated'] or (r['rounds'], r['warmup_rounds'], r['sessions_per_round']) != (20, 2, 5):
            raise ValueError('unvalidated or unexpected workload')
        expected = {'normal': 100} if r['group'] == 'normal_control' else dict.fromkeys(
            ['normal', 'backlog', 'send_timeout', 'tail_timeout', 'write_timeout'], 20)
        if r['outcomes'] != expected:
            raise ValueError('missing session outcomes')
        totals.update(r['outcomes'])
        regular = [s for s in r['samples'] if not s['settled']]
        settled = [s for s in r['samples'] if s['settled']]
        if [s['round'] for s in regular] != list(range(21)) or len(settled) != 1 or settled[0]['round'] != 20:
            raise ValueError('missing warmup baseline, round or settled sample')
        for s in r['samples']:
            if any(s[k] != 0 for k in ('active_sessions', 'open_ws', 'active_workers', 'active_sends')) or s['open_grpc'] != 1:
                raise ValueError('nonzero per-session resources or changed persistent connection count')
            samples.append(dict(record=number, repetition=(number+1)//2, group=r['group'], **s))
        base, final, later = regular[0], regular[-1], settled[0]
        # 拟合仅为描述性趋势；GC、池和时间顺序均可能影响斜率，不能作为泄漏判定阈值。
        measured = regular[1:]
        mean_x = sum(s['round'] for s in measured) / len(measured)
        mean_y = sum(s['heap_alloc'] for s in measured) / len(measured)
        slope = sum((s['round']-mean_x)*(s['heap_alloc']-mean_y) for s in measured) / sum((s['round']-mean_x)**2 for s in measured)
        summary.append(dict(record=number, repetition=(number+1)//2, group=r['group'], outcomes=r['outcomes'],
            goroutines_baseline=base['goroutines'], goroutines_final=final['goroutines'], goroutines_settled=later['goroutines'],
            goroutines_min=min(s['goroutines'] for s in regular), goroutines_max=max(s['goroutines'] for s in regular),
            heap_baseline=base['heap_alloc'], heap_round20=final['heap_alloc'], heap_settled=later['heap_alloc'],
            heap_min=min(s['heap_alloc'] for s in measured), heap_max=max(s['heap_alloc'] for s in measured),
            heap_delta_round20=final['heap_alloc']-base['heap_alloc'], heap_delta_settled=later['heap_alloc']-base['heap_alloc'],
            heap_ols_bytes_per_round=slope, objects_baseline=base['heap_objects'], objects_round20=final['heap_objects'], objects_settled=later['heap_objects']))
    result = dict(validated_groups=6, measured_sessions=sum(totals.values()), warmup_sessions=60,
                  measured_outcomes=dict(totals), runs=summary,
                  limits=['descriptive resource observations, not proof of zero leaks',
                          'heap includes client, Gateway, Worker and test/runtime allocations',
                          'GC and 250ms settling are measurement interventions',
                          'serial sessions, not capacity, load or long-duration validation'])
    prefix = path.with_suffix('')
    Path(str(prefix)+'-summary.json').write_text(json.dumps(result, indent=2, ensure_ascii=False)+'\n')
    with Path(str(prefix)+'-samples.csv').open('w', newline='') as f:
        writer=csv.DictWriter(f,fieldnames=list(samples[0]));writer.writeheader();writer.writerows(samples)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
