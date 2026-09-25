#!/usr/bin/env python3
"""从完整原始 Go 测试日志提取对照数据；拒绝缺失、失败或未验证的正式测量。"""
import collections
import csv
import json
from pathlib import Path
import re
import sys


def summarize(path):
    raw = path.read_text()
    rows = [json.loads(line.split('BACKLOG_COMPARISON_JSON ', 1)[1])
            for line in raw.splitlines() if 'BACKLOG_COMPARISON_JSON ' in line]
    if len(rows) != 18 or not all(r['validated'] for r in rows):
        raise ValueError('expected 18 validated main sessions; keep failed raw logs for diagnosis')
    if '\nFAIL' in raw or '\nPASS\n' not in raw:
        raise ValueError('Go experiment did not pass')
    groups = collections.defaultdict(list)
    samples = []
    flat = []
    for number, r in enumerate(rows, 1):
        groups[(r['case'], r['budget_bytes'])].append(r)
        for s in r['samples']:
            samples.append(dict(run=number, case=r['case'], budget_bytes=r['budget_bytes'], **s))
        r['max_send_ms'] = max((s['returned_ms']-s['started_ms'] for s in r['sends']), default=0)
        r['max_write_ms'] = max((s['returned_ms']-s['started_ms'] for s in r['writes']), default=0)
        r['max_schedule_lag_ms'] = max((s['started_ms']-s['planned_ms'] for s in r['writes']), default=0)
        r['partial_count'] = len(r['partials'])
        r['max_partial_wait_ms'] = max((p['wait_ms'] for p in r['partials']), default=None)
        r['trigger_pending_bytes'] = None
        if r['outcome'] == 'audio_backlog_exceeded':
            match = re.search(r'audio backlog exceeded \((\d+) > (\d+)\)', r['run_error'])
            if not match or int(match[2]) != r['budget_bytes']:
                raise ValueError('missing exact comparison values in preserved run error')
            r['trigger_pending_bytes'] = int(match[1])
        flat.append(dict(run=number, **{k:v for k,v in r.items() if k not in ('samples','writes','sends','partials','final_progress')},
                         received_bytes=r['final_progress']['received_bytes'],
                         processed_bytes=r['final_progress']['processed_bytes'],
                         pending_bytes=r['final_progress']['pending_bytes']))
    expected = {(case, budget) for case in ('normal','pause_500ms','slow_200ms') for budget in (0,32000)}
    if set(groups) != expected or any(len(rs) != 3 for rs in groups.values()):
        raise ValueError('each of six conditions must have three repetitions')
    metrics = ('observed_peak_pending_bytes','tail_ms','close_ms','run_returned_ms','handler_returned_ms',
               'worker_returned_ms','client_written_bytes','send_succeeded_bytes','worker_received_bytes',
               'worker_progress_sent_bytes','max_sample_gap_ms','max_snapshot_call_ms','max_send_ms',
               'max_write_ms','max_schedule_lag_ms','max_partial_wait_ms','partial_count','trigger_pending_bytes')
    summary=[]
    for (case,budget), rs in groups.items():
        row=dict(case=case,budget_bytes=budget,runs=len(rs),outcomes=dict(collections.Counter(r['outcome'] for r in rs)),
                 cleaned=sum(r['active_after_cleanup']==0 for r in rs),reused=sum(r['reused'] for r in rs))
        for metric in metrics:
            values=[r[metric] for r in rs if r[metric] is not None]
            row[metric]=dict(min=min(values),max=max(values)) if values else None
        for metric in ('received_bytes','processed_bytes','pending_bytes'):
            values=[r['final_progress'][metric] for r in rs]
            row['final_'+metric]=dict(min=min(values),max=max(values))
        summary.append(row)
    prefix=path.with_suffix('')
    output={'main_runs':18,'validated_runs':sum(r['validated'] for r in rows),'groups':summary,
            'limits':['min/max of three runs, not percentiles','sampled peak may miss transients',
                      'trigger count comes from error, no precise trigger timestamp',
                      'failed sessions have no tail latency; no model speedup claim']}
    Path(str(prefix)+'-summary.json').write_text(json.dumps(output,indent=2,ensure_ascii=False)+'\n')
    for suffix, records in (('-runs.csv',flat),('-samples.csv',samples)):
        with Path(str(prefix)+suffix).open('w',newline='') as f:
            # 失败行额外包含错误字段，表头需覆盖全部记录，不能只取首个成功行。
            fields=list(dict.fromkeys(key for record in records for key in record))
            writer=csv.DictWriter(f,fieldnames=fields)
            writer.writeheader();writer.writerows(records)
    print(json.dumps(output,indent=2,ensure_ascii=False))


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
