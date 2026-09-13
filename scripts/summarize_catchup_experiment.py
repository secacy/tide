#!/usr/bin/env python3
"""Verify EXP-015 coverage, bounded completion, clock ordering and raw evidence."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path

CASES = ['healthy_40ms','recover_40ms','recover_80ms','recover_100ms']


def summarize(raw):
    meta=json.loads(raw.with_suffix(raw.suffix+'.meta.json').read_text())
    if meta['experiment']!='EXP-015' or meta['exit_code']!=0:
        raise ValueError('experiment failed; preserve failure evidence')
    if hashlib.sha256(raw.read_bytes()).hexdigest()!=meta['raw_sha256']:
        raise ValueError('raw hash mismatch')
    chunks=defaultdict(list)
    for line in raw.read_text().splitlines():
        e=json.loads(line)
        if e.get('Action')=='fail':raise ValueError('raw test failed')
        chunks[(e.get('Package'),e.get('Test'))].append(e.get('Output',''))
    samples=[]
    for parts in chunks.values():
        for line in ''.join(parts).splitlines():
            if 'CATCHUP_SAMPLE ' in line:samples.append(json.loads(line.split('CATCHUP_SAMPLE ',1)[1]))
    if Counter(s['case'] for s in samples)!=Counter({c:meta['count'] for c in CASES}):raise ValueError('missing/extra samples')
    rows=[]
    for s in samples:
        if not s['quality_pass'] or not s['gateway_drained'] or s['worker_active_after_cleanup']!=0:raise ValueError('cleanup failure')
        if s['cache_peak_bytes']>s['buffer_limit_bytes']:raise ValueError('cache bound exceeded')
        spans=[(p['fromSample'],p['throughSample']) for p in s['segments'] or []]+[(g['FromSample'],g['ThroughSample']) for g in s['gaps'] or []]
        frontier=0
        for a,b in sorted(spans):
            if a!=frontier or b<=a:raise ValueError('coverage hole/overlap')
            frontier=b
        if frontier!=s['captured_through']:raise ValueError('unaccounted captured audio')
        if s['complete'] and (s['error'] or s['gaps'] or not s['ended']):raise ValueError('false completion')
        events=s['events']
        def first(kind):return next((e for e in events if e['kind']==kind),None)
        fault,start,caught,returned=map(first,['fault','recovery_start','caught_up','returned'])
        ready=next((e for e in events if e['kind']=='ready' and start and e['ms']>start['ms']),None)
        if s['case']=='healthy_40ms':
            if fault or start or not s['complete']:raise ValueError('bad healthy control')
        else:
            if not fault or not start or not ready:raise ValueError('missing phase observations')
            if sum(e['kind']=='recovery_start' for e in events)!=1:raise ValueError('budget renewed')
            if abs(start['deadline_ms']-start['ms']-10000)>50:raise ValueError('wrong budget')
            if caught and caught['ms']>start['deadline_ms']:raise ValueError('late recovery')
            if s['complete'] and not caught:raise ValueError('missing live catchup')
            if not caught and s['error']!='recovery budget exhausted':raise ValueError('unclassified failed recovery')
            if not caught and not (start['deadline_ms']<=returned['ms']<start['deadline_ms']+1000):raise ValueError('terminal budget boundary missed')
        rows.append({'case':s['case'],'complete':s['complete'],'attempts':s['attempts'],'rejected':s['rejected'],'cache_peak':s['cache_peak_bytes'],
                     'fault_to_start':None if not fault else start['ms']-fault['ms'],
                     'start_to_ready':None if not ready else ready['ms']-start['ms'],
                     'ready_to_caught':None if not caught else caught['ms']-ready['ms'],
                     'recovery_elapsed':None if not start else (caught or returned)['ms']-start['ms'],
                     'gap_samples':sum(g['ThroughSample']-g['FromSample'] for g in s['gaps'] or [])})
    lines=['# EXP-015 continuous recovery summary','',f"Source: `{meta['commit']}`; race: {meta['race']}; samples: {len(samples)}; excluded: 0.",'',
           '| Case | Complete | Attempts | 503 | Fault→budget ms | Budget→Ready ms | Ready→caught up ms | Recovery / stop ms | PCM peak B | Gap samples |',
           '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    def fmt(v):return '—' if v is None else f'{v:.1f}'
    for r in rows:lines.append(f"| {r['case']} | {r['complete']} | {r['attempts']} | {r['rejected']} | {fmt(r['fault_to_start'])} | {fmt(r['start_to_ready'])} | {fmt(r['ready_to_caught'])} | {fmt(r['recovery_elapsed'])} | {r['cache_peak']} | {r['gap_samples']} |")
    lines+=['','Checkpoint catchup uses the production coordinator predicate. Failed rows retain terminal gaps. These are single-session synthetic-worker measurements, not ASR capacity or speech-quality evidence.']
    return '\n'.join(lines)+'\n'


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('raw',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();result=summarize(a.raw)
    if a.output:
        if a.output.exists():p.error('preserve existing summary')
        a.output.write_text(result)
    else:print(result,end='')


if __name__=='__main__':main()
