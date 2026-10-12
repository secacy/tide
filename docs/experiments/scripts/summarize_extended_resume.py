#!/usr/bin/env python3
"""Independently verify EXP-006-05 events and recompute per-outage and resource metrics."""
import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import statistics

from extended_resume_contract import (CASES, CHUNK, EXPERIMENT, LOG_LIMITS, NS, PERIOD, PLAN, ROOT_TEST,
                                      client_config, configured, gateway_config, order, pattern, windows)
from summarize_client_resume import digest, require, stamp_ns

KINDS = {
    'run': {'configuration','application','returned','natural_idle','joined'},
    'source': {'read','eof','close'},
    'worker': {'start','received','processed','progress_sent','result_sent','end','tail','exit'},
    'proxy': {'handshake','handshake_forwarded','audio_read','fault','opened','opening_cancelled','audio_write_started',
              'audio_write_returned','control_write_started','control_write_returned','ready','audio_ack','entry_error',
              'server_message','handler_exit'},
    'sample': {'resources'},
}
NUMERIC_DATA={'connection','generation','next_offset','acked_seq','applied_seq','begin','end','offset','seq','round','rpc','count',
              'captured','received','processed','finished_ns','planned_ns','applied_ns','duration_ns','source_closes',
              'worker_calls','picks','connections','faults','sessions','handshakes','registry','worker_active',
              'gateway_handlers','proxy_handlers','fault_timers','goroutines','heap_alloc','heap_inuse','total_alloc',
              'num_gc','queue_depth','queue_peak','written'}
SCHEMA={
 ('run','configuration'):'experiment config client gateway log_limits origin_utc',
 ('run','application'):'seq end final applied_ns',('run','returned'):'result error_class outer_expired',
 ('run','natural_idle'):'sessions handshakes registry worker_active gateway_handlers proxy_handlers fault_timers stopping service_cancelled',
 ('run','joined'):'captured received processed source_sha256 worker_sha256 source_closes worker_calls picks connections faults worker_ended',
 ('source','read'):'begin end sha256 finished_ns planned_ns',('source','eof'):'offset',('source','close'):'count captured sha256',
 ('worker','start'):'rpc',('worker','received'):'begin end sha256',('worker','processed'):'offset',('worker','progress_sent'):'offset',
 ('worker','result_sent'):'offset',('worker','end'):'offset',('worker','tail'):'sent',('worker','exit'):'rpc error_class',
 ('proxy','handshake'):'connection kind applied_seq',('proxy','handshake_forwarded'):'connection',
 ('proxy','audio_read'):'connection begin end sha256',('proxy','fault'):'round offset connection',('proxy','opened'):'round duration_ns',
 ('proxy','opening_cancelled'):'round',('proxy','audio_write_started'):'connection begin end sha256',
 ('proxy','audio_write_returned'):'connection success',('proxy','control_write_started'):'connection kind offset seq',
 ('proxy','control_write_returned'):'connection success',('proxy','ready'):'connection generation next_offset acked_seq input_ended',
 ('proxy','audio_ack'):'connection generation next_offset input_ended',('proxy','entry_error'):'connection code',
 ('proxy','server_message'):'connection type',('proxy','handler_exit'):'connection',
 ('sample','resources'):'finished_ns sessions handshakes registry worker_active gateway_handlers proxy_handlers fault_timers goroutines heap_alloc heap_inuse total_alloc num_gc queue_depth queue_peak written',
}


def integer(value, message, minimum=0):
    require(type(value) is int and value >= minimum, message)
    return value


def same(a,b):
    """JSON types matter: Python's True == 1 must not authorize a configuration change."""
    return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))


def read_events(path):
    """Require full bounded lines and continuous component sequences, not filesystem write order as time order."""
    rows, sequences = [], Counter()
    with Path(path).open('rb') as stream:
        for line in stream:
            require(line.endswith(b'\n') and len(line) <= 8192, 'incomplete or oversized event line')
            require(not any(word in line for word in (b'resumeToken', b'resume_token', b'SessionID', b'session_id')), 'credentials in events')
            row=json.loads(line)
            require(set(row)=={'version','component','seq','kind','elapsed_ns','data'} and type(row['version']) is int and row['version']==1, 'event schema differs')
            component=row['component'];require(component in KINDS and row['kind'] in KINDS[component], 'unknown event component or kind')
            integer(row['seq'],'invalid component sequence',1);integer(row['elapsed_ns'],'invalid monotonic timestamp')
            require(row['seq']==sequences[component]+1,'component sequence gap or duplicate')
            sequences[component]=row['seq'];require(row['data'] is None or isinstance(row['data'],dict),'invalid event payload')
            require(isinstance(row['data'],dict) and set(row['data'])==set(SCHEMA[component,row['kind']].split()),'event payload fields differ')
            for key,value in (row['data'] or {}).items():
                if key in NUMERIC_DATA:integer(value,'numeric payload type/value differs')
            rows.append(row)
    require(rows and rows[-1]['component']=='run' and rows[-1]['kind']=='joined','terminal join missing')
    return rows


def stats(values):
    return None if not values else dict(n=len(values),min=min(values),median=statistics.median(values),max=max(values))


def verify_case(directory, mode, case):
    """Verify input, results, RPC ownership and cleanup before interpreting any durations."""
    cfg=configured(case,mode);total=cfg['chunks']*CHUNK
    s=json.loads((directory/'summary.json').read_text());rows=read_events(directory/'events.jsonl')
    for key in ('captured_bytes','received_bytes','processed_bytes','source_closes','worker_calls','worker_picks','connections','faults','observed_queue_peak','events','buffer_accepted_bytes','returned_ns','idle_ns'):
        integer(s[key],'invalid terminal number')
    by=defaultdict(list)
    for row in rows:by[row['component'],row['kind']].append(row)
    def one(component,kind):
        values=by[component,kind];require(len(values)==1,f'{component}/{kind} missing or duplicated');return values[0]
    configuration=one('run','configuration')['data']
    require(configuration['experiment']==EXPERIMENT and same(configuration['config'],cfg) and same(s['config'],cfg),'case profile drift')
    require(same(configuration['client'],client_config(case)) and same(s['client_config'],client_config(case)),'client config drift')
    require(same(configuration['gateway'],gateway_config()) and same(s['gateway_config'],gateway_config()),'effective gateway config drift')
    require(same(configuration['log_limits'],LOG_LIMITS) and same(s['log_limits'],LOG_LIMITS),'recorder config drift')
    stamp_ns(configuration['origin_utc'])
    require(s['log_error'] is False and s['forced_cleanup'] is False and s['natural_idle'] is True,'tool error or forced cleanup')
    require(integer(s['events'],'invalid event count')==len(rows) and integer(s['buffer_accepted_bytes'],'invalid event byte count')==(directory/'events.jsonl').stat().st_size,'event file count/length differs')
    integer(s['observed_queue_peak'],'invalid queue peak');require(s['observed_queue_peak']<=LOG_LIMITS['queue'],'queue overflow')
    returned=one('run','returned');joined=one('run','joined');idle=one('run','natural_idle')
    require(returned['data']['result']==s['result'] and returned['data']['error_class']==s['error_class'],'terminal outcome differs')
    require(one('run','configuration')['elapsed_ns']<=s['returned_ns']<=returned['elapsed_ns']<=s['idle_ns']<=idle['elapsed_ns']<=joined['elapsed_ns'],'terminal time order differs')
    require(idle['elapsed_ns']-returned['elapsed_ns']<=cfg['cleanup_timeout_ns'],'natural cleanup outside observation')
    for key in ('sessions','handshakes','registry','worker_active','gateway_handlers','proxy_handlers','fault_timers'):
        require(type(idle['data'][key]) is int and idle['data'][key]==0,'natural idle counters not zero')
    require(idle['data']['stopping'] is False and idle['data']['service_cancelled'] is False,'service cancellation caused natural idle')
    close=one('source','close');require(close['data']['count']==s['source_closes']==joined['data']['source_closes']==1,'source close ownership differs')
    start,exit=one('worker','start'),one('worker','exit')
    require(start['data']['rpc']==exit['data']['rpc']==s['worker_calls']==s['worker_picks']==joined['data']['worker_calls']==joined['data']['picks']==1,'replacement or missing Worker RPC')
    require(start['elapsed_ns']<=exit['elapsed_ns']<=idle['elapsed_ns'],'Worker did not actually exit before idle')
    result=s['result']
    require(set(result)=={'Completed','FinalOffset','AppliedSeq','ResumeAttempts','ResumedConnections','CompletionAckWritten'},'RunResult schema differs')
    for key in ('Completed','CompletionAckWritten'):require(type(result[key]) is bool,'invalid result boolean')
    for key in ('FinalOffset','AppliedSeq','ResumeAttempts','ResumedConnections'):integer(result[key],'invalid result count')
    require(result['ResumeAttempts']<=cfg['attempts'] and result['ResumedConnections']<=result['ResumeAttempts'],'attempt budget exceeded')

    source=by['source','read'];require(source,'no actual input capture')
    source_digest=hashlib.sha256();read_by_end={};first=source[0]['elapsed_ns'];lags=[]
    for i,r in enumerate(source):
        d=r['data'];require(d['begin']==i*CHUNK and d['end']==(i+1)*CHUNK and i<cfg['chunks'],'source range gap/duplicate')
        block=pattern(i);require(d['sha256']==hashlib.sha256(block).hexdigest(),'source payload digest differs')
        require(d['planned_ns']==first+i*PERIOD and d['finished_ns']>=r['elapsed_ns'],'source pacing origin changed')
        source_digest.update(block);read_by_end[d['end']]=r;lags.append(max(0,r['elapsed_ns']-d['planned_ns']))
    captured=len(source)*CHUNK
    require(captured==s['captured_bytes']==close['data']['captured']==joined['data']['captured'] and source_digest.hexdigest()==s['source_sha256']==close['data']['sha256']==joined['data']['source_sha256'],'source terminal facts differ')
    require(close['elapsed_ns']>=max(r['data']['finished_ns'] for r in source),'source closed before capture finished')
    worker=by['worker','received'];worker_digest=hashlib.sha256()
    for i,r in enumerate(worker):
        d=r['data'];require(d['begin']==i*CHUNK and d['end']==(i+1)*CHUNK and d['end']<=captured,'Worker input prefix differs')
        block=pattern(i);require(d['sha256']==hashlib.sha256(block).hexdigest(),'Worker payload digest differs');worker_digest.update(block)
    received=len(worker)*CHUNK
    require(received==s['received_bytes']==joined['data']['received'] and worker_digest.hexdigest()==s['worker_sha256']==joined['data']['worker_sha256'],'Worker received facts differ')
    processed=by['worker','processed']
    for i,r in enumerate(processed):
        require(i<len(worker) and r['data']['offset']==(i+1)*CHUNK and r['elapsed_ns']>=worker[i]['elapsed_ns'],'Worker processed prefix differs')
    processed_bytes=len(processed)*CHUNK
    require(processed_bytes==s['processed_bytes']==joined['data']['processed'],'Worker processed facts differ')
    for kind in ('progress_sent','result_sent'):
        for i,r in enumerate(by['worker',kind]):require(i<len(processed) and r['data']['offset']==(i+1)*CHUNK and r['elapsed_ns']>=processed[i]['elapsed_ns'],'Worker output position differs')
    require(len(by['worker','result_sent'])<=len(by['worker','progress_sent'])<=len(processed),'Worker output event missing')
    apps=by['run','application'];partial=[r for r in apps if not r['data']['final']];tail=[r for r in apps if r['data']['final']]
    for i,r in enumerate(apps):
        require(r['data']['seq']==i+1 and r['data']['applied_ns']>=r['elapsed_ns'],'application seq/time differs')
        if not r['data']['final']:
            require(r['data']['end']==(i+1)*CHUNK and i<len(processed) and i<len(by['worker','result_sent']),'application claims unprocessed result')
    require(len(apps)==result['AppliedSeq'] and len(tail)<=1 and (not tail or apps[-1] is tail[0]),'application count/tail differs')
    eof=by['source','eof'];end=by['worker','end'];wtail=by['worker','tail']
    if result['Completed']:
        require(s['error_class']=='' and result['CompletionAckWritten'] and result['FinalOffset']==total and captured==received==processed_bytes==total and len(partial)==cfg['chunks'] and len(tail)==len(eof)==len(end)==len(wtail)==1,'false complete result')
        require(end[0]['data']['offset']==eof[0]['data']['offset']==total and wtail[0]['data']['sent'] is True and tail[0]['data']['end']==total and joined['data']['worker_ended'] is True and exit['data']['error_class']=='','complete end/tail facts differ')
    else:
        require(not result['CompletionAckWritten'] and result['FinalOffset']==0 and s['error_class']!='','incomplete outcome concealed')
        require(not tail or end,'tail without end')

    handshakes=by['proxy','handshake'];forwarded=by['proxy','handshake_forwarded'];readies=by['proxy','ready'];frames=by['proxy','audio_read']
    require(len(handshakes)==s['connections']==joined['data']['connections'] and handshakes,'connection count differs')
    for i,r in enumerate(handshakes):
        d=r['data'];require(d['connection']==i+1 and d['kind']==('start' if i==0 else 'resume'),'fallback start or connection sequence differs')
        require(d['applied_seq']<=result['AppliedSeq'],'resume position beyond applied result')
    require(len(handshakes)-1<=result['ResumeAttempts'],'more observed requests than actual Dial count')
    require(Counter(r['data']['connection'] for r in by['proxy','handler_exit'])==Counter(range(1,len(handshakes)+1)),'proxy handlers did not exit')
    for i,r in enumerate(readies):
        d=r['data'];integer(d['next_offset'],'invalid ready offset');require(d['generation']==i+1 and d['next_offset']<=captured and d['next_offset']%CHUNK==0,'ready generation/position differs')
        request=handshakes[d['connection']-1]['data'];require(d['acked_seq']==request['applied_seq'],'ready result confirmation differs')
    require(result['ResumedConnections']<=max(0,len(readies)-1),'client accepted more connections than forwarded ready')
    require(len({r['data']['connection'] for r in forwarded})==len(forwarded),'handshake forwarded twice')
    for r in forwarded:require(1<=r['data']['connection']<=len(handshakes),'forwarded unknown connection')
    forwarded_by={r['data']['connection']:r for r in forwarded}
    for r in readies:
        c=r['data']['connection'];require(c in forwarded_by and r['elapsed_ns']>=forwarded_by[c]['elapsed_ns'],'ready without forwarded handshake')
    pending={}
    for r in rows:
        if r['component']=='proxy' and r['kind'].endswith(('_write_started','_write_returned')):
            c=r['data']['connection'];kind=r['kind'].split('_',1)[0]
            if r['kind'].endswith('_started'):
                require(c not in pending,'overlapping proxy writes');pending[c]=(kind,r)
            else:
                require(c in pending and pending[c][0]==kind and r['elapsed_ns']>=pending[c][1]['elapsed_ns'],'proxy write return missing/mismatched')
                require(type(r['data']['success']) is bool,'proxy Write status invalid');del pending[c]
    require(not pending,'proxy write task ended without return')
    coverage=Counter()
    for r in frames:
        d=r['data'];require(1<=d['connection']<=len(handshakes) and d['begin']%CHUNK==0 and d['end']==d['begin']+CHUNK and d['end']<=captured,'proxy frame outside capture')
        require(d['sha256']==hashlib.sha256(pattern(d['begin']//CHUNK)).hexdigest(),'proxy payload digest differs');coverage[d['begin']]+=1
    require(all(i*CHUNK in coverage for i in range(len(worker))),'Worker bytes have no proxy provenance')
    ready_by_connection={r['data']['connection']:r['data'] for r in readies};acked={}
    for r in by['proxy','audio_ack']:
        d=r['data'];connection=d['connection'];require(connection in ready_by_connection and d['generation']==ready_by_connection[connection]['generation'] and acked.get(connection,0)<=d['next_offset']<=captured and d['next_offset']%CHUNK==0,'audio ACK backwards/outside input')
        acked[connection]=d['next_offset']
    faults=by['proxy','fault'];opened=by['proxy','opened'];planned=cfg['fault_chunks'] or []
    require(len(faults)==s['faults']==joined['data']['faults'] and len(faults)<=len(planned),'fault count differs')
    require(not by['proxy','opening_cancelled'] and len(opened)==len(faults),'opening tasks missing or cancelled')
    for i,r in enumerate(faults):
        d=r['data'];require(d['round']==i+1 and d['offset']==planned[i]*CHUNK,'fault round/position differs')
        require(any(f['data']['begin']==d['offset'] and f['data']['connection']==d['connection'] and f['elapsed_ns']<=r['elapsed_ns'] for f in frames),'fault has no observed dropped frame')
        require(opened[i]['data']['round']==i+1 and opened[i]['elapsed_ns']-r['elapsed_ns']>=cfg['outage_ns'],'opening early or wrong round')
        require(not any(w['data'].get('begin')==d['offset'] and w['data']['connection']==d['connection'] for w in by['proxy','audio_write_started']),'dropped fault frame forwarded')
    for r in forwarded:
        c=r['data']['connection'];request=handshakes[c-1]
        require(r['elapsed_ns']>=request['elapsed_ns'],'handshake forwarded before arrival')
        preceding=[i for i,f in enumerate(faults) if f['elapsed_ns']<=request['elapsed_ns']]
        if c>1 and preceding:require(r['elapsed_ns']>=opened[preceding[-1]]['elapsed_ns'],'resume bypassed closed fault gate')

    quality=[];active_samples=[r for r in by['sample','resources'] if first<=r['elapsed_ns']<=returned['elapsed_ns']]
    require(active_samples,'active resource sampling missing')
    last_gc=last_alloc=0
    for r in by['sample','resources']:
        d=r['data'];require(d['finished_ns']>=r['elapsed_ns'],'sample finish before begin')
        for key in ('sessions','handshakes','registry','worker_active','gateway_handlers','proxy_handlers','fault_timers','goroutines','heap_alloc','heap_inuse','num_gc','total_alloc','queue_depth','queue_peak','written'):integer(d[key],'invalid resource sample')
        require(d['num_gc']>=last_gc and d['total_alloc']>=last_alloc and d['queue_depth']<=LOG_LIMITS['queue'] and d['queue_peak']<=LOG_LIMITS['queue'],'sample counter/bound drift')
        last_gc,last_alloc=d['num_gc'],d['total_alloc']
    gaps=[b['elapsed_ns']-a['elapsed_ns'] for a,b in zip(active_samples,active_samples[1:])]
    require(all(x>=0 for x in gaps),'sampling time backwards')
    max_gap=max([active_samples[0]['elapsed_ns']-first,returned['elapsed_ns']-active_samples[-1]['elapsed_ns'],*gaps])
    if max_gap>NS:quality.append('sampling_gap_over_1s')
    if len(active_samples)<.9*(returned['elapsed_ns']-first)/cfg['sample_period_ns']:quality.append('sampling_coverage_under_90_percent')
    if max(lags)>PERIOD:quality.append('source_lag_over_100ms')
    partial_waits=[r['elapsed_ns']-read_by_end[r['data']['end']]['data']['planned_ns'] for r in partial]
    partial_gaps=[b['elapsed_ns']-a['elapsed_ns'] for a,b in zip(partial,partial[1:])]
    require(all(x>=0 for x in partial_gaps),'partial time backwards')
    outages=[]
    for i,f in enumerate(faults):
        t0=f['elapsed_ns'];release=opened[i]['elapsed_ns'];next_cut=faults[i+1]['elapsed_ns'] if i+1<len(faults) else returned['elapsed_ns']
        new_reads=[r for r in source if r['elapsed_ns']>t0];fresh_end=new_reads[0]['data']['end'] if new_reads else None
        fresh=next((r for r in partial if fresh_end is not None and r['data']['end']>=fresh_end and r['elapsed_ns']>=t0 and r['elapsed_ns']<next_cut),None)
        ready=next((r for r in readies if t0<=r['elapsed_ns']<next_cut),None)
        confirmation=None;streak=0
        for r in partial:
            if release<=r['elapsed_ns']<next_cut:
                wait=r['elapsed_ns']-read_by_end[r['data']['end']]['data']['planned_ns']
                streak=streak+1 if wait<=200_000_000 else 0
                if streak==3:confirmation=r['elapsed_ns'];break
        before=[r for r in partial if r['elapsed_ns']<=t0];after=next((r for r in partial if r['elapsed_ns']>t0),None)
        gap=after['elapsed_ns']-before[-1]['elapsed_ns'] if before and after else None
        actual_outage=release-t0
        duration_valid=500_000_000<=actual_outage<=650_000_000
        if not duration_valid:quality.append(f'outage_{i+1}_duration_outside_range')
        outages.append(dict(round=i+1,offset=f['data']['offset'],fault_ns=t0,opened_ns=release,outage_ns=actual_outage,
                            duration_quality=duration_valid,ready_wait_ns=None if ready is None else ready['elapsed_ns']-t0,
                            fresh_end=fresh_end,fresh_wait_ns=None if fresh is None else fresh['elapsed_ns']-t0,
                            fresh_after_open_ns=None if fresh is None else fresh['elapsed_ns']-release,
                            catchup_after_open_ns=None if confirmation is None else confirmation-release,
                            caught_up_before_next=confirmation is not None,partial_gap_ns=gap,
                            right_censored=before!=[] and after is None,
                            censored_until_return_ns=returned['elapsed_ns']-before[-1]['elapsed_ns'] if before and after is None else None,
                            observed_requests=sum(t0<=r['elapsed_ns']<next_cut for r in handshakes[1:]),
                            captured_during_outage=sum(t0<r['elapsed_ns']<release for r in source)))

    resource=[];online_end=returned['elapsed_ns']-first
    for i,(begin,end_ns) in enumerate(windows(mode),1):
        selected=[r for r in active_samples if begin<=r['elapsed_ns']-first<end_ns]
        state='not_reached' if online_end<=begin else 'partial' if online_end<end_ns else 'online'
        if state=='online' and any(o['fault_ns']-first<end_ns and (o['catchup_after_open_ns'] is None or o['opened_ns']+o['catchup_after_open_ns']-first>begin) for o in outages):state='recovering'
        row=dict(window=i,begin_ns=begin,end_ns=end_ns,state=state,samples=len(selected),coverage=len(selected)/((end_ns-begin)/cfg['sample_period_ns']))
        for key in ('goroutines','heap_alloc','heap_inuse','queue_depth','queue_peak','written'):row[key]=stats([r['data'][key] for r in selected])
        row['gc_count']=None if not selected else selected[-1]['data']['num_gc']-selected[0]['data']['num_gc']
        row['resource_counts']={key:stats([r['data'][key] for r in selected]) for key in ('sessions','handshakes','registry','worker_active','gateway_handlers','proxy_handlers','fault_timers')}
        row['quality_ok']=state=='online' and row['coverage']>=.9 and not quality
        resource.append(row)
    growth={}
    eligible=[w for w in resource if w['quality_ok'] and w['samples']]
    for key in ('goroutines','heap_alloc','heap_inuse'):
        streak=0;signal=None
        if eligible and eligible[0]['window']==1:
            signal=False
            baseline=eligible[0][key]['min']
            previous=1
            for w in eligible[1:]:
                streak=(streak+1 if w['window']==previous+1 else 1) if w[key]['min']>baseline else 0
                previous=w['window']
                if streak>=3:signal=True
        growth[key]=dict(needs_investigation=signal,eligible_windows=len(eligible),meaning='descriptive increase, not proof of a leak')
    complete=result['Completed']
    behavior=(complete and not faults and result['ResumeAttempts']==0) if case==CASES[0] else (
        complete and len(faults)==6 and result['ResumedConnections']==6 and all(o['caught_up_before_next'] for o in outages)) if case==CASES[1] else (
        not complete and len(faults)==6 and result['ResumeAttempts']==result['ResumedConnections']==5 and s['error_class']=='recovery_exhausted' and outages[-1]['observed_requests']==0 and not returned['data']['outer_expired'])
    return dict(case=case,completed=complete,error_class=s['error_class'],behavior_passed=behavior,
                timing_resource_quality=not quality,quality_issues=quality,captured_bytes=captured,received_bytes=received,
                processed_bytes=processed_bytes,missing_captured=[processed_bytes,captured] if captured>processed_bytes else None,
                uncaptured=[captured,total] if captured<total else None,applied_results=len(apps),attempts=result['ResumeAttempts'],
                resumed=result['ResumedConnections'],repeated_audio_bytes=sum(max(0,n-1)*CHUNK for n in coverage.values()),
                max_partial_gap_ns=max(partial_gaps,default=None),partial_waits_ns=stats(partial_waits),source_max_lag_ns=max(lags),
                max_sampling_gap_ns=max_gap,cleanup_after_return_ns=idle['elapsed_ns']-returned['elapsed_ns'],
                worker_exit_after_return_ns=exit['elapsed_ns']-returned['elapsed_ns'],outages=outages,resources=resource,growth_signals=growth,
                position_series=dict(captured=[dict(at_ns=r['elapsed_ns'],offset=r['data']['end']) for r in source],
                                     accepted=[dict(at_ns=r['elapsed_ns'],offset=r['data']['next_offset'],connection=r['data']['connection']) for r in by['proxy','audio_ack']],
                                     processed=[dict(at_ns=r['elapsed_ns'],offset=r['data']['offset']) for r in processed]))


def safe_relative(root,name):
    path=Path(name);require(not path.is_absolute() and '..' not in path.parts and str(path)==name,'unsafe archive path')
    require((root/path).resolve().is_relative_to(root.resolve()),'archive symlink leaves root')
    return root/path


def verify_manifest(root,m):
    mode,rounds=m['mode'],m['planned_rounds'];expected=order(rounds)
    require(mode in ('probe','formal') and (mode!='formal' or rounds==2),'collection mode/count differs')
    require(type(m['version']) is int and m['version']==1 and m['experiment']==EXPERIMENT and m['root_test']==ROOT_TEST and m['order']==expected and m['planned_sessions']==len(expected),'manifest identity/order differs')
    require(m['status']=='collected' and m['build_exit']==0 and m['source_unchanged'] is True,'collection failed or source drift')
    require(not any(flag=='-race' or flag.startswith('-race=') for flag in m['go_env']['GOFLAGS'].split()),'race collection')
    require(Path(m['collection_directory']).is_absolute(),'original root missing')
    require(m['build_argv']==['go','test','-c','-o',str(Path(m['collection_directory'])/'extended-resume.test'),'./internal/gateway'],'build command drift')
    require(m['source_sha256'] and PLAN in m['source_sha256'],'source/plan snapshot missing')
    mandatory={PLAN,'go.mod','go.sum','internal/gateway/extended_resume_harness_test.go','internal/gateway/extended_resume_log_test.go',
               'internal/gateway/extended_resume_collection_test.go','internal/wsclient/client_v2_run.go',
               'docs/experiments/scripts/run_extended_resume.py','docs/experiments/scripts/summarize_extended_resume.py',
               'docs/experiments/scripts/extended_resume_contract.py'}
    require(mandatory<=m['source_sha256'].keys(),'required implementation snapshot missing')
    for name,sha in m['source_sha256'].items():require(digest(safe_relative(root/'source',name))==sha,'source snapshot digest differs')
    required={'extended-resume.test','build.log','git-status.txt','preregistration.md'}
    for i,case in enumerate(expected,1):
        required|={f'sessions/{i:02d}_{case}/{name}' for name in ('stdout.log','stderr.log','data/events.jsonl','data/summary.json')}
    require(required<=m['artifacts_sha256'].keys(),'required artifact missing')
    for name,sha in m['artifacts_sha256'].items():require(digest(safe_relative(root,name))==sha,'artifact digest differs')
    require(digest(root/'preregistration.md')==digest(root/'source'/PLAN),'preregistration snapshot differs')
    require(len(m['slots'])==len(expected),'slot missing/duplicated')
    previous=None
    for i,(case,slot) in enumerate(zip(expected,m['slots']),1):
        relative=f'sessions/{i:02d}_{case}';original=Path(m['collection_directory'])/relative
        require(slot['index']==i and slot['round']==(1 if i<=3 else 2) and slot['case']==case and slot['path']==relative,'slot order/path differs')
        timeout=90 if mode=='probe' else 660
        require(slot['argv']==[str(Path(m['collection_directory'])/'extended-resume.test'),'-test.run=^'+ROOT_TEST+'$','-test.count=1',f'-test.timeout={timeout}s','-test.v'],'case command drift')
        require(slot['environment']==dict(TIDE_EXTENDED_MODE=mode,TIDE_EXTENDED_CASE=case,TIDE_EXTENDED_CASE_OUTPUT=str(original/'data'),TIDE_RUN_EXTENDED_FORMAL='1' if mode=='formal' else '0'),'case environment drift')
        require(slot['status']=='finished' and slot['exit_code']==0 and slot['source_unchanged'] is True,'case process failed/incomplete')
        begin,end=stamp_ns(slot['started_at']),stamp_ns(slot['finished_at']);require(begin<=end and (previous is None or begin>=previous),'case processes overlap');previous=end
        text=(root/relative/'stdout.log').read_text()
        require('--- PASS: '+ROOT_TEST+' (' in text and 'WARNING: DATA RACE' not in text,'case root did not pass')
        require((root/relative/'stderr.log').read_bytes()==b'','unexpected case stderr')
        for part in ('session_id','resume_token','resumeToken'):require(part not in text,'credentials in case log')
        require(f'go={m["go_env"]["GOVERSION"]} os={m["go_env"]["GOOS"]} arch={m["go_env"]["GOARCH"]}' in text and f'mode={mode} case={case}' in text,'build/run profile differs')
    if mode=='formal' and m['host']['system']=='Darwin':require(m.get('awake_guard')=='owned_caffeinate' and m.get('awake_guard_exit_before_cleanup') is None,'awake guard failed')


def summarize(root,output):
    root,output=Path(root),Path(output);m=json.loads((root/'manifest.json').read_text())
    mode,rounds=m['mode'],m['planned_rounds'];expected=order(rounds);require(mode in ('probe','formal'),'unknown mode')
    output.mkdir(parents=True,exist_ok=False)
    errors,verified=[],[]
    try:verify_manifest(root,m)
    except (ValueError,OSError,KeyError,TypeError) as exc:errors.append('manifest: '+str(exc))
    archive_valid=not errors
    for i,case in enumerate(expected,1):
        try:
            row=verify_case(root/f'sessions/{i:02d}_{case}'/'data',mode,case)
            row.update(index=i,round=1 if i<=3 else 2);verified.append(row)
        except (ValueError,OSError,KeyError,TypeError,IndexError) as exc:errors.append(f'slot {i} {case}: '+str(exc))
    # Evidence failures remain unknown in fixed denominators; corrupted archives do not authorize metrics.
    valid_rows=verified if archive_valid else []
    power=m.get('power_audit')
    if mode=='formal' and (not power or power.get('exit_code')!=0 or power.get('transitions')):
        for row in valid_rows:
            row['timing_resource_quality']=False
            row['quality_issues'].append('host_sleep_transition_or_missing_audit')
            for window in row['resources']:window['quality_ok']=False
    groups=[]
    for case in CASES:
        rows=[r for r in valid_rows if r['case']==case];groups.append(dict(case=case,planned=rounds,verified=len(rows),
            completed=sum(r['completed'] for r in rows),failed=sum(not r['completed'] for r in rows),unknown=rounds-len(rows),
            behavior_passed=sum(r['behavior_passed'] for r in rows),quality_passed=sum(r['timing_resource_quality'] for r in rows)))
    result=dict(experiment=EXPERIMENT,mode=mode,evidence_valid=not errors,planned_sessions=len(expected),verified_sessions=len(valid_rows),groups=groups,errors=errors,sessions=valid_rows)
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    for filename,rows in [('sessions', [{k:v for k,v in r.items() if k not in ('outages','resources','position_series')} for r in valid_rows]),
                          ('outages',[dict(index=r['index'],case=r['case'],**o) for r in valid_rows for o in r['outages']]),
                          ('resource-windows',[dict(index=r['index'],case=r['case'],**w) for r in valid_rows for w in r['resources']])]:
        fields=list(dict.fromkeys(k for row in rows for k in row))
        with (output/(filename+'.csv')).open('w',newline='') as stream:
            writer=csv.DictWriter(stream,fields);writer.writeheader();writer.writerows({k:json.dumps(v,sort_keys=True) if isinstance(v,(dict,list)) else v for k,v in row.items()} for row in rows)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('collection',type=Path);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();raise SystemExit(0 if summarize(args.collection,args.output)['evidence_valid'] else 1)
