#!/usr/bin/env python3
"""Verify heap checkpoints against raw business events; preserve complete pprof reports."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from heap_resume_contract import (EXPERIMENT, PLAN, ROOT_TEST, CASES, POINTS, INDICES, DIFFS,
                                  MEMORY, RESOURCES, order, environment, deadlines)
from summarize_extended_resume import verify_case, read_events, integer, safe_relative, same
from summarize_client_resume import digest, require, stamp_ns

CHECKPOINT_FIELDS = set('version name planned_ns start_ns end_ns fixture_ns rate_before rate_after gc_requested before after_gc after_write resources_before resources_after file bytes sha256 write_error close_error valid reason'.split())
REPORT_FIELDS = set('version experiment case origin_utc fixture_origin_ns first_ns returned_ns tcp_cleanup owned_joined detached child_ok checkpoints'.split())


def verify_checkpoints(directory, mode, case):
    """Require all five phases, exact metadata/files and recorded teardown facts; existence alone is insufficient."""
    order(mode)
    require(case in CASES, 'unknown heap case')
    directory = Path(directory)
    h = json.loads((directory/'heap-report.json').read_text())
    require(set(h) == REPORT_FIELDS and same(h['version'], 1) and h['experiment'] == EXPERIMENT and h['case'] == case, 'heap report schema/identity differs')
    stamp_ns(h['origin_utc'])
    origin = integer(h['fixture_origin_ns'], 'invalid fixture origin')
    first = integer(h['first_ns'], 'missing first capture')
    returned = integer(h['returned_ns'], 'invalid returned timestamp')
    require(same(h['tcp_cleanup'], dict(client_closed=True, serve_exited=True)) and h['owned_joined'] is True and h['detached'] is True and h['child_ok'] is True, 'fixture teardown not confirmed')
    rows = read_events(directory/'events.jsonl')
    source = next(r for r in rows if r['component']=='source' and r['kind']=='read')
    idle = next(r for r in rows if r['component']=='run' and r['kind']=='natural_idle')
    terminal = json.loads((directory/'summary.json').read_text())
    require(first == source['elapsed_ns'] and returned == terminal['returned_ns'], 'heap/event timeline differs')
    require(type(h['checkpoints']) is list and len(h['checkpoints'])==5, 'five checkpoints required')
    previous = None
    for i, (name, c) in enumerate(zip(POINTS, h['checkpoints'])):
        require(set(c)==CHECKPOINT_FIELDS and same(c['version'], 1) and c['name']==name and c['file']==name+'.pb.gz', 'checkpoint schema/order/path differs')
        start, end = integer(c['start_ns'], 'invalid start'), integer(c['end_ns'], 'invalid end')
        require(start <= end and (previous is None or previous <= start), 'checkpoint overlap/order differs')
        previous = end
        require(c['valid'] is True and c['reason']==c['write_error']==c['close_error']=='', 'invalid checkpoint capture or phase')
        require(same(c['rate_before'], 1) and same(c['rate_after'], 1) and same(c['gc_requested'], 2), 'profile rate/GC differs')
        for key in ('before', 'after_gc', 'after_write'):
            require(set(c[key])==set(MEMORY), 'memory fields differ')
            for value in c[key].values():integer(value, 'invalid memory number')
        require(c['after_gc']['num_gc'] >= c['before']['num_gc']+2 and c['after_write']['num_gc']>=c['after_gc']['num_gc'], 'GC not observed')
        for key in ('resources_before', 'resources_after'):
            require(set(c[key])==set(RESOURCES), 'resource fields differ')
            for value in c[key].values():integer(value, 'invalid resource number')
        if i in (1, 2):
            target=origin+first+((2 if i==1 else 10) if mode=='probe' else (90 if i==1 else 570))*1_000_000_000
            require(integer(c['planned_ns'], 'missing planned target')==target and 0<=start-target<=1_000_000_000 and end<origin+returned, 'active target late/crosses return')
            for key in ('resources_before','resources_after'):
                require(all(c[key][k]==1 for k in ('sessions','registry','worker_active')), 'active session absent')
        else:require(c['planned_ns'] is None, 'non-active target fabricated')
        if i in (1,2,3):require(integer(c['fixture_ns'],'missing fixture timestamp')==start-origin, 'fixture time alignment differs')
        else:require(c['fixture_ns'] is None, 'fixture-relative P0/P4 fabricated')
        if i==0:require(end<origin, 'P0 after fixture began')
        if i==3:
            require(start>=origin+idle['elapsed_ns'] and end<=origin+rows[-1]['elapsed_ns'], 'P3 outside idle-before-teardown interval')
            require(all(v==0 for key in ('resources_before','resources_after') for v in c[key].values()), 'P3 resources not idle')
        if i==4:require(start>origin+rows[-1]['elapsed_ns'], 'P4 before fixture join')
        profile=directory/c['file']
        require(integer(c['bytes'],'invalid profile length',1)==profile.stat().st_size and digest(profile)==c['sha256'], 'profile missing/truncated/digest differs')
        require(same(json.loads((directory/(name+'.json')).read_text()), c), 'checkpoint standalone metadata differs')
    return h


def export_profiles(directory, binary, output):
    """Parse every binary profile with pprof; keep negatives and all nodes. Never normalize differences."""
    directory, binary, output = Path(directory), Path(binary), Path(output)
    output.mkdir(parents=True, exist_ok=False)
    # Relative argv/cwd keep symbolized reports reproducible when an archive is moved.
    working = binary.parent.resolve()
    profile_dir = Path(os.path.relpath(directory.resolve(), working))
    executable = './'+binary.name
    commands = []
    specs = [(name, None) for name in POINTS]+list(DIFFS)
    for name, base in specs:
        stem = name if base is None else name+'-minus-'+base
        for index in INDICES:
            filename = stem+'.'+index+'.top.txt'
            command = ['go','tool','pprof','-top','-nodecount=0','-nodefraction=0','-edgefraction=0',
                       '-drop_negative=false','-normalize=false','-sample_index='+index]
            if base is not None:command += ['-base='+str(profile_dir/(base+'.pb.gz'))]
            command += [executable,str(profile_dir/(name+'.pb.gz'))]
            with (output/filename).open('wb') as out, (output/(filename+'.stderr')).open('wb') as err:
                try:code=subprocess.run(command,cwd=working,stdout=out,stderr=err,timeout=30).returncode
                except (OSError,subprocess.TimeoutExpired):code=None
            commands.append(dict(argv=command,cwd='collection',exit_code=code,output=filename))
        filename=stem+'.raw.txt'
        command=['go','tool','pprof','-raw','-drop_negative=false','-normalize=false']
        if base is not None:command += ['-base='+str(profile_dir/(base+'.pb.gz'))]
        command += [executable,str(profile_dir/(name+'.pb.gz'))]
        with (output/filename).open('wb') as out, (output/(filename+'.stderr')).open('wb') as err:
            try:code=subprocess.run(command,cwd=working,stdout=out,stderr=err,timeout=30).returncode
            except (OSError,subprocess.TimeoutExpired):code=None
        commands.append(dict(argv=command,cwd='collection',exit_code=code,output=filename))
    (output/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
    require(all(c['exit_code']==0 for c in commands), 'pprof parse/export failed; raw profiles preserved')
    # pprof can succeed with partial symbol resolution; flag that separately from profile parse success.
    warnings = [c['output'] for c in commands if (output/(c['output']+'.stderr')).stat().st_size]
    return dict(commands=len(commands), stderr_reports=warnings, reports_sha256={str(p.relative_to(output)):digest(p) for p in output.iterdir() if p.is_file()})


def verify_manifest(root, m):
    expected=order(m['mode']);mode=m['mode']
    require(same(m['version'],1) and m['experiment']==EXPERIMENT and m['root_test']==ROOT_TEST and same(m['planned_sessions'],len(expected)) and m['order']==expected and same(m['planned_rounds'],1 if mode=='probe' else 2), 'heap manifest identity/order differs')
    require(m['status']=='collected' and same(m['build_exit'],0) and m['source_unchanged'] is True, 'collection failed/source drift')
    require(not any(x=='-race' or x.startswith('-race=') for x in m['go_env']['GOFLAGS'].split()), 'race diagnostic collection')
    require(m['source_sha256'] and PLAN in m['source_sha256'], 'frozen sources/plan missing')
    for name,sha in m['source_sha256'].items():require(digest(safe_relative(root/'source',name))==sha,'source digest differs')
    mandatory={PLAN,'go.mod','go.sum','internal/gateway/heap_diagnostic_test.go','internal/gateway/extended_resume_harness_test.go',
               'internal/gateway/network_fixture_test.go','docs/experiments/scripts/heap_resume_contract.py',
               'docs/experiments/scripts/run_heap_resume.py','docs/experiments/scripts/summarize_heap_resume.py'}
    require(mandatory<=m['source_sha256'].keys(),'required implementation snapshot missing')
    require(digest(root/'preregistration.md')==digest(root/'source'/PLAN),'frozen plan differs')
    required={'heap-resume.test','build.log','git-status.txt','preregistration.md'}
    for i,case in enumerate(expected,1):
        base=f'sessions/{i:02d}_{case}'
        required|={base+'/'+name for name in ('stdout.log','stderr.log','data/events.jsonl','data/summary.json','data/heap-report.json')}
        required|={base+'/data/'+name+suffix for name in POINTS for suffix in ('.json','.pb.gz')}
    require(required<=m['artifacts_sha256'].keys(),'required artifact missing')
    for name,sha in m['artifacts_sha256'].items():require(digest(safe_relative(root,name))==sha,'artifact digest differs')
    require(len(m['slots'])==len(expected),'missing/extra slots')
    original=Path(m['collection_directory']);require(original.is_absolute(),'original root missing')
    require(m['build_argv']==['go','test','-c','-o',str(original/'heap-resume.test'),'./internal/gateway'],'build command drift')
    previous=None
    for i,(case,s) in enumerate(zip(expected,m['slots']),1):
        relative=f'sessions/{i:02d}_{case}';seconds,external=deadlines(mode)
        require(same(s['index'],i) and same(s['round'],1 if i<=2 else 2) and s['case']==case and s['path']==relative,'slot identity differs')
        require(s['argv']==[str(original/'heap-resume.test'),'-test.run=^'+ROOT_TEST+'$','-test.count=1',f'-test.timeout={seconds}s','-test.v'],'run command drift')
        require(same(s['environment'],environment(mode,case,original/relative/'data',m['runtime_environment']['GODEBUG'] or '')) and s['external_timeout_s']==external,'child settings differ')
        require(s['status']=='finished' and same(s['exit_code'],0) and s['source_unchanged'] is True,'slot process failed')
        begin,end=stamp_ns(s['started_at']),stamp_ns(s['finished_at']);require(begin<=end and (previous is None or previous<=begin),'slot overlap');previous=end
        text=(root/relative/'stdout.log').read_text()
        require('--- PASS: '+ROOT_TEST+' (' in text and 'WARNING: DATA RACE' not in text and f'go={m["go_env"]["GOVERSION"]} mode={mode} case={case} rate=1' in text,'runtime/pass evidence differs')
        require((root/relative/'stderr.log').read_bytes()==b'','unexpected child stderr')
    if mode=='formal' and m['host']['system']=='Darwin':
        require(m.get('awake_guard')=='owned_caffeinate' and m.get('awake_guard_exit_before_cleanup') is None,'awake guard failed')
        require(m.get('power_audit',{}).get('exit_code')==0 and not m['power_audit']['transitions'],'sleep audit missing/transition observed')


def summarize(root, output, export=True):
    root,output=Path(root),Path(output);m=json.loads((root/'manifest.json').read_text());expected=order(m['mode'])
    output.mkdir(parents=True,exist_ok=False)
    errors=[];cases=[]
    try:verify_manifest(root,m)
    except (OSError,ValueError,TypeError,KeyError) as exc:errors.append('manifest: '+str(exc))
    archive_ok=not errors
    tool=subprocess.run(['go','env','GOVERSION'],capture_output=True,text=True,timeout=15)
    tool_version=tool.stdout.strip()
    tool_ok=tool.returncode==0 and tool_version==m.get('go_env',{}).get('GOVERSION')
    for i,case in enumerate(expected,1):
        directory=root/f'sessions/{i:02d}_{case}'/'data';row=dict(index=i,case=case,business_valid=False,checkpoints_valid=False,export_valid=False)
        try:
            business=verify_case(directory,m['mode'],case)
            require(business['completed'] and business['resumed']==(0 if case=='A_normal' else 6),'diagnostic business did not complete planned recoveries')
            # Timing under profiling is descriptive; not a performance acceptance comparison.
            row.update(business_valid=True,business=business)
        except (OSError,ValueError,TypeError,KeyError,IndexError) as exc:errors.append(f'slot {i} business: {exc}')
        try:
            row['heap']=verify_checkpoints(directory,m['mode'],case);row['checkpoints_valid']=True
        except (OSError,ValueError,TypeError,KeyError,IndexError,StopIteration) as exc:errors.append(f'slot {i} heap: {exc}')
        if export and archive_ok and row['checkpoints_valid']:
            try:
                require(tool_ok,'analysis Go version differs')
                row['exports']=export_profiles(directory,root/'heap-resume.test',output/f'{i:02d}_{case}')
                row['export_valid']=True
            except (OSError,ValueError,subprocess.SubprocessError) as exc:errors.append(f'slot {i} export: {exc}')
        cases.append(row)
    result=dict(version=1,experiment=EXPERIMENT,mode=m['mode'],planned_sessions=len(expected),archive_valid=archive_ok,
                business_valid=sum(c['business_valid'] for c in cases),checkpoints_valid=sum(c['checkpoints_valid'] for c in cases),
                exported=sum(c['export_valid'] for c in cases),analysis_go_version=tool_version,export_requested=export,
                evidence_valid=archive_ok and not errors and all(c['business_valid'] and c['checkpoints_valid'] and (c['export_valid'] or not export) for c in cases),
                errors=errors,cases=cases,scope='diagnostic under full allocation profiling and forced GC; no leak/performance verdict')
    (output/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    args=p.parse_args();result=summarize(args.input,args.output)
    print(json.dumps({k:result[k] for k in ('evidence_valid','planned_sessions','business_valid','checkpoints_valid','exported','errors')}))
    raise SystemExit(0 if result['evidence_valid'] else 1)
