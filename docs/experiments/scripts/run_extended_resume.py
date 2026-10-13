#!/usr/bin/env python3
"""Collect EXP-006-05 with one build and one owned process per case."""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess

import extended_resume_contract as legacy_profile
from extended_resume_contract import CASES, EXPERIMENT, PLAN, ROOT_TEST, order
from run_group_resume import REPO, now, source_hashes
from summarize_client_resume import digest, stamp_ns


def audit_power(begin,end):
    """Read only Sleep/Wake/DarkWake transitions in the owned run window; discard unrelated system log text."""
    if platform.system()!='Darwin':return dict(exit_code=None,transitions=[],reason='unsupported_host_audit')
    from datetime import datetime,timezone
    try:
        result=subprocess.run(['/usr/bin/pmset','-g','log'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=20)
        transitions=[]
        for line in result.stdout.decode(errors='replace').splitlines():
            match=re.match(r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d [+-]\d{4})\s+(Sleep|Wake|DarkWake)\s+',line)
            if not match:continue
            utc=datetime.strptime(match[1],'%Y-%m-%d %H:%M:%S %z').astimezone(timezone.utc).isoformat()
            if stamp_ns(begin)-1_000_000_000<=stamp_ns(utc)<=stamp_ns(end)+1_000_000_000:
                transitions.append(dict(at_utc=utc,kind=match[2]))
        return dict(exit_code=result.returncode,transitions=transitions)
    except (OSError,subprocess.SubprocessError):return dict(exit_code=None,transitions=[],reason='audit_failed')


def collect(output, mode='probe', rounds=1, repo=REPO, profile=legacy_profile):
    """Preserve every slot on failure; never replace it with a successful run."""
    if mode not in ('probe', 'formal') or mode == 'formal' and rounds != 2:
        raise ValueError('formal requires two rounds')
    sequence = profile.order(rounds)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    m = dict(version=1, experiment=profile.EXPERIMENT, mode=mode, planned_rounds=rounds,
             planned_sessions=len(sequence), order=sequence, collection_directory=str(output), root_test=profile.ROOT_TEST,
             started_at=now(), status='initializing', build_exit=None, source_sha256={}, source_unchanged=False,
             artifacts_sha256={}, slots=[], host=dict(system=platform.system(), machine=platform.machine(), cpus=os.cpu_count()),
             runtime_environment={k: os.environ.get(k) for k in ('GOMAXPROCS','GOGC','GOMEMLIMIT','GODEBUG')})
    awake = None
    try:
        if hasattr(profile, 'validate_environment'):profile.validate_environment(mode, m['runtime_environment'])
        m['commit'] = subprocess.check_output(['git','rev-parse','HEAD'], cwd=repo).decode().strip()
        (output/'git-status.txt').write_bytes(subprocess.check_output(['git','status','--short'], cwd=repo))
        m['source_sha256'] = source_hashes(repo, profile.PLAN)
        for name in m['source_sha256']:
            target=output/'source'/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(repo/name,target)
        shutil.copyfile(output/'source'/profile.PLAN,output/'preregistration.md')
        m['go_env']=json.loads(subprocess.check_output(['go','env','-json','GOVERSION','GOOS','GOARCH','GOFLAGS'],cwd=repo))
        if any(flag=='-race' or flag.startswith('-race=') for flag in m['go_env']['GOFLAGS'].split()): raise ValueError('collector requires non-race build')
        if source_hashes(repo,profile.PLAN)!=m['source_sha256']:raise ValueError('source changed while snapshotting')
        binary=str(output/'extended-resume.test')
        m['build_argv']=['go','test','-c','-o',binary,'./internal/gateway']
        with (output/'build.log').open('wb') as stream:
            m['build_exit']=subprocess.run(m['build_argv'],cwd=repo,stdout=stream,stderr=subprocess.STDOUT,timeout=180).returncode
        if m['build_exit'] != 0:
            m['status']='build_failed'
        else:
            if source_hashes(repo,profile.PLAN)!=m['source_sha256']:raise ValueError('source changed during build')
            if mode=='formal' and platform.system()=='Darwin':
                awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-s'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                m['awake_guard']='owned_caffeinate'
            else:m['awake_guard']='not_requested_for_probe' if mode=='probe' else 'external_wake_policy'
            m['run_started_at']=now()
            for i,case in enumerate(sequence,1):
                name=f'sessions/{i:02d}_{case}'; directory=output/name; directory.mkdir(parents=True)
                timeout=90 if mode=='probe' else 660
                command=[binary,'-test.run=^'+profile.ROOT_TEST+'$','-test.count=1',f'-test.timeout={timeout}s','-test.v']
                env_overrides=profile.environment(mode, case, directory/'data')
                slot=dict(index=i,round=(i-1)//len(profile.CASES)+1,case=case,path=name,argv=command,environment=env_overrides,
                          started_at=now(),exit_code=None,status='running')
                m['slots'].append(slot)
                env=os.environ.copy();env.update(env_overrides)
                # Direct binary has no test2json wrapper or helper child; TimeoutExpired terminates and waits for this owned process.
                try:
                    with (directory/'stdout.log').open('wb') as stream,(directory/'stderr.log').open('wb') as errors:
                        slot['exit_code']=subprocess.run(command,cwd=repo,env=env,stdout=stream,stderr=errors,timeout=timeout+5).returncode
                    slot['status']='finished'
                except subprocess.TimeoutExpired:
                    slot['status']='timeout'
                slot['finished_at']=now()
                slot['source_unchanged']=source_hashes(repo,profile.PLAN)==m['source_sha256']
                if not slot['source_unchanged']:raise ValueError('source changed during case')
            m['status']='collected'
    except (OSError,ValueError,subprocess.SubprocessError) as exc:
        m['status']='collection_failed';m['collection_error']=type(exc).__name__
    finally:
        m['run_finished_at']=now()
        if mode=='formal' and 'run_started_at' in m:m['power_audit']=audit_power(m['run_started_at'],m['run_finished_at'])
        if awake is not None:
            m['awake_guard_exit_before_cleanup']=awake.poll()
            if awake.poll() is None:awake.terminate()
            try:awake.wait(timeout=5)
            except subprocess.TimeoutExpired:awake.kill();awake.wait(timeout=5)
        try:m['source_unchanged']=bool(m['source_sha256']) and source_hashes(repo,profile.PLAN)==m['source_sha256']
        except (OSError,subprocess.SubprocessError):m['source_unchanged']=False
        m['finished_at']=now()
        m['artifacts_sha256']={str(p.relative_to(output)):digest(p) for p in output.rglob('*')
                              if p.is_file() and 'source' not in p.relative_to(output).parts and p.name!='manifest.json'}
        (output/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
    from summarize_extended_resume import summarize
    result=summarize(output,output/'analysis',profile=profile)
    print(json.dumps({k:result[k] for k in ('mode','evidence_valid','planned_sessions','verified_sessions','errors')}))
    return 0 if result['evidence_valid'] else 1


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=('probe','formal'),default='probe')
    parser.add_argument('--count',type=int,default=1)
    args=parser.parse_args()
    raise SystemExit(collect(args.output,args.mode,args.count))
