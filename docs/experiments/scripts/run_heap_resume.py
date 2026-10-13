#!/usr/bin/env python3
"""Collect EXP-006-06 heap diagnostics; each case is an owned independent process."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from heap_resume_contract import EXPERIMENT, PLAN, ROOT_TEST, order, environment, deadlines
from run_group_resume import REPO, now, source_hashes
from summarize_client_resume import digest
from run_extended_resume import audit_power


def collect(output, mode='probe', repo=REPO):
    """Preserve every slot on failure; never replace it with a successful run."""
    sequence = order(mode)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    m = dict(version=1, experiment=EXPERIMENT, mode=mode, planned_rounds=1 if mode=='probe' else 2,
             planned_sessions=len(sequence), order=sequence, collection_directory=str(output), root_test=ROOT_TEST,
             started_at=now(), status='initializing', build_exit=None, source_sha256={}, source_unchanged=False,
             artifacts_sha256={}, slots=[], host=dict(system=platform.system(), machine=platform.machine(), cpus=os.cpu_count()),
             runtime_environment={k: os.environ.get(k) for k in ('GOMAXPROCS','GOGC','GOMEMLIMIT','GODEBUG')})
    awake = None
    try:
        m['commit'] = subprocess.check_output(['git','rev-parse','HEAD'], cwd=repo).decode().strip()
        (output/'git-status.txt').write_bytes(subprocess.check_output(['git','status','--short'], cwd=repo))
        m['source_sha256'] = source_hashes(repo, PLAN)
        for name in m['source_sha256']:
            target=output/'source'/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(repo/name,target)
        shutil.copyfile(output/'source'/PLAN,output/'preregistration.md')
        m['go_env']=json.loads(subprocess.check_output(['go','env','-json','GOVERSION','GOOS','GOARCH','GOFLAGS'],cwd=repo))
        if any(flag=='-race' or flag.startswith('-race=') for flag in m['go_env']['GOFLAGS'].split()): raise ValueError('collector requires non-race build')
        if source_hashes(repo,PLAN)!=m['source_sha256']:raise ValueError('source changed while snapshotting')
        binary=str(output/'heap-resume.test')
        m['build_argv']=['go','test','-c','-o',binary,'./internal/gateway']
        with (output/'build.log').open('wb') as stream:
            m['build_exit']=subprocess.run(m['build_argv'],cwd=repo,stdout=stream,stderr=subprocess.STDOUT,timeout=180).returncode
        if m['build_exit'] != 0:
            m['status']='build_failed'
        else:
            if source_hashes(repo,PLAN)!=m['source_sha256']:raise ValueError('source changed during build')
            if mode=='formal' and platform.system()=='Darwin':
                awake=subprocess.Popen(['/usr/bin/caffeinate','-i','-s'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                m['awake_guard']='owned_caffeinate'
            else:m['awake_guard']='not_requested_for_probe' if mode=='probe' else 'external_wake_policy'
            m['run_started_at']=now()
            for i,case in enumerate(sequence,1):
                name=f'sessions/{i:02d}_{case}'; directory=output/name; directory.mkdir(parents=True)
                timeout,external_timeout=deadlines(mode)
                command=[binary,'-test.run=^'+ROOT_TEST+'$','-test.count=1',f'-test.timeout={timeout}s','-test.v']
                env_overrides=environment(mode,case,directory/'data',os.environ.get('GODEBUG',''))
                slot=dict(index=i,round=1 if i<=2 else 2,case=case,path=name,argv=command,environment=env_overrides,
                          started_at=now(),exit_code=None,status='running',external_timeout_s=external_timeout)
                m['slots'].append(slot)
                env=os.environ.copy();env.update(env_overrides)
                # Direct binary has no test2json wrapper or helper child; TimeoutExpired terminates and waits for this owned process.
                try:
                    with (directory/'stdout.log').open('wb') as stream,(directory/'stderr.log').open('wb') as errors:
                        slot['exit_code']=subprocess.run(command,cwd=repo,env=env,stdout=stream,stderr=errors,timeout=external_timeout).returncode
                    slot['status']='finished'
                except subprocess.TimeoutExpired:
                    slot['status']='timeout'
                slot['finished_at']=now()
                slot['source_unchanged']=source_hashes(repo,PLAN)==m['source_sha256']
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
        try:m['source_unchanged']=bool(m['source_sha256']) and source_hashes(repo,PLAN)==m['source_sha256']
        except (OSError,subprocess.SubprocessError):m['source_unchanged']=False
        m['finished_at']=now()
        m['artifacts_sha256']={str(p.relative_to(output)):digest(p) for p in output.rglob('*')
                              if p.is_file() and 'source' not in p.relative_to(output).parts and p.name!='manifest.json'}
        (output/'manifest.json').write_text(json.dumps(m,indent=2)+'\n')
    from summarize_heap_resume import summarize
    result=summarize(output,output/'analysis')
    print(json.dumps({k:result[k] for k in ('mode','evidence_valid','planned_sessions','business_valid','checkpoints_valid','exported','errors')}))
    return 0 if result['evidence_valid'] else 1


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=('probe','formal'),default='probe')
    args=parser.parse_args()
    raise SystemExit(collect(args.output,args.mode))
