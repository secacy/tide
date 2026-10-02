#!/usr/bin/env python3
"""复核双 Worker 短时参考点，在 16/18 场各做两次 120 秒观察。"""
import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize

DEFAULT_POINTS = Path(__file__).resolve().parent.parent/'results/dual-worker-short-operating-points-2026-10-02.json'


def extended_decision(sweep, refinement):
    """16/18 须三次短时达标，19 不得三次达标；固定选择 16 对照和 18 高负载。"""
    require(isinstance(sweep,list) and [g['planned_sessions'] for g in sweep] == [16,18,20], 'incomplete sweep groups')
    require(isinstance(refinement,list) and [g['planned_sessions'] for g in refinement] == [19], 'incomplete refinement groups')
    for g in sweep+refinement:
        p=g['batches_meeting_criteria']
        require(type(p) is int and 0<=p<=3 and g['attempts']==g['planned_sessions']*3, 'invalid short repetitions')
        require(type(g['all_repetitions_meet_criteria']) is bool and g['all_repetitions_meet_criteria']==(p==3), 'inconsistent short criteria')
    require(sweep[0]['batches_meeting_criteria']==sweep[1]['batches_meeting_criteria']==3 and refinement[0]['batches_meeting_criteria']<3, 'short evidence does not justify fixed 16/18 observation')
    return dict(rule_version=1,normal=16,near_boundary=18,overload=19,reason='highest_repeated_short_pass_with_lower_load_control')


def validate_selection(selection):
    """由清单中的来源分组复算决定，并核对参考点数字；分析时拒绝变更选点。"""
    sweep,refinement=selection.get('source_sweep_groups'),selection.get('source_refinement_groups')
    decision=extended_decision(sweep,refinement)
    require(all(selection.get(k)==v for k,v in decision.items()), 'extended selection differs')
    doc=selection.get('source_points',{})
    require(type(doc.get('schema_version')) is int and doc['schema_version']==1 and doc.get('status')=='short_reference_points_established', 'not a short reference document')
    require(type(doc.get('worker_count')) is int and doc['worker_count']==2 and doc.get('policy')=='round_robin', 'reference topology differs')
    from worker_comparison_contract import WORKER, GATEWAY, CRITERIA
    require(doc.get('worker')==WORKER and doc.get('gateway')==GATEWAY and doc.get('criteria')==CRITERIA, 'reference fixture differs')
    require(doc.get('input')==dict(bytes_per_second=32000,chunk_bytes=3200,realtime=True), 'reference input differs')
    expected=[]
    for role,n,key,groups in [('normal_candidate',16,'sweep',sweep),('near_boundary_candidate',18,'sweep',sweep),('overload_candidate',19,'refinement',refinement)]:
        g=next(g for g in groups if g['planned_sessions']==n)
        expected.append(dict(role=role,sessions=n,source=key,audio_duration_s=20,repetitions=3,attempts=g['attempts'],
                             outcomes=g['outcomes'],batches_meeting_criteria=g['batches_meeting_criteria'],
                             completed_only_tail=g['tail_completed_only'],max_audio_schedule_lag_ns=g['max_audio_schedule_lag_ns']))
    require(doc.get('points')==expected, 'reference points differ from source groups')
    return decision


def selection_from_points(path):
    """逐来源查哈希并在临时副本复算；旧文件保持原样，拒绝越界来源目录。"""
    doc=json.loads(path.read_text())
    require(set(doc.get('sources',{}))=={'sweep','refinement'}, 'missing source reference')
    results,manifests={},{}
    for key,experiment in [('sweep','dual_worker_sweep'),('refinement','dual_worker_boundary_short')]:
        ref=doc['sources'][key];source=(path.parent/ref['directory']).resolve()
        require(source.is_relative_to(path.parent.resolve()), 'reference directory escapes results')
        m=json.loads((source/'manifest.json').read_text())
        require(m.get('experiment')==experiment and m.get('status')=='passed', 'source is not completed expected experiment')
        require(m['source_commit']==ref['source_commit'] and digest(source/'manifest.json')==ref['manifest_sha256'] and digest(source/'groups.json')==ref['groups_sha256'], 'source hashes differ')
        a=json.loads((source/'analysis-manifest.json').read_text())
        require(a['input_sha256']['manifest.json']==ref['manifest_sha256'] and a['groups_sha256']==ref['groups_sha256'], 'source analysis references differ')
        with tempfile.TemporaryDirectory(prefix='tide-dual-extended-reference-') as t:
            target=Path(t)/'source';shutil.copytree(source,target)
            with contextlib.redirect_stdout(io.StringIO()):result=summarize(target)
        require(result==json.loads((source/'groups.json').read_text()), 'source analysis differs from raw evidence')
        for k in ('worker','gateway','criteria'):
            require(doc.get(k)==m[k], 'reference fixture differs: '+k)
        results[key],manifests[key]=result,m
    require(manifests['sweep']['source_sha256']==manifests['refinement']['source_sha256'], 'reference Go source differs')
    require(doc.get('input')==dict(bytes_per_second=32000,chunk_bytes=3200,realtime=True), 'reference input differs')
    decision=extended_decision(results['sweep']['groups'],results['refinement']['groups'])
    selection=dict(**decision,source_points_path=str(path.resolve()),source_points_sha256=digest(path),source_points=doc,
                   source_sweep_groups=results['sweep']['groups'],source_refinement_groups=results['refinement']['groups'],
                   source_sha256=manifests['sweep']['source_sha256'])
    validate_selection(selection)
    return selection


def run_extended(directory,points=DEFAULT_POINTS):
    """先验证短时来源，再执行四批；全部失败也保留，不按结果改变档位。"""
    selection=selection_from_points(points)
    run(directory,pressure=True,profile=pressure_profile('dual_worker_extended'),worker_count=2,selection=selection)
    return summarize(directory)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True,help='new directory; preserve all attempts')
    parser.add_argument('--points',type=Path,default=DEFAULT_POINTS,help='short reference points with source hashes')
    parser.add_argument('--analyze-only',action='store_true',help='recheck completed observation without producing load')
    args=parser.parse_args();directory=args.output.resolve()
    if args.analyze_only:
        require(json.loads((directory/'manifest.json').read_text()).get('experiment')=='dual_worker_extended','not an extended observation')
        summarize(directory)
    else:
        run_extended(directory,args.points.resolve())
