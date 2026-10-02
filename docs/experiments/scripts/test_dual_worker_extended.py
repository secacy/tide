"""双实例长窗口、参考点来源和历史兼容验证；合成样本不作为性能结果。"""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from run_dual_worker_extended import DEFAULT_POINTS, extended_decision, selection_from_points, validate_selection
from run_joint_observation_baseline import run
from summarize_pressure_sweep import phase_observations, pressure_profile, summarize
from test_dual_worker_boundary import groups as sweep_groups
from test_worker_comparison import fixture as comparison_fixture
from worker_comparison_contract import validate_manifest


def refinement(p=0):
    return [dict(planned_sessions=19,attempts=57,batches_meeting_criteria=p,all_repetitions_meet_criteria=p==3)]


def fixture():
    m=comparison_fixture(2);m['experiment']='dual_worker_extended'
    m['schedule'].update(session_counts=[16,18],repetitions=2,audio_bytes=3840000,session_timeout_s=140)
    m['sampling']['duration_ns']=150_000_000_000
    m['batches']=[m['batches'][i] for i in (0,1,2,4,5)]
    artifacts={}
    for b in m['batches']:
        old=b['label'];new=old
        if old!='warmup':
            rep,n=old.split('-n');new=rep+'-n'+str(int(n)*2);b['planned_sessions']*=2
        b['label']=new
        for item in b['sampling'].values():
            item['directory']=item['directory'].replace(old,new);item['command'][-1]=item['command'][-1].replace(old,new)
            if old!='warmup':item['command'][4]='-duration=150s'
        for k,v in m['artifact_sha256'].items():
            if k.startswith(old+'-') or k in (old+'.json',old+'.log'):artifacts[k.replace(old,new)]=v
    m['artifact_sha256']=artifacts
    return m


class DualWorkerExtendedTests(unittest.TestCase):
    def test_fixed_long_profile_and_contract(self):
        self.assertEqual(pressure_profile('dual_worker_extended'),dict(experiment='dual_worker_extended',counts=(16,18),repetitions=2,audio_bytes=3840000,timeout_s=140,duration_s=150))
        m=fixture();before=copy.deepcopy(m);validate_manifest(m);self.assertEqual(m,before)
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'attempt'
            with self.assertRaises(ValueError):run(p,pressure=True,profile=pressure_profile('dual_worker_extended'),worker_count=1)
            self.assertFalse(p.exists())

    def test_short_budgets_missing_batches_and_worker_evidence_rejected(self):
        for mode in ('audio','timeout','duration','sampler_duration','repetitions','missing_batch','wrong_order','missing_worker','forced'):
            m=fixture()
            if mode=='audio':m['schedule']['audio_bytes']=640000
            elif mode=='timeout':m['schedule']['session_timeout_s']=35
            elif mode=='duration':m['sampling']['duration_ns']=40_000_000_000
            elif mode=='sampler_duration':m['batches'][1]['sampling']['worker2']['command'][4]='-duration=40s'
            elif mode=='repetitions':m['schedule']['repetitions']=3
            elif mode=='missing_batch':m['batches'].pop()
            elif mode=='wrong_order':m['batches'][1:3]=reversed(m['batches'][1:3])
            elif mode=='missing_worker':del m['batches'][1]['sampling']['worker2']
            else:m['batches'][1]['sampling']['worker2']['forced_cleanup']=True
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_selection_requires_repeated_pass_and_preserves_partial_upper_point(self):
        for p in (0,1,2):
            g=sweep_groups();r=refinement(p);before=copy.deepcopy((g,r));d=extended_decision(g,r)
            self.assertEqual((d['normal'],d['near_boundary'],d['overload']),(16,18,19));self.assertEqual((g,r),before)
        for g,r in [(sweep_groups((3,2,0)),refinement()),(sweep_groups(),refinement(3)),(sweep_groups()[:2],refinement()),(sweep_groups(),[])]:
            with self.assertRaises(ValueError):extended_decision(g,r)
        g=sweep_groups();g[0]['batches_meeting_criteria']=True
        with self.assertRaises(ValueError):extended_decision(g,refinement())

    def test_real_reference_points_and_decision_tampering(self):
        s=selection_from_points(DEFAULT_POINTS);validate_selection(s)
        self.assertEqual(s['near_boundary'],18)
        for mode in ('normal','near','point_numbers','point_fixture','source_counts','point_status'):
            changed=copy.deepcopy(s)
            if mode=='normal':changed['normal']=18
            elif mode=='near':changed['near_boundary']=19
            elif mode=='point_numbers':changed['source_points']['points'][1]['outcomes']['completed']-=1
            elif mode=='point_fixture':changed['source_points']['worker']['processing_concurrency']=2
            elif mode=='source_counts':changed['source_sweep_groups'][1]['attempts']-=1
            else:changed['source_points']['status']='running'
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_selection(changed)

    def test_file_reference_tampering_rejected(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t);doc=json.loads(DEFAULT_POINTS.read_text())
            for ref in doc['sources'].values():shutil.copytree(DEFAULT_POINTS.parent/ref['directory'],p/ref['directory'])
            points=p/'points.json';points.write_text(json.dumps(doc))
            self.assertEqual(selection_from_points(points)['near_boundary'],18)
            for mode in ('manifest_hash','raw','point_outcome','path'):
                changed=copy.deepcopy(doc)
                raw=p/doc['sources']['refinement']['directory']/'r1-n19-worker2/samples.jsonl';original=raw.read_bytes()
                if mode=='manifest_hash':changed['sources']['sweep']['manifest_sha256']='changed'
                elif mode=='raw':raw.write_bytes(b'')
                elif mode=='point_outcome':changed['points'][0]['attempts']=99
                else:changed['sources']['sweep']['directory']='../escape'
                points.write_text(json.dumps(changed))
                with self.subTest(mode=mode),self.assertRaises(ValueError):selection_from_points(points)
                raw.write_bytes(original)

    def test_two_workers_have_complete_120_second_phase_analysis(self):
        def stamp(s):return f'2026-10-02T00:{s//60:02d}:{s%60:02d}Z'
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for kind in ('gateway','worker1','worker2'):
                q=p/('r1-n18-'+kind);q.mkdir()
                state=dict(active_sessions=18) if kind=='gateway' else dict(processing_limit_enabled=True,processing=dict(in_use=1,waiting=8))
                times=(20,65,119,120) if kind!='worker2' else (65,119)
                rows=[dict(started_at=stamp(s),finished_at=stamp(s),error=None,state=state) for s in times]
                (q/'samples.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
            result=phase_observations(p,dict(label='r1-n18',started_at=stamp(0),finished_at=stamp(121)),120,('worker1','worker2'))
            self.assertEqual(len(result),24)
            self.assertEqual(result[23]['worker1']['samples'],1);self.assertEqual(result[23]['worker2']['samples'],1)
            self.assertEqual(sum(x['worker1']['samples'] for x in result),3)
            self.assertEqual(sum(x['worker2']['samples'] for x in result),2)
            self.assertIsNone(result[0]['worker2']['observed_peak_waiting'])

    def test_historical_refinement_reanalysis_unchanged(self):
        source=DEFAULT_POINTS.parent/'dual-worker-boundary-2026-10-02'
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'copy';shutil.copytree(source,p)
            with contextlib.redirect_stdout(io.StringIO()):got=summarize(p)
            self.assertEqual(got,json.loads((source/'groups.json').read_text()))


if __name__=='__main__':
    unittest.main(verbosity=2)
