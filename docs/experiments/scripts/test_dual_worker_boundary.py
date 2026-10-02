"""细测选点、来源哈希及历史兼容检查；合成输入不作为容量结果。"""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from run_dual_worker_boundary import DEFAULT_SOURCE, decision_from_groups, source_selection
from run_joint_observation_baseline import run
from summarize_pressure_sweep import pressure_profile, summarize
from test_worker_comparison import fixture as comparison_fixture
from worker_comparison_contract import validate_manifest


def groups(passes=(3,3,0)):
    return [dict(planned_sessions=n,attempts=n*3,batches_meeting_criteria=p,all_repetitions_meet_criteria=p==3) for n,p in zip((16,18,20),passes)]


def manifest_fixture():
    m=comparison_fixture(2);m['experiment']='dual_worker_boundary_short';m['schedule']['session_counts']=[19]
    m['batches']=[m['batches'][0]]+[m['batches'][r*3+1] for r in range(3)]
    artifacts={}
    for b in m['batches']:
        old=b['label'];new=old.split('-n')[0]+'-n19' if old!='warmup' else old
        b['label']=new;b['planned_sessions']=19 if old!='warmup' else 2
        for s in b['sampling'].values():
            s['directory']=s['directory'].replace(old,new);s['command'][-1]=s['command'][-1].replace(old,new)
        for k,v in m['artifact_sha256'].items():
            if k.startswith(old+'-') or k in (old+'.json',old+'.log'):artifacts[k.replace(old,new)]=v
    m['artifact_sha256']=artifacts
    return m


class DualWorkerBoundaryTests(unittest.TestCase):
    def test_fixed_profile_and_two_workers(self):
        self.assertEqual(pressure_profile('dual_worker_boundary_short'),dict(experiment='dual_worker_boundary_short',counts=(19,),repetitions=3,audio_bytes=640000,timeout_s=35,duration_s=40))
        validate_manifest(manifest_fixture())
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'attempt'
            with self.assertRaises(ValueError):run(p,pressure=True,profile=pressure_profile('dual_worker_boundary_short'),worker_count=1)
            self.assertFalse(p.exists())
        for mode in ('missing','wrong_count','extra','old_schedule'):
            m=manifest_fixture()
            if mode=='missing':m['batches'].pop()
            elif mode=='wrong_count':m['batches'][1]['planned_sessions']=18
            elif mode=='extra':m['batches'].append(copy.deepcopy(m['batches'][-1]))
            else:m['schedule']['session_counts']=[18,19,20]
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_selection_requires_repeated_lower_pass_and_upper_unmet(self):
        g=groups();before=copy.deepcopy(g)
        self.assertEqual(decision_from_groups(g),dict(rule_version=1,refinement_sessions=19,lower_repeated_pass=18,upper_repeated_unmet=20))
        self.assertEqual(g,before)
        for passes in ((3,2,0),(3,3,1),(0,3,0),(3,3,3),(0,0,0)):
            with self.subTest(passes=passes),self.assertRaises(ValueError):decision_from_groups(groups(passes))

    def test_incomplete_or_inconsistent_selection_rejected(self):
        for mode in ('missing','order','attempts','bool','agreement'):
            g=groups()
            if mode=='missing':g.pop()
            elif mode=='order':g.reverse()
            elif mode=='attempts':g[0]['attempts']-=1
            elif mode=='bool':g[0]['batches_meeting_criteria']=True
            else:g[0]['all_repetitions_meet_criteria']=False
            with self.subTest(mode=mode),self.assertRaises(ValueError):decision_from_groups(g)

    def test_real_reference_reanalysis_and_tampering(self):
        s=source_selection(DEFAULT_SOURCE)
        self.assertEqual(s['refinement_sessions'],19)
        self.assertEqual(s['source_sha256'],json.loads((DEFAULT_SOURCE/'manifest.json').read_text())['source_sha256'])
        for mode in ('groups','raw','status'):
            with tempfile.TemporaryDirectory() as t:
                p=Path(t)/'source';shutil.copytree(DEFAULT_SOURCE,p)
                if mode=='groups':(p/'groups.json').write_text('{}')
                elif mode=='raw':(p/'r1-n16-worker2/samples.jsonl').write_text('')
                else:
                    m=json.loads((p/'manifest.json').read_text());m['status']='running';(p/'manifest.json').write_text(json.dumps(m))
                with self.subTest(mode=mode),self.assertRaises(ValueError):source_selection(p)

    def test_source_change_rejected_before_build_or_network(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'attempt'
            with patch('run_joint_observation_baseline.capture',return_value=''),patch('run_joint_observation_baseline.subprocess.Popen') as spawn,patch('run_joint_observation_baseline.socket.socket') as socket:
                with self.assertRaisesRegex(ValueError,'Go source differs'):
                    run(p,pressure=True,profile=pressure_profile('dual_worker_boundary_short'),worker_count=2,selection={'source_sha256':{'changed.go':'changed'}})
                spawn.assert_not_called();socket.assert_not_called()
            m=json.loads((p/'manifest.json').read_text());self.assertEqual(m['status'],'error')

    def test_historical_dual_sweep_reanalysis_unchanged(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'copy';shutil.copytree(DEFAULT_SOURCE,p)
            with contextlib.redirect_stdout(io.StringIO()):result=summarize(p)
            self.assertEqual(result,json.loads((DEFAULT_SOURCE/'groups.json').read_text()))


if __name__=='__main__':
    unittest.main(verbosity=2)
