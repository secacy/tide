"""双 Worker 加压的固定档位、资源与历史兼容检查；合成数据不作为性能依据。"""
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from run_dual_worker_sweep import run_sweep
from run_joint_observation_baseline import run
from summarize_pressure_sweep import pressure_profile, summarize
from test_worker_comparison import fixture as comparison_fixture
from worker_comparison_contract import validate_manifest, validate_pair

HERE = Path(__file__).resolve().parent


def fixture():
    """在原多实例校验夹具上替换预先固定的档位，保留独立来源。"""
    m = comparison_fixture(2)
    m['experiment'] = 'dual_worker_sweep'
    m['schedule']['session_counts'] = [16,18,20]
    for b in m['batches'][1:]:
        old = b['label'];rep,n=old.split('-n');new=rep+'-n'+str(int(n)*2)
        b['label'] = new;b['planned_sessions']*=2
        for s in b['sampling'].values():
            s['directory']=s['directory'].replace(old,new)
            s['command'][-1]=s['command'][-1].replace(old,new)
    m['artifact_sha256'] = {k.replace('-n8','-n16').replace('-n9','-n18').replace('-n10','-n20'):v for k,v in m['artifact_sha256'].items()}
    return m


class DualWorkerSweepTests(unittest.TestCase):
    def test_fixed_profile_and_topology(self):
        self.assertEqual(pressure_profile('dual_worker_sweep'),dict(experiment='dual_worker_sweep',counts=(16,18,20),repetitions=3,audio_bytes=640000,timeout_s=35,duration_s=40))
        m=fixture();before=copy.deepcopy(m);validate_manifest(m);self.assertEqual(m,before)

    def test_single_worker_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'attempt'
            for n in (1,True,3):
                with self.assertRaises(ValueError):run(p,pressure=True,profile=pressure_profile('dual_worker_sweep'),worker_count=n)
                self.assertFalse(p.exists())
        m=fixture();m['worker_count']=1
        with self.assertRaises(ValueError):validate_manifest(m)

    def test_missing_or_changed_schedule_rejected(self):
        for mode in ('count','order','rep','missing','label','warmup','timeout'):
            m=fixture()
            if mode=='count':m['schedule']['session_counts']=[16,18,19]
            elif mode=='order':m['batches'][1:3]=reversed(m['batches'][1:3])
            elif mode=='rep':m['schedule']['repetitions']=2
            elif mode=='missing':m['batches'].pop()
            elif mode=='label':m['batches'][1]['label']='r1-n8'
            elif mode=='warmup':m['schedule']['warmup_sessions']=1
            else:m['schedule']['session_timeout_s']=140
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_each_worker_evidence_still_required(self):
        for mode in ('missing','duplicate_endpoint','redirect','unhashed','forced'):
            m=fixture();b=m['batches'][1];s=b['sampling']['worker2']
            if mode=='missing':del b['sampling']['worker2']
            elif mode=='duplicate_endpoint':m['workers'][1]['endpoint']=m['workers'][0]['endpoint']
            elif mode=='redirect':s['command'][1]=b['sampling']['worker1']['command'][1]
            elif mode=='unhashed':del m['artifact_sha256'][s['directory']+'/samples.jsonl']
            else:s['forced_cleanup']=True
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_pair_comparison_cannot_accept_sweep(self):
        points=json.loads((HERE.parent/'results/boundary-study-operating-points-2026-10-02.json').read_text())
        with self.assertRaises(ValueError):validate_pair(comparison_fixture(1),fixture(),points)

    def test_runner_uses_existing_pipeline_with_exact_profile(self):
        with patch('run_dual_worker_sweep.run') as execute,patch('run_dual_worker_sweep.summarize',return_value={'groups':[]}) as analyze:
            p=Path('/synthetic-unused-path')
            self.assertEqual(run_sweep(p),{'groups':[]})
            execute.assert_called_once_with(p,pressure=True,profile=pressure_profile('dual_worker_sweep'),worker_count=2)
            analyze.assert_called_once_with(p)

    def test_previous_worker_comparison_reanalysis_unchanged(self):
        root=HERE.parent/'results/worker-count-comparison-2026-10-02'
        with tempfile.TemporaryDirectory() as t:
            for count in (1,2):
                source=root/f'workers-{count}';target=Path(t)/str(count);shutil.copytree(source,target)
                with contextlib.redirect_stdout(io.StringIO()):result=summarize(target)
                self.assertEqual(result,json.loads((source/'groups.json').read_text()))
                m=json.loads((target/'manifest.json').read_text());m['experiment']='dual_worker_sweep'
                (target/'manifest.json').write_text(json.dumps(m))
                with self.assertRaises(ValueError):summarize(target)


if __name__=='__main__':
    unittest.main(verbosity=2)
