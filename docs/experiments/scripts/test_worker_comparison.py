"""多 Worker 身份/条件/缺失数据和失败回收测试；合成数据不进入实验结果。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock

from run_joint_observation_baseline import run
from summarize_pressure_sweep import phase_observations, pressure_profile
from worker_comparison_contract import validate_manifest, validate_pair, WORKER, GATEWAY, CRITERIA, SAMPLING

HERE = Path(__file__).resolve().parent


def fixture(count):
    m = dict(experiment='worker_comparison', status='passed', worker_count=count, policy='round_robin',
             worker=WORKER, gateway=GATEWAY, criteria=CRITERIA, sampling=SAMPLING,
             schedule=dict(session_counts=[8,9,10], repetitions=3, audio_bytes=640000, chunk_bytes=3200,
                           realtime=True, session_timeout_s=35, warmup_sessions=count, warmup_audio_bytes=64000),
             tracked_diff='', race=False, source_sha256={'source.go':'same'}, commands=[], batches=[], artifact_sha256={},
             workers=[dict(id=f'worker{i}', address=f'127.0.0.1:{50050+i}', endpoint=f'http://127.0.0.1:{50150+i}/debug/worker') for i in range(1,count+1)],
             shutdown=[dict(process='gateway', exit_code=0)]+[dict(process=f'asr-worker-{i}',exit_code=0) for i in range(count,0,-1)])
    for k in ('source_commit','binary_sha256','script_sha256','go','os','cpu','logical_cpus','memory_bytes','environment'):
        m[k] = 'same'
    m['commands'] = [['/tmp/asr-worker','-listen=127.0.0.1:0','-debug-listen=127.0.0.1:0','-processing-concurrency=1','-processing-delay=10ms','-response-delay=5ms'] for _ in range(count)]
    m['commands'].append(['/tmp/gateway','-workers='+','.join(w['address'] for w in m['workers']),'-max-pending-audio-bytes=32000'])
    for label,n in [('warmup',count)]+[(f'r{r}-n{n}',n) for r in (1,2,3) for n in (8,9,10)]:
        b=dict(label=label,planned_sessions=n,sampling={})
        for key,endpoint in {'gateway':'http://127.0.0.1:8080/debug/gateway',**{w['id']:w['endpoint'] for w in m['workers']}}.items():
            kind='gateway' if key=='gateway' else 'worker'
            directory=label+'-'+key
            b['sampling'][key]=dict(directory=directory,command=['/tmp/'+kind+'-sampler','-url='+endpoint,'-interval='+('100ms' if kind=='gateway' else '20ms'),'-request-timeout=1s','-duration='+('10s' if label=='warmup' else '40s'),'-output-dir=/tmp/'+directory])
            for suffix in ('/samples.jsonl','/manifest.json','.log'):m['artifact_sha256'][directory+suffix]='hash'
        for suffix in ('.json','.log'):m['artifact_sha256'][label+suffix]='hash'
        m['batches'].append(b)
    return copy.deepcopy(m)


class WorkerComparisonTests(unittest.TestCase):
    def test_profile_and_valid_topologies(self):
        self.assertEqual(pressure_profile('worker_comparison'),dict(experiment='worker_comparison',counts=(8,9,10),repetitions=3,audio_bytes=640000,timeout_s=35,duration_s=40))
        for count in (1,2):
            m=fixture(count);before=copy.deepcopy(m);validate_manifest(m);self.assertEqual(m,before)

    def test_missing_and_duplicate_workers_rejected(self):
        for mode in ('missing','duplicate_id','duplicate_endpoint','duplicate_address','wrong_count','boolean_count'):
            m=fixture(2)
            if mode=='missing':m['workers'].pop()
            elif mode=='duplicate_id':m['workers'][1]['id']='worker1'
            elif mode=='duplicate_endpoint':m['workers'][1]['endpoint']=m['workers'][0]['endpoint']
            elif mode=='duplicate_address':m['workers'][1]['address']=m['workers'][0]['address']
            elif mode=='wrong_count':m['worker_count']=3
            else:m['worker_count']=True
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_sampling_misdirection_missing_and_forced_cleanup_rejected(self):
        for mode in ('missing','redirect','directory','forced','unhashed','duration'):
            m=fixture(2);b=m['batches'][1];s=b['sampling']['worker2']
            if mode=='missing':del b['sampling']['worker2']
            elif mode=='redirect':s['command'][1]=b['sampling']['worker1']['command'][1]
            elif mode=='directory':s['directory']=b['sampling']['worker1']['directory']
            elif mode=='forced':s['forced_cleanup']=True
            elif mode=='unhashed':del m['artifact_sha256'][s['directory']+'/samples.jsonl']
            else:s['command'][4]='-duration=5s'
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_fixture_and_shutdown_drift_rejected(self):
        for mode in ('processing','policy','gateway','schedule','warmup','routing','worker_command','shutdown','source','sampling','status'):
            m=fixture(2)
            if mode=='processing':m['worker']['processing_concurrency']=2
            elif mode=='policy':m['policy']='least_loaded'
            elif mode=='gateway':m['gateway']['max_pending_audio_bytes']=64000
            elif mode=='schedule':m['schedule']['session_counts']=[8,9,11]
            elif mode=='warmup':m['batches'][0]['planned_sessions']=1
            elif mode=='routing':m['commands'][-1][1]='-workers='+m['workers'][0]['address']
            elif mode=='worker_command':m['commands'][0][3]='-processing-concurrency=2'
            elif mode=='shutdown':m['shutdown'][-1]['exit_code']=-9
            elif mode=='source':m['tracked_diff']='dirty'
            elif mode=='sampling':m['sampling']['worker_interval_ns']=100_000_000
            else:m['status']='running'
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_pair_requires_equal_resources_per_worker_and_environment(self):
        points=json.loads((HERE.parent/'results/boundary-study-operating-points-2026-10-02.json').read_text())
        validate_pair(fixture(1),fixture(2),points)
        for key in ('binary_sha256','source_sha256','script_sha256','environment','cpu'):
            b=fixture(2);b[key]={'different':'value'}
            with self.subTest(key=key),self.assertRaises(ValueError):validate_pair(fixture(1),b,points)
        with self.assertRaises(ValueError):validate_pair(fixture(2),fixture(1),points)
        points['points'][2]['sessions']=12
        with self.assertRaises(ValueError):validate_pair(fixture(1),fixture(2),points)

    def test_phase_workers_remain_separate_and_missing_is_not_zero(self):
        def stamp(s):return f'2026-10-02T00:00:{s:02d}Z'
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)
            for kind in ('gateway','worker1','worker2'):
                p=root/('r1-n8-'+kind);p.mkdir()
                state=dict(active_sessions=8) if kind=='gateway' else dict(processing_limit_enabled=True,processing=dict(in_use=1,waiting=3 if kind=='worker1' else 2))
                rows=[] if kind=='worker2' else [dict(started_at=stamp(1),finished_at=stamp(1),error=None,state=state)]
                (p/'samples.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
            result=phase_observations(root,dict(label='r1-n8',started_at=stamp(0),finished_at=stamp(20)),worker_ids=('worker1','worker2'))
            self.assertEqual(result[0]['worker1']['observed_peak_waiting'],3)
            self.assertIsNone(result[0]['worker2']['observed_peak_waiting'])
            self.assertNotIn('worker',result[0])

    def test_second_worker_start_failure_reaps_first_and_keeps_manifest(self):
        processes=[]
        def start(command, **kwargs):
            p=MagicMock();p.poll.return_value=None if not processes else 1
            if not processes:
                kwargs['stdout'].write('address=127.0.0.1:50051 debug_address=127.0.0.1:50081\n');kwargs['stdout'].flush()
            processes.append(p)
            return p
        with tempfile.TemporaryDirectory() as t:
            target=Path(t)/'attempt'
            with patch('run_joint_observation_baseline.capture',return_value=''),patch('run_joint_observation_baseline.digest',return_value='hash'),patch('run_joint_observation_baseline.socket.socket'),patch('run_joint_observation_baseline.subprocess.run'),patch('run_joint_observation_baseline.subprocess.Popen',side_effect=start):
                with self.assertRaisesRegex(RuntimeError,'Worker exited before readiness'):
                    run(target,pressure=True,profile=pressure_profile('worker_comparison'),worker_count=2)
            self.assertEqual(len(processes),2)
            processes[0].kill.assert_called_once();processes[0].wait.assert_called_once()
            processes[1].kill.assert_not_called()
            m=json.loads((target/'manifest.json').read_text())
            self.assertEqual(m['status'],'error')
            self.assertIn('asr-worker-1.log',m['artifact_sha256'])
            self.assertIn('asr-worker-2.log',m['artifact_sha256'])

    def test_wrong_profile_rejected_before_creating_output(self):
        with tempfile.TemporaryDirectory() as t:
            target=Path(t)/'attempt'
            with self.assertRaises(ValueError):run(target,worker_count=2)
            self.assertFalse(target.exists())


if __name__=='__main__':
    unittest.main(verbosity=2)
