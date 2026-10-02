"""异速实例的配置与实测身份校验；合成日志仅用于正确性测试。"""
import copy
from pathlib import Path
import tempfile
import unittest

from run_joint_observation_baseline import run
from summarize_pressure_sweep import pressure_profile
from test_worker_comparison import fixture as base_fixture
from worker_comparison_contract import worker_configs, validate_manifest, validate_startup_logs


def fixture():
    m = base_fixture(2)
    m.update(experiment='dual_worker_heterogeneous', worker=None,
             worker_configs=worker_configs('dual_worker_heterogeneous', 2))
    m['schedule']['session_counts'] = [8, 10, 12]
    m['commands'][1][4] = '-processing-delay=20ms'
    for w, config in zip(m['workers'], m['worker_configs']):
        w['config'] = copy.deepcopy(config)
    rename = {}
    for b in m['batches'][1:]:
        old = b['label']; rep, n = old.split('-n')
        new_n = {8:8, 9:10, 10:12}[int(n)]
        new = rep+'-n'+str(new_n); rename[old] = new
        b.update(label=new, planned_sessions=new_n)
        for s in b['sampling'].values():
            s['directory'] = s['directory'].replace(old, new)
            s['command'][-1] = s['command'][-1].replace(old, new)
    hashes = {}
    for k, v in m['artifact_sha256'].items():
        for old, new in rename.items():
            if k == old+'.json' or k == old+'.log' or k.startswith(old+'-'):
                k = new+k[len(old):]; break
        hashes[k] = v
    hashes.update({'asr-worker-1.log':'hash', 'asr-worker-2.log':'hash'})
    m['artifact_sha256'] = hashes
    return m


def startup(w):
    """沿用真实 slog 默认文本输出格式。"""
    return ('2026/10/02 21:00:00 INFO mock ASR worker started '
            f'address={w["address"]} debug_enabled=true '
            f'debug_address={w["endpoint"].split("/")[2]} processing_concurrency=1 '
            f'processing_delay={w["config"]["processing_delay_ns"]//1_000_000}ms response_delay=5ms\n')


class HeterogeneousWorkerTests(unittest.TestCase):
    def test_fixed_profile_and_manifest(self):
        self.assertEqual(pressure_profile('dual_worker_heterogeneous'),
                         dict(experiment='dual_worker_heterogeneous', counts=(8,10,12), repetitions=3,
                              audio_bytes=640000, timeout_s=35, duration_s=40))
        m=fixture(); before=copy.deepcopy(m); validate_manifest(m); self.assertEqual(m,before)

    def test_config_drift_or_wrong_identity_rejected(self):
        for mode in ('root','registry','swapped','command','uniform','unhashed'):
            m=fixture()
            if mode=='root':m['worker_configs'][1]['processing_delay_ns']=10_000_000
            elif mode=='registry':m['workers'][1]['config']['processing_delay_ns']=10_000_000
            elif mode=='swapped':m['workers'][0]['config'],m['workers'][1]['config']=m['workers'][1]['config'],m['workers'][0]['config']
            elif mode=='command':m['commands'][1][4]='-processing-delay=10ms'
            elif mode=='uniform':m['worker']=m['worker_configs'][0]
            else:del m['artifact_sha256']['asr-worker-2.log']
            with self.subTest(mode=mode),self.assertRaises(ValueError):validate_manifest(m)

    def test_wrong_topology_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as t:
            for n in (1,3,True):
                p=Path(t)/'unused'
                with self.assertRaises(ValueError):run(p,pressure=True,profile=pressure_profile('dual_worker_heterogeneous'),worker_count=n)
                self.assertFalse(p.exists())

    def test_logs_bind_actual_config_and_address_to_identity(self):
        m=fixture()
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)
            for i,w in enumerate(m['workers'],1):(p/f'asr-worker-{i}.log').write_text(startup(w))
            validate_startup_logs(p,m)
            second=p/'asr-worker-2.log'; original=second.read_text()
            variants=[original.replace('20ms','10ms'),startup(m['workers'][0]),'',original*2,
                      original.replace('processing_concurrency=1','processing_concurrency=10'),
                      original.replace('response_delay=5ms','response_delay=50ms')]
            for text in variants:
                second.write_text(text)
                with self.subTest(text=text),self.assertRaises(ValueError):validate_startup_logs(p,m)

    def test_archived_real_log_format_is_supported(self):
        import json
        p=Path(__file__).resolve().parent.parent/'results/dual-worker-extended-2026-10-02'
        m=json.loads((p/'manifest.json').read_text())
        for w in m['workers']:w['config']=m['worker']
        validate_startup_logs(p,m)

    def test_configs_do_not_share_mutable_text_lists(self):
        configs=worker_configs('dual_worker_heterogeneous',2)
        configs[0]['partial_texts'].append('changed')
        self.assertNotIn('changed',configs[1]['partial_texts'])
        self.assertNotIn('changed',worker_configs('dual_worker_heterogeneous',2)[0]['partial_texts'])


if __name__=='__main__':unittest.main(verbosity=2)
