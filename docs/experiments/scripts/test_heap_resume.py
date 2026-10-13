"""Heap tool evidence counterexamples; default fixture is an archived short probe."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from heap_resume_contract import CASES, POINTS, order, environment, deadlines
from run_heap_resume import collect
from summarize_heap_resume import verify_checkpoints, verify_manifest, export_profiles, summarize
from summarize_client_resume import digest

ARCHIVE=Path(__file__).resolve().parents[1]/'results/heap-resume-tools-2026-10-13/collection.tar.xz'


class HeapResumeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory()
        provided=os.environ.get('TIDE_HEAP_ANALYSIS_FIXTURE')
        if provided:cls.root=Path(provided)
        else:
            with tarfile.open(ARCHIVE) as archive:archive.extractall(cls.temporary.name,filter='data')
            cls.root=Path(cls.temporary.name)/'collection'
        cls.manifest=json.loads((cls.root/'manifest.json').read_text())
        cls.case=cls.root/'sessions/02_B_budget_8/data'

    @classmethod
    def tearDownClass(cls):cls.temporary.cleanup()

    def changed(self, change, repair=True):
        with tempfile.TemporaryDirectory() as temp:
            data=Path(temp)/'data';shutil.copytree(self.case,data)
            h=json.loads((data/'heap-report.json').read_text());change(h,data)
            (data/'heap-report.json').write_text(json.dumps(h))
            if repair:
                for c in h['checkpoints']:(data/(c['name']+'.json')).write_text(json.dumps(c))
            return verify_checkpoints(data,'probe','B_budget_8')

    def test_fixed_conditions_and_owned_environment(self):
        self.assertEqual(order('formal'),['A_normal','B_budget_8','B_budget_8','A_normal'])
        self.assertEqual(order('probe'),list(CASES));self.assertEqual(deadlines('formal'),(720,725))
        with patch.dict(os.environ,{'GODEBUG':'gctrace=0,memprofilerate=99'}):
            before=dict(os.environ)
            env=environment('probe',CASES[0],Path('/tmp/data'),'memprofilerate=8,gctrace=0,memprofilerate=99')
            self.assertEqual(env['GODEBUG'],'gctrace=0,memprofilerate=1');self.assertEqual(dict(os.environ),before)
        for value in ('unknown',''):
            with self.assertRaises(ValueError):order(value)
        with self.assertRaises(ValueError):environment('probe','C_budget_5',Path('/tmp/data'))

    def test_real_probe_and_manifest(self):
        verify_manifest(self.root,self.manifest)
        for i,case in enumerate(CASES,1):
            h=verify_checkpoints(self.root/f'sessions/{i:02d}_{case}/data','probe',case)
            self.assertEqual([c['name'] for c in h['checkpoints']],list(POINTS))

    def test_missing_or_reordered_points(self):
        for change in (lambda h,d:h['checkpoints'].pop(1),lambda h,d:h['checkpoints'].reverse()):
            with self.assertRaises(ValueError):self.changed(change)

    def test_invalid_capture_outcomes(self):
        for key,value in [('valid',False),('reason','late'),('write_error','partial'),('close_error','failed'),('rate_before',512*1024),('rate_after',0),('gc_requested',1)]:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):self.changed(lambda h,d:h['checkpoints'][1].update({key:value}))

    def test_wrong_active_target_and_crossed_return(self):
        for change in (lambda h,d:h['checkpoints'][1].update(planned_ns=0),
                       lambda h,d:h['checkpoints'][1].update(start_ns=h['checkpoints'][1]['planned_ns']+1_000_000_001),
                       lambda h,d:h['checkpoints'][2].update(end_ns=h['fixture_origin_ns']+h['returned_ns']),
                       lambda h,d:h['checkpoints'][1]['resources_after'].update(worker_active=0)):
            with self.assertRaises(ValueError):self.changed(change)

    def test_teardown_not_confirmed(self):
        for key in ('owned_joined','detached','child_ok'):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):self.changed(lambda h,d:h.update({key:False}))
        for key in ('serve_exited','client_closed'):
            with self.assertRaises(ValueError):self.changed(lambda h,d:h['tcp_cleanup'].update({key:False}))

    def test_idle_and_fixture_phases(self):
        for change in (lambda h,d:h['checkpoints'][3]['resources_before'].update(registry=1),
                       lambda h,d:h['checkpoints'][3].update(start_ns=0),
                       lambda h,d:h['checkpoints'][4].update(start_ns=h['checkpoints'][3]['end_ns']),
                       lambda h,d:h['checkpoints'][0].update(end_ns=h['fixture_origin_ns']),
                       lambda h,d:h['checkpoints'][4].update(fixture_ns=0)):
            with self.assertRaises(ValueError):self.changed(change)

    def test_numeric_boolean_and_schema_drift(self):
        for change in (lambda h,d:h.update(first_ns=True),lambda h,d:h.update(extra=True),
                       lambda h,d:h['checkpoints'][0].update(rate_before=True),
                       lambda h,d:h['checkpoints'][0]['after_gc'].update(heap_alloc=True),
                       lambda h,d:h['checkpoints'][0]['resources_before'].update(sessions=True),
                       lambda h,d:h['checkpoints'][0]['after_gc'].update(num_gc=0)):
            with self.assertRaises(ValueError):self.changed(change)

    def test_metadata_disagreement(self):
        with self.assertRaises(ValueError):self.changed(lambda h,d:h['checkpoints'][0]['before'].update(heap_alloc=1),repair=False)

    def test_missing_truncated_or_digest_changed(self):
        for change in (lambda h,d:(d/'early_active.pb.gz').unlink(),
                       lambda h,d:(d/'early_active.pb.gz').write_bytes(b'truncated'),
                       lambda h,d:h['checkpoints'][1].update(sha256='0'*64)):
            with self.assertRaises((ValueError,OSError)):self.changed(change)

    def test_pprof_rejects_corruption_even_with_repaired_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            data=Path(temp)/'data';shutil.copytree(self.case,data)
            h=json.loads((data/'heap-report.json').read_text())
            f=data/'early_active.pb.gz';f.write_bytes(b'not a profile')
            c=h['checkpoints'][1];c['bytes']=f.stat().st_size;c['sha256']=digest(f)
            (data/'early_active.json').write_text(json.dumps(c));(data/'heap-report.json').write_text(json.dumps(h))
            verify_checkpoints(data,'probe','B_budget_8')
            with self.assertRaises(ValueError):export_profiles(data,self.root/'heap-resume.test',Path(temp)/'reports')
            commands=json.loads((Path(temp)/'reports/commands.json').read_text())
            self.assertTrue(any(c['exit_code']!=0 for c in commands));self.assertEqual(len(commands),40)

    def test_manifest_counterexamples(self):
        for change in (lambda m:m.update(order=list(reversed(m['order']))),lambda m:m.update(planned_sessions=3),
                       lambda m:m.update(source_unchanged=False),lambda m:m.update(planned_rounds=2),
                       lambda m:m['slots'][0].update(exit_code=1),lambda m:m['slots'][0].update(external_timeout_s=999),
                       lambda m:m['slots'][0]['environment'].update(GODEBUG='memprofilerate=2'),
                       lambda m:m['slots'][0].update(round=2),lambda m:m['slots'][0].update(index=True),
                       lambda m:m['go_env'].update(GOFLAGS='-race'),
                       lambda m:m['artifacts_sha256'].pop('sessions/01_A_normal/data/before_fixture.pb.gz')):
            m=copy.deepcopy(self.manifest);change(m)
            with self.assertRaises(ValueError):verify_manifest(self.root,m)

    def test_fixed_denominator_on_missing_slot(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'collection';shutil.copytree(self.root,root)
            shutil.rmtree(root/'sessions/02_B_budget_8')
            result=summarize(root,Path(temp)/'analysis',export=False)
            self.assertFalse(result['evidence_valid']);self.assertEqual(result['planned_sessions'],2)
            self.assertEqual(len(result['cases']),2);self.assertEqual(result['business_valid'],1)

    def test_collector_build_failure_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'failed'
            actual_run=subprocess.run
            def fail_build(argv, **kwargs):
                if argv[:3]==['go','test','-c']:
                    return subprocess.CompletedProcess(argv,1)
                return actual_run(argv,**kwargs)
            with patch('run_heap_resume.subprocess.run',side_effect=fail_build):
                self.assertEqual(collect(output),1)
            m=json.loads((output/'manifest.json').read_text());self.assertEqual(m['status'],'build_failed');self.assertEqual(m['slots'],[])
            self.assertTrue((output/'preregistration.md').exists())

    def test_collector_exclusive_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileExistsError):collect(Path(temp))


if __name__=='__main__':unittest.main()
