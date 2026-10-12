"""Evidence counterexamples for EXP-006-05; fixture is a collected probe, never formal capacity data."""
import copy
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from extended_resume_contract import CASES, PLAN, configured, order, pattern
from run_extended_resume import collect
from summarize_extended_resume import read_events, summarize, verify_case, verify_manifest
from summarize_client_resume import digest

ARCHIVE=Path(__file__).resolve().parents[1]/'results/extended-resume-tools-2026-10-13/collection.tar.xz'


class ExtendedResumeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory()
        provided=os.environ.get('TIDE_EXTENDED_ANALYSIS_FIXTURE')
        if provided:cls.root=Path(provided)
        else:
            with tarfile.open(ARCHIVE) as archive:archive.extractall(cls.temporary.name,filter='data')
            cls.root=Path(cls.temporary.name)/'collection'
        cls.manifest=json.loads((cls.root/'manifest.json').read_text())
        cls.case=cls.root/'sessions/02_B_budget_8/data'
        cls.rows=read_events(cls.case/'events.jsonl')

    @classmethod
    def tearDownClass(cls):cls.temporary.cleanup()

    def changed_case(self,change):
        with tempfile.TemporaryDirectory() as temp:
            target=Path(temp)/'data';shutil.copytree(self.case,target)
            rows=copy.deepcopy(self.rows);summary=json.loads((target/'summary.json').read_text());change(rows,summary)
            # Reassign envelope sequences so payload/event completeness checks must also detect deleted facts.
            counts={}
            for row in rows:
                c=row['component'];counts[c]=counts.get(c,0)+1;row['seq']=counts[c]
            raw=b''.join((json.dumps(r,separators=(',',':'))+'\n').encode() for r in rows)
            summary['events']=len(rows);summary['buffer_accepted_bytes']=len(raw)
            (target/'events.jsonl').write_bytes(raw);(target/'summary.json').write_text(json.dumps(summary))
            return verify_case(target,'probe','B_budget_8')

    def test_collected_cases_and_metrics(self):
        for i,case in enumerate(CASES,1):
            r=verify_case(self.root/f'sessions/{i:02d}_{case}/data','probe',case)
            self.assertTrue(r['behavior_passed'])
            self.assertEqual(r['completed'],case!='C_budget_5')
            if case=='B_budget_8':
                self.assertEqual(r['repeated_audio_bytes'],6*3200)
                self.assertTrue(all(x['fresh_wait_ns'] is not None and x['caught_up_before_next'] for x in r['outages']))
            if case=='C_budget_5':
                self.assertEqual(r['missing_captured'],[272000,275200])
                self.assertIsNone(r['outages'][-1]['fresh_wait_ns'])
                self.assertTrue(r['outages'][-1]['right_censored'])
                self.assertEqual(r['resources'][-1]['state'],'not_reached')

    def test_manifest_valid(self):verify_manifest(self.root,self.manifest)

    def test_payload_contradictions(self):
        cases={
            'source_digest':('source','read','sha256','0'*64),
            'source_origin':('source','read','planned_ns',999),
            'worker_offset':('worker','processed','offset',6400),
            'proxy_range':('proxy','audio_read','begin',1),
            'proxy_digest':('proxy','audio_read','sha256','f'*64),
            'application_seq':('run','application','seq',2),
            'application_end':('run','application','end',6400),
            'fault_offset':('proxy','fault','offset',999),
            'fault_round':('proxy','fault','round',2),
            'ready_generation':('proxy','ready','generation',2),
            'ack_offset':('proxy','audio_ack','next_offset',999999),
            'idle_count':('run','natural_idle','sessions',1),
            'idle_cancel':('run','natural_idle','service_cancelled',True),
            'worker_rpc':('worker','start','rpc',2),
            'queue_overflow':('sample','resources','queue_depth',1025),
        }
        for name,(component,kind,key,value) in cases.items():
            with self.subTest(name=name):
                def change(rows,summary):
                    row=next(r for r in rows if r['component']==component and r['kind']==kind);row['data'][key]=value
                with self.assertRaises(ValueError):self.changed_case(change)

    def test_missing_facts_with_repaired_envelope(self):
        for component,kind in [('source','read'),('source','close'),('worker','received'),('worker','progress_sent'),('worker','exit'),('proxy','fault'),('proxy','opened'),('proxy','audio_write_returned'),('proxy','handler_exit'),('run','returned'),('run','joined')]:
            with self.subTest(component=component,kind=kind):
                def change(rows,summary):rows.remove(next(r for r in rows if r['component']==component and r['kind']==kind))
                with self.assertRaises(ValueError):self.changed_case(change)

    def test_terminal_contradictions(self):
        for key,value in [('forced_cleanup',True),('log_error',True),('captured_bytes',1),('worker_picks',2),('connections',99)]:
            with self.subTest(key=key):
                with self.assertRaises(ValueError):self.changed_case(lambda rows,s:s.update({key:value}))

    def test_fixed_profiles(self):
        for name in ['attempts','processing_ns','run_timeout_ns']:
            with self.subTest(name=name):
                def change(rows,s):
                    s['config'][name]+=1
                    next(r for r in rows if r['kind']=='configuration')['data']['config'][name]+=1
                with self.assertRaises(ValueError):self.changed_case(change)

    def test_numeric_boolean_is_not_integer_evidence(self):
        def change(rows,s):next(r for r in rows if r['component']=='worker' and r['kind']=='start')['data']['rpc']=True
        with self.assertRaises(ValueError):self.changed_case(change)
        def config(rows,s):
            s['gateway_config']['MaxSessions']=True
            next(r for r in rows if r['kind']=='configuration')['data']['gateway']['MaxSessions']=True
        with self.assertRaises(ValueError):self.changed_case(config)

    def test_sampling_gap_degrades_quality_not_business(self):
        def change(rows,s):rows[:]=[r for r in rows if r['component']!='sample' or r['seq']%2]
        r=self.changed_case(change)
        self.assertTrue(r['completed']);self.assertFalse(r['timing_resource_quality'])
        self.assertIn('sampling_coverage_under_90_percent',r['quality_issues'])

    def test_manifest_counterexamples(self):
        for field,value in [('source_unchanged',False),('build_exit',1),('order',list(reversed(CASES))),('planned_sessions',4),('root_test','wrong')]:
            with self.subTest(field=field):
                m=copy.deepcopy(self.manifest);m[field]=value
                with self.assertRaises(ValueError):verify_manifest(self.root,m)
        for key in ['exit_code','environment','argv','case','path']:
            with self.subTest(key=key):
                m=copy.deepcopy(self.manifest);m['slots'][0][key]=99
                with self.assertRaises((ValueError,TypeError)):verify_manifest(self.root,m)

    def test_line_and_envelope_rejections(self):
        good=(self.case/'events.jsonl').read_bytes()
        with tempfile.TemporaryDirectory() as temp:
            p=Path(temp)/'events'
            for bad in [good[:-1],good[:-12],b'{}\n',good.replace(b'"seq":1',b'"seq":2',1),good+b'{"resumeToken":"secret"}\n']:
                p.write_bytes(bad)
                with self.assertRaises((ValueError,KeyError)):read_events(p)

    def test_archive_relocation_and_repeat_analysis(self):
        with tempfile.TemporaryDirectory() as temp:
            temp=Path(temp);moved=temp/'moved';shutil.copytree(self.root,moved)
            a=summarize(self.root,temp/'a');b=summarize(moved,temp/'b')
            self.assertTrue(a['evidence_valid']);self.assertEqual(a,b)
            for p in (temp/'a').iterdir():self.assertEqual(p.read_bytes(),(temp/'b'/p.name).read_bytes())

    def test_corrupt_artifact_gates_metrics_and_keeps_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            temp=Path(temp);root=temp/'collection';shutil.copytree(self.root,root)
            (root/'sessions/02_B_budget_8/data/events.jsonl').write_bytes(b'corrupt\n')
            r=summarize(root,temp/'analysis')
            self.assertFalse(r['evidence_valid']);self.assertEqual(r['planned_sessions'],self.manifest['planned_sessions']);self.assertEqual(r['verified_sessions'],0)
            self.assertEqual(sum(g['unknown'] for g in r['groups']),self.manifest['planned_sessions'])

    def test_payload_unknown_fields_rejected(self):
        def change(rows,s):next(r for r in rows if r['kind']=='read')['data']['unplanned_field']='not evidence'
        with self.assertRaises(ValueError):self.changed_case(change)

    def test_manifest_requires_implementation_snapshots(self):
        m=copy.deepcopy(self.manifest);del m['source_sha256']['internal/gateway/extended_resume_collection_test.go']
        with self.assertRaises(ValueError):verify_manifest(self.root,m)

    def test_race_flag_variants_rejected(self):
        for flag in ['-race','-race=true','-race=1']:
            m=copy.deepcopy(self.manifest);m['go_env']['GOFLAGS']=flag
            with self.assertRaises(ValueError):verify_manifest(self.root,m)

    def test_pattern_known_vector_and_conditions(self):
        import hashlib
        self.assertEqual(hashlib.sha256(pattern(5999)).hexdigest(),'46a1e92a1c8b753a64eceef3f75d2938c00f106ded3799185906a28c5dccc049')
        self.assertEqual(configured('B_budget_8','formal')['chunks'],6000)
        self.assertEqual(order(2),['A_normal','B_budget_8','C_budget_5','C_budget_5','B_budget_8','A_normal'])
        with self.assertRaises(ValueError):order(3)

    def test_collector_rejects_existing_directory_and_formal_count(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(FileExistsError):collect(Path(temp),'probe',1)
            with self.assertRaises(ValueError):collect(Path(temp)/'absent','formal',1)
            self.assertFalse((Path(temp)/'absent').exists())

    def test_build_failure_preserves_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            temp=Path(temp);repo=temp/'repo';repo.mkdir();(repo/PLAN).parent.mkdir(parents=True);(repo/PLAN).write_text('test plan')
            def check(args,**kwargs):
                if args[:2]==['git','rev-parse']:return b'abc\n'
                if args[:2]==['git','status']:return b''
                return json.dumps(dict(GOVERSION='go1.test',GOOS='darwin',GOARCH='arm64',GOFLAGS='')).encode()
            with patch('run_extended_resume.subprocess.check_output',side_effect=check),patch('run_extended_resume.source_hashes',return_value={PLAN:digest(repo/PLAN)}),patch('run_extended_resume.subprocess.run') as run:
                run.return_value.returncode=1
                self.assertEqual(collect(temp/'out','probe',1,repo),1)
            m=json.loads((temp/'out/manifest.json').read_text());self.assertEqual(m['status'],'build_failed');self.assertEqual(m['planned_sessions'],3)


if __name__=='__main__':unittest.main()
