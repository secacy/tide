"""预算比较的独立证据与反例；合成时间不是测量结果。"""
import base64
import copy
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from group_resume_budget_contract import CASES, EXPERIMENT, MARKER, PLAN, ROOT_TEST, configured, environment, order
from run_group_resume import ARTIFACTS
from run_group_resume_budget import collect
from summarize_group_resume import digest, verify_report
from summarize_group_resume_budget import extract, summarize, verify_budget_report, verify_manifest
from test_group_resume import at, fixture


def report(case=CASES[0], round=1, failed=False):
    r = fixture(True, failed)
    cfg = configured(case, 'probe')
    r.update(config=cfg, experiment=EXPERIMENT, round=round, occupied_at=at(150),
             released_at=at(200 + cfg['busy_ns']//1_000_000))
    r['samples'][1]['handshakes'] = 2
    r['gateway_config']['V2']['MaxHandshakes'] = 2
    r['attempts'] = [a for a in r['attempts'] if a['connection'] == 1]
    release = 200 + cfg['busy_ns']//1_000_000
    for session in r['sessions']:
        obs = session['observation']
        obs['client_config']['resume'] = copy.deepcopy(cfg['resume'])
        attempts = cfg['resume']['MaxAttempts'] if failed else (6 if case.endswith('8') else 2)
        obs['run_result'].update(ResumeAttempts=attempts, ResumedConnections=0 if failed else 1)
        if failed:
            obs['worker']['audio'] = base64.b64encode(base64.b64decode(obs['worker']['audio'])[:6400]).decode()
            obs['worker']['chunks'] = obs['worker']['chunks'][:2]
            obs['worker']['tail_sent_at'] = None
            obs['frames'] = obs['frames'][:3]
            obs['applications'] = obs['applications'][:2]
            obs['readies'] = obs['readies'][:1]
            obs['run_result']['AppliedSeq'] = 2
            session['last_entry_code'] = 'handshake_limit'
        else:
            obs['readies'][1].update(connection=attempts+1, written_at=at(release+5))
            for f in obs['frames']:
                if f['connection'] > 1 or f['begin'] >= 9600:
                    t = max(f['begin']//3200*100, release+6)
                    f.update(connection=attempts+1, read_at=at(t), forward_started_at=at(t), forward_returned_at=at(t))
            previous = release+7
            for i in range(2,8):
                t = max(i*100, previous+1)
                previous = t
                obs['worker']['chunks'][i].update(received_at=at(t), processed_at=at(t+1), progress_sent_at=at(t+2), result_sent_at=at(t+3))
                obs['applications'][i].update(received_at=at(t+4), applied_at=at(t+5))
        for n in range(1,attempts+1):
            accepted = not failed and n == attempts
            t = release+1 if accepted else 203+(n-1)*20
            r['attempts'].append(dict(id=session['id'], connection=n+1, arrived_at=at(t), backend_at=at(t+1),
                                     exited_at=at(800 if accepted else t+2), kind='resume',
                                     code='' if accepted else 'handshake_limit', code_written=not accepted, http_status=101 if accepted else 503))
    return r


def shifted(r, ms):
    def shift(value):
        if isinstance(value,dict): return {k:shift(v) for k,v in value.items()}
        if isinstance(value,list): return [shift(v) for v in value]
        if isinstance(value,str) and value.startswith('2026-10-11T00:00:'):
            second, fraction = value.split('T')[1].split(':')[2].split('.')
            return at(int(second)*1000 + int(fraction[:9])//1_000_000 + ms)
        return value
    return shift(r)


def archive(root, rounds=1):
    root.mkdir()
    reports = [shifted(report(case, i//4+1, case == CASES[2]), i*1000) for i,case in enumerate(order(rounds))]
    events = [dict(Action='output', Test=ROOT_TEST['probe']+'/round_'+str(r['round'])+'/'+r['config']['case'],
                   Output=MARKER['probe']+json.dumps(r)+'\n') for r in reports]
    events.append(dict(Action='pass',Test=ROOT_TEST['probe']))
    for name in ARTIFACTS: (root/name).write_text('fixture\n')
    (root/'go-events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
    source = root/'source'/PLAN
    source.parent.mkdir(parents=True)
    source.write_text('fixture\n')
    binary = str(root.resolve()/'group-resume.test')
    m = dict(version=2,experiment=EXPERIMENT,plan=PLAN,mode='probe',planned_rounds=rounds,planned_sessions=32*rounds,
             order=order(rounds),root_test=ROOT_TEST['probe'],test_count=1,collection_directory=str(root.resolve()),
             experiment_environment=environment('probe',rounds),status='collected',build_exit=0,run_exit=0,source_unchanged=True,
             build_argv=['go','test','-c','-o',binary,'./internal/gateway'],
             run_argv=['go','tool','test2json','-t','-p','github.com/secacy/tide-artisan/internal/gateway',binary,
                       '-test.run=^'+ROOT_TEST['probe']+'$','-test.count=1','-test.timeout=5m','-test.v=test2json'],
             go_env=dict(GOVERSION='go1.26.5',GOOS='darwin',GOARCH='arm64',GOFLAGS=''),
             source_sha256={PLAN:digest(source)},artifacts_sha256={name:digest(root/name) for name in ARTIFACTS})
    (root/'manifest.json').write_text(json.dumps(m))
    return m


def change_events(root, mutate):
    events = [json.loads(line) for line in (root/'go-events.jsonl').read_text().splitlines()]
    mutate(events)
    (root/'go-events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
    m = json.loads((root/'manifest.json').read_text())
    m['artifacts_sha256']['go-events.jsonl'] = digest(root/'go-events.jsonl')
    (root/'manifest.json').write_text(json.dumps(m))


class GroupResumeBudgetTest(unittest.TestCase):
    def test_archived_real_network_comparison_contract(self):
        root=Path(__file__).resolve().parent.parent/'results/group-resume-budget-tools-2026-10-11'
        index=json.loads((root/'evidence-index.json').read_text())
        raw=gzip.decompress((root/'go-events.jsonl.gz').read_bytes())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),index['event_sha256'])
        for name,sha in index['files_sha256'].items():self.assertEqual(digest(root/name),sha)
        reports=extract([json.loads(line) for line in raw.splitlines()],'probe')
        self.assertEqual([r['config']['case'] for r in reports],order(3))
        rows=[verify_budget_report(r,'probe') for r in reports]
        self.assertEqual(sum(r['resource_samples'] for r in rows),index['samples'])
        self.assertEqual(sum(r['failed'] for r in rows),24)
        for r in rows:
            if r['case']==CASES[3]:
                self.assertTrue(all(s['resume_attempts']>5 for s in r['sessions']))

    def test_canonical_conditions_only_change_attempts(self):
        self.assertEqual(len(order(3)),12)
        for mode in ('formal','probe'):
            for a,b in ((CASES[0],CASES[1]),(CASES[2],CASES[3])):
                left,right = configured(a,mode), configured(b,mode)
                self.assertEqual(left['resume']['MaxAttempts'],5)
                self.assertEqual(right['resume']['MaxAttempts'],8)
                left['case']=right['case']='pair'; right['resume']['MaxAttempts']=5
                self.assertEqual(left,right)

    def test_old_baseline_does_not_accept_budget_report(self):
        for r in (report(),report(CASES[1])):
            with self.assertRaises(ValueError): verify_report(r,'probe')
        r=fixture(True); r['config']['resume']['MaxAttempts']=8
        with self.assertRaises(ValueError): verify_report(r,'probe')

    def test_success_and_eight_attempt_failure(self):
        for case in CASES:
            for failed in (False,True):
                row=verify_budget_report(report(case,failed=failed),'probe')
                self.assertEqual(row['completed'],0 if failed else 8)
                self.assertEqual(row['failed'],8 if failed else 0)

    def test_fixed_denominator_and_pair_differences(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run'; archive(root,3)
            result=summarize(root,root/'analysis')
            self.assertTrue(result['evidence_valid'],result['errors'])
            self.assertEqual(result['planned_sessions'],96)
            self.assertEqual([g['verified_completed'] for g in result['groups']],[24,24,0,24])
            self.assertEqual(result['comparisons'][1]['difference']['completion_percentage_points'],100)
            self.assertEqual(result['comparisons'][1]['difference']['resume_attempts'],24)
            self.assertEqual(len(result['pairs']),6)
            self.assertEqual(result['groups'][1]['metrics']['result_actual_wait_ms']['n'],24*8)
            self.assertIsNone(result['groups'][2]['metrics']['fresh_after_fault_ms'])
            self.assertEqual(result['groups'][2]['metrics']['failed_return_after_fault_ms']['n'],24)

    def test_rpc_retention_does_not_depend_on_client_return(self):
        r=report(CASES[1],failed=True)
        a=verify_budget_report(r,'probe')['sessions'][0]
        r['sessions'][0]['observation']['returned_at']=at(750)
        b=verify_budget_report(r,'probe')['sessions'][0]
        self.assertEqual(a['rpc_after_fault_ms'],b['rpc_after_fault_ms'])
        self.assertEqual(a['rpc_after_return_ms'],0)
        self.assertEqual(b['rpc_after_return_ms'],40)

    def test_report_counterexamples(self):
        changes={
            'race collection':lambda r:r['environment'].update(race=True),
            'attempt budget':lambda r:r['sessions'][0]['observation']['run_result'].update(ResumeAttempts=9),
            'window':lambda r:r['config']['resume'].update(Window=99),
            'backoff':lambda r:r['config']['resume'].update(InitialBackoff=99),
            'effective client':lambda r:r['sessions'][0]['observation']['client_config']['resume'].update(MaxAttempts=5),
            'gate':lambda r:r['gateway_config']['V2'].update(MaxHandshakes=3),
            'identity':lambda r:r.update(experiment='EXP-006-02'),
            'round':lambda r:r.update(round=0),
            'ack':lambda r:r['sessions'][0]['observation']['run_result'].update(CompletionAckWritten=False),
            'replacement RPC':lambda r:r.update(worker_calls=9),
            'audio mixing':lambda r:r['sessions'][0]['observation']['worker'].update(audio='AAAA'),
            'sequence':lambda r:r['sessions'][0]['observation']['applications'][1].update(seq=8),
            'RPC dead before fault':lambda r:r['sessions'][0]['observation']['worker'].update(exited_at=at(100)),
            'RPC exit after idle':lambda r:r['sessions'][0]['observation']['worker'].update(exited_at=at(802)),
            'forced cleanup':lambda r:r.update(forced_cleanup=True),
        }
        for name,change in changes.items():
            with self.subTest(name=name),self.assertRaises(ValueError):
                r=report(CASES[1]);change(r);verify_budget_report(r,'probe')

    def test_missing_duplicate_swapped_and_wrong_round(self):
        changes={
            'missing':lambda e:e.pop(0),
            'duplicate':lambda e:e.insert(0,copy.deepcopy(e[0])),
            'wrong path':lambda e:e[0].update(Test=ROOT_TEST['probe']+'/round_2/'+CASES[0]),
        }
        for name,change in changes.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'run';archive(root);change_events(root,change)
                result=summarize(root,root/'analysis')
                self.assertFalse(result['evidence_valid'])
                self.assertEqual(result['planned_sessions'],32)
                self.assertIsNone(result['comparisons'][0]['difference'])
                self.assertLessEqual(result['groups'][0]['verified_completed'],8)
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';archive(root)
            def swap(e):
                # 更换真实报告时间，使排序也出现错误，而不是只重排 JSON 日志。
                a=json.loads(e[0]['Output'][len(MARKER['probe']):]); b=json.loads(e[1]['Output'][len(MARKER['probe']):])
                a=shifted(a,1000); b=shifted(b,-1000)
                e[0]['Output']=MARKER['probe']+json.dumps(a);e[1]['Output']=MARKER['probe']+json.dumps(b)
            change_events(root,swap)
            result=summarize(root,root/'analysis')
            self.assertFalse(result['evidence_valid'])
            self.assertTrue(all(c['difference'] is None for c in result['comparisons']))

    def test_corrupt_archive_and_root_failure_suppress_comparison(self):
        for corrupt in (False,True):
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'run';archive(root)
                if corrupt: (root/'group-resume.test').write_text('tampered')
                else: change_events(root,lambda e:e[-1].update(Action='fail'))
                result=summarize(root,root/'analysis')
                self.assertFalse(result['evidence_valid'])
                self.assertTrue(all(c['difference'] is None for c in result['comparisons']))

    def test_manifest_counterexamples(self):
        changes={
            'identity':lambda m:m.update(experiment='EXP-006-02'),
            'denominator':lambda m:m.update(planned_sessions=40),
            'order':lambda m:m['order'].reverse(),
            'test count':lambda m:m['run_argv'].__setitem__(-3,'-test.count=3'),
            'wrong env':lambda m:m['experiment_environment'].update(TIDE_GROUP_RESUME_BUDGET_ROUNDS='3'),
            'source changed':lambda m:m.update(source_unchanged=False),
            'missing hash':lambda m:m['artifacts_sha256'].pop('go-events.jsonl'),
            'path escape':lambda m:m['source_sha256'].update({'../manifest.json':'bad'}),
            'formal shortened':lambda m:m.update(mode='formal'),
            'race':lambda m:m['go_env'].update(GOFLAGS='-race'),
        }
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';m=archive(root);verify_manifest(root,m)
            for name,change in changes.items():
                with self.subTest(name=name),self.assertRaises(ValueError):
                    bad=copy.deepcopy(m);change(bad);verify_manifest(root,bad)

    def test_archive_relocation_and_reanalysis_are_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';archive(root);summarize(root,root/'analysis')
            moved=Path(temp)/'moved';shutil.copytree(root,moved)
            result=summarize(moved,moved/'reanalysis')
            self.assertTrue(result['evidence_valid'],result['errors'])
            for name in ('reports.json','summary.json','sessions.csv','attempt-bins.csv'):
                self.assertEqual((root/'analysis'/name).read_bytes(),(moved/'reanalysis'/name).read_bytes())
            with self.assertRaises(FileExistsError):summarize(root,root/'analysis')

    def test_fragmented_logs_and_credentials(self):
        r=report();text=MARKER['probe']+json.dumps(r)
        test=ROOT_TEST['probe']+'/round_1/'+CASES[0]
        events=[dict(Action='output',Test=test,Output=text[i:i+71]) for i in range(0,len(text),71)]
        self.assertEqual(extract(events,'probe'),[r])
        with self.assertRaises(ValueError):extract(events[:-5],'probe')
        with self.assertRaisesRegex(ValueError,'credentials'):
            extract([dict(Action='output',Test=test,Output=s) for s in ('resume_','token')],'probe')

    def test_probe_is_not_formal(self):
        with self.assertRaises(ValueError):verify_budget_report(report(),'formal')

    def test_invalid_report_keeps_unknown_in_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';archive(root)
            def bad_report(events):
                r=json.loads(events[0]['Output'][len(MARKER['probe']):])
                r['sessions'][0]['observation']['run_result']['ResumeAttempts']=99
                events[0]['Output']=MARKER['probe']+json.dumps(r)
            change_events(root,bad_report)
            result=summarize(root,root/'analysis')
            control=result['groups'][0]
            self.assertEqual(control['planned_sessions'],8)
            self.assertEqual(control['unknown_sessions'],8)
            self.assertEqual(control['completion_rate_bounds'],[0,1])
            self.assertIsNone(result['comparisons'][0]['difference'])

    def test_invalid_collection_does_not_create_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            output=Path(temp)/'run'
            with self.assertRaises(ValueError):collect(output,'formal',1)
            self.assertFalse(output.exists())
            with self.assertRaises(FileExistsError):collect(Path(temp),'probe',1)

    def test_build_failure_records_new_identity_and_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            repo=Path(temp)/'repo';repo.mkdir();plan=repo/PLAN;plan.parent.mkdir(parents=True);plan.write_text('plan')
            (repo/'go.mod').write_text('module fixture')
            def check_output(cmd,**kwargs):
                if cmd[:3]==['git','ls-files','-z']:return ('go.mod\0'+PLAN+'\0').encode()
                if cmd[:2]==['go','env']:return b'{"GOVERSION":"go1.26.5","GOOS":"darwin","GOARCH":"arm64","GOFLAGS":""}'
                return b'fixture'
            with patch('run_group_resume.subprocess.check_output',side_effect=check_output),patch('run_group_resume.subprocess.run') as run:
                run.return_value.returncode=1
                out=Path(temp)/'failed';self.assertEqual(collect(out,'probe',1,repo),1);self.assertEqual(run.call_count,1)
            m=json.loads((out/'manifest.json').read_text())
            self.assertEqual(m['experiment'],EXPERIMENT)
            self.assertEqual(m['planned_sessions'],32)
            self.assertEqual(m['status'],'build_failed')
            self.assertEqual((out/'preregistration.md').read_text(),'plan')


if __name__=='__main__':unittest.main()
