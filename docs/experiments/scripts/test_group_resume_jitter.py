"""随机对照的独立校验、合成反例；模拟时间不是恢复性能数据。"""
import copy
import hashlib
import json
import lzma
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from group_resume_jitter_contract import (CASES, EXPERIMENT, MARKER, PLAN, REFERENCE_PLAN, ROOT_TEST,
                                        PCG64, configured, environment, order, retry_plan, session_resume)
from run_group_resume_jitter import collect
from summarize_group_resume import digest, verify_report
from summarize_group_resume_budget import verify_budget_report
from summarize_group_resume_jitter import extract, summarize, verify_jitter_report, verify_manifest
from test_group_resume_budget import archive as budget_archive, report as budget_report, shifted, change_events


def report(case=CASES[1], round=1, failed=False):
    r = budget_report(case.rsplit('_', 1)[0]+'_a8', round, failed)
    cfg = configured(case, 'probe')
    plans = [retry_plan(cfg, round, identity, 'probe') for identity in range(1, 9)]
    r.update(config=cfg, experiment=EXPERIMENT, retry_plans=plans)
    for s, plan in zip(r['sessions'], plans):
        s['observation']['client_config']['resume'] = session_resume(cfg, plan)
    return r


def archive(root, rounds=1):
    m = budget_archive(root)
    reports = [shifted(report(case, i//4+1, case == CASES[2]), i*1000) for i, case in enumerate(order(rounds))]
    events = [dict(Action='output', Test=ROOT_TEST['probe']+'/round_'+str(r['round'])+'/'+r['config']['case'],
                   Output=MARKER['probe']+json.dumps(r)+'\n') for r in reports]
    events.append(dict(Action='pass', Test=ROOT_TEST['probe']))
    (root/'go-events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in events))
    source = root/'source'/PLAN
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text('fixture\n')
    m.update(experiment=EXPERIMENT, plan=PLAN, root_test=ROOT_TEST['probe'], planned_rounds=rounds,
             planned_sessions=32*rounds, order=order(rounds), experiment_environment=environment('probe', rounds))
    m['run_argv'][-4] = '-test.run=^'+ROOT_TEST['probe']+'$'
    m['source_sha256'][PLAN] = digest(source)
    m['artifacts_sha256']['go-events.jsonl'] = digest(root/'go-events.jsonl')
    (root/'manifest.json').write_text(json.dumps(m))
    return m


class GroupResumeJitterTest(unittest.TestCase):
    def test_archived_real_four_round_probe(self):
        root=Path(__file__).resolve().parent.parent/'results/group-resume-jitter-tools-2026-10-12'
        index=json.loads((root/'evidence-index.json').read_text())
        raw=lzma.decompress((root/'go-events.jsonl.xz').read_bytes())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),index['event_sha256'])
        for name,sha in index['files_sha256'].items():self.assertEqual(digest(root/name),sha)
        reports=extract([json.loads(line) for line in raw.splitlines()],'probe')
        self.assertEqual([r['config']['case'] for r in reports],order(4))
        rows=[verify_jitter_report(r,'probe') for r in reports]
        self.assertEqual(sum(r['resource_samples'] for r in rows),index['samples'])
        self.assertEqual(sum(r['completed']+r['failed'] for r in rows),128)
        self.assertTrue(all(r['environment']['race'] is False for r in rows))

    def test_pcg_reduction_rejects_bias_and_handles_power_of_two(self):
        p=PCG64(1)
        with patch.object(p,'uint64',side_effect=[0,(1<<64)-1]) as draw:
            self.assertEqual(p.int64n(1000000001),1000000000)
            self.assertEqual(draw.call_count,2)
        with patch.object(p,'uint64',return_value=(1<<64)-1):
            self.assertEqual(p.int64n(1),0)
            self.assertEqual(p.int64n(8),7)

    def test_fixed_conditions_and_balanced_order(self):
        self.assertEqual(len(order(4)), 16)
        for mode in ('formal', 'probe'):
            for a,b in ((CASES[0],CASES[1]),(CASES[2],CASES[3])):
                left,right = configured(a, mode),configured(b, mode)
                left['case'] = right['case'] = 'pair'
                self.assertEqual(left['resume']['MaxAttempts'],8)
                self.assertTrue(right['resume'].pop('Jitter'))
                self.assertEqual(left,right)
        for prefix in ('D_','E_'):
            self.assertEqual(sum(row[0 if prefix=='D_' else 2].endswith('_none') for row in
                                 [order(4)[i:i+4] for i in range(0,16,4)]),2)

    def test_seed_plan_unique_and_scenario_reuse(self):
        for mode in ('formal','probe'):
            seeds=set()
            for n in range(1,5):
                d=[retry_plan(configured(CASES[1],mode),n,i,mode) for i in range(1,9)]
                e=[retry_plan(configured(CASES[3],mode),n,i,mode) for i in range(1,9)]
                self.assertEqual(d,e)
                for p in d:
                    self.assertNotIn(p['seed'],seeds);seeds.add(p['seed'])
                    self.assertEqual(len(p['candidate_delays_ns']),7)
                    self.assertEqual(sum(p['candidate_delays_ns']),p['candidate_sum_ns'])
            self.assertEqual(len(seeds),32)
        self.assertNotEqual(retry_plan(configured(CASES[1],'formal'),1,1,'formal')['seed'],
                            retry_plan(configured(CASES[1],'probe'),1,1,'probe')['seed'])

    def test_success_and_explicit_failures(self):
        for case in CASES:
            for failed in (False,True):
                row=verify_jitter_report(report(case,failed=failed),'probe')
                self.assertEqual(row['completed'],0 if failed else 8)
                self.assertEqual(sum(s['resume_attempts'] for s in row['sessions']),64 if failed else 48)

    def test_old_contracts_reject_new_strategy(self):
        r=report()
        for checker in (verify_report,verify_budget_report):
            with self.assertRaises(ValueError): checker(r,'probe')
        old=budget_report('D_busy_500ms_a8')
        old['config']['resume']['Jitter']=True
        with self.assertRaises(ValueError): verify_budget_report(old,'probe')

    def test_report_counterexamples(self):
        changes={
            'seed':lambda r:r['retry_plans'][0].update(seed=1),
            'same seed every session':lambda r:r['retry_plans'][1].update(seed=r['retry_plans'][0]['seed']),
            'algorithm':lambda r:r['retry_plans'][0].update(algorithm='full_jitter'),
            'strategy':lambda r:r['retry_plans'][0].update(strategy='none'),
            'sequence':lambda r:r['retry_plans'][0]['candidate_delays_ns'].__setitem__(0,99),
            'sum':lambda r:r['retry_plans'][0].update(candidate_sum_ns=0),
            'missing plan':lambda r:r['retry_plans'].pop(),
            'duplicate plan':lambda r:r['retry_plans'].__setitem__(1,copy.deepcopy(r['retry_plans'][0])),
            'actual seed':lambda r:r['sessions'][0]['observation']['client_config']['resume'].update(JitterSeed=1),
            'actual strategy':lambda r:r['sessions'][0]['observation']['client_config']['resume'].update(Jitter=False),
            'race':lambda r:r['environment'].update(race=True),
            '32bit':lambda r:r['environment'].update(goarch='386'),
            'attempt overflow':lambda r:r['sessions'][0]['observation']['run_result'].update(ResumeAttempts=9),
            'window':lambda r:r['config']['resume'].update(Window=99),
            'backoff':lambda r:r['config']['resume'].update(MaxBackoff=99),
            'identity':lambda r:r.update(experiment='EXP-006-03'),
            'round':lambda r:r.update(round=0),
            'replacement RPC':lambda r:r.update(worker_calls=9),
            'audio':lambda r:r['sessions'][0]['observation']['worker'].update(audio='AAAA'),
            'result gap':lambda r:r['sessions'][0]['observation']['applications'][1].update(seq=9),
            'false ack':lambda r:r['sessions'][0]['observation']['run_result'].update(CompletionAckWritten=False),
            'forced cleanup':lambda r:r.update(forced_cleanup=True),
        }
        for name,change in changes.items():
            with self.subTest(name=name),self.assertRaises(ValueError):
                r=report();change(r);verify_jitter_report(r,'probe')

    def test_release_metrics_use_arrival_not_reply_time(self):
        r=report(CASES[3])
        row=verify_jitter_report(r,'probe')
        self.assertEqual(row['pre_release_handshake_limit'],40)
        self.assertEqual(row['post_release_handshake_limit'],0)
        self.assertEqual(row['post_release_arrivals'],8)
        self.assertEqual(sum(b['arrivals'] for b in row['release_bins']),8)
        self.assertTrue(any(b['arrivals']==0 for b in row['release_bins']))
        # 最后一次失败在释放前到达、释放后退出，仍归释放前拒绝。
        a=next(a for a in r['attempts'] if a['connection']==6)
        a['exited_at']=r['released_at']
        again=verify_jitter_report(r,'probe')
        self.assertEqual(again['post_release_handshake_limit'],0)

    def test_fixed_denominator_pairs_and_null_failure_latency(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';archive(root,4)
            result=summarize(root,root/'analysis')
            self.assertTrue(result['evidence_valid'],result['errors'])
            self.assertEqual(result['planned_sessions'],128)
            self.assertEqual([g['verified_completed'] for g in result['groups']],[32,32,0,32])
            self.assertEqual(len(result['pairs']),8)
            self.assertEqual(result['comparisons'][1]['difference']['completed'],32)
            self.assertIsNone(result['groups'][2]['metrics']['fresh_after_fault_ms'])
            self.assertEqual(result['groups'][3]['metrics']['max_release_bin']['n'],4)
            self.assertTrue((root/'analysis/release-bins.csv').exists())

    def test_missing_duplicate_and_wrong_path_keep_denominator(self):
        changes={'missing':lambda e:e.pop(0),'duplicate':lambda e:e.insert(0,copy.deepcopy(e[0])),
                 'path':lambda e:e[0].update(Test=ROOT_TEST['probe']+'/round_2/'+CASES[0])}
        for name,change in changes.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'run';archive(root);change_events(root,change)
                result=summarize(root,root/'analysis')
                self.assertFalse(result['evidence_valid'])
                self.assertEqual(result['planned_sessions'],32)
                self.assertTrue(all(c['difference'] is None for c in result['comparisons']))
                self.assertLessEqual(result['groups'][0]['verified_completed'],8)

    def test_swapped_batches_and_invalid_session_remain_unknown(self):
        for swap in (False,True):
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'run';archive(root)
                def change(e):
                    a=json.loads(e[0]['Output'][len(MARKER['probe']):])
                    if swap:
                        b=json.loads(e[1]['Output'][len(MARKER['probe']):])
                        e[0]['Output']=MARKER['probe']+json.dumps(shifted(a,1000))
                        e[1]['Output']=MARKER['probe']+json.dumps(shifted(b,-1000))
                    else:
                        a['sessions'][0]['observation']['client_config']['resume']['JitterSeed']=1
                        e[0]['Output']=MARKER['probe']+json.dumps(a)
                change_events(root,change)
                result=summarize(root,root/'analysis')
                self.assertFalse(result['evidence_valid'])
                self.assertEqual(result['planned_sessions'],32)
                self.assertIsNone(result['comparisons'][0]['difference'])
                if not swap:self.assertEqual(result['groups'][0]['unknown_sessions'],8)

    def test_archive_corruption_and_root_failure_suppress_metrics(self):
        for corrupt in (False,True):
            with tempfile.TemporaryDirectory() as temp:
                root=Path(temp)/'run';archive(root)
                if corrupt:(root/'group-resume.test').write_text('tampered')
                else:change_events(root,lambda e:e[-1].update(Action='fail'))
                result=summarize(root,root/'analysis')
                self.assertFalse(result['evidence_valid'])
                self.assertTrue(all(g['metrics'] is None for g in result['groups']))

    def test_manifest_counterexamples(self):
        changes={'identity':lambda m:m.update(experiment='EXP-006-03'),
                 'denominator':lambda m:m.update(planned_sessions=96),
                 'order':lambda m:m['order'].reverse(),
                 'test.count':lambda m:m['run_argv'].__setitem__(-3,'-test.count=4'),
                 'environment':lambda m:m['experiment_environment'].update(TIDE_GROUP_RESUME_JITTER_ROUNDS='2'),
                 'reference snapshot':lambda m:m['source_sha256'].pop(REFERENCE_PLAN),
                 'source change':lambda m:m.update(source_unchanged=False),
                 'path escape':lambda m:m['source_sha256'].update({'../manifest.json':'x'}),
                 'formal shortened':lambda m:m.update(mode='formal'),
                 'race':lambda m:m['go_env'].update(GOFLAGS='-race')}
        for name,change in changes.items():
            with self.subTest(name=name),tempfile.TemporaryDirectory() as temp,self.assertRaises(ValueError):
                root=Path(temp)/'run';m=archive(root);change(m);verify_manifest(root,m)

    def test_fragment_reassembly_and_credentials(self):
        r=report();text=MARKER['probe']+json.dumps(r)
        name=ROOT_TEST['probe']+'/round_1/'+CASES[1]
        events=[dict(Action='output',Test=name,Output=text[i:i+700]) for i in range(0,len(text),700)]
        self.assertEqual(extract(events,'probe'),[r])
        events.extend([dict(Action='output',Test='other',Output='resume_'),dict(Action='output',Test='other',Output='token')])
        with self.assertRaises(ValueError):extract(events,'probe')

    def test_archive_relocation_and_independent_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'run';archive(root,4)
            self.assertTrue(summarize(root,root/'analysis')['evidence_valid'])
            moved=Path(temp)/'moved';shutil.copytree(root,moved)
            self.assertTrue(summarize(moved,moved/'again')['evidence_valid'])
            for name in ('summary.json','reports.json','sessions.csv','attempt-bins.csv','release-bins.csv'):
                self.assertEqual((root/'analysis'/name).read_bytes(),(moved/'again'/name).read_bytes())

    def test_collector_rounds_and_existing_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            for mode,n in [('formal',3),('probe',0),('probe',5),('bad',4)]:
                with self.assertRaises(ValueError):collect(Path(temp)/'new',mode,n)
            with self.assertRaises(FileExistsError):collect(Path(temp),'probe',4)

    def test_collector_build_failure_preserves_four_round_plan(self):
        with tempfile.TemporaryDirectory() as temp:
            repo=Path(temp)/'repo';repo.mkdir()
            for name in (PLAN,REFERENCE_PLAN):
                p=repo/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('plan')
            def check_output(cmd,**kwargs):
                if cmd[:2]==['git','ls-files']:return (PLAN+'\0'+REFERENCE_PLAN+'\0').encode()
                if cmd[:2]==['go','env']:return b'{"GOVERSION":"go1.26.5","GOOS":"darwin","GOARCH":"arm64","GOFLAGS":""}'
                return b'fixture'
            with patch('run_group_resume.subprocess.check_output',side_effect=check_output),patch('run_group_resume.subprocess.run') as run:
                run.return_value.returncode=1
                out=Path(temp)/'failed';self.assertEqual(collect(out,'probe',4,repo),1);self.assertEqual(run.call_count,1)
            m=json.loads((out/'manifest.json').read_text())
            self.assertEqual(m['planned_sessions'],128)
            self.assertEqual(m['status'],'build_failed')
            self.assertIn(REFERENCE_PLAN,m['source_sha256'])


if __name__=='__main__':unittest.main()
