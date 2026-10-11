"""独立合成证据及反例，不将工具 probe 当成正式性能数据。"""
import base64
import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from run_group_resume import ARTIFACTS, collect
from summarize_group_resume import (PROBE, ROOT_TEST, configured, digest, distribution,
                                    extract, group_pattern, summarize, verify_artifacts, verify_report)


def at(ms):
    return f'2026-10-11T00:00:{ms // 1000:02d}.{(ms % 1000) * 1000000:09d}Z'


def fixture(fault=False, failed=False):
    """构造八场、固定位置、处理先于应用的证据；不依赖 Go 导出的派生指标。"""
    case = 'B_resume_h8' if fault else 'A_normal'
    cfg = configured(case, 'probe')
    r = dict(version=2, config=cfg, environment=dict(go_version='go1.26.5', goos='darwin', goarch='arm64',
             cpus=12, gomaxprocs=12, race=False, transport='loopback_tcp', topology='same_process'),
             gateway_config=dict(MaxSessions=8, MaxMessageBytes=1048576, StartTimeout=10000000000,
             InputIdleTimeout=30000000000, WorkerSendTimeout=2000000000, TailTimeout=15000000000,
             ResultWriteTimeout=2000000000, MaxPendingAudioBytes=0, V2=dict(MaxHandshakes=cfg['max_handshakes'],
             EntryTimeout=10000000000, InputProgressTimeout=30000000000, ResumeWindow=1000000000,
             ResultRetentionTimeout=1000000000, WorkerStatusTimeout=2000000000, MaxAudioBytes=1048576,
             MaxAudioChunks=256, MaxResultBytes=1048576, MaxResults=256)), started_at=at(0), returned_at=at(800),
             natural_idle_at=at(801), fault_at=at(200) if fault else None, occupied_at=None, released_at=None,
             picks=8, worker_calls=8, tool_errors=[], evidence_errors=[], forced_cleanup=False,
             barrier_at=[at(200)] * 8, close_started_at=[at(201) if fault else None] * 8,
             close_finished_at=[at(202) if fault else None] * 8, sessions=[], attempts=[], samples=[])
    for identity in range(1, 9):
        data = group_pattern(identity, 25600)
        obs = dict(input_sha256=hashlib.sha256(data).hexdigest(), started_at=at(0), returned_at=at(800),
                   client_config=dict(chunk_bytes=3200, realtime=True, read_limit=65536, max_replay_bytes=320000,
                   max_replay_chunks=128, handshake_timeout_ns=10000000000, write_timeout_ns=5000000000,
                   apply_timeout_ns=5000000000, completion_timeout_ns=30000000000, resume=cfg['resume']),
                   tool_errors=[], source_closes=1, run_error_class='recovery_exhausted' if failed else '', outer_deadline=False,
                   source_reads=[], frames=[], applications=[], readies=[dict(connection=1, generation=1, next_offset=0, acked_seq=0, written_at=at(0))],
                   worker=dict(calls=1, started_at=at(0), exited_at=at(790), audio=base64.b64encode(data).decode(), chunks=[], tail_sent_at=at(710)))
        for i in range(8):
            begin, end, t = i * 3200, (i + 1) * 3200, i * 100
            obs['source_reads'].append(dict(begin=begin, end=end, started_at=at(t), finished_at=at(t), eof=False))
            obs['frames'].append(dict(begin=begin, end=end, connection=1, read_at=at(t), forward_started_at=at(t),
                                     forward_returned_at=at(t), forwarded=True, dropped=False, sha256=hashlib.sha256(data[begin:end]).hexdigest()))
            obs['worker']['chunks'].append(dict(begin=begin, end=end, received_at=at(t), processed_at=at(t + 1),
                                                progress_sent_at=at(t + 2), result_sent_at=at(t + 3)))
            obs['applications'].append(dict(seq=i + 1, audio_end=end, final=False, received_at=at(t + 4), applied_at=at(t + 5)))
        obs['source_reads'].append(dict(begin=25600, end=25600, started_at=at(701), finished_at=at(701), eof=True))
        obs['applications'].append(dict(seq=9, audio_end=25600, final=True, received_at=at(712), applied_at=at(713)))
        r['attempts'].append(dict(id=identity, connection=1, arrived_at=at(0), backend_at=at(0),
                                 exited_at=at(202) if fault else at(800), kind='start', code='', code_written=False, http_status=101))
        if fault:
            dropped = obs['frames'][2]
            replay = copy.deepcopy(dropped)
            replay.update(connection=2, read_at=at(210), forward_started_at=at(211), forward_returned_at=at(212))
            dropped.update(dropped=True, forwarded=False, forward_started_at=None, forward_returned_at=None)
            obs['frames'].insert(3, replay)
            obs['worker']['chunks'][2].update(received_at=at(212), processed_at=at(213), progress_sent_at=at(214), result_sent_at=at(215))
            obs['applications'][2].update(received_at=at(216), applied_at=at(217))
            obs['readies'].append(dict(connection=2, generation=2, next_offset=6400, acked_seq=2, written_at=at(210)))
            r['attempts'].append(dict(id=identity, connection=2, arrived_at=at(203), backend_at=at(204),
                                     exited_at=at(800), kind='resume', code='', code_written=False, http_status=101))
        if failed:
            obs['applications'].pop()  # 音频已处理，但没有最终完成事实，仍保留失败场。
        obs['run_result'] = dict(Completed=not failed, FinalOffset=0 if failed else 25600, AppliedSeq=len(obs['applications']),
                                ResumeAttempts=1 if fault else 0, ResumedConnections=1 if fault else 0, CompletionAckWritten=not failed)
        r['sessions'].append(dict(id=identity, started=True, last_entry_code='', observation=obs))
    for ms in (0, 200, 800, 801):
        active = 0 if ms >= 801 else 8
        r['samples'].append(dict(begin_at=at(ms), at=at(ms), sessions=active, registry=active, worker_active=active,
                                  handshakes=0, gateway_handlers=active, proxy_handlers=active, fault_timers=0,
                                  goroutines=40, heap_alloc_bytes=1000000, heap_inuse_bytes=2000000))
    return r


def archive(root, reports):
    root.mkdir()
    manifest = dict(version=1, mode='probe', planned_rounds=1, planned_sessions=40, order=PROBE, status='collected',
                    build_exit=0, run_exit=0, source_unchanged=True, go_env=dict(GOVERSION='go1.26.5', GOOS='darwin', GOARCH='arm64'))
    events = []
    for r in reports:
        test = ROOT_TEST['probe'] + '/' + r['config']['case']
        events.append(dict(Action='output', Test=test, Output='GROUP_RESUME_PROBE_JSON ' + json.dumps(r)))
    events.append(dict(Action='pass', Test=ROOT_TEST['probe']))
    for name in ARTIFACTS:
        (root / name).write_text('fixture\n')
    (root / 'go-events.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))
    (root / 'source').mkdir()
    (root / 'source' / 'fixture.go').write_text('package fixture\n')
    manifest['source_sha256'] = {'fixture.go': digest(root / 'source' / 'fixture.go')}
    manifest['artifacts_sha256'] = {name: digest(root / name) for name in ARTIFACTS}
    (root / 'manifest.json').write_text(json.dumps(manifest))
    return manifest


class GroupResumeAnalysisTest(unittest.TestCase):
    def test_archived_real_network_report_contract(self):
        root = Path(__file__).resolve().parent.parent / 'results/group-resume-tools-2026-10-11'
        raw = gzip.decompress((root / 'go-events.jsonl.gz').read_bytes())
        index = json.loads((root / 'evidence-index.json').read_text())
        self.assertEqual(hashlib.sha256(raw).hexdigest(), index['event_sha256'])
        reports = extract([json.loads(line) for line in raw.splitlines()], 'probe')
        self.assertEqual([r['config']['case'] for r in reports], PROBE)
        rows = [verify_report(r, 'probe') for r in reports]
        self.assertEqual([r['completed'] for r in rows], [8, 8, 8, 8, 0])
        self.assertEqual(sum(r['failed'] for r in rows), 8)
        self.assertEqual(sum(r['resource_samples'] for r in rows), index['samples'])

    def test_synthetic_complete_and_failed_evidence(self):
        for fault in (False, True):
            for failed in (False, True):
                with self.subTest(fault=fault, failed=failed):
                    r = verify_report(fixture(fault, failed), 'probe')
                    self.assertEqual(r['completed'], 0 if failed else 8)
                    self.assertEqual(r['failed'], 8 if failed else 0)
                    self.assertEqual(r['sessions'][0]['result_actual_wait_ms']['n'], 8)

    def test_distinct_input_and_embedded_identity(self):
        self.assertNotEqual(group_pattern(1, 25600), group_pattern(2, 25600))
        self.assertEqual(int.from_bytes(group_pattern(8, 25600)[:8], 'little'), 8)

    def test_fragmented_interleaved_reports(self):
        reports = [fixture(), fixture(True)]
        texts = ['GROUP_RESUME_PROBE_JSON ' + json.dumps(r) for r in reports]
        events = [dict(Action='output', Test=ROOT_TEST['probe'] + '/' + reports[j]['config']['case'], Output=s[i:i + 83])
                  for i in range(0, max(map(len, texts)), 83) for j, s in enumerate(texts)]
        self.assertEqual(extract(events, 'probe'), reports)
        with self.assertRaises(ValueError):
            extract(events[:-10], 'probe')

    def test_fragmented_credentials_rejected(self):
        with self.assertRaisesRegex(ValueError, 'credentials'):
            extract([dict(Action='output', Test='x', Output=x) for x in ('resume_', 'token')], 'probe')

    def test_probe_cannot_be_formal(self):
        with self.assertRaises(ValueError):
            verify_report(fixture(), 'formal')

    def test_evidence_mutations_rejected(self):
        mutations = {
            'hash': lambda r, s: s.update(input_sha256='wrong'),
            'bytes': lambda r, s: s['worker'].update(audio='AAAA'),
            'capture gap': lambda r, s: s['source_reads'][1].update(begin=0),
            'frame hash': lambda r, s: s['frames'][0].update(sha256='wrong'),
            'sequence': lambda r, s: s['applications'][1].update(seq=99),
            'processing': lambda r, s: s['worker']['chunks'][0].update(processed_at=None),
            'false complete': lambda r, s: s['applications'].pop(),
            'source owner': lambda r, s: s.update(source_closes=2),
            'client budget': lambda r, s: s['client_config'].update(max_replay_bytes=1),
            'gateway budget': lambda r, s: r['gateway_config']['V2'].update(MaxHandshakes=99),
            'replacement RPC': lambda r, s: r.update(worker_calls=9),
            'forced cleanup': lambda r, s: r.update(forced_cleanup=True),
            'no natural idle': lambda r, s: r['samples'][-1].update(registry=1),
            'fake runtime': lambda r, s: r['samples'][0].update(goroutines=0),
            'no environment': lambda r, s: r['environment'].update(cpus=0),
            'false completion type': lambda r, s: s['run_result'].update(Completed='true'),
            'ready on rejected upgrade': lambda r, s: r['attempts'][1].update(http_status=503, code_written=True, code='handshake_limit'),
            'accepted without ready': lambda r, s: s['readies'].pop(),
            'missing session': lambda r, s: r['sessions'].pop(),
            'cross session': lambda r, s: r['sessions'][0].update(id=8),
            'fallback start': lambda r, s: r['attempts'][1].update(kind='start'),
            'attempt budget': lambda r, s: s['run_result'].update(ResumeAttempts=99),
            'startup spread': lambda r, s: s['source_reads'][0].update(started_at=at(601), finished_at=at(601)),
            'cut spread': lambda r, s: r['close_finished_at'].__setitem__(0, at(799)),
        }
        for name, change in mutations.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                r = fixture(True); change(r, r['sessions'][0]['observation']); verify_report(r, 'probe')

    def test_bins_include_rejection_unknown_kind_and_empty_intervals(self):
        r = fixture(True, True)
        a = r['attempts'][1]
        a.update(arrived_at=at(401), backend_at=at(402), kind='', code='handshake_limit', http_status=503, code_written=True)
        r['sessions'][0]['observation']['readies'].pop()
        r['sessions'][0]['observation']['run_result']['ResumedConnections'] = 0
        row = verify_report(r, 'probe')
        self.assertEqual([b['arrivals'] for b in row['attempt_bins'][:5]], [7, 0, 0, 0, 1])
        self.assertEqual(len(row['attempt_bins']), 13)
        self.assertEqual(row['attempt_bins'][-1]['arrivals'], 0)
        self.assertEqual(sum(b['first'] for b in row['attempt_bins']), 8)
        self.assertEqual(sum(b['retries'] for b in row['attempt_bins']), 0)
        self.assertEqual(row['rejection_codes'], {'handshake_limit': 1})

    def test_fresh_result_excludes_replay_and_actual_vs_planned_wait(self):
        r = fixture(True)
        row = verify_report(r, 'probe')['sessions'][0]
        self.assertEqual(row['fresh_after_fault']['audio_end'], 12800)
        self.assertEqual(row['fresh_after_fault']['after_anchor_ms'], 105)
        self.assertEqual(row['fresh_after_fault']['actual_wait_ms'], 5)
        s = r['sessions'][0]['observation']
        s['source_reads'][3].update(started_at=at(303), finished_at=at(303))
        row = verify_report(r, 'probe')['sessions'][0]
        self.assertEqual(row['max_capture_lag_ms'], 3)
        self.assertEqual(row['fresh_after_fault']['actual_wait_ms'], 2)

    def test_absent_result_is_null_not_zero(self):
        r = fixture(True, True)
        for session in r['sessions']:
            session['observation']['applications'] = session['observation']['applications'][:2]
            session['observation']['run_result']['AppliedSeq'] = 2
        self.assertIsNone(verify_report(r, 'probe')['sessions'][0]['fresh_after_fault'])
        self.assertIsNone(distribution([]))

    def test_manifest_missing_hash_tamper_and_path_escape(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'run'; m = archive(root, [])
            verify_artifacts(root, m)
            for name, change in {
                'missing artifact': lambda x: x['artifacts_sha256'].pop('go-events.jsonl'),
                'hash mismatch': lambda x: x['artifacts_sha256'].update({'go-events.jsonl': 'wrong'}),
                'source changed': lambda x: x.update(source_unchanged=False),
                'path escape': lambda x: x['source_sha256'].update({'../manifest.json': 'wrong'}),
                'run failed': lambda x: x.update(run_exit=1),
            }.items():
                with self.subTest(name=name), self.assertRaises(ValueError):
                    bad = copy.deepcopy(m); change(bad); verify_artifacts(root, bad)

    def test_missing_duplicate_batches_keep_fixed_denominator(self):
        for duplicate in (False, True):
            with self.subTest(duplicate=duplicate), tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / 'run'; archive(root, [fixture()] * (2 if duplicate else 1))
                summary = summarize(root, root / 'analysis')
                self.assertFalse(summary['evidence_valid'])
                self.assertEqual(summary['planned_sessions'], 40)
                self.assertEqual(summary['groups'][1]['unknown_sessions'], 8)
                if duplicate:
                    self.assertIsNone(summary['groups'][0]['completion_rate'])
                with self.assertRaises(FileExistsError):
                    summarize(root, root / 'analysis')

    def test_corrupt_archive_suppresses_metrics_even_when_rows_validate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / 'run'; archive(root, [fixture()])
            (root / 'group-resume.test').write_text('changed')
            summary = summarize(root, root / 'analysis')
            self.assertEqual(summary['verified_batches'], 1)
            self.assertIsNone(summary['groups'][0]['completion_rate'])
            self.assertFalse(summary['evidence_valid'])

    def test_collection_rejects_invalid_rounds_and_existing_output(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            with self.assertRaises(ValueError): collect(path / 'x', 'formal', 1)
            self.assertFalse((path / 'x').exists())
            with self.assertRaises(FileExistsError): collect(path, 'probe', 1)

    def test_build_failure_preserves_manifest_and_does_not_run(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / 'repo'; repo.mkdir(); (repo / 'docs/experiments').mkdir(parents=True)
            (repo / 'docs/experiments/group-resume-baseline.md').write_text('plan')
            (repo / 'go.mod').write_text('module fixture')
            def output(command, **kwargs):
                if command[:3] == ['git', 'ls-files', '-z']: return b'go.mod\0docs/experiments/group-resume-baseline.md\0'
                if command[:2] == ['go', 'env']: return b'{"GOVERSION":"go1.26.5","GOOS":"darwin","GOARCH":"arm64","GOFLAGS":""}'
                return b'fixture'
            with patch('run_group_resume.subprocess.check_output', side_effect=output), patch('run_group_resume.subprocess.run') as run:
                run.return_value.returncode = 1
                archive_dir = Path(temp) / 'failed'
                self.assertEqual(collect(archive_dir, 'probe', 1, repo), 1)
                self.assertEqual(run.call_count, 1)
            m = json.loads((archive_dir / 'manifest.json').read_text())
            self.assertEqual(m['status'], 'build_failed')
            self.assertIsNone(m['run_exit'])
            self.assertEqual(m['planned_sessions'], 40)
            self.assertFalse((archive_dir / 'analysis').exists())


if __name__ == '__main__':
    unittest.main()
