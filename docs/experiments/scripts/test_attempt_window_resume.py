"""EXP-006-07 configuration and evidence counterexamples; probe is not performance data."""
import copy
import json
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest

import attempt_window_resume_contract as profile
from summarize_extended_resume import read_events, summarize, verify_case, verify_manifest
from run_extended_resume import collect

ARCHIVE = Path(__file__).resolve().parents[1]/'results/recovery-budget-tools-2026-10-14/collection.tar.xz'


class ProfileTest(unittest.TestCase):
    def test_fixed_formal_and_probe_windows(self):
        self.assertEqual(profile.order(2), ['L_lifetime_5', 'S_sliding_5', 'S_sliding_5', 'L_lifetime_5'])
        for mode, window in [('probe', 1_000_000_000), ('formal', 60_000_000_000)]:
            self.assertNotIn('AttemptWindow', profile.client_config(profile.CASES[0], mode)['resume'])
            self.assertEqual(profile.client_config(profile.CASES[1], mode)['resume']['AttemptWindow'], window)
            self.assertEqual(profile.configured(profile.CASES[1], mode)['attempts'], 5)
        for rounds in [True, 1, 3]:
            with self.assertRaises(ValueError): profile.order(rounds)
        for case, mode in [('B_budget_8', 'probe'), (profile.CASES[1], 'wrong')]:
            with self.assertRaises(ValueError): profile.configured(case, mode)

    def test_formal_profiling_guard(self):
        for value in ['memprofilerate=1', 'gctrace=0,memprofilerate=01', 'memprofilerate=1,memprofilerate=100']:
            with self.assertRaises(ValueError): profile.validate_environment('formal', {'GODEBUG': value})
        profile.validate_environment('formal', {'GODEBUG': None})
        profile.validate_environment('probe', {'GODEBUG': 'memprofilerate=1'})

    def test_collector_invalid_entry_before_creation(self):
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)/'absent'
            with self.assertRaises(ValueError): collect(target, 'formal', 1, profile=profile)
            self.assertFalse(target.exists())
            with self.assertRaises(FileExistsError): collect(Path(temp), 'probe', 2, profile=profile)


class EvidenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        provided = os.environ.get('TIDE_ATTEMPT_WINDOW_ANALYSIS_FIXTURE')
        if provided:
            cls.root = Path(provided)
        else:
            with tarfile.open(ARCHIVE) as archive: archive.extractall(cls.temp.name, filter='data')
            cls.root = Path(cls.temp.name)/'collection'
        cls.manifest = json.loads((cls.root/'manifest.json').read_text())

    @classmethod
    def tearDownClass(cls): cls.temp.cleanup()

    def mutate(self, change, case=profile.CASES[1]):
        index = 1 if case == profile.CASES[0] else 2
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)/'data'
            shutil.copytree(self.root/f'sessions/{index:02d}_{case}/data', target)
            rows = read_events(target/'events.jsonl')
            summary = json.loads((target/'summary.json').read_text())
            change(rows, summary)
            counts = {}
            for row in rows:
                c = row['component']; counts[c] = counts.get(c, 0)+1; row['seq'] = counts[c]
            raw = b''.join((json.dumps(r, separators=(',', ':'))+'\n').encode() for r in rows)
            summary.update(events=len(rows), buffer_accepted_bytes=len(raw))
            (target/'events.jsonl').write_bytes(raw)
            (target/'summary.json').write_text(json.dumps(summary))
            return verify_case(target, 'probe', case, profile)

    def test_four_real_probe_outcomes(self):
        for i, case in enumerate(profile.order(2), 1):
            row = verify_case(self.root/f'sessions/{i:02d}_{case}/data', 'probe', case, profile)
            self.assertTrue(row['behavior_passed'])
            self.assertEqual(row['completed'], case == profile.CASES[1])
            self.assertEqual(row['attempts'], 5 if case == profile.CASES[0] else 6)
            if row['completed']:
                self.assertEqual(row['processed_bytes'], 384000)
                self.assertEqual(row['applied_results'], 121)
            else:
                self.assertEqual(row['outages'][-1]['observed_requests'], 0)
                self.assertIsNone(row['outages'][-1]['fresh_wait_ns'])
                self.assertTrue(row['outages'][-1]['right_censored'])

    def test_actual_window_drift_rejected(self):
        for window in [0, 60_000_000_000, True]:
            def change(rows, s):
                s['client_config']['resume']['AttemptWindow'] = window
                next(r for r in rows if r['kind'] == 'configuration')['data']['client']['resume']['AttemptWindow'] = window
            with self.subTest(window=window), self.assertRaises(ValueError): self.mutate(change)

    def test_profile_identity_and_config_drift(self):
        for field, value in [('experiment', 'EXP-006-05'), ('attempt_window_ns', 60_000_000_000), ('attempts', 6)]:
            def change(rows, s):
                s['config'][field] = value
                next(r for r in rows if r['kind'] == 'configuration')['data']['config'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): self.mutate(change)

    def test_lifetime_limit_still_enforced(self):
        def change(rows, s):
            s['result']['ResumeAttempts'] = 6
            next(r for r in rows if r['kind'] == 'returned')['data']['result']['ResumeAttempts'] = 6
        with self.assertRaises(ValueError): self.mutate(change, profile.CASES[0])

    def test_sliding_counts_not_unchecked(self):
        for key, value in [('ResumeAttempts', 5), ('ResumedConnections', 7)]:
            def change(rows, s):
                s['result'][key] = value
                next(r for r in rows if r['kind'] == 'returned')['data']['result'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): self.mutate(change)

    def test_missing_or_false_terminal_facts(self):
        for component, kind in [('worker', 'exit'), ('run', 'joined'), ('proxy', 'fault'), ('proxy', 'handler_exit'), ('source', 'close')]:
            def change(rows, s): rows.remove(next(r for r in rows if r['component'] == component and r['kind'] == kind))
            with self.subTest(kind=kind), self.assertRaises(ValueError): self.mutate(change)
        with self.assertRaises(ValueError): self.mutate(lambda rows, s: s.update(forced_cleanup=True))
        def completed(rows, s):
            s['result']['Completed'] = True
            next(r for r in rows if r['kind'] == 'returned')['data']['result']['Completed'] = True
        with self.assertRaises(ValueError): self.mutate(completed, profile.CASES[0])

    def test_position_gap_and_duplicate_result(self):
        for component, kind, key, value in [('worker', 'received', 'begin', 1), ('run', 'application', 'seq', 2)]:
            def change(rows, s): next(r for r in rows if r['component'] == component and r['kind'] == kind)['data'][key] = value
            with self.subTest(kind=kind), self.assertRaises(ValueError): self.mutate(change)

    def test_legacy_analyzer_cannot_guess_new_policy(self):
        with self.assertRaises(ValueError): verify_manifest(self.root, self.manifest)

    def test_manifest_order_slot_and_profile_snapshots(self):
        verify_manifest(self.root, self.manifest, profile)
        for field, value in [('order', [profile.CASES[1], profile.CASES[0], profile.CASES[1], profile.CASES[0]]), ('planned_sessions', 3), ('root_test', 'TestExtendedResumeCollectedCase')]:
            m = copy.deepcopy(self.manifest); m[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError): verify_manifest(self.root, m, profile)
        for path in profile.EXTRA_SOURCES:
            m = copy.deepcopy(self.manifest); del m['source_sha256'][path]
            with self.subTest(path=path), self.assertRaises(ValueError): verify_manifest(self.root, m, profile)
        m = copy.deepcopy(self.manifest); m['slots'][2]['round'] = 1
        with self.assertRaises(ValueError): verify_manifest(self.root, m, profile)

    def test_corrupt_archive_keeps_four_unknown_slots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)/'collection'; shutil.copytree(self.root, root)
            (root/'sessions/02_S_sliding_5/data/events.jsonl').write_bytes(b'broken\n')
            result = summarize(root, Path(temp)/'analysis', profile)
            self.assertFalse(result['evidence_valid'])
            self.assertEqual(result['planned_sessions'], 4)
            self.assertEqual(result['verified_sessions'], 0)
            self.assertEqual(sum(g['unknown'] for g in result['groups']), 4)

    def test_portable_reanalysis(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)/'moved'; shutil.copytree(self.root, root)
            a = summarize(self.root, Path(temp)/'a', profile)
            b = summarize(root, Path(temp)/'b', profile)
            self.assertTrue(a['evidence_valid']); self.assertEqual(a, b)
            for p in (Path(temp)/'a').iterdir(): self.assertEqual(p.read_bytes(), (Path(temp)/'b'/p.name).read_bytes())


if __name__ == '__main__': unittest.main()
