"""恢复分析器测试：长 Go 日志重组、真实归档复核和证据矛盾拒绝。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from summarize_client_resume import extract, pattern, stamp_ns, summarize, verify_report

ROOT = Path(__file__).resolve().parent.parent/'results/client-resume-baseline-2026-10-09-run-1'


class ClientResumeAnalysisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reports = json.loads((ROOT/'reports.json').read_text())

    def test_nanosecond_time_and_timezone(self):
        self.assertEqual(stamp_ns('1970-01-01T00:00:00.000000001Z'), 1)
        self.assertEqual(stamp_ns('1970-01-01T08:00:00.000000001+08:00'), 1)
        self.assertEqual(stamp_ns('1969-12-31T23:59:59.999999999Z'), -1)
        for value in (None, 'no-time', '2026-01-01T00:00:00.1234567890Z'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                stamp_ns(value)

    def test_long_log_fragments_reassemble(self):
        report = dict(config=dict(case='example'), payload='x'*20000)
        text = '    helper: CLIENT_RESUME_JSON '+json.dumps(report)+'\nPASS\n'
        events = [dict(Action='output', Output=text[i:i+1024]) for i in range(0, len(text), 1024)]
        self.assertEqual(extract(events), [report])
        with self.assertRaises(json.JSONDecodeError):
            extract(events[:-2])

    def test_credentials_rejected(self):
        with self.assertRaisesRegex(ValueError, 'credentials'):
            extract([dict(Action='output', Output='resume_token')])

    def test_pattern_corresponds_to_archived_worker(self):
        for report in self.reports:
            with self.subTest(case=report['config']['case']):
                row = verify_report(report)
                self.assertTrue(row['criteria_met'])
                self.assertLess(row['max_clock_difference_ms'], 1)
        self.assertNotEqual(pattern(6400)[:3200], pattern(6400)[3200:])

    def test_false_completed_and_missing_failures_rejected(self):
        r = copy.deepcopy(self.reports[1])
        self.assertFalse(r['business_completed'])
        self.assertFalse(verify_report(r)['business_completed'])
        r['business_completed'] = True
        with self.assertRaisesRegex(ValueError, 'business completion'):
            verify_report(r)

    def test_mutated_evidence_rejected(self):
        cases = {
            'input hash': lambda r: r.update(input_sha256='wrong'),
            'Worker bytes': lambda r: r['worker'].update(audio='AAAA'),
            'source gap': lambda r: r['source_reads'][1].update(begin=0),
            'frame hash': lambda r: r['frames'][0].update(sha256='wrong'),
            'application gap': lambda r: r['applications'][1].update(seq=9),
            'effective client budget': lambda r: r['client_config'].update(max_replay_bytes=1000000),
            'configured outage': lambda r: r['config'].update(outage_ns=900000000),
            'ready duration': lambda r: r['metrics'].update(ready_after_open_ms=9000),
            'repeated bytes': lambda r: r['metrics'].update(repeated_transfer_bytes=0),
            'forced cleanup': lambda r: r.update(forced_cleanup=True),
            'missing natural task exits': lambda r: r.update(natural_tasks_exited=False),
            'fault missing': lambda r: r['frames'][19].update(dropped=False),
            'phantom committed results': lambda r: r['run_result'].update(AppliedSeq=99),
        }
        for name, mutate in cases.items():
            r = copy.deepcopy(self.reports[2])
            mutate(r)
            with self.subTest(name=name), self.assertRaises(ValueError):
                verify_report(r)

    def test_mutated_clock_difference_not_silently_ignored(self):
        r = copy.deepcopy(self.reports[0])
        r['metrics']['waits'][0]['wait_ms'] += 2
        with self.assertRaisesRegex(ValueError, 'exceeds 1ms'):
            verify_report(r)

    def test_incomplete_round_not_replaced(self):
        # 只复制输入产物；分析发现缺报告时，不能输出一个缩小分母的摘要。
        with tempfile.TemporaryDirectory(prefix='tide-resume-incomplete-') as temp:
            root = Path(temp)
            for name in ('manifest.json', 'preregistration.md'):
                (root/name).write_bytes((ROOT/name).read_bytes())
            first = self.reports[0]
            (root/'go-events.jsonl').write_text(json.dumps(dict(Action='output',Output='CLIENT_RESUME_JSON '+json.dumps(first)))+'\n')
            with self.assertRaisesRegex(ValueError, 'reports missing'):
                summarize(root)
            self.assertFalse((root/'summary.json').exists())


if __name__ == '__main__':
    unittest.main()
