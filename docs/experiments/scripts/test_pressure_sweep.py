#!/usr/bin/env python3
"""加压报告包含失败时的统计与判据测试，合成数据不作为实测结果。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from run_loadgen_baseline import distribution
from summarize_pressure_sweep import (check_pressure_data, check_pressure_gateway, phase_observations,
                                      TAIL_BUDGET_NS, LAG_BUDGET_NS)

HERE = Path(__file__).resolve().parent


def fixture(outcomes=("completed",)):
    original = json.loads((HERE.parent/'results/joint-observation-baseline-2026-10-02/r1-n1.json').read_text())
    source = original['sessions'][0]
    original['config']['sessions'] = len(outcomes)
    original['config']['session']['timeout_ns'] = 35_000_000_000
    original['sessions'] = []
    for i, outcome in enumerate(outcomes):
        s = copy.deepcopy(source)
        s.update(index=i, outcome=outcome)
        if outcome != 'completed':
            s.update(error='synthetic '+outcome, tail_latency_ns=None)
            s['observation'].update(audio_bytes_written=32000, audio_chunks_written=10, audio_schedule_samples=11,
                                    write_failures=1, end_write=None, result_count=1, final_result_count=0, last_final_at=None)
        original['sessions'].append(s)
    recount(original)
    return original


def recount(doc):
    ss = doc['sessions']; summary = doc['summary']
    summary.update(planned_sessions=len(ss), planned_audio_bytes=len(ss)*160000, audio_bytes_written=sum(s['observation']['audio_bytes_written'] for s in ss))
    for outcome in ('completed','failed','canceled','timed_out'):
        summary[outcome] = sum(s['outcome'] == outcome for s in ss)
    summary['completion_rate'] = summary['completed']/len(ss)
    summary['tail'] = distribution([s['tail_latency_ns'] for s in ss if s['outcome'] == 'completed'])


def check(doc):
    return check_pressure_data(doc, len(doc['sessions']), 160000)


class PressureSweepTests(unittest.TestCase):
    def test_mixed_outcomes_keep_denominator_and_bytes(self):
        d = fixture(('completed','failed','canceled','timed_out'))
        before = copy.deepcopy(d)
        r = check(d)
        self.assertTrue(r['checks_passed'])
        self.assertFalse(r['criteria_met'])
        self.assertEqual(r['expected_exit_code'], 1)
        self.assertEqual(r['summary']['completion_rate'], .25)
        self.assertEqual(r['audio_delivery_fraction'], .4)
        self.assertEqual(r['summary']['tail']['samples'], 1)
        self.assertEqual(len(r['failure_errors']), 3)
        self.assertEqual(r['sessions_with_schedule_samples'], 4)
        self.assertEqual(d, before)

    def test_all_failed_has_no_tail(self):
        d = fixture(('failed','failed'))
        r = check(d)
        self.assertIsNone(r['summary']['tail'])
        self.assertFalse(r['criteria']['tail_p95_within_budget'])
        self.assertEqual(r['failure_errors'], {'synthetic failed':2})
        self.assertEqual(r['summary']['completed'], 0)

    def test_empty_error_is_still_failure(self):
        d = fixture(('failed',)); d['sessions'][0]['error'] = ''
        self.assertEqual(check(d)['failure_errors'], {'':1})

    def test_zero_audio_is_unknown_schedule(self):
        d = fixture(('failed',)); o = d['sessions'][0]['observation']
        o.update(audio_bytes_written=0, audio_chunks_written=0, audio_schedule_samples=0, max_audio_schedule_lag_ns=None,
                 first_audio_started_at=None,last_audio_finished_at=None,result_count=0,first_result_at=None,last_result_at=None)
        recount(d)
        r = check(d)
        self.assertIsNone(r['max_audio_schedule_lag_ns'])
        self.assertFalse(r['criteria']['sending_within_budget'])
        self.assertEqual(r['audio_delivery_fraction'], 0)

    def test_threshold_boundaries_and_batch_error(self):
        d = fixture(); s = d['sessions'][0]
        s['tail_latency_ns'] = TAIL_BUDGET_NS
        s['observation']['max_audio_schedule_lag_ns'] = LAG_BUDGET_NS
        recount(d)
        self.assertTrue(check(d)['criteria_met'])
        s['tail_latency_ns'] += 1; recount(d)
        self.assertFalse(check(d)['criteria']['tail_p95_within_budget'])
        s['tail_latency_ns'] -= 1; s['observation']['max_audio_schedule_lag_ns'] += 1; recount(d)
        self.assertFalse(check(d)['criteria']['sending_within_budget'])
        s['observation']['max_audio_schedule_lag_ns'] -= 1
        d['batch_error'] = 'canceled batch'
        self.assertFalse(check(d)['criteria_met'])
        self.assertEqual(check(d)['expected_exit_code'], 1)

    def test_failed_session_lag_included(self):
        d = fixture(('completed','failed'))
        d['sessions'][1]['observation']['max_audio_schedule_lag_ns'] = LAG_BUDGET_NS+1
        r = check(d)
        self.assertEqual(r['max_audio_schedule_lag_ns'], LAG_BUDGET_NS+1)
        self.assertFalse(r['criteria']['sending_within_budget'])

    def test_corrupt_report_rejected(self):
        for mode in ('version','summary_count','summary_bool','tail_summary','index','outcome','completed_error','completed_missing_tail','partial_completed','failed_tail','failed_no_error','bool_bytes','no_schedule','clock','results','missing_control','control_error','wrong_timeout','failed_write_count'):
            with self.subTest(mode=mode):
                d = fixture(('completed','failed')); s = d['sessions'][0]; o = s['observation']
                if mode == 'version': d['schema_version'] = True
                elif mode == 'summary_count': d['summary']['failed'] = 0
                elif mode == 'summary_bool': d['summary']['failed'] = True
                elif mode == 'tail_summary': d['summary']['tail']['samples'] += 1
                elif mode == 'index': s['index'] = 8
                elif mode == 'outcome': s['outcome'] = 'unknown'
                elif mode == 'completed_error': s['error'] = ''
                elif mode == 'completed_missing_tail': s['tail_latency_ns'] = None
                elif mode == 'partial_completed': o['audio_bytes_written'] -= 3200; o['audio_chunks_written'] -= 1
                elif mode == 'failed_tail': d['sessions'][1]['tail_latency_ns'] = 0
                elif mode == 'failed_no_error': d['sessions'][1]['error'] = None
                elif mode == 'bool_bytes': o['audio_bytes_written'] = True
                elif mode == 'no_schedule': del o['audio_schedule_samples']
                elif mode == 'clock': s['started_at'] = s['finished_at']
                elif mode == 'results': o['final_result_count'] = 6
                elif mode == 'missing_control': del o['end_write']
                elif mode == 'control_error': o['end_write']['error'] = 'write failed'
                elif mode == 'wrong_timeout': d['config']['session']['timeout_ns'] = 20_000_000_000
                elif mode == 'failed_write_count': d['sessions'][1]['observation']['write_failures'] = 0
                with self.assertRaises(ValueError): check(d)

    def test_gateway_unobserved_peak_does_not_hide_quality_failure(self):
        for gap in (True, False):
            original = dict(checks_passed=False, checks=dict(observed_planned_peak=False, bracket_gap_within_budget=gap))
            with patch('summarize_pressure_sweep.check_sampling', return_value=original):
                r = check_pressure_gateway(None, '', '', 12)
            self.assertEqual(r['checks_passed'], gap)
            self.assertFalse(r['observed_planned_peak'])

    def test_phases_exclude_post_exit_idle(self):
        def stamp(sec): return f'2026-10-02T00:00:{sec:02d}Z'
        with tempfile.TemporaryDirectory() as t:
            p = Path(t)
            for kind in ('gateway','worker'):
                q=p/('r1-n12-'+kind);q.mkdir()
                values=[]
                for sec in (1, 5, 6, 16):
                    state = dict(active_sessions=12 if sec<5 else 8) if kind=='gateway' else dict(processing_limit_enabled=True,processing=dict(in_use=1,waiting=11 if sec<5 else 7))
                    values.append(dict(started_at=stamp(sec),finished_at=stamp(sec),state=state,error=None))
                (q/'samples.jsonl').write_text(''.join(json.dumps(v)+'\n' for v in values))
            r=phase_observations(p,dict(label='r1-n12',started_at=stamp(0),finished_at=stamp(12)))
            self.assertEqual(r[0]['gateway']['active_histogram'], {'12':1})
            # 恰在 5 秒边界的零时长查询只能进入后一段，不能重复计数。
            self.assertEqual(r[1]['gateway']['active_histogram'], {'8':2})
            self.assertEqual(r[3]['covered_wall_ns'], 0)
            self.assertEqual(r[3]['worker']['samples'], 0)
            self.assertIsNone(r[3]['worker']['observed_peak_waiting'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
