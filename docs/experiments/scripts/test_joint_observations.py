#!/usr/bin/env python3
"""联合观测分析的独立合成样本；不使用正式结果作为预期值。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from run_joint_observation_baseline import wait_sampling_ready
from summarize_joint_observations import check_worker_data
from test_gateway_observations import stamp, recount

ENDPOINT = "http://127.0.0.1:50081/debug/worker"


def fixture():
    states = [(0, 0), (1, 1), (0, 0), (0, 0)]
    rows = [dict(source_kind="worker", schema_version=1, index=i, started_at=stamp(ms), finished_at=stamp(ms+1), duration_ns=1_000_000,
                 state=dict(processing_limit_enabled=True, processing=dict(limit=1, in_use=u, waiting=w)), error=None)
            for i, (ms, (u, w)) in enumerate(zip((0, 100, 200, 300), states))]
    manifest = dict(source_kind="worker", schema_version=1, output_path="samples.jsonl", started_at=stamp(0), finished_at=stamp(999),
                    config=dict(endpoint=ENDPOINT, interval_ns=20_000_000, request_timeout_ns=1_000_000_000),
                    samples_written=4, successful_samples=4, failed_samples=0, stop_reason="deadline_exceeded",
                    sampling_error="context deadline exceeded", output_error=None, close_error=None)
    return manifest, rows


def check(manifest, rows, finish=250):
    return check_worker_data(manifest, rows, stamp(50), stamp(finish), 2, ENDPOINT)


class JointObservationTests(unittest.TestCase):
    def test_valid_window_and_sample_denominator(self):
        m, rows = fixture()
        before = copy.deepcopy((m, rows))
        got = check(m, rows)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["window_state_histogram"], {"0,0": 1, "1,1": 1})
        self.assertEqual(got["window_successful_samples"], 2)
        self.assertEqual(got["busy_sample_fraction"], 0.5)
        self.assertEqual(got["waiting_sample_fraction"], 0.5)
        self.assertEqual(got["max_bracketing_success_gap_ns"], 100_000_000)
        self.assertEqual(got["first_post_exit_idle_sample_delay_ns"], 51_000_000)
        self.assertEqual((m, rows), before)

    def test_disabled_is_not_zero(self):
        m, rows = fixture()
        rows[1]["state"] = dict(processing_limit_enabled=False, processing=None)
        got = check(m, rows)
        self.assertFalse(got["checks_passed"])
        self.assertEqual(got["successful_samples"], 4)
        self.assertEqual(got["disabled_samples"], 1)
        self.assertEqual(got["window_successful_samples"], 1)
        self.assertEqual(got["window_state_histogram"], {"0,0": 1})

    def test_failed_is_unknown_and_kept(self):
        m, rows = fixture()
        rows[1].update(state=None, error="503")
        recount(m, rows)
        got = check(m, rows)
        self.assertFalse(got["checks_passed"])
        self.assertEqual(got["window_failed_samples"], 1)
        self.assertEqual(got["window_successful_samples"], 1)
        # 负载窗口后的失败仍保存，但不会否定已覆盖的负载窗口。
        m, rows = fixture()
        tail = copy.deepcopy(rows[-1])
        tail.update(started_at=stamp(400), finished_at=stamp(401), state=None, error="deadline")
        rows.append(tail)
        recount(m, rows)
        got = check(m, rows)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["failed_samples"], 1)

    def test_coverage_and_fixture_failures(self):
        for mode in ("no_pre", "no_post", "no_idle_after", "wide_gap", "all_failed", "all_disabled", "wrong_limit", "too_many_waiting"):
            with self.subTest(mode=mode):
                m, rows = fixture()
                finish = 250
                if mode == "no_pre": rows = rows[1:]
                elif mode == "no_post": rows = rows[:-1]
                elif mode == "no_idle_after": rows[-1]["state"]["processing"]["waiting"] = 1
                elif mode == "wide_gap":
                    for r, ms in zip(rows[2:], (700, 800)):
                        r.update(started_at=stamp(ms), finished_at=stamp(ms+1))
                    finish = 750
                elif mode == "all_failed":
                    for r in rows: r.update(state=None, error="unknown")
                elif mode == "all_disabled":
                    for r in rows: r["state"] = dict(processing_limit_enabled=False, processing=None)
                elif mode == "wrong_limit": rows[1]["state"]["processing"]["limit"] = 2
                elif mode == "too_many_waiting": rows[1]["state"]["processing"]["waiting"] = 2
                recount(m, rows)
                got = check(m, rows, finish)
                self.assertFalse(got["checks_passed"])
                if mode in ("all_failed", "all_disabled"):
                    self.assertIsNone(got["observed_peak_in_use"])
                    self.assertIsNone(got["busy_sample_fraction"])

    def test_absent_waiting_is_not_failure(self):
        m, rows = fixture()
        rows[1]["state"]["processing"]["waiting"] = 0
        got = check(m, rows)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["observed_peak_waiting"], 0)

    def test_corrupt_contract_rejected(self):
        for mode in ("count", "index", "schema", "kind", "manifest_kind", "bool_count", "both", "missing_error", "clock", "path", "save", "duration", "config", "disabled_counts", "missing_processing", "enabled_null", "negative", "over_limit"):
            with self.subTest(mode=mode):
                m, rows = fixture()
                r = rows[1]
                if mode == "count": m["successful_samples"] += 1
                elif mode == "index": r["index"] = 99
                elif mode == "schema": r["schema_version"] = True
                elif mode == "kind": r["source_kind"] = "gateway"
                elif mode == "manifest_kind": del m["source_kind"]
                elif mode == "bool_count": r["state"]["processing"]["waiting"] = True
                elif mode == "both": r["error"] = "failed"
                elif mode == "missing_error": del r["error"]
                elif mode == "clock": rows[2]["started_at"] = stamp(90)
                elif mode == "path": m["output_path"] = "../samples.jsonl"
                elif mode == "save": m["close_error"] = "disk"
                elif mode == "duration": r["duration_ns"] = -1
                elif mode == "config": m["config"]["interval_ns"] = 1
                elif mode == "disabled_counts": r["state"]["processing_limit_enabled"] = False
                elif mode == "missing_processing": del r["state"]["processing"]
                elif mode == "enabled_null": r["state"]["processing"] = None
                elif mode == "negative": r["state"]["processing"]["waiting"] = -1
                elif mode == "over_limit": r["state"]["processing"]["in_use"] = 2
                with self.assertRaises(ValueError): check(m, rows)

    def test_crossing_query_not_used_in_window_histogram(self):
        m, rows = fixture()
        rows[1].update(started_at=stamp(40))
        got = check(m, rows)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["window_successful_samples"], 1)
        self.assertEqual(got["window_busy_samples"], 0)

    def test_worker_readiness_requires_enabled_idle(self):
        class Process:
            def __init__(self, code=None): self.code = code
            def poll(self): return self.code
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _, rows = fixture()
            p = directory/"samples.jsonl"
            p.write_text(json.dumps(rows[0])+"\n{")
            self.assertEqual(wait_sampling_ready(Process(), directory, "worker"), 0)
            for state in (rows[1]["state"], dict(processing_limit_enabled=False, processing=None)):
                rows[0]["state"] = state
                p.write_text(json.dumps(rows[0])+"\n")
                with self.assertRaises(RuntimeError): wait_sampling_ready(Process(), directory, "worker")
            with self.assertRaises(RuntimeError): wait_sampling_ready(Process(1), directory, "worker")


if __name__ == "__main__":
    unittest.main()
