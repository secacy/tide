#!/usr/bin/env python3
"""离线核对采样分析与就绪逻辑，所有合成数据只写临时目录。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from run_gateway_observation_baseline import wait_sampling_ready
from summarize_gateway_observations import check_sampling_data, read_complete_rows, timestamp_ns


def stamp(ms):
    return f"2026-10-01T00:00:00.{ms * 1_000_000:09d}Z"


def fixture():
    rows = [dict(schema_version=1, index=i, started_at=stamp(ms), finished_at=stamp(ms+1), duration_ns=1_000_000,
                 state=dict(active_sessions=n, max_sessions=64, stopping=False), error=None)
            for i, (ms, n) in enumerate(((0, 0), (100, 2), (200, 2), (300, 0)))]
    manifest = dict(schema_version=1, output_path="samples.jsonl", started_at=stamp(0), finished_at=stamp(999),
                    config=dict(endpoint="http://127.0.0.1:8080/debug/gateway", interval_ns=100_000_000, request_timeout_ns=1_000_000_000),
                    samples_written=4, successful_samples=4, failed_samples=0, stop_reason="deadline_exceeded",
                    sampling_error="context deadline exceeded", output_error=None, close_error=None)
    return manifest, rows


def recount(manifest, rows):
    for i, row in enumerate(rows):
        row["index"] = i
    manifest.update(samples_written=len(rows), successful_samples=sum(r["error"] is None for r in rows), failed_samples=sum(r["error"] is not None for r in rows))


class GatewayObservationTests(unittest.TestCase):
    def test_valid_window(self):
        manifest, rows = fixture()
        before = copy.deepcopy((manifest, rows))
        got = check_sampling_data(manifest, rows, stamp(50), stamp(250), 2)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["observed_peak_active"], 2)
        self.assertEqual(got["max_bracketing_success_gap_ns"], 100_000_000)
        self.assertEqual(got["first_post_exit_zero_sample_delay_ns"], 51_000_000)
        self.assertEqual(got["window_active_histogram"], {"2": 2})
        self.assertEqual((manifest, rows), before)

    def test_utc_nanoseconds(self):
        a = timestamp_ns("2026-10-01T00:00:00.123456789Z")
        b = timestamp_ns("2026-10-01T00:00:00.123456+00:00")
        self.assertEqual(a-b, 789)
        self.assertEqual(timestamp_ns("2026-10-01T00:00:00Z"), timestamp_ns(stamp(0)))
        with self.assertRaises(ValueError):
            timestamp_ns("2026-10-01T00:00:00+08:00")

    def test_failed_query_is_unknown(self):
        manifest, rows = fixture()
        rows[2].update(state=None, error="503")
        recount(manifest, rows)
        got = check_sampling_data(manifest, rows, stamp(50), stamp(250), 2)
        self.assertFalse(got["checks_passed"])
        self.assertEqual(got["window_failed_samples"], 1)
        self.assertEqual(got["window_active_histogram"], {"2": 1})

    def test_final_failed_query_kept_outside_window(self):
        manifest, rows = fixture()
        tail = copy.deepcopy(rows[-1])
        tail.update(started_at=stamp(400), finished_at=stamp(401), state=None, error="context deadline exceeded")
        rows.append(tail)
        recount(manifest, rows)
        got = check_sampling_data(manifest, rows, stamp(50), stamp(250), 2)
        self.assertTrue(got["checks_passed"])
        self.assertEqual(got["failed_samples"], 1)
        self.assertEqual(got["window_failed_samples"], 0)

    def test_coverage_gaps(self):
        for mode in ("no_pre", "no_post", "no_peak", "no_zero", "wide_gap", "all_failed", "stopping", "wrong_capacity"):
            with self.subTest(mode=mode):
                manifest, rows = fixture()
                finish = stamp(250)
                if mode == "no_pre": rows = rows[1:]
                elif mode == "no_post": rows = rows[:-1]
                elif mode == "no_peak":
                    for r in rows: r["state"]["active_sessions"] = 0
                elif mode == "no_zero": rows[-1]["state"]["active_sessions"] = 1
                elif mode == "wide_gap":
                    for r, ms in zip(rows[2:], (700, 800)):
                        r.update(started_at=stamp(ms), finished_at=stamp(ms+1))
                    finish = stamp(750)
                elif mode == "all_failed":
                    for r in rows: r.update(state=None, error="unknown")
                elif mode == "stopping": rows[1]["state"]["stopping"] = True
                elif mode == "wrong_capacity": rows[1]["state"]["max_sessions"] = 32
                recount(manifest, rows)
                got = check_sampling_data(manifest, rows, stamp(50), finish, 2)
                self.assertFalse(got["checks_passed"])
                if mode == "all_failed": self.assertIsNone(got["observed_peak_active"])

    def test_rejects_corrupt_evidence(self):
        for mode in ("count", "index", "schema", "bool_count", "both_state_error", "no_state", "missing_error", "clock_backwards", "path", "save_error", "negative_duration", "wrong_config"):
            with self.subTest(mode=mode):
                manifest, rows = fixture()
                if mode == "count": manifest["samples_written"] += 1
                elif mode == "index": rows[1]["index"] = 7
                elif mode == "schema": rows[1]["schema_version"] = True
                elif mode == "bool_count": rows[1]["state"]["active_sessions"] = True
                elif mode == "both_state_error": rows[1]["error"] = "failed"
                elif mode == "no_state": rows[1]["state"] = None
                elif mode == "missing_error": del rows[1]["error"]
                elif mode == "clock_backwards": rows[2]["started_at"] = stamp(90)
                elif mode == "path": manifest["output_path"] = "../samples.jsonl"
                elif mode == "save_error": manifest["close_error"] = "disk error"
                elif mode == "negative_duration": rows[1]["duration_ns"] = -1
                elif mode == "wrong_config": manifest["config"]["interval_ns"] = 1
                with self.assertRaises(ValueError):
                    check_sampling_data(manifest, rows, stamp(50), stamp(250), 2)

    def test_live_partial_line_and_final_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/"samples.jsonl"
            path.write_bytes(b'{"index":0}\n{"index":')
            self.assertEqual(read_complete_rows(path, live=True), [{"index": 0}])
            with self.assertRaises(ValueError): read_complete_rows(path)

    def test_readiness_requires_idle_record(self):
        class Process:
            def __init__(self, code=None): self.code = code
            def poll(self): return self.code
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            _, rows = fixture()
            path = directory/"samples.jsonl"
            path.write_text(json.dumps(rows[0])+"\n{" )
            self.assertEqual(wait_sampling_ready(Process(), directory), 0)
            rows[0]["state"]["active_sessions"] = 1
            path.write_text(json.dumps(rows[0])+"\n")
            with self.assertRaises(RuntimeError): wait_sampling_ready(Process(), directory)
            with self.assertRaises(RuntimeError): wait_sampling_ready(Process(1), directory)


if __name__ == "__main__":
    unittest.main()
