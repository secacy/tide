#!/usr/bin/env python3
"""离线验证 v1/v2 读取兼容；只在临时目录生成派生或合成数据。"""
import contextlib
import copy
import hashlib
import io
import json
from pathlib import Path
import runpy
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
READER = runpy.run_path(str(HERE/"run_loadgen_baseline.py"))
SUMMARIZE = runpy.run_path(str(HERE/"summarize_loadgen_baseline.py"))["summarize"]
BASELINE = HERE.parent/"results"/"loadgen-baseline-2026-09-29-run2"


def copy_inputs(destination):
    """只复制正式原始报告和清单，保护历史统计文件与哈希记录。"""
    for name in ["manifest.json"] + [f"r{r}-n{n}.json" for r in (1, 2, 3) for n in (1, 2, 4)]:
        (destination/name).write_bytes((BASELINE/name).read_bytes())


class LoadgenReportTests(unittest.TestCase):
    def test_legacy_is_unknown(self):
        self.assertEqual(READER["read_schedule_metrics"](1, {}),
                         dict(audio_schedule_samples=None, max_audio_schedule_lag_ns=None))

    def test_v2_values(self):
        for count, lag in [(0, None), (1, 0), (3, 7), (2**63-1, 2**63-1)]:
            with self.subTest(count=count, lag=lag):
                obs = dict(audio_schedule_samples=count, max_audio_schedule_lag_ns=lag)
                before = copy.deepcopy(obs)
                self.assertEqual(READER["read_schedule_metrics"](2, obs), obs)
                self.assertEqual(obs, before)

    def test_rejects_missing_and_invalid_fields(self):
        invalid = [{}, {"audio_schedule_samples": 0}, {"max_audio_schedule_lag_ns": None}]
        invalid += [dict(audio_schedule_samples=c, max_audio_schedule_lag_ns=l) for c, l in
                    [(None, None), (True, 0), (1, False), (1.0, 0), (1, 0.0), (-1, None),
                     (1, -1), (0, 0), (0, 7), (1, None), (2**63, 0), (1, 2**63)]]
        for obs in invalid:
            with self.subTest(observation=obs):
                with self.assertRaises(ValueError):
                    READER["read_schedule_metrics"](2, obs)

    def test_rejects_unknown_version(self):
        for version in [0, 3, "2", True, None]:
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    READER["read_schedule_metrics"](version, {})
                with tempfile.TemporaryDirectory() as temp:
                    doc = json.loads((BASELINE/"r1-n1.json").read_text())
                    doc["schema_version"] = version
                    path = Path(temp)/"report.json"
                    path.write_text(json.dumps(doc))
                    with self.assertRaises(ValueError):
                        READER["check_report"](path, 1, 160000)

    def test_historical_baseline_unchanged(self):
        before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in BASELINE.iterdir() if p.is_file()}
        old = json.loads((BASELINE/"groups.json").read_text())
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)
            copy_inputs(target)
            with contextlib.redirect_stdout(io.StringIO()):
                SUMMARIZE(target)
            new = json.loads((target/"groups.json").read_text())
            self.assertEqual(new["completed_sessions"], 21)
            for actual, previous in zip(new["groups"], old["groups"], strict=True):
                self.assertEqual(actual.pop("schema_versions"), [1])
                self.assertIsNone(actual.pop("audio_schedule"))
                self.assertEqual(actual, previous)
            checked = READER["check_report"](target/"r1-n4.json", 4, 160000)
            self.assertTrue(checked["checks_passed"])
            for row in checked["sessions"]:
                self.assertIsNone(row["audio_schedule_samples"])
                self.assertIsNone(row["max_audio_schedule_lag_ns"])
        after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in BASELINE.iterdir() if p.is_file()}
        self.assertEqual(before, after)

    def test_synthetic_v2_and_mixed_versions(self):
        # 合成数据只验证读取与聚合，不能当作实际测出的发送落后。
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)
            copy_inputs(target)
            for r in (1, 2, 3):
                for n in (1, 2, 4):
                    path = target/f"r{r}-n{n}.json"
                    doc = json.loads(path.read_text())
                    doc["schema_version"] = 2
                    for i, session in enumerate(doc["sessions"]):
                        session["observation"].update(audio_schedule_samples=50, max_audio_schedule_lag_ns=i+1)
                    path.write_text(json.dumps(doc))
            with contextlib.redirect_stdout(io.StringIO()):
                SUMMARIZE(target)
            groups = json.loads((target/"groups.json").read_text())["groups"]
            for group, n in zip(groups, (1, 2, 4), strict=True):
                self.assertEqual(group["schema_versions"], [2])
                self.assertEqual(group["audio_schedule"], dict(samples=150*n, sessions_with_samples=3*n, max_lag_ns=n))
            # 将一个批次还原为历史 v1，不能输出混合组的部分统计冒充完整统计。
            (target/"r1-n1.json").write_bytes((BASELINE/"r1-n1.json").read_bytes())
            with contextlib.redirect_stdout(io.StringIO()):
                SUMMARIZE(target)
            group = json.loads((target/"groups.json").read_text())["groups"][0]
            self.assertEqual(group["schema_versions"], [1, 2])
            self.assertIsNone(group["audio_schedule"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
