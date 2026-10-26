"""Paired scoring tests use synthetic call events, not model output or audio."""
import json
from pathlib import Path
import tempfile
import unittest

from compare_decoding import METHODS, PLAN, compare, sha
from fetch_assets import ASSETS
from verify import verify_case
from test_quality import fixture, write_json


def paired_fixture(root):
    baseline = root / "baseline"
    baseline.mkdir()
    fixture(baseline)
    manifest = json.loads((baseline / "input-manifest.json").read_text())
    for case in manifest["cases"]:
        case["wav_sha256"] = sha(case["id"].encode())
    write_json(baseline / "input-manifest.json", manifest)
    write_json(baseline / "score.json", {"cases": [{"id": c["id"], "hypothesis": c["reference"]} for c in manifest["cases"]]})
    models = {k: v for k, v in list(ASSETS.items())[:4]}
    frozen = {"schema": "tide.asr-decoding-comparison.v1", "plan": PLAN,
              "baseline_sha256": {n: sha((baseline / n).read_bytes()) for n in ["input-manifest.json", "score.json"]},
              "probe_sha256": "same-test-binary", "model_sha256": models}
    write_json(root / "before-run.json", frozen)
    executions = []
    for name in PLAN:
        group, identifier = name.split("_")
        case = manifest["cases"][int(identifier[1:])-1]
        text = "有胸痛" if name == "B_q01" else case["reference"]
        target = root / name
        target.mkdir()
        events = [
            {"kind": "leading_padding", "begin_ns": 0, "end_ns": 1, "input_end_bytes": 0, "samples": 4800},
            {"kind": "input", "begin_ns": 2, "end_ns": 3, "input_end_bytes": 2, "samples": 1, "planned_ns": 0},
            {"kind": "decode", "begin_ns": 4, "end_ns": 5, "input_end_bytes": 2},
            {"kind": "get_result", "begin_ns": 6, "end_ns": 7, "input_end_bytes": 2},
            {"kind": "text_snapshot", "begin_ns": 8, "end_ns": 8, "input_end_bytes": 2, "text": text},
            {"kind": "trailing_padding", "begin_ns": 9, "end_ns": 10, "input_end_bytes": 2, "samples": 9600},
            {"kind": "input_finished", "begin_ns": 11, "end_ns": 12, "input_end_bytes": 2},
        ]
        (target / "events.jsonl").write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in events)+"\n")
        report = {"version": 2, "threads": 2, "realtime": True, "decoding": METHODS[group], "cancel_after_ns": 0, "character_score": None,
                  "file_sha256": {**models, "input.wav": case["wav_sha256"], "probe_binary": "same-test-binary"},
                  "build": {"Deps": [{"Path": "github.com/k2-fsa/sherpa-onnx-go-macos", "Version": "v1.13.8"}]},
                  "metrics": {"status": "completed_probe", "close_returned": True, "processed_audio_bytes": None,
                              "submitted_bytes": 2, "input_bytes": 2, "engine_call_ns": 6, "max_schedule_lag_ns": 2,
                              "decode_calls": 1, "text_changes": 1, "last_text_snapshot": text,
                              "first_nonempty_text_ns": 8, "first_text_before_input_end": False,
                              "input_finished": True, "leading_padding_samples": 4800, "trailing_padding_samples": 9600,
                              "call_rtf": 6/(2/32000*1e9), "last_input_call_end_to_flush_end_ns": 9}}
        write_json(target / "summary.json", report)
        executions.append({"name": name, "exit_code": 0, "timed_out": False})
    write_json(root / "execution.json", executions)


class PairedComparisonTests(unittest.TestCase):
    def test_weighted_score_and_unfavorable_difference_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paired_fixture(root)
            r = compare(root)
            self.assertTrue(r["all_six_pairs_complete"])
            self.assertEqual(r["groups"]["G"]["completed_subset"]["cer"], 0)
            self.assertEqual(r["groups"]["B"]["completed_subset"]["cer"], 1/19)
            self.assertEqual(r["groups"]["G"]["texts_equal_to_original_gateway"], 6)
            self.assertLess(r["cer_reduction_percentage_points"], 0)

    def test_wrong_method_path_budget_and_missing_v2_settings_are_unknown(self):
        for mutation in ["method", "paths", "missing", "text", "wav", "release"]:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                paired_fixture(root)
                path = root / "B_q04/summary.json"
                report = json.loads(path.read_text())
                if mutation == "method": report["decoding"]["method"] = "greedy_search"
                elif mutation == "paths": report["decoding"]["max_active_paths"] = 8
                elif mutation == "missing": report.pop("decoding")
                elif mutation == "text": report["metrics"]["last_text_snapshot"] = "静默纠错"
                elif mutation == "wav": report["file_sha256"]["input.wav"] = "other"
                else: report["metrics"]["close_returned"] = False
                write_json(path, report)
                r = compare(root)
                self.assertFalse(r["all_six_pairs_complete"])
                self.assertIsNone(r["cer_reduction_percentage_points"])
                self.assertEqual(r["groups"]["B"]["unknown"], 1)

    def test_failed_slot_is_not_dropped_or_scored_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paired_fixture(root)
            executions = json.loads((root / "execution.json").read_text())
            executions[0]["exit_code"] = 124
            executions[0]["timed_out"] = True
            write_json(root / "execution.json", executions)
            r = compare(root)
            self.assertEqual(r["planned"], 12)
            self.assertEqual(r["groups"]["G"]["failed"], 1)
            self.assertIsNone(r["cases"][0]["quality"])
            self.assertIsNone(r["cer_reduction_percentage_points"])
            write_json(root / "execution.json", executions[:-1])
            with self.assertRaises(ValueError):
                compare(root)

    def test_legacy_report_is_only_inferred_as_greedy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paired_fixture(root)
            path = root / "G_q01/summary.json"
            report = json.loads(path.read_text())
            report["version"] = 1
            report.pop("decoding")
            write_json(path, report)
            execution = {"name": "G_q01", "exit_code": 0}
            verify_case(root, execution)
            with self.assertRaises(AssertionError):
                verify_case(root, execution, expected_decoding=METHODS["B"])


if __name__ == "__main__":
    unittest.main()
