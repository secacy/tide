"""Model comparisons use synthetic evidence to exercise independent identities."""
import json
from pathlib import Path
import tempfile
import unittest

from compare_models import PLAN, PROFILES, compare, expected_model
from test_decoding import paired_fixture
from test_quality import write_json
from model_assets import STREAM_CONFIG


def model_fixture(root):
    paired_fixture(root)
    frozen = json.loads((root / "before-run.json").read_text())
    frozen.update(schema="tide.asr-model-comparison.v1", plan=PLAN,
                  models={g: expected_model(p) for g, p in PROFILES.items()})
    write_json(root / "before-run.json", frozen)
    executions = []
    for name in PLAN:
        group, identifier = name.split("_")
        old = root / (('G' if group == 'A' else 'B') + '_' + identifier)
        path = root / name
        if old != path:
            old.rename(path)
        report = json.loads((path / "summary.json").read_text())
        expected = frozen["models"][group]
        report.update(version=3, model_profile=expected["profile"], stream_config=STREAM_CONFIG,
                      provider="cpu", load_ns=123, decoding={"method": "greedy_search", "max_active_paths": 0})
        report["file_sha256"] = {**report["file_sha256"], **expected["files"], "model_asset_lock": expected["asset_lock_sha256"]}
        write_json(path / "summary.json", report)
        executions.append({"name": name, "exit_code": 0, "timed_out": False})
    write_json(root / "execution.json", executions)


class ModelComparisonTests(unittest.TestCase):
    def test_correct_pairing_and_negative_quality_result_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_fixture(root)
            r = compare(root)
            self.assertTrue(r["all_six_pairs_complete"])
            self.assertEqual(r["groups"]["A"]["completed_subset"]["cer"], 0)
            self.assertEqual(r["groups"]["B"]["completed_subset"]["cer"], 1/19)
            self.assertLess(r["cer_reduction_percentage_points"], 0)
            self.assertEqual(r["groups"]["A"]["texts_equal_to_original_gateway"], 6)
            self.assertEqual(r["human_fact_review"], "pending")

    def test_different_model_recipe_audio_program_or_raw_text_is_unknown(self):
        for mutation in ["profile", "model-hash", "lock", "input", "program", "decoder", "recipe", "text", "close", "reference", "finish", "sdk"]:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                model_fixture(root)
                path = root / "B_q04/summary.json"
                r = json.loads(path.read_text())
                if mutation == "profile": r["model_profile"]["id"] = PROFILES["A"]
                elif mutation == "model-hash": r["file_sha256"]["encoder.int8.onnx"] = "changed"
                elif mutation == "lock": r["file_sha256"]["model_asset_lock"] = "changed"
                elif mutation == "input": r["file_sha256"]["input.wav"] = "changed"
                elif mutation == "program": r["file_sha256"]["probe_binary"] = "changed"
                elif mutation == "decoder": r["decoding"]["method"] = "modified_beam_search"
                elif mutation == "recipe": r["stream_config"]["enable_endpoint"] = 1
                elif mutation == "text": r["metrics"]["last_text_snapshot"] = "silent correction"
                elif mutation == "close": r["metrics"]["close_returned"] = False
                elif mutation == "reference": r["file_sha256"]["reference.txt"] = "unexpected"
                elif mutation == "finish": r["metrics"]["input_finished"] = False
                else: r["build"]["Deps"][0]["Version"] = "other"
                write_json(path, r)
                result = compare(root)
                self.assertEqual(result["groups"]["B"]["unknown"], 1)
                self.assertIsNone(result["cer_reduction_percentage_points"])

    def test_failed_and_unrun_slots_are_not_scored_as_empty_text(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_fixture(root)
            executions = json.loads((root / "execution.json").read_text())
            executions[0].update(exit_code=124, timed_out=True)
            executions[1]["exit_code"] = None
            write_json(root / "execution.json", executions)
            result = compare(root)
            self.assertEqual(result["groups"]["A"]["failed"], 1)
            self.assertEqual(result["groups"]["B"]["unknown"], 1)
            self.assertIsNone(result["cer_reduction_percentage_points"])
            self.assertIsNone(result["cases"][0]["quality"])

    def test_completed_empty_text_is_scored_as_deletions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_fixture(root)
            path = root / "B_q03/summary.json"
            report = json.loads(path.read_text())
            report["metrics"].update(last_text_snapshot="", first_nonempty_text_ns=None, first_text_before_input_end=False)
            write_json(path, report)
            path = root / "B_q03/events.jsonl"
            events = [json.loads(line) for line in path.read_text().splitlines()]
            for event in events:
                if event["kind"] == "text_snapshot":
                    event["text"] = ""
            path.write_text("".join(json.dumps(e)+"\n" for e in events))
            result = compare(root)
            row = next(r for r in result["cases"] if r["name"] == "B_q03")
            self.assertEqual(row["outcome"], "completed")
            self.assertEqual(row["quality"]["D"], row["quality"]["N"])

    def test_frozen_expectations_cannot_bless_an_arbitrary_model(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_fixture(root)
            path = root / "before-run.json"
            frozen = json.loads(path.read_text())
            frozen["models"]["B"]["files"]["encoder.int8.onnx"] = "different"
            write_json(path, frozen)
            with self.assertRaisesRegex(ValueError, "wrong frozen model identity"):
                compare(root)

    def test_missing_plan_slot_rejected_instead_of_shrinking_denominator(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            model_fixture(root)
            path = root / "execution.json"
            write_json(path, json.loads(path.read_text())[:-1])
            with self.assertRaisesRegex(ValueError, "missing planned slots"):
                compare(root)


if __name__ == "__main__":
    unittest.main()
