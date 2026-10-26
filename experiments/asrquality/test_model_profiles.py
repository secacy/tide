"""V3 model identity verification uses synthetic events; no model is loaded."""
import json
from pathlib import Path
import tempfile
import unittest

from test_decoding import paired_fixture
from test_quality import write_json
from model_assets import CANDIDATE, LEGACY, STREAM_CONFIG, expected_model
from fetch_assets import ASSETS
from verify import verify_case


def v3_fixture(root, profile_id):
    paired_fixture(root)
    path = root / "G_q01/summary.json"
    report = json.loads(path.read_text())
    expected = expected_model(profile_id)
    report.update(version=3, model_profile=expected["profile"], stream_config=STREAM_CONFIG, provider="cpu")
    report["file_sha256"] = {**expected["files"], "model_asset_lock": expected["asset_lock_sha256"]}
    write_json(path, report)
    return expected


class ModelProfileVerificationTests(unittest.TestCase):
    def test_legacy_lock_keeps_original_assets(self):
        self.assertEqual(expected_model(LEGACY)["files"], dict(list(ASSETS.items())[:4]))

    def test_v3_legacy_default_and_explicit_candidate(self):
        for profile in [LEGACY, CANDIDATE]:
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                expected = v3_fixture(root, profile)
                kwargs = {} if profile == LEGACY else {"expected_model": expected}
                verify_case(root, {"name": "G_q01", "exit_code": 0}, **kwargs)
                if profile == CANDIDATE:
                    with self.assertRaises(AssertionError):
                        verify_case(root, {"name": "G_q01", "exit_code": 0})

    def test_wrong_profile_assets_recipe_and_lock_are_rejected(self):
        for mutation in ["id", "decoder", "unit", "hash", "lock", "padding", "provider"]:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                expected = v3_fixture(root, CANDIDATE)
                path = root / "G_q01/summary.json"
                report = json.loads(path.read_text())
                if mutation == "id": report["model_profile"]["id"] = LEGACY
                elif mutation == "decoder": report["model_profile"]["decoder"] = "decoder.int8.onnx"
                elif mutation == "unit": report["model_profile"]["modeling_unit"] = ""
                elif mutation == "hash": report["file_sha256"]["encoder.int8.onnx"] = "different"
                elif mutation == "lock": report["file_sha256"]["model_asset_lock"] = "different"
                elif mutation == "padding": report["stream_config"]["trailing_padding_samples"] = 8000
                else: report["provider"] = "cuda"
                write_json(path, report)
                with self.assertRaises(AssertionError):
                    verify_case(root, {"name": "G_q01", "exit_code": 0}, expected_model=expected)

    def test_old_reports_cannot_be_reinterpreted_as_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paired_fixture(root)
            with self.assertRaises(AssertionError):
                verify_case(root, {"name": "G_q01", "exit_code": 0}, expected_model=expected_model(CANDIDATE))


if __name__ == "__main__":
    unittest.main()
