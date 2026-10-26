"""Scoring and evidence tests use synthetic data; no recordings or model needed."""
import functools
import itertools
import json
from pathlib import Path
import tempfile
import unittest
import wave

from prepare import digest, prepare, read_pcm
from score import character_errors, normalize, score


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False) + "\n", encoding="utf-8")


def fixture(folder):
    """Six small valid reports exercise the real reconciliation entry point."""
    cases = []
    reports = []
    for index in range(6):
        name = f"q{index+1:02}"
        text = "没有胸痛" if index == 0 else "三十八"
        case = {"id": name, "reference": text, "reference_sha256": digest((text+"\n").encode()), "facts": ["否定或数值"], "pcm_bytes": 2, "pcm_sha256": digest(b"\0\0")}
        cases.append(case)
        message = {"type": "result", "segmentId": "1", "seq": "1", "text": text, "isFinal": True}
        event = {"received_ns": 4, "source_read_bytes": 2, "message": message}
        (folder / (name+".jsonl")).write_text(json.dumps(event, ensure_ascii=False)+"\n")
        rpc = {"audio_bytes": 2, "audio_sha256": case["pcm_sha256"], "chunks": [2], "half_closed": True, "terminal": "EOF",
               "results": [{"segment_id": "1", "text": text, "final": True, "has_progress": False}]}
        r = {"name": name, "input_bytes": 2, "input_sha256": case["pcm_sha256"], "source_read_bytes": 2, "source_closes": 1,
             "gateway_active_after_cleanup": 0, "native_closed_after_cleanup": index+1, "processed_audio_bytes": None, "cer": None,
             "segments": 1, "run_return_to_observed_cleanup_ns": 1, "source_eof_ns": 3, "source_eof_to_run_return_ns": 2,
             "run_ns": 5, "callback_updates": 1, "finals": 1, "transcript": text, "canceled": False,
             "run_start_to_first_text_ns": 4, "first_text_before_all_source_read": False, "gateway_active_at_run_return": 0,
             "client": {"Completed": True, "CompletionAckWritten": True, "FinalOffset": 2, "AppliedSeq": 1, "ResumeAttempts": 0, "ResumedConnections": 0}, "grpc": rpc}
        write_json(folder / (name+"-grpc.json"), rpc)
        write_json(folder / (name+"-client-return.json"), r)
        reports.append({"id": name, "outcome": "completed", "report": r})
    write_json(folder / "input-manifest.json", {"schema": "tide.asr-quality-input.v1", "cases": cases})
    write_json(folder / "conditions.json", {"input_manifest_sha256": digest((folder / "input-manifest.json").read_bytes()), "max_pending_audio_bytes": 0,
        "max_sessions": 1, "realtime": True, "chunk_bytes": 3200, "client_resume_enabled": False, "resume_window_ns": 10_000_000_000, "result_retention_ns": 30_000_000_000})
    write_json(folder / "summary.json", reports)
    (folder / "server.log").write_text("\n".join(json.dumps({"msg": kind, "model_id": i}) for i in range(1, 7) for kind in ("model_created", "engine_closed")))
    write_json(folder / "process.json", {"process_exit": 0, "models_closed": 6, "gateway_active_before_fixture_cleanup": 0})
    return reports


class CharacterErrorsTests(unittest.TestCase):
    def test_normalization_preserves_numeric_meaning_and_unicode(self):
        self.assertEqual(normalize(' A，e\u0301。 三十七点八 37.8 -2 +3 5% /\n'), 'aé三十七点八37.8-2+35%/')
        self.assertEqual(normalize('À'), 'À')  # Only ASCII case is folded.

    def test_known_errors_and_empty_outputs(self):
        for ref, hyp, expected in [("没有胸痛", "有胸痛", (0,1,0)), ("右膝", "左膝", (1,0,0)), ("五天", "五五天", (0,0,1)), ("三天", "", (0,2,0)), ("", "多字", (0,0,2))]:
            with self.subTest(reference=ref, hypothesis=hyp):
                r = character_errors(ref, hyp)
                self.assertEqual((r["S"], r["D"], r["I"]), expected)
                self.assertEqual(r["N"], len(ref))
                self.assertEqual(r["cer"], sum(expected)/len(ref) if ref else None)
        self.assertGreater(character_errors("三十七点八", "37.8")["errors"], 0)

    def test_exhaustive_small_alignment_matches_independent_distance(self):
        @functools.lru_cache(None)
        def distance(a, b):
            if not a or not b:
                return max(len(a), len(b))
            return min(distance(a[1:], b[1:])+(a[0] != b[0]), distance(a[1:], b)+1, distance(a, b[1:])+1)
        strings = ["".join(s) for n in range(4) for s in itertools.product("有无", repeat=n)]
        for a in strings:
            for b in strings:
                r = character_errors(a, b)
                self.assertEqual(r["errors"], distance(a, b))
                self.assertEqual("".join(e["reference"] for e in r["alignment"]), a)
                self.assertEqual("".join(e["hypothesis"] for e in r["alignment"]), b)
                self.assertEqual(sum(e["op"] != "=" for e in r["alignment"]), r["errors"])
        self.assertEqual([e["op"] for e in character_errors("有无", "无有")["alignment"]], ["S", "S"])


class EvidenceTests(unittest.TestCase):
    def test_complete_six_and_weighted_not_mean_cer(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            reports = fixture(folder)
            # One longer reference, one deletion; change all agreeing raw surfaces.
            name = "q01"
            reports[0]["report"]["transcript"] = "有胸痛"
            reports[0]["report"]["grpc"]["results"][0]["text"] = "有胸痛"
            write_json(folder / "summary.json", reports)
            write_json(folder / (name+"-client-return.json"), reports[0]["report"])
            write_json(folder / (name+"-grpc.json"), reports[0]["report"]["grpc"])
            event = json.loads((folder / (name+".jsonl")).read_text())
            event["message"]["text"] = "有胸痛"
            write_json(folder / (name+".jsonl"), event)
            r = score(folder)
            self.assertEqual(r["outcomes"], {"completed": 6, "failed": 0, "unknown": 0})
            self.assertEqual(r["completed_subset"]["cer"], 1/19)
            self.assertTrue(r["six_models_created_and_closed"])
            self.assertEqual(r["cases"][0]["quality"]["D"], 1)

    def test_corruptions_are_not_scored_as_success(self):
        for corruption in ("seq", "hash", "final", "ack", "progress", "text", "missing_raw"):
            with self.subTest(corruption=corruption), tempfile.TemporaryDirectory() as tmp:
                folder = Path(tmp)
                reports = fixture(folder)
                row = reports[0]["report"]
                if corruption == "seq":
                    event = json.loads((folder / "q01.jsonl").read_text())
                    event["message"]["seq"] = "2"
                    write_json(folder / "q01.jsonl", event)
                elif corruption == "hash":
                    row["input_sha256"] = "wrong"
                elif corruption == "final":
                    row["finals"] = 0
                elif corruption == "ack":
                    row["client"]["CompletionAckWritten"] = False
                elif corruption == "progress":
                    row["processed_audio_bytes"] = 2
                elif corruption == "text":
                    row["transcript"] = "没有错误"
                else:
                    (folder / "q01.jsonl").unlink()
                write_json(folder / "summary.json", reports)
                r = score(folder)
                self.assertEqual(r["outcomes"], {"completed": 5, "failed": 0, "unknown": 1})
                self.assertIsNone(r["cases"][0]["quality"])

    def test_failure_unknown_and_missing_slots_keep_denominator(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            reports = fixture(folder)
            reports[0] = {"id": "q01", "outcome": "failed", "issue": "RPC failure"}
            reports[1] = {"id": "q02", "outcome": "unknown", "issue": "not run"}
            write_json(folder / "summary.json", reports)
            r = score(folder)
            self.assertEqual(r["planned"], 6)
            self.assertEqual(r["outcomes"], {"completed": 4, "failed": 1, "unknown": 1})
            self.assertEqual(r["cases"][0]["facts"][0]["judgment"], "unable_to_judge")
            write_json(folder / "summary.json", reports[1:])
            with self.assertRaises(ValueError):
                score(folder)

    def test_changed_frozen_reference_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            fixture(folder)
            data = json.loads((folder / "input-manifest.json").read_text())
            data["cases"][0]["reference"] = "修改参考"
            write_json(folder / "input-manifest.json", data)
            with self.assertRaises(ValueError):
                score(folder)


class PCMTests(unittest.TestCase):
    def test_prepare_rejects_existing_leaf_without_overwrite(self):
        # An existing collection must survive even if it contains partial output.
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            source = base / "source"
            source.mkdir()
            for i in range(1, 7):
                (source / f"q{i:02}.m4a").write_bytes(b"preserved original")
            cases = base / "cases.json"
            write_json(cases, {"schema": "tide.asr-quality-cases.v1", "cases": [{"id": f"q{i:02}"} for i in range(1, 7)]})
            target = base / "nested" / "output"
            target.mkdir(parents=True)
            sentinel = target / "do-not-overwrite"
            sentinel.write_text("retained")
            with self.assertRaises(FileExistsError):
                prepare(source, target, cases, "reviewer")
            self.assertEqual(sentinel.read_text(), "retained")

    def test_format_and_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            for rate, channels, size in [(16000, 1, 2), (48000, 1, 2), (16000, 2, 4), (16000, 1, 0), (16000, 1, 960002)]:
                path = Path(tmp) / "audio.wav"
                with wave.open(str(path), "wb") as audio:
                    audio.setparams((channels, 2, rate, 0, "NONE", "not compressed"))
                    audio.writeframes(bytes(size))
                if (rate, channels, size) == (16000, 1, 2):
                    self.assertEqual(read_pcm(path), b"\0\0")
                else:
                    with self.assertRaises(ValueError):
                        read_pcm(path)


if __name__ == "__main__":
    unittest.main()
