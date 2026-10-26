"""Synthetic evidence exercises reconciliation, not model performance."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from verify import reconcile

SECOND = 1_000_000_000


def write(path, value, jsonl=False):
    text = "".join(json.dumps(v) + "\n" for v in value) if jsonl else json.dumps(value) + "\n"
    path.write_text(text)


def fixture(root):
    source = root / "source"
    source.mkdir()
    (source / "test.go").write_bytes(b"fixture")
    import hashlib
    assets = {"input_bytes": 321700, "input_sha256": "input", "model_files": {"model": "digest"}, "worker_sha256": "binary"}
    write(root / "manifest.json", {"schema": "tide.real-backend-freeze.v1", "assets": assets,
                                  "source": {"test.go": hashlib.sha256(b"fixture").hexdigest()}, "worker_build_info": "non-race fixture"})
    data = root / "data"
    data.mkdir()
    cfg = dict(assets, schema="tide.real-backend-baseline.v1", rounds=5, warmup=1, threads=2, decoding="greedy_search",
               max_sessions=1, max_pending_audio_bytes=0, resume_window_ns=10*SECOND,
               max_audio_bytes=960000, max_chunk_bytes=3200, max_text_bytes=4096,
               sample_interval_ns=SECOND//2, max_sample_gap_ns=3*SECOND,
               normal_cleanup_budget_ns=5*SECOND, cancel_cleanup_budget_ns=15*SECOND,
               warmup_idle_ns=5*SECOND, case_idle_ns=2*SECOND, final_idle_ns=5*SECOND,
               realtime=True, client_resume=False, forced_gc=False, gateway_in_driver=True,
               sdk="sherpa-onnx-go-macos@v1.13.8", os="darwin", arch="arm64")
    write(data / "conditions.json", cfg)
    slots, events, native = [], [], []
    for index in range(11):
        cancel = index > 0 and index % 2 == 1
        round_no = (index+1)//2
        name = "warmup" if index == 0 else f"r{round_no}_{'cancel' if cancel else 'normal'}"
        start = 2*SECOND + index*25*SECOND
        run_ns = 1_500_000_000 if cancel else 10_100_000_000
        returned = start+run_ns
        closed = returned+10*SECOND if cancel else returned-1_000_000
        idle = max(closed, returned)+1_000_000
        model_events = [{"observed_ns": start+SECOND//2, "kind": "model_created", "model_id": index+1, "load_ms": 500.0},
                        {"observed_ns": closed, "kind": "engine_closed", "model_id": index+1, "close_ms": 3.0}]
        events.extend(model_events)
        for e in model_events:
            raw = dict(e, msg=e["kind"])
            native.append(raw)
        callbacks = [{"received_ns": SECOND, "source_read_bytes": 32000,
                      "message": {"type": "result", "seq": "1", "segmentId": "1", "text": "患者", "isFinal": False}}]
        if not cancel:
            callbacks.append({"received_ns": 10_050_000_000, "source_read_bytes": 321700,
                              "message": {"type": "result", "seq": "2", "segmentId": "1", "text": "患者咳嗽", "isFinal": True}})
        results = [{"segment_id": "1", "text": c["message"]["text"], "final": c["message"]["isFinal"], "has_progress": False} for c in callbacks]
        rpc = {"audio_bytes": 32000 if cancel else 321700, "audio_sha256": "partial" if cancel else "input",
               "chunks": [3200]*10 if cancel else [3200]*100+[1700], "results": results,
               "half_closed": not cancel, "terminal": "Canceled" if cancel else "EOF"}
        case = {"name": name, "input_bytes": 321700, "input_sha256": "input", "source_read_bytes": 32000 if cancel else 321700,
                "source_closes": 1, "source_eof_ns": None if cancel else 10*SECOND,
                "run_ns": run_ns, "run_start_to_first_text_ns": SECOND, "first_text_before_all_source_read": True,
                "source_eof_to_run_return_ns": None if cancel else 100_000_000,
                "client": {"Completed": not cancel, "CompletionAckWritten": not cancel, "FinalOffset": 0 if cancel else 321700,
                           "AppliedSeq": 0 if cancel else 2, "ResumeAttempts": 0, "ResumedConnections": 0},
                "error": "context canceled" if cancel else "", "canceled": cancel, "callback_updates": len(callbacks),
                "finals": 0 if cancel else 1, "transcript": callbacks[-1]["message"]["text"], "segments": 1,
                "gateway_active_at_run_return": 1 if cancel else 0, "gateway_active_after_cleanup": 0,
                "run_return_to_observed_cleanup_ns": idle-returned+1_000_000, "native_closed_after_cleanup": index+1,
                "grpc": rpc, "processed_audio_bytes": None, "cer": None}
        slot = {"name": name, "round": round_no, "cancel": cancel, "state": "passed", "start_ns": start,
                "return_ns": returned, "cancel_ns": start+SECOND if cancel else None,
                "source_read_at_cancel": 32000 if cancel else None, "callback_seq_at_cancel": 1 if cancel else None,
                "gateway_idle_observed_ns": idle, "idle_start_ns": idle+1_000_000,
                "idle_end_ns": idle+1_000_000+(5 if index == 0 else 2)*SECOND, "model_events": model_events}
        slots.append(slot)
        write(data / f"{name}-case.json", case)
        write(data / f"{name}-client-return.json", copy.deepcopy(case))
        write(data / f"{name}-grpc.json", rpc)
        write(data / f"{name}.jsonl", callbacks, jsonl=True)
    write(data / "slots.json", slots)
    write(data / "model-events.jsonl", events, jsonl=True)
    write(data / "server.log", native, jsonl=True)
    write(data / "process-start.json", {"worker_pid": 100, "driver_pid": 200})
    write(data / "process.json", {"worker_pid": 100, "driver_pid": 200, "process_exit": 0, "models_created": 11,
                                 "models_closed": 11, "gateway_active_before_fixture_cleanup": 0})
    samples = []
    for at in range(0, slots[-1]["idle_end_ns"]+5*SECOND, SECOND//2):
        current = next((s for s in slots if s["start_ns"] <= at <= s["idle_end_ns"]), None)
        phase = "startup"
        if at >= slots[-1]["idle_end_ns"]:
            phase = "final_idle"
        elif current:
            phase = "idle" if at >= current["idle_start_ns"] else "cancel" if current["cancel"] else "normal"
        samples.append({"elapsed_ns": at, "wall_ns": 123456789000000+at, "duration_ns": 1_000_000,
                        "phase": phase, "round": current["round"] if current else 0,
                        "active_sessions": int(current is not None and at < current["gateway_idle_observed_ns"]),
                        "driver_goroutines": 12, "driver_heap_bytes": 1_000_000, "worker_rss_bytes": 10_000_000,
                        "models_created": sum(e["kind"] == "model_created" and e["observed_ns"] <= at for e in events),
                        "models_closed": sum(e["kind"] == "engine_closed" and e["observed_ns"] <= at for e in events)})
    write(data / "samples.jsonl", samples, jsonl=True)


class ReconcileTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        fixture(self.root)

    def change(self, name, mutate, jsonl=False):
        path = self.root / "data" / name
        value = [json.loads(s) for s in path.read_text().splitlines()] if jsonl else json.loads(path.read_text())
        mutate(value)
        write(path, value, jsonl)

    def test_complete_evidence_reconciles_separate_denominators(self):
        result = reconcile(self.root)
        self.assertEqual((result["normal_completed"], result["cancel_expected"], result["warmup_completed"]), (5, 5, 1))
        self.assertTrue(result["resource_sampling_quality_ok"])
        self.assertEqual(len(result["idle"]), 11)

    def test_missing_native_release_rejected(self):
        self.change("model-events.jsonl", lambda v: v.pop(), True)
        with self.assertRaisesRegex(ValueError, "lifecycle event count"):
            reconcile(self.root)

    def test_forged_completion_ack_rejected(self):
        for name in ("r1_normal-case.json", "r1_normal-client-return.json"):
            self.change(name, lambda v: v["client"].update(CompletionAckWritten=False))
        with self.assertRaisesRegex(ValueError, "normal outcome"):
            reconcile(self.root)

    def test_gateway_corrupted_text_rejected(self):
        self.change("r1_normal.jsonl", lambda v: v[0]["message"].update(text="伪造"), True)
        with self.assertRaisesRegex(ValueError, "callback changed"):
            reconcile(self.root)

    def test_zero_rss_does_not_hide_missing_measurement(self):
        self.change("samples.jsonl", lambda v: v[0].update(worker_rss_bytes=0), True)
        with self.assertRaisesRegex(ValueError, "invalid RSS"):
            reconcile(self.root)

    def test_rss_failure_is_reported_without_losing_outcomes(self):
        self.change("samples.jsonl", lambda v: v[0].update(worker_rss_bytes=None, worker_rss_error="ps timeout"), True)
        result = reconcile(self.root)
        self.assertEqual(result["normal_completed"], 5)
        self.assertFalse(result["resource_sampling_quality_ok"])
        self.assertEqual(result["rss_unavailable_indices"], [0])

    def test_clock_gap_is_reported_without_rewriting_completion(self):
        self.change("samples.jsonl", lambda v: v.__delitem__(slice(0, 8)), True)
        # Initial gaps are invalid coverage, not zero-filled samples.
        with self.assertRaisesRegex(ValueError, "sampling started too late"):
            reconcile(self.root)

    def test_internal_gap_marks_resource_quality(self):
        self.change("samples.jsonl", lambda v: v.__delitem__(slice(1, 9)), True)
        result = reconcile(self.root)
        self.assertFalse(result["resource_sampling_quality_ok"])
        self.assertEqual(result["normal_completed"], 5)
        self.assertTrue(result["sampling_gaps"])

    def test_frozen_source_tampering_rejected(self):
        (self.root / "source/test.go").write_text("changed")
        with self.assertRaisesRegex(ValueError, "source digest"):
            reconcile(self.root)

    def test_resume_window_cannot_be_shortened_to_improve_cancel_time(self):
        self.change("conditions.json", lambda v: v.update(resume_window_ns=1))
        with self.assertRaisesRegex(ValueError, "resume_window_ns"):
            reconcile(self.root)


if __name__ == "__main__":
    unittest.main()
