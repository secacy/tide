"""Synthetic corruption checks; fixture timings are not runtime measurements."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from verify import reconcile, plan

S = 1_000_000_000


def write(path, value, jsonl=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(v)+"\n" for v in value) if jsonl else json.dumps(value)+"\n")


def fixture(root):
    write(root / "source/fixture.json", {})
    assets = {"long": {"bytes": 321700, "sha256": "long"}, "short": {"bytes": 163200, "sha256": "short"}}
    write(root / "manifest.json", {"schema": "tide.admission-freeze.v1", "source": {"fixture.json": hashlib.sha256((root / "source/fixture.json").read_bytes()).hexdigest()},
                                  "worker_build_info": "non-race fixture", "native_libraries": {"lib": "hash"}, "inputs": assets,
                                  "worker_sha256": "worker", "model_files": {"model": "hash"}})
    data = root / "data"
    gateway = {"MaxSessions": 2, "MaxMessageBytes": 3208, "StartTimeout": 10*S, "InputIdleTimeout": 30*S,
               "WorkerSendTimeout": 2*S, "TailTimeout": 15*S, "ResultWriteTimeout": 2*S, "MaxPendingAudioBytes": 0,
               "V2": {"MaxHandshakes": 64, "EntryTimeout": 10*S, "InputProgressTimeout": 30*S,
                      "ResumeWindow": 10*S, "ResultRetentionTimeout": 30*S, "WorkerStatusTimeout": 2*S,
                      "MaxAudioBytes": 1048576, "MaxAudioChunks": 256, "MaxResultBytes": 1048576, "MaxResults": 256}}
    cfg = dict(schema="tide.admission-comparison.v1", worker_sha256="worker", model_files={"model": "hash"},
               input_long_bytes=321700, input_long_sha256="long", input_short_bytes=163200, input_short_sha256="short",
               threads_per_worker=2, worker_limit=1, sample_interval_ns=S//2, max_sample_gap_ns=3*S, between_idle_ns=2*S,
               client_realtime=True, client_resume=False, forced_gc=False, sdk="sherpa-onnx-go-macos@v1.13.8",
               decoding="greedy_search", arm_budget_ns=45*S, cleanup_budget_ns=5*S, gateway=gateway)
    write(data / "conditions.json", cfg)
    counts = [0, 0]
    events = {"A": [], "B": []}
    arms, intervals = [], []
    for arm_index, (name, strategy, kind, names) in enumerate(plan()):
        base = arm_index*30*S+S
        before = counts.copy()
        routing = []
        trigger = None if kind == "warmup" else base+(5_300_000_000 if kind == "staggered" else 800_000_000)
        cleanup = base+(22*S if kind == "warmup" else 12*S)
        for case_name in names:
            outcome = "session_limit" if case_name == "N" and kind == "overload" else "busy" if case_name == "N" and strategy == "old" else "normal"
            worker = "A" if case_name in ("WA", "L") or (case_name == "N" and strategy == "old") else "B"
            start = base if case_name in ("WA", "L") else base+11*S if case_name == "WB" else base+100_000_000 if case_name == "S" else trigger+10_000_000
            duration = 10_100_000_000 if kind == "warmup" or case_name == "L" or kind == "overload" and case_name == "S" else 5_100_000_000
            if outcome != "normal":
                duration = 10_000_000
            returned = start+duration
            input_name = "long" if kind == "warmup" or case_name == "L" or kind == "overload" and case_name == "S" else "short"
            reference = assets[input_name]
            callbacks = []
            rpc = {"audio_bytes": 0, "audio_sha256": "", "chunks": [], "results": [], "half_closed": False, "terminal": ""}
            assignment, model_id = None, None
            if outcome != "session_limit":
                assignment = {"worker": worker, "selected_ns": start+1, "rpc_start_ns": start+2, "rpc_end_ns": returned-1,
                              "terminal_message": "worker is busy" if outcome == "busy" else "EOF"}
                code = "ResourceExhausted" if outcome == "busy" else "EOF"
                routing += [{"type": "selected", "case": case_name, "worker": worker, "at_ns": start+1},
                            {"type": "rpc_open", "case": case_name, "worker": worker, "at_ns": start+2},
                            {"type": "rpc_end", "case": case_name, "worker": worker, "at_ns": returned-1, "code": code,
                             "message": assignment["terminal_message"]}]
                rpc["terminal"] = code
            if outcome == "normal":
                index = ord(worker)-ord("A")
                counts[index] += 1
                model_id = counts[index]
                events[worker] += [{"kind": "model_created", "model_id": model_id, "observed_ns": start+500_000_000, "load_ms": 500.0},
                                   {"kind": "engine_closed", "model_id": model_id, "observed_ns": returned-1000, "close_ms": 3.0}]
                for seq, text in enumerate(("患者", "患者咳嗽"), 1):
                    callbacks.append({"received_ns": S if seq == 1 else duration-1000,
                                      "source_read_bytes": 32000 if seq == 1 else reference["bytes"],
                                      "message": {"type": "result", "seq": str(seq), "segmentId": "1", "text": text, "isFinal": seq == 2}})
                size = reference["bytes"]
                rpc.update(audio_bytes=size, audio_sha256=reference["sha256"], half_closed=True,
                           chunks=[3200]*(size//3200)+([size % 3200] if size % 3200 else []),
                           results=[{"segment_id": "1", "text": e["message"]["text"], "final": e["message"]["isFinal"], "has_progress": False} for e in callbacks])
            report = {"name": case_name, "input_bytes": reference["bytes"], "input_sha256": reference["sha256"],
                      "source_read_bytes": reference["bytes"] if outcome == "normal" else 0, "source_closes": 1,
                      "source_eof_ns": duration-40_000_000 if outcome == "normal" else None, "run_ns": duration,
                      "source_eof_to_run_return_ns": 40_000_000 if outcome == "normal" else None,
                      "run_start_to_first_text_ns": S if outcome == "normal" else None, "first_text_before_all_source_read": outcome == "normal",
                      "callback_updates": len(callbacks), "finals": int(outcome == "normal"), "segments": int(outcome == "normal"),
                      "transcript": "患者咳嗽" if outcome == "normal" else "", "grpc": rpc, "error": "" if outcome == "normal" else "failed",
                      "processed_audio_bytes": None, "cer": None,
                      "client": {"Completed": outcome == "normal", "CompletionAckWritten": outcome == "normal", "AppliedSeq": len(callbacks),
                                 "FinalOffset": reference["bytes"] if outcome == "normal" else 0, "ResumeAttempts": 0, "ResumedConnections": 0}}
            case = {"name": case_name, "start_ns": start, "return_ns": returned, "entry_code": "session_limit" if outcome == "session_limit" else "",
                    "assignment": assignment, "model_id": model_id, "report": report}
            write(data / name / f"{case_name}-case.json", case)
            write(data / name / f"{case_name}-return.json", copy.deepcopy(case))
            write(data / name / f"{case_name}.jsonl", callbacks, True)
        write(data / name / "routing.jsonl", routing, True)
        arms.append({"name": name, "strategy": strategy, "kind": kind, "cases": names, "state": "passed", "trigger_ns": trigger,
                     "before_models": before, "after_models": counts.copy(), "cleanup_ns": cleanup,
                     "trigger_snapshot": {"ActiveSessions": 1 if kind == "staggered" else 2} if kind != "warmup" else None,
                     "trigger_created": [x+1 for x in before] if kind != "warmup" else [0, 0],
                     "trigger_closed": [before[0], before[1]+1] if kind == "staggered" else before if kind == "overload" else [0, 0],
                     "local_pool_reusable": None if strategy == "old" else True})
        intervals.append((base, cleanup, name))
    write(data / "arms.json", arms)
    for worker in ("A", "B"):
        write(data / f"worker{worker}/model-events.jsonl", events[worker], True)
        write(data / f"worker{worker}/server.log", [dict(e, msg=e["kind"]) for e in events[worker]], True)
    write(data / "process-start.json", {"worker_pids": [100, 101], "driver_pid": 200})
    write(data / "process.json", {"worker_pids": [100, 101], "driver_pid": 200, "process_exit": [0, 0], "models_created": counts, "models_closed": counts})
    samples = []
    for at in range(0, intervals[-1][1]+3*S, S//2):
        arm_name = next((name for start, end, name in intervals if start <= at <= end), "between")
        workers = {}
        for i, worker in enumerate(("A", "B")):
            workers[worker] = {"pid": 100+i, "rss_bytes": 400000000, "created": sum(e["kind"] == "model_created" and e["observed_ns"] <= at for e in events[worker]),
                               "closed": sum(e["kind"] == "engine_closed" and e["observed_ns"] <= at for e in events[worker])}
        samples.append({"elapsed_ns": at, "wall_ns": 1_000_000_000_000+at, "arm": arm_name, "active_sessions": 0 if arm_name == "between" else 1,
                        "driver_goroutines": 12, "driver_heap_bytes": 2000000, "workers": workers})
    write(data / "samples.jsonl", samples, True)


class AdmissionEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        fixture(self.root)

    def change(self, name, mutate, jsonl=False):
        path = self.root / "data" / name
        value = [json.loads(x) for x in path.read_text().splitlines()] if jsonl else json.loads(path.read_text())
        mutate(value)
        write(path, value, jsonl)

    def test_complete_evidence_separates_outcomes(self):
        result = reconcile(self.root)
        self.assertEqual((result["old_newcomer_completed"], result["new_newcomer_completed"], result["overload_entry_rejected"]), (0, 3, 3))
        self.assertEqual((result["attempts"], result["rpcs"], result["models_created"]), (29, 26, [10, 13]))

    def test_busy_rpc_accepts_go_nil_slices(self):
        for filename in ("p1_old/N-case.json", "p1_old/N-return.json"):
            self.change(filename, lambda c: c["report"]["grpc"].update(results=None, chunks=None))
        self.assertEqual(reconcile(self.root)["old_newcomer_completed"], 0)

    def test_nil_slice_handling_does_not_accept_invalid_arrays(self):
        self.change("p1_old/N-case.json", lambda c: c["report"]["grpc"].update(results=False))
        with self.assertRaisesRegex(ValueError, "invalid RPC evidence arrays"):
            reconcile(self.root)

    def test_forged_completed_ack_rejected(self):
        for filename in ("p1_capacity/N-case.json", "p1_capacity/N-return.json"):
            self.change(filename, lambda c: c["report"]["client"].update(CompletionAckWritten=False))
        with self.assertRaisesRegex(ValueError, "completion mismatch"):
            reconcile(self.root)

    def test_resource_exhausted_is_not_automatically_busy(self):
        self.change("p1_old/N-case.json", lambda c: c["assignment"].update(terminal_message="audio stream limit exceeded"))
        with self.assertRaisesRegex(ValueError, "busy rejection"):
            reconcile(self.root)

    def test_wrong_worker_mapping_rejected(self):
        self.change("p1_capacity/N-case.json", lambda c: c["assignment"].update(worker="A"))
        with self.assertRaisesRegex(ValueError, "wrong Worker"):
            reconcile(self.root)

    def test_modified_snapshot_rejected(self):
        self.change("p1_capacity/N.jsonl", lambda rows: rows[0]["message"].update(text="changed"), True)
        with self.assertRaisesRegex(ValueError, "snapshot changed"):
            reconcile(self.root)

    def test_missing_model_close_rejected(self):
        self.change("workerB/model-events.jsonl", lambda rows: rows.pop(), True)
        with self.assertRaisesRegex(ValueError, "native event denominator"):
            reconcile(self.root)

    def test_third_call_without_overlap_rejected(self):
        self.change("arms.json", lambda rows: rows[1].update(trigger_ns=999999999999))
        with self.assertRaisesRegex(ValueError, "overlap"):
            reconcile(self.root)

    def test_unreleased_local_capacity_rejected(self):
        self.change("arms.json", lambda rows: rows[2].update(local_pool_reusable=False))
        with self.assertRaisesRegex(ValueError, "reservation cleanup"):
            reconcile(self.root)

    def test_sample_gap_marks_data_quality(self):
        self.change("samples.jsonl", lambda rows: rows.__delitem__(slice(2, 10)), True)
        result = reconcile(self.root)
        self.assertFalse(result["sampling_quality_ok"])
        self.assertEqual(result["new_newcomer_completed"], 3)

    def test_rss_failure_preserved(self):
        self.change("samples.jsonl", lambda rows: rows[0]["workers"]["A"].update(rss_bytes=None, rss_error="timeout"), True)
        self.assertFalse(reconcile(self.root)["sampling_quality_ok"])

    def test_frozen_source_change_rejected(self):
        (self.root / "source/fixture.json").write_text("changed")
        with self.assertRaisesRegex(ValueError, "source mismatch"):
            reconcile(self.root)


if __name__ == "__main__":
    unittest.main()
