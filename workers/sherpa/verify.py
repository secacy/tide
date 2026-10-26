#!/usr/bin/env python3
"""Reconcile the fixed Worker process test reports with raw result events/logs."""
import argparse
import hashlib
import json
from pathlib import Path


def verify(folder):
    reports = json.loads((folder / "summary.json").read_text())
    assert [r["name"] for r in reports[:4]] == ["speech0", "speech1", "silence", "cancel_after_partial"]
    probes = reports[4:]
    assert probes and [p["name"] for p in probes] == [f"after_cancel_{i}" for i in range(len(probes))]
    assert all(p["status"] == "ResourceExhausted" for p in probes[:-1])
    assert probes[-1]["status"] == "completed"
    expected_bytes = {"speech0": 321700, "speech1": 163200, "cancel_after_partial": 321700}
    for r in reports:
        events = [json.loads(line) for line in (folder / (r["name"] + ".jsonl")).read_text().splitlines()]
        assert r["input_bytes"] == expected_bytes.get(r["name"], 65120)
        assert 0 <= r["sent_bytes"] <= r["input_bytes"] and r["sent_bytes"] % 2 == 0
        assert r["processed_audio_bytes"] is None and r["cer"] is None
        assert len(events) == r["updates"]
        finals = [e for e in events if e["response"].get("is_final", False)]
        assert len(finals) == r["finals"]
        assert not finals or finals == events[-1:]
        assert r["last_text_snapshot"] == (events[-1]["response"].get("text", "") if events else "")
        first = next((e for e in events if e["response"].get("text", "").strip()), None)
        assert r["rpc_start_to_first_text_ns"] == (first["elapsed_ns"] if first else None)
        assert r["first_text_before_all_bytes_sent"] == (first is not None and first["sent_bytes"] < r["input_bytes"])
        previous_time = previous_offset = 0
        for e in events:
            assert e["elapsed_ns"] >= previous_time and previous_offset <= e["sent_bytes"] <= r["sent_bytes"]
            assert e["response"]["segment_id"] == "1" and e["response"].get("progress") is None
            previous_time, previous_offset = e["elapsed_ns"], e["sent_bytes"]
        if r["status"] == "completed":
            assert r["sent_bytes"] == r["input_bytes"] and r["finals"] == 1
            assert r["close_send_start_to_eof_ns"] >= 0
        else:
            assert r["finals"] == 0 and r["close_send_start_to_eof_ns"] is None
    assert all(r["first_text_before_all_bytes_sent"] for r in reports[:2])
    assert reports[2]["last_text_snapshot"] == "" and probes[-1]["last_text_snapshot"] == ""
    assert reports[3]["status"] == "Canceled" and reports[3]["sent_bytes"] < reports[3]["input_bytes"]
    model_events = []
    for line in (folder / "server.log").read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue  # Native diagnostics are retained but are not structured evidence.
        if event.get("msg") in ("model_created", "engine_closed"):
            model_events.append((event["msg"], event["model_id"]))
    assert model_events == [(kind, i) for i in range(1, 6) for kind in ("model_created", "engine_closed")]
    process = json.loads((folder / "process.json").read_text())
    assert process["process_exit"] == 0 and process["models_created"] == process["models_closed"] == 5
    return {"reconciled": True, "cases": len(reports), "completed": sum(r["status"] == "completed" for r in reports),
            "expected_cancellation": 1, "busy_probes": len(probes) - 1, "models_created_and_closed": 5}


def verify_gateway(folder, manifest):
    reports = json.loads((folder / "summary.json").read_text())
    assert [r["name"] for r in reports] == ["speech0", "speech1", "silence", "cancel_after_partial", "after_cancel"]
    conditions = json.loads((folder / "conditions.json").read_text())
    assert conditions["max_pending_audio_bytes"] == 0 and conditions["max_sessions"] == 1
    assert conditions["resume_window_ns"] == 10_000_000_000 and conditions["result_retention_ns"] == 30_000_000_000
    assert not conditions["client_resume_enabled"] and conditions["realtime"]
    reference = json.loads(manifest.read_text())["pcm"]
    silence = {"bytes": 65120, "sha256": hashlib.sha256(bytes(65120)).hexdigest()}
    for index, r in enumerate(reports):
        expected = reference["speech0"] if index in (0, 3) else reference["speech1"] if index == 1 else silence
        assert r["input_bytes"] == expected["bytes"] and r["input_sha256"] == expected["sha256"]
        assert 0 < r["source_read_bytes"] <= r["input_bytes"] and r["source_read_bytes"] % 2 == 0
        assert r["source_closes"] == 1 and r["gateway_active_after_cleanup"] == 0 and r["native_closed_after_cleanup"] == index + 1
        assert r["processed_audio_bytes"] is None and r["cer"] is None and r["segments"] == 1
        returned = json.loads((folder / (r["name"] + "-client-return.json")).read_text())
        for key in ["client", "source_read_bytes", "source_closes", "run_ns", "source_eof_ns",
                    "callback_updates", "finals", "transcript", "gateway_active_at_run_return"]:
            assert returned[key] == r[key]
        events = [json.loads(line) for line in (folder / (r["name"] + ".jsonl")).read_text().splitlines()]
        assert len(events) == r["callback_updates"]
        messages = [e["message"] for e in events]
        assert [int(m["seq"]) for m in messages] == list(range(1, len(messages) + 1))
        assert all(m["type"] == "result" and m["segmentId"] == "1" for m in messages)
        assert sum(m["isFinal"] for m in messages) == r["finals"]
        assert not r["finals"] or (r["finals"] == 1 and messages[-1]["isFinal"])
        assert r["transcript"] == messages[-1]["text"]
        previous_time = previous_read = 0
        for e in events:
            assert previous_time <= e["received_ns"] <= r["run_ns"]
            assert previous_read <= e["source_read_bytes"] <= r["source_read_bytes"]
            previous_time, previous_read = e["received_ns"], e["source_read_bytes"]
        first = next((e for e in events if e["message"]["text"].strip()), None)
        assert r["run_start_to_first_text_ns"] == (first["received_ns"] if first else None)
        assert r["first_text_before_all_source_read"] == (first is not None and first["source_read_bytes"] < r["input_bytes"])
        rpc = json.loads((folder / (r["name"] + "-grpc.json")).read_text())
        assert rpc == r["grpc"]
        assert sum(rpc["chunks"]) == rpc["audio_bytes"] <= r["source_read_bytes"]
        assert all(0 < n <= 3200 and n % 2 == 0 for n in rpc["chunks"])
        assert all(m["segment_id"] == "1" and not m["has_progress"] for m in rpc["results"])
        for original, applied in zip(rpc["results"], messages):
            assert (original["text"], original["final"]) == (applied["text"], applied["isFinal"])
        assert len(rpc["results"]) >= len(messages)
        client = r["client"]
        assert client["ResumeAttempts"] == client["ResumedConnections"] == 0
        if index == 3:
            assert r["canceled"] and not client["Completed"] and not client["CompletionAckWritten"]
            assert r["finals"] == 0 and r["source_read_bytes"] < r["input_bytes"] and r["source_eof_ns"] is None
            assert r["source_eof_to_run_return_ns"] is None and r["gateway_active_at_run_return"] == 1
            assert not rpc["half_closed"] and rpc["terminal"] == "Canceled"
            assert 0 <= client["AppliedSeq"] <= len(messages)
        else:
            assert not r.get("error") and not r["canceled"] and client["Completed"] and client["CompletionAckWritten"]
            assert client["FinalOffset"] == r["input_bytes"] == r["source_read_bytes"] == rpc["audio_bytes"]
            assert client["AppliedSeq"] == r["callback_updates"] == len(rpc["results"]) and r["finals"] == 1
            assert rpc["audio_sha256"] == r["input_sha256"] and rpc["half_closed"] and rpc["terminal"] == "EOF"
            assert rpc["chunks"][-1] == (r["input_bytes"] - 1) % 3200 + 1
            assert r["source_eof_to_run_return_ns"] == r["run_ns"] - r["source_eof_ns"] >= 0
            assert 0 <= r["run_return_to_observed_cleanup_ns"] <= 5_000_000_000
    assert all(r["first_text_before_all_source_read"] for r in reports[:2])
    assert reports[2]["transcript"] == reports[4]["transcript"] == ""
    model_events = []
    for line in (folder / "server.log").read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("msg") in ("model_created", "engine_closed"):
            model_events.append((event["msg"], event["model_id"]))
    assert model_events == [(kind, i) for i in range(1, 6) for kind in ("model_created", "engine_closed")]
    process = json.loads((folder / "process.json").read_text())
    assert process == {"gateway_active_before_fixture_cleanup": 0, "models_closed": 5, "process_exit": 0}
    return {"reconciled": True, "completed": 4, "expected_cancellation": 1,
            "full_audio_hashes_matched": 4, "models_created_and_closed": 5}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--gateway", action="store_true", help="verify Gateway/v2 evidence instead of direct RPC")
    parser.add_argument("--manifest", type=Path, help="pre-run manifest with independently computed PCM hashes")
    args = parser.parse_args()
    if args.gateway and args.manifest is None:
        parser.error("--gateway requires --manifest")
    print(json.dumps(verify_gateway(args.folder, args.manifest) if args.gateway else verify(args.folder), indent=2))
