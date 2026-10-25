#!/usr/bin/env python3
"""Reconcile the fixed Worker process test reports with raw result events/logs."""
import argparse
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    print(json.dumps(verify(parser.parse_args().folder), indent=2))
