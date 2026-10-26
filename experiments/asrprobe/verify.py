#!/usr/bin/env python3
"""Reconcile final probe summaries with completed-call events, without inference."""
import argparse
import json
from pathlib import Path
from fetch_assets import ASSETS
from model_assets import LEGACY, STREAM_CONFIG, expected_model as locked_model


def verify_case(root, case, *, expected_decoding=None, expected_model=None):
    name = case["name"]
    report = json.loads((root / name / "summary.json").read_text())
    m = report["metrics"]
    events = [json.loads(line) for line in (root / name / "events.jsonl").read_text().splitlines()]
    expected_status = "canceled" if name == "canceled" else "completed_probe"
    assert case["exit_code"] == (1 if name == "canceled" else 0), name
    assert m["status"] == expected_status and m["close_returned"], name
    assert report["threads"] == 2 and report["realtime"], name
    # Version 1 was hard-coded greedy. Version 2 must declare actual SDK settings.
    greedy = {"method": "greedy_search", "max_active_paths": 0}
    assert report["version"] in (1, 2, 3), name
    assert report.get("decoding", greedy if report["version"] == 1 else None) == (expected_decoding or greedy), name
    assert m["processed_audio_bytes"] is None and report["character_score"] is None, name
    if report["version"] == 3:
        expected = expected_model if expected_model is not None else locked_model(LEGACY)
        assert report["model_profile"] == expected["profile"], name
        assert report["stream_config"] == STREAM_CONFIG, name
        assert report["provider"] == "cpu", name
        assert report["file_sha256"]["model_asset_lock"] == expected["asset_lock_sha256"], name
        for asset, digest in expected["files"].items():
            assert report["file_sha256"][asset] == digest, asset
    else:
        # Never reinterpret v1/v2 as a different model, even if requested.
        assert expected_model is None, name
        for asset in list(ASSETS)[:4]:
            assert report["file_sha256"][asset] == ASSETS[asset], asset
    assert any(d["Path"] == "github.com/k2-fsa/sherpa-onnx-go-macos" and d["Version"] == "v1.13.8"
               for d in report["build"]["Deps"]), name
    offset = last_time = native_ns = max_lag = decodes = 0
    snapshots = []
    input_ends = []
    for event in events:
        assert last_time <= event["begin_ns"] <= event["end_ns"], (name, event)
        last_time = event["end_ns"]
        kind = event["kind"]
        if kind == "input":
            assert event["planned_ns"] == offset // 2 * 1_000_000_000 // 16000, name
            assert 0 < event["samples"] <= 1600, name
            max_lag = max(max_lag, event["begin_ns"] - event["planned_ns"])
            offset += event["samples"] * 2
            input_ends.append(event["end_ns"])
        assert event["input_end_bytes"] == offset, (name, event)
        if kind == "decode":
            decodes += 1
        if kind == "text_snapshot":
            snapshots.append(event)
        else:
            native_ns += event["end_ns"] - event["begin_ns"]
    assert offset == m["submitted_bytes"] <= m["input_bytes"], name
    assert native_ns == m["engine_call_ns"] and max_lag == m["max_schedule_lag_ns"], name
    assert decodes == m["decode_calls"] and len(snapshots) == m["text_changes"], name
    assert (snapshots[-1].get("text", "") if snapshots else "") == m["last_text_snapshot"], name
    nonempty = [e for e in snapshots if e.get("text", "").strip()]
    assert (nonempty[0]["end_ns"] if nonempty else None) == m["first_nonempty_text_ns"], name
    assert (bool(nonempty) and nonempty[0]["input_end_bytes"] < m["input_bytes"]) == m["first_text_before_input_end"], name
    if name == "canceled":
        assert not m["input_finished"] and m["call_rtf"] is None and m["last_input_call_end_to_flush_end_ns"] is None, name
        assert not any(e["kind"] in ("trailing_padding", "input_finished") for e in events), name
    else:
        assert offset == m["input_bytes"] and m["input_finished"], name
        assert sum(e["kind"] == "input_finished" for e in events) == 1, name
        assert m["trailing_padding_samples"] == 9600 and m["leading_padding_samples"] == 4800, name
        assert abs(m["call_rtf"] - native_ns / (m["input_bytes"] / 32000 * 1e9)) < 1e-12, name
        if name.startswith("speech"):
            assert m["first_text_before_input_end"], name
        if name == "silence":
            assert not nonempty and m["last_text_snapshot"] == "", name
    return {"name": name, "status": m["status"], "submitted_bytes": offset,
            "decode_calls": decodes, "text_changes": len(snapshots), "call_rtf": m["call_rtf"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.run / "manifest.json").read_text())
    assert [c["name"] for c in manifest["cases"]] == ["speech-0", "speech-1", "silence", "canceled"]
    print(json.dumps([verify_case(args.run, c) for c in manifest["cases"]], indent=2))


if __name__ == "__main__":
    main()
