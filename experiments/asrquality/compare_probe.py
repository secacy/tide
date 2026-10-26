#!/usr/bin/env python3
"""Reconcile q04/q05 direct probes against frozen Gateway baseline evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from score import character_errors, reconcile_completed, require

# Reuse the original completed-call verifier. Frozen archives preserve this layout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "asrprobe"))
from verify import verify_case


def sha(data):
    return hashlib.sha256(data).hexdigest()


def changes(texts):
    """Remove identical adjacent snapshots, including a repeated terminal final.

    Never remove characters inside a snapshot or merge different snapshots.
    """
    result = []
    for text in texts:
        if not result or result[-1] != text:
            result.append(text)
    return result


def compare(folder):
    frozen = json.loads((folder / "before-run.json").read_text())
    require(frozen["schema"] == "tide.asr-quality-direct.v1" and frozen["plan"] == ["q04", "q05"], "unexpected diagnostic plan")
    baseline = folder / "baseline"
    for name, expected in frozen["baseline_sha256"].items():
        require(sha((baseline / name).read_bytes()) == expected, "baseline evidence changed: " + name)
    manifest = json.loads((baseline / "input-manifest.json").read_text())
    reports = json.loads((baseline / "summary.json").read_text())
    require([c["id"] for c in manifest["cases"]] == [f"q{i:02}" for i in range(1, 7)], "missing baseline inputs")
    require([c["id"] for c in reports] == [f"q{i:02}" for i in range(1, 7)], "missing baseline slots")
    executions = json.loads((folder / "execution.json").read_text())
    require([c["name"] for c in executions] == frozen["plan"], "missing diagnostic slots")
    rows = []
    for execution in executions:
        name = execution["name"]
        index = int(name[1:]) - 1
        case, item = manifest["cases"][index], reports[index]
        row = {"id": name, "outcome": "unknown"}
        try:
            require(case["reference_sha256"] == sha((case["reference"] + "\n").encode()), "changed reference")
            require(item["outcome"] == "completed", "baseline was not complete")
            original = reconcile_completed(baseline, item, case, index)
            require(execution["exit_code"] == 0 and not execution["timed_out"], "probe failed or timed out")
            verified = verify_case(folder, execution)
            report = json.loads((folder / name / "summary.json").read_text())
            require(report["file_sha256"]["probe_binary"] == frozen["probe_sha256"], "wrong probe binary")
            require(report["file_sha256"]["input.wav"] == case["wav_sha256"], "different WAV")
            require(all(report["file_sha256"][n] == h for n, h in frozen["model_sha256"].items()), "different model files")
            require(report["cancel_after_ns"] == 0 and report["metrics"]["input_bytes"] == case["pcm_bytes"], "different input/cancellation")
            events = [json.loads(line) for line in (folder / name / "events.jsonl").read_text().splitlines()]
            chunks = [e["samples"]*2 for e in events if e["kind"] == "input"]
            expected_chunks = [min(3200, case["pcm_bytes"]-p) for p in range(0, case["pcm_bytes"], 3200)]
            require(chunks == expected_chunks, "different chunk boundaries")
            require([e["samples"] for e in events if e["kind"] == "leading_padding"] == [4800], "wrong leading padding")
            require([e["samples"] for e in events if e["kind"] == "trailing_padding"] == [9600], "wrong trailing padding")
            direct = report["metrics"]["last_text_snapshot"]
            native_changes = [e.get("text", "") for e in events if e["kind"] == "text_snapshot"]
            gateway_changes = changes([r["text"] for r in item["report"]["grpc"]["results"]])
            row.update({"outcome": "completed", "input_bytes": case["pcm_bytes"], "pcm_sha256": case["pcm_sha256"],
                "reference": case["reference"], "gateway_text": original, "direct_text": direct,
                "last_text_equal": direct == original, "changed_snapshots_equal": native_changes == gateway_changes,
                "direct_changed_snapshots": len(native_changes), "gateway_changed_snapshots": len(gateway_changes),
                "direct_quality": character_errors(case["reference"], direct), "gateway_quality": character_errors(case["reference"], original),
                "direct_call_verification": verified})
        except (AssertionError, ValueError, KeyError, TypeError, OSError) as error:
            row["issue"] = str(error)
        rows.append(row)
    return {"schema": "tide.asr-quality-direct-comparison.v1", "planned": 2,
            "completed": sum(r["outcome"] == "completed" for r in rows),
            "unknown": sum(r["outcome"] == "unknown" for r in rows), "cases": rows,
            "scope": "diagnostic samples selected after baseline; not an independent quality estimate"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    data = json.dumps(compare(args.folder), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(data)
    else:
        print(data, end="")
