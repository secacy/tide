#!/usr/bin/env python3
"""Verify the predeclared six-pair greedy/beam probe comparison and score raw text."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

from score import character_errors, require

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "asrprobe"))
from verify import verify_case

METHODS = {"G": {"method": "greedy_search", "max_active_paths": 0},
           "B": {"method": "modified_beam_search", "max_active_paths": 4}}
PLAN = [f"{group}_q{i:02}" for i in range(1, 7) for group in ("GB" if i % 2 else "BG")]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def compare(folder):
    frozen = json.loads((folder / "before-run.json").read_text())
    require(frozen["schema"] == "tide.asr-decoding-comparison.v1" and frozen["plan"] == PLAN, "wrong paired plan")
    for name, expected in frozen["baseline_sha256"].items():
        require(sha((folder / "baseline" / name).read_bytes()) == expected, "changed baseline: " + name)
    manifest = json.loads((folder / "baseline/input-manifest.json").read_text())
    old_score = json.loads((folder / "baseline/score.json").read_text())
    ids = [f"q{i:02}" for i in range(1, 7)]
    require([c["id"] for c in manifest["cases"]] == ids and [c["id"] for c in old_score["cases"]] == ids, "incomplete baseline")
    executions = json.loads((folder / "execution.json").read_text())
    require([e["name"] for e in executions] == PLAN, "missing planned slots")
    rows = []
    for execution in executions:
        group, case_id = execution["name"].split("_")
        index = int(case_id[1:]) - 1
        case = manifest["cases"][index]
        row = {"name": execution["name"], "group": group, "case": case_id, "outcome": "unknown", "quality": None}
        try:
            require(case["reference_sha256"] == sha((case["reference"] + "\n").encode()), "reference mismatch")
            if execution.get("timed_out") or execution.get("exit_code") not in (None, 0):
                row["outcome"] = "failed"
                raise ValueError("probe failed or timed out")
            require(execution.get("exit_code") == 0, "probe not run")
            verified = verify_case(folder, execution, expected_decoding=METHODS[group])
            report = json.loads((folder / execution["name"] / "summary.json").read_text())
            require(report["version"] == 2 and report["decoding"] == METHODS[group], "wrong effective search settings")
            require(report["file_sha256"]["input.wav"] == case["wav_sha256"] and report["file_sha256"]["probe_binary"] == frozen["probe_sha256"], "different WAV or program")
            require(all(report["file_sha256"][n] == h for n, h in frozen["model_sha256"].items()), "different models")
            m = report["metrics"]
            require(report["cancel_after_ns"] == 0 and m["input_bytes"] == case["pcm_bytes"], "different input or cancellation")
            events = [json.loads(line) for line in (folder / execution["name"] / "events.jsonl").read_text().splitlines()]
            chunks = [e["samples"] * 2 for e in events if e["kind"] == "input"]
            require(chunks == [min(3200, case["pcm_bytes"]-p) for p in range(0, case["pcm_bytes"], 3200)], "different input blocks")
            require([e["samples"] for e in events if e["kind"] == "leading_padding"] == [4800] and [e["samples"] for e in events if e["kind"] == "trailing_padding"] == [9600], "different padding")
            text = m["last_text_snapshot"]
            quality = character_errors(case["reference"], text)
            require(quality["N"] > 0, "empty reference")
            row.update({"outcome": "completed", "text": text, "reference": case["reference"], "quality": quality,
                "equal_to_original_gateway_text": text == old_score["cases"][index]["hypothesis"],
                "input_bytes": case["pcm_bytes"], "engine_call_ns": m["engine_call_ns"], "call_rtf": m["call_rtf"],
                "max_schedule_lag_ns": m["max_schedule_lag_ns"], "first_text_ns": m["first_nonempty_text_ns"],
                "tail_ns": m["last_input_call_end_to_flush_end_ns"], "verification": verified})
        except (AssertionError, ValueError, KeyError, TypeError, OSError) as error:
            row["issue"] = str(error) or type(error).__name__
        rows.append(row)
    groups = {}
    for group in METHODS:
        selected = [r for r in rows if r["group"] == group]
        complete = [r for r in selected if r["outcome"] == "completed"]
        totals = {k: sum(r["quality"][k] for r in complete) for k in ["N", "S", "D", "I", "errors"]}
        totals["cer"] = totals["errors"] / totals["N"] if totals["N"] else None
        call_ns = sum(r["engine_call_ns"] for r in complete)
        duration_ns = sum(r["input_bytes"] for r in complete) / 32000 * 1e9
        groups[group] = {"planned": 6, "completed": len(complete),
            "failed": sum(r["outcome"] == "failed" for r in selected), "unknown": sum(r["outcome"] == "unknown" for r in selected),
            "completed_subset": totals, "sum_engine_call_ns": call_ns,
            "aggregate_call_rtf": call_ns / duration_ns if duration_ns else None,
            "max_schedule_lag_ns": max((r["max_schedule_lag_ns"] for r in complete), default=None),
            "texts_equal_to_original_gateway": sum(r["equal_to_original_gateway_text"] for r in complete)}
    paired = all(g["completed"] == 6 for g in groups.values())
    return {"schema": "tide.asr-decoding-result.v1", "planned": 12, "groups": groups,
        "all_six_pairs_complete": paired,
        "cer_reduction_percentage_points": (groups["G"]["completed_subset"]["cer"] - groups["B"]["completed_subset"]["cer"])*100 if paired else None,
        "cases": rows, "human_fact_review": "pending", "scope": "six previously seen recordings, one run per setting; diagnostic only"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = json.dumps(compare(args.folder), ensure_ascii=False, indent=2) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(result)
    else:
        print(result, end="")
