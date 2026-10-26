#!/usr/bin/env python3
"""Reconcile the frozen six-pair v3 model/feature-recipe comparison."""
import argparse
import json
from pathlib import Path
import sys

from compare_decoding import sha
from score import character_errors, require

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "asrprobe"))
from model_assets import CANDIDATE, LEGACY, expected_model
from verify import verify_case

PROFILES = {"A": LEGACY, "B": CANDIDATE}
PLAN = [f"{g}_q{i:02}" for i in range(1, 7) for g in ("AB" if i % 2 else "BA")]
GREEDY = {"method": "greedy_search", "max_active_paths": 0}


def compare(folder):
    """Compare only complete matching input sets; failures retain their slots.

    Frozen expectations must match the independently pinned source lock. Raw
    snapshots are scored unchanged; fact judgments belong to a separate review.
    """
    frozen = json.loads((folder / "before-run.json").read_text())
    require(frozen["schema"] == "tide.asr-model-comparison.v1" and frozen["plan"] == PLAN, "wrong paired model plan")
    for group, profile in PROFILES.items():
        require(frozen["models"][group] == expected_model(profile), "wrong frozen model identity: " + group)
    for name, digest in frozen["baseline_sha256"].items():
        require(sha((folder / "baseline" / name).read_bytes()) == digest, "changed baseline: " + name)
    manifest = json.loads((folder / "baseline/input-manifest.json").read_text())
    baseline_score = json.loads((folder / "baseline/score.json").read_text())
    ids = [f"q{i:02}" for i in range(1, 7)]
    require([c["id"] for c in manifest["cases"]] == ids and [c["id"] for c in baseline_score["cases"]] == ids, "incomplete baseline")
    executions = json.loads((folder / "execution.json").read_text())
    require([e["name"] for e in executions] == PLAN, "missing planned slots")
    rows = []
    for execution in executions:
        group, identifier = execution["name"].split("_")
        index = int(identifier[1:]) - 1
        case = manifest["cases"][index]
        row = {"name": execution["name"], "group": group, "case": identifier, "outcome": "unknown", "quality": None}
        try:
            require(case["reference_sha256"] == sha((case["reference"] + "\n").encode()), "reference mismatch")
            if execution.get("timed_out") or execution.get("exit_code") not in (None, 0):
                row["outcome"] = "failed"
                raise ValueError("probe failed or timed out")
            require(execution.get("exit_code") == 0, "probe not run")
            verified = verify_case(folder, execution, expected_decoding=GREEDY, expected_model=frozen["models"][group])
            report = json.loads((folder / execution["name"] / "summary.json").read_text())
            require(report["version"] == 3, "expected v3 model identity")
            require(report["file_sha256"]["input.wav"] == case["wav_sha256"], "different WAV")
            require(report["file_sha256"]["probe_binary"] == frozen["probe_sha256"], "different program")
            require("reference.txt" not in report["file_sha256"], "reference passed to probe")
            m = report["metrics"]
            require(report["cancel_after_ns"] == 0 and m["input_bytes"] == case["pcm_bytes"], "different input or cancellation")
            events = [json.loads(line) for line in (folder / execution["name"] / "events.jsonl").read_text().splitlines()]
            chunks = [e["samples"] * 2 for e in events if e["kind"] == "input"]
            require(chunks == [min(3200, case["pcm_bytes"]-p) for p in range(0, case["pcm_bytes"], 3200)], "different input blocks")
            require([e["samples"] for e in events if e["kind"] == "leading_padding"] == [4800], "different leading padding")
            require([e["samples"] for e in events if e["kind"] == "trailing_padding"] == [9600], "different trailing padding")
            text = m["last_text_snapshot"]
            quality = character_errors(case["reference"], text)
            require(quality["N"] > 0, "empty reference")
            row.update(outcome="completed", text=text, reference=case["reference"], quality=quality,
                       equal_to_original_gateway_text=text == baseline_score["cases"][index]["hypothesis"],
                       input_bytes=case["pcm_bytes"], engine_call_ns=m["engine_call_ns"], call_rtf=m["call_rtf"],
                       max_schedule_lag_ns=m["max_schedule_lag_ns"], first_text_ns=m["first_nonempty_text_ns"],
                       first_text_before_input_end=m["first_text_before_input_end"], tail_ns=m["last_input_call_end_to_flush_end_ns"],
                       load_ns=report["load_ns"], verification=verified)
        except (AssertionError, ValueError, KeyError, TypeError, OSError) as error:
            row["issue"] = str(error) or type(error).__name__
        rows.append(row)
    groups = {}
    for group, profile in PROFILES.items():
        selected = [r for r in rows if r["group"] == group]
        complete = [r for r in selected if r["outcome"] == "completed"]
        totals = {k: sum(r["quality"][k] for r in complete) for k in ("N", "S", "D", "I", "errors")}
        totals["cer"] = totals["errors"] / totals["N"] if totals["N"] else None
        call_ns = sum(r["engine_call_ns"] for r in complete)
        duration_ns = sum(r["input_bytes"] for r in complete) / 32000 * 1e9
        groups[group] = {"profile": profile, "planned": 6, "completed": len(complete),
                         "failed": sum(r["outcome"] == "failed" for r in selected),
                         "unknown": sum(r["outcome"] == "unknown" for r in selected),
                         "completed_subset": totals, "sum_engine_call_ns": call_ns,
                         "aggregate_call_rtf": call_ns / duration_ns if duration_ns else None,
                         "input_period_text_cases": sum(r["first_text_before_input_end"] for r in complete),
                         "max_schedule_lag_ns": max((r["max_schedule_lag_ns"] for r in complete), default=None),
                         "texts_equal_to_original_gateway": sum(r["equal_to_original_gateway_text"] for r in complete)}
    paired = all(g["completed"] == 6 for g in groups.values())
    return {"schema": "tide.asr-model-result.v1", "planned": 12, "groups": groups, "all_six_pairs_complete": paired,
            "cer_reduction_percentage_points": (groups["A"]["completed_subset"]["cer"] - groups["B"]["completed_subset"]["cer"])*100 if paired else None,
            "cases": rows, "human_fact_review": "pending", "weight_use_terms": "unresolved",
            "scope": "local model and native feature recipe comparison; six seen recordings, one run per setting; no deployment decision"}


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
