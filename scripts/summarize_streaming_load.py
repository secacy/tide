#!/usr/bin/env python3
"""Extract complete EXP-003 records from Go test JSON (which splits long lines)."""

import argparse
from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path


def timing_gaps(events):
    """Flag >1s disagreement between subtest wall-clock spans and reported elapsed time."""
    started, rounds, gaps = {}, defaultdict(int), []
    for event in events:
        name = event.get("Test", "")
        if "/" not in name:
            continue
        if event.get("Action") == "run":
            rounds[name] += 1
            started[name] = datetime.fromisoformat(event["Time"])
        elif event.get("Action") == "pass" and name in started:
            wall = (datetime.fromisoformat(event["Time"]) - started[name]).total_seconds()
            elapsed = event["Elapsed"]
            if abs(wall - elapsed) > 1:
                gaps.append({"test": name, "round": rounds[name], "wall_seconds": wall,
                             "reported_elapsed_seconds": elapsed, "gap_seconds": wall - elapsed})
    return gaps


def records(path, strict_timing=False):
    buffers = defaultdict(str)
    result = []
    events = [json.loads(line) for line in path.read_text().splitlines()]
    if not events or events[-1].get("Action") != "pass":
        raise ValueError(f"experiment did not pass: {path}")
    gaps = timing_gaps(events)
    if strict_timing and gaps:
        raise ValueError(f"timing continuity uncertain: {path}: {gaps}")
    for event in events:
        key = event.get("Test", "")
        buffers[key] += event.get("Output", "")
        while "\n" in buffers[key]:
            line, buffers[key] = buffers[key].split("\n", 1)
            if "EXPERIMENT_RESULT " in line:
                result.append(json.loads(line.split("EXPERIMENT_RESULT ", 1)[1]))
    if not result:
        raise ValueError(f"no experiment records: {path}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--strict-timing", action="store_true", help="Reject runs with wall/elapsed gaps greater than 1s")
    args = parser.parse_args()
    samples = [record for path in args.inputs for record in records(path, args.strict_timing)]
    grouped = defaultdict(list)
    for record in samples:
        grouped[record["case"]].append(record)
    summary = {}
    for name, runs in grouped.items():
        def span(values):
            return {"min": min(values), "max": max(values)}
        summary[name] = {
            "runs": len(runs), "concurrency": runs[0]["concurrency"], "input_ms": runs[0]["input_ms"],
            "normal_complete": [r["normal_complete"] for r in runs], "close_codes": [r["close_codes"] for r in runs],
            "end_to_end_p95_ms": span([r["metrics"]["chunk_result_latency"]["p95_ms"] for r in runs]),
            "end_to_end_max_ms": span([r["metrics"]["chunk_result_latency"]["max_ms"] for r in runs]),
            "queue_wait_p95_ms": span([r["metrics"]["queue_wait"]["p95_ms"] for r in runs]),
            "queue_wait_max_ms": span([r["metrics"]["queue_wait"]["max_ms"] for r in runs]),
            "queue_peak_bytes_per_session": span([r["metrics"]["queue_peak_bytes_session"] for r in runs]),
            "grpc_send_max_ms": span([r["metrics"]["grpc_send_wait"]["max_ms"] for r in runs]),
            "schedule_lateness_p99_ms": span([r["metrics"]["client_schedule_lateness"]["p99_ms"] for r in runs]),
            "heap_idle_bytes": span([r["heap_idle"] for r in runs]),
            "heap_peak_bytes": span([r["heap_peak_sampled"] for r in runs]),
            "heap_after_sessions_gc_bytes": span([r["heap_after_sessions_gc"] for r in runs]),
            "registered_after_cleanup": [r["registered_after_cleanup"] for r in runs],
        }
    encoded = json.dumps(summary, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
