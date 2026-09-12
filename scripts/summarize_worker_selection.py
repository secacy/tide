#!/usr/bin/env python3
"""Validate EXP-008 accounting and preserve source provenance and timing exclusions."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess

from summarize_streaming_load import records, timing_gaps


def validate(r):
    checks = [r["experiment"] == "EXP-008", all(s["reserved"] == 0 for s in r["after"]),
              all(0 <= s["reserved"] <= s["capacity"] for s in r["initial"])]
    if r["kind"] == "logical":
        checks += [r["admitted"] + r["rejected"] == len(r["events"]),
                   r["admitted"] == sum(r["assignments"]),
                   r["rejected"] == sum(e["worker"] == "rejected" for e in r["events"]),
                   0 <= r["mean_reserved_ratio_gap"] <= 1]
    else:
        clients, workers = r["clients_detail"], r["workers"]
        checks += [len(clients) == len({c["id"] for c in clients}) == r["clients"] == 4,
                   sum(w["opened"] for w in workers) == 4,
                   r["registered_after"] == r["metrics"]["queues_after"] == r["metrics"]["queue_bytes_after"] == 0,
                   all(not c["error"] and c["close_code"] in (1000, 1013) for c in clients),
                   all(c["final"] and c["sent"] == c["received"] == r["planned_chunks"] // 4
                       for c in clients if c["close_code"] == 1000),
                   r["normal_complete"] == sum(c["close_code"] == 1000 for c in clients),
                   r["sent"] == sum(c["sent"] for c in clients),
                   r["received"] == sum(c["received"] for c in clients) == r["metrics"]["latency"]["count"],
                   r["sent"] >= sum(w["received"] for w in workers) >= r["received"]]
        for w in workers:
            p = w["processing"]
            checks += [w["active_after"] == p["active"] == p["waiting"] == 0,
                       p["peak_active"] <= p["slots"],
                       w["received"] == p["started"] == p["processed"] + p["canceled_waiting"] + p["canceled_processing"]]
    if not all(checks):
        raise ValueError(f"invalid {r['case']}: {[i for i, c in enumerate(checks) if not c]}")


def benchmark_rows(events):
    # Benchmarks have no individual pass event. Compare each output's wall span
    # with N * ns/op; setup/GC are excluded by Go but should not introduce a >1s gap.
    started, rounds = {}, Counter()
    pattern = re.compile(r"(BenchmarkWorkerSelection/\S+)-(\d+)\s+(\d+)\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op")
    for e in events:
        name = e.get("Test", "")
        if e["Action"] == "run" and name.startswith("BenchmarkWorkerSelection/"):
            started[name] = datetime.fromisoformat(e["Time"])
        match = pattern.search(e.get("Output", ""))
        if not match:
            continue
        name, cpu, n, ns, size, allocations = match.groups()
        now = datetime.fromisoformat(e["Time"])
        if name not in started:
            raise ValueError(f"missing benchmark start: {name}")
        gap = (now - started[name]).total_seconds() - int(n) * float(ns) / 1e9
        started[name] = now
        rounds[name] += 1
        yield {"case": name.removeprefix("BenchmarkWorkerSelection/"), "round": rounds[name],
               "gomaxprocs": int(cpu), "iterations": int(n), "ns_per_op": float(ns),
               "bytes_per_op": int(size), "allocs_per_op": int(allocations), "timing_gap_seconds": gap}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--exclude-timing-gaps", action="store_true")
    args = parser.parse_args()
    grouped, benchmarks, provenance = defaultdict(list), defaultdict(list), []
    root = Path(__file__).resolve().parents[1]
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + ".meta.json").read_text())
        if meta["experiment"] != "selection" or meta["race"] or meta["exit_code"] != 0:
            raise ValueError(f"not a formal selection run: {path}")
        for relative, sha in meta["source_sha256"].items():
            data = subprocess.check_output(["git", "show", f"{meta['commit']}:{relative}"], cwd=root)
            if hashlib.sha256(data).hexdigest() != sha:
                raise ValueError(f"uncommitted experiment source: {relative}")
        rows = records(path, strict_timing=not args.exclude_timing_gaps)
        events = [json.loads(line) for line in path.read_text().splitlines()]
        gaps = timing_gaps(events)
        bad = {(g["test"].rsplit("/", 1)[-1], g["round"]) for g in gaps}
        rounds = Counter()
        quality = {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                   "commit": meta["commit"], "source_matches_commit": True, "timing_gaps": gaps,
                   "accepted": [], "excluded": [], "benchmark_excluded": []}
        for r in rows:
            validate(r)
            rounds[r["case"]] += 1
            key = (r["case"], rounds[r["case"]])
            if key in bad:
                quality["excluded"].append(key)
            else:
                quality["accepted"].append(key)
                grouped[r["case"]].append(r)
        if any(n != meta["count"] for n in rounds.values()):
            raise ValueError("record count does not match run count")
        for b in benchmark_rows(events):
            if abs(b["timing_gap_seconds"]) > 1:
                quality["benchmark_excluded"].append(b)
                if not args.exclude_timing_gaps:
                    raise ValueError(f"benchmark timing continuity uncertain: {b}")
            else:
                benchmarks[b["case"]].append(b)
        provenance.append(quality)
    if not grouped:
        raise ValueError("no valid samples")

    def span(values):
        values = list(values)
        return {"min": min(values), "max": max(values)}

    summary = {}
    for name, runs in grouped.items():
        r = runs[0]
        if r["kind"] == "stream" and len({x["input_ms"] for x in runs}) != 1:
            raise ValueError("do not combine different input durations")
        s = {"runs": len(runs), "initial_reserved": [x["reserved"] for x in r["initial"]]}
        if r["kind"] == "logical":
            s.update(admitted=[x["admitted"] for x in runs], rejected=[x["rejected"] for x in runs],
                     mean_ratio_gap=span(x["mean_reserved_ratio_gap"] for x in runs))
        else:
            s.update(input_ms=r["input_ms"], normal_complete=[x["normal_complete"] for x in runs],
                     sent=sum(x["sent"] for x in runs), received=sum(x["received"] for x in runs),
                     p95_ms=span(x["metrics"]["latency"]["p95_ms"] for x in runs),
                     max_ms=span(x["metrics"]["latency"]["max_ms"] for x in runs),
                     schedule_p99_ms=span(x["metrics"]["schedule_lateness"]["p99_ms"] for x in runs),
                     queue_peak_bytes=span(x["metrics"]["queue_peak_bytes_session"] for x in runs))
        summary[name] = s
    result = {"quality": provenance, "scenarios": summary, "benchmarks": {
        name: {"runs": len(runs), "ns_per_op": span(b["ns_per_op"] for b in runs),
               "bytes_per_op": span(b["bytes_per_op"] for b in runs),
               "allocs_per_op": span(b["allocs_per_op"] for b in runs), "samples": runs}
        for name, runs in benchmarks.items()}}
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
