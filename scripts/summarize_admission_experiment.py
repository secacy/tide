#!/usr/bin/env python3
"""Validate EXP-007 and distinguish admission rejection from failure after admission."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from summarize_streaming_load import records


def validate(r):
    clients, pool, queue = r["clients"], r["pool"], r["queue"]
    admitted = [c for c in clients if c["http_status"] == 101]
    rejected = [c for c in clients if c["http_status"] == 503]
    checks = [
        r["experiment"] == "EXP-007",
        len(clients) == len({c["id"] for c in clients}) == 16,
        Counter(c["cohort"] for c in clients) == {"existing": 6, "incoming": 10},
        len(admitted) + len(rejected) == 16,
        len(admitted) == r["admitted"] == r["rpc_opened"],
        len(rejected) == r["rejected"],
        all(not c.get("error") for c in clients),
        all(c["sent"] == c["received"] == c["planned"] == c["close_code"] == 0 for c in rejected),
        all(c["close_code"] in (1000, 1013) for c in admitted),
        all(c["final"] and c["sent"] == c["received"] == c["planned"] for c in admitted if c["close_code"] == 1000),
        r["registered_after_cleanup"] == pool["waiting"] == pool["active"] == 0,
        pool["peak_active"] <= pool["slots"] == 4,
        pool["started"] == r["worker_received"] == pool["processed"] + pool["canceled_waiting"] + pool["canceled_processing"],
        queue["enqueued"] == queue["dequeued"] + queue["abandoned"],
        queue["remaining_queues"] == queue["remaining_bytes"] == 0,
        queue["dequeued"] >= pool["started"] >= pool["processed"] >= sum(c["received"] for c in clients),
        r["reuse_probe_complete"],
        max(s["sessions"] for s in r["series"]) <= r["max_sessions"],
        (pool["pause_hits"] > 0) == (r["pause_ms"] > 0),
    ]
    for name, metrics in r["cohorts"].items():
        checks.append(metrics["result_latency"]["count"] == sum(c["received"] for c in clients if c["cohort"] == name))
    if r["max_sessions"] == 6:
        checks += [all(c["close_code"] == 1000 for c in clients if c["cohort"] == "existing"), len(rejected) == 10]
    else:
        checks.append(len(admitted) == 16)
    if not all(checks):
        raise ValueError(f"invalid {r['case']}: failed checks {[i for i, v in enumerate(checks) if not v]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    grouped = defaultdict(list)
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + ".meta.json").read_text())
        if meta["experiment"] != "admission" or meta["race"] or meta["exit_code"] != 0:
            raise ValueError(f"not a formal admission run: {path}")
        rows = records(path, strict_timing=True)
        counts = Counter(r["case"] for r in rows)
        if any(n != meta["count"] for n in counts.values()):
            raise ValueError(f"wrong case count: {path}")
        for r in rows:
            validate(r)
            grouped[(r["case"], r["input_ms"])].append(r)

    def span(values):
        values = list(values)
        return {"min": min(values), "max": max(values)} if values else None

    summary = {}
    for (name, duration), runs in grouped.items():
        groups = {}
        for cohort in ("existing", "incoming"):
            clients = [c for r in runs for c in r["clients"] if c["cohort"] == cohort]
            latency = [r["cohorts"][cohort]["result_latency"] for r in runs]
            groups[cohort] = {
                "attempted": len(clients),
                "admitted": sum(c["http_status"] == 101 for c in clients),
                "rejected": sum(c["http_status"] == 503 for c in clients),
                "normal_complete": sum(c["close_code"] == 1000 for c in clients),
                "failed_after_admission": dict(Counter(c["reason"] for c in clients if c["http_status"] == 101 and c["close_code"] != 1000)),
                "rejection_ms": span(c["dial_ms"] for c in clients if c["http_status"] == 503),
                "result_p95_ms": span(h["p95_ms"] for h in latency if h["count"]),
                "result_max_ms": span(h["max_ms"] for h in latency if h["count"]),
                "sent_without_result": sum(c["sent"] - c["received"] for c in clients),
                "schedule_lateness_p99_ms": span(r["cohorts"][cohort]["schedule_lateness"]["p99_ms"] for r in runs if r["cohorts"][cohort]["schedule_lateness"]["count"]),
            }
        summary[f"{name}_{duration}ms"] = {
            "runs": len(runs), "max_sessions": runs[0]["max_sessions"], "input_ms": duration,
            "cohorts": groups, "rpc_opened": [r["rpc_opened"] for r in runs],
            "queue_peak_bytes_session": span(r["queue"]["peak_bytes_session"] for r in runs),
            "pause_hits": [r["pool"]["pause_hits"] for r in runs],
            "registered_after_cleanup": [r["registered_after_cleanup"] for r in runs],
            "reuse_probe_complete": [r["reuse_probe_complete"] for r in runs],
        }
    encoded = json.dumps(summary, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
