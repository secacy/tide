#!/usr/bin/env python3
"""Validate and summarize EXP-006; preserve failures and pre-exit throughput."""

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path

from summarize_streaming_load import records


def validate(r):
    p, m, clients = r["pool"], r["metrics"], r["clients"]
    checks = (
        r["experiment"] == "EXP-006",
        r["registered_after_cleanup"] == 0,
        m["observed_queues_remaining"] == m["queue_bytes_remaining"] == 0,
        p["active"] == p["waiting"] == 0,
        p["peak_active"] <= p["slots"],
        p["started"] == p["processed"] + p["canceled_waiting"] + p["canceled_processing"],
        p["started"] == r["worker_received"],
        r["chunks_sent"] + r["concurrency"] >= m["audio_enqueued"] >= m["audio_dequeued"] >= r["worker_received"] >= p["processed"] >= r["results_received"],
        m["audio_enqueued"] == m["audio_dequeued"] + m["queued_chunks_abandoned_on_failure"],
        len(clients) == len({c["id"] for c in clients}) == r["concurrency"],
        sum(c["sent"] for c in clients) == r["chunks_sent"],
        sum(c["received"] for c in clients) == r["results_received"],
        sum(c["code"] == 1000 for c in clients) == r["normal_complete"],
        dict(Counter(str(c["code"]) for c in clients)) == r["close_codes"],
        not r["client_errors"],
    )
    if not all(checks):
        raise ValueError(f"invalid accounting: {r['case']}: {checks}")


def pre_exit_rate(r):
    # 固定窗口避开接入与尾部；若窗口内已有会话退出，不报告满负载吞吐。
    if min(c["finished_ms"] for c in r["clients"]) <= 5200:
        return None
    series = r["series"]
    start = min(series, key=lambda s: abs(s["at_ms"] - 2000))
    end = min(series, key=lambda s: abs(s["at_ms"] - 5000))
    if abs(start["at_ms"] - 2000) > 300 or abs(end["at_ms"] - 5000) > 300:
        return None
    return (end["pool"]["processed"] - start["pool"]["processed"]) * 1000 / (end["at_ms"] - start["at_ms"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    grouped = defaultdict(list)
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + ".meta.json").read_text())
        if meta["race"] or meta["exit_code"] != 0 or meta["experiment"] != "capacity":
            raise ValueError(f"not a formal capacity run: {path}")
        rows = records(path, strict_timing=True)
        counts = Counter(r["case"] for r in rows)
        if any(n != meta["count"] for n in counts.values()):
            raise ValueError(f"missing or duplicate records: {path}")
        for r in rows:
            validate(r)
            grouped[(r["case"], r["input_ms"])].append(r)

    output = {}
    for (name, duration), runs in grouped.items():
        def span(values):
            values = [v for v in values if v is not None]
            return {"min": min(values), "max": max(values)} if values else None
        output[f"{name}_{duration}ms"] = {
            "runs": len(runs), "concurrency": runs[0]["concurrency"], "input_ms": duration,
            "normal_complete": [r["normal_complete"] for r in runs],
            "close_codes": [r["close_codes"] for r in runs],
            "close_reasons": [dict(Counter(c["reason"] for c in r["clients"] if c["code"] != 1000)) for r in runs],
            "end_to_end_p95_ms": span(r["metrics"]["chunk_result_latency"]["p95_ms"] for r in runs),
            "end_to_end_max_ms": span(r["metrics"]["chunk_result_latency"]["max_ms"] for r in runs),
            "pre_exit_processed_per_second_2_to_5s": span(pre_exit_rate(r) for r in runs),
            "slot_wait_p95_ms": span(r["pool"]["slot_wait"]["p95_ms"] for r in runs),
            "slot_hold_mean_ms": span(r["pool"]["slot_hold"]["mean_ms"] for r in runs),
            "slot_occupancy_ratio": span(r["pool"]["slot_occupancy_ratio"] for r in runs),
            "first_failure_ms": span(min((c["finished_ms"] for c in r["clients"] if c["code"] != 1000), default=None) for r in runs),
            "queue_wait_p95_ms": span(r["metrics"]["queue_wait"]["p95_ms"] for r in runs),
            "queue_peak_bytes_per_session": span(r["metrics"]["queue_peak_bytes_session"] for r in runs),
            "schedule_lateness_p99_ms": span(r["metrics"]["client_schedule_lateness"]["p99_ms"] for r in runs),
            "unreturned_sent_chunks": [r["chunks_sent"] - r["results_received"] for r in runs],
            "heap_peak_bytes": span(r["heap_peak_sampled"] for r in runs),
            "process_average_cpu_cores": span(r["process_average_cpu_cores"] for r in runs),
            "registered_after_cleanup": [r["registered_after_cleanup"] for r in runs],
        }
    encoded = json.dumps(output, indent=2) + "\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
