#!/usr/bin/env python3
"""Validate EXP-009; report each Worker and failed clients, preserving timing exclusions."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

from summarize_streaming_load import records, timing_gaps


def validate(r):
    clients, caps, queue = r["clients"], r["capacities"], r["queue"]
    n = sum(caps)
    checks = [r["experiment"] == "EXP-009", len(clients) == len({c["id"] for c in clients}) == n,
              sum(w["opened"] for w in r["workers"]) == n,
              r["max_sessions"] > n + 10,
              r["normal_complete"] == sum(c["code"] == 1000 for c in clients),
              all(c["code"] in (1000, 1013) and not c.get("error") for c in clients),
              all(c["final"] and c["sent"] == c["received"] == c["planned"]
                  for c in clients if c["code"] == 1000),
              all(0 <= c["received"] <= c["sent"] <= c["planned"] for c in clients),
              r["registered_after"] == queue["remaining_bytes"] == queue["remaining_queues"] == 0,
              all(w["reserved"] == 0 for w in r["reservations_after"]),
              queue["enqueued"] == queue["dequeued"] + queue["abandoned"],
              r["probe_complete"] and r["probe_sessions"] == n,
              Counter(r["probe_routes"]) == dict(enumerate(caps)),
              len(r["rejections"] or []) == (10 if r["mode"] == "burst" else 0),
              all(x["status"] == 503 and not x.get("error") for x in r["rejections"] or [])]
    for i, w in enumerate(r["workers"]):
        group = [c for c in clients if c["worker"] == i]
        p = w["processing"]
        checks += [len(group) == w["opened"] == caps[i],
                   w["latency"]["count"] == sum(c["received"] for c in group),
                   sum(x["count"] for x in w["windows"]) == w["latency"]["count"],
                   p["active"] == p["waiting"] == 0, p["peak_active"] <= p["slots"] == r["slots"][i],
                   p["started"] == w["received"] == p["processed"] + p["canceled_waiting"] + p["canceled_processing"],
                   sum(c["sent"] for c in group) >= w["received"] >= p["processed"] >= w["latency"]["count"]]
    for s in r["series"]:
        checks += [0 <= s["registered"] <= n + 10,
                   all(0 <= w["reserved"] <= w["capacity"] for w in s["reservations"]),
                   all(w["processing"]["active"] <= w["processing"]["slots"] for w in s["workers"])]
    if not all(checks):
        raise ValueError(f"invalid {r['case']}: {[i for i, c in enumerate(checks) if not c]}")


def worker_view(r, i):
    """End-to-end outstanding is successful client writes minus results, not Gateway acknowledgements."""
    w = r["workers"][i]
    clients = [c for c in r["clients"] if c["worker"] == i]
    timeline = [(s["at_ms"], s["workers"][i]["client_sent"] - s["workers"][i]["results"])
                for s in r["series"] if 1000 <= s["at_ms"] < r["input_ms"]]
    first = [v for at, v in timeline if at < 11_000]
    last = [v for at, v in timeline if at >= r["input_ms"] - 10_000]
    windows = w["windows"]
    return {"normal": sum(c["code"] == 1000 for c in clients), "initial": len(clients),
            "failures": [{k: c[k] for k in ("id", "reason", "finished_ms", "sent", "received")} for c in clients if c["code"] != 1000],
            "p95_ms": w["latency"]["p95_ms"], "max_ms": w["latency"]["max_ms"],
            "max_window_p95_ms": max(x["p95_ms"] for x in windows), "windows": windows,
            "unreturned_peak": max((v for _, v in timeline), default=0),
            "unreturned_first_10s_mean": statistics.mean(first) if first else None,
            "unreturned_last_10s_mean": statistics.mean(last) if last else None,
            "sent_without_result": sum(c["sent"] - c["received"] for c in clients),
            "schedule_p99_ms": w["schedule_lateness"]["p99_ms"],
            "pause_hits": w["processing"]["pause_hits"]}


def recoveries(r):
    """Sampled recovery: <=2 unreturned chunks/session for 1s, sampled every 200ms.

    This is an observation convention, not a new production timeout. It includes
    transit and result writes; client counters are sampled approximately together.
    """
    if r["mode"] != "jitter":
        return []
    rows = []
    period, pause, threshold = r["fault_period_ms"], r["pause_ms"], r["capacities"][1] * 2
    for start in range(period, r["input_ms"], period):
        streak, confirmed = None, None
        for s in r["series"]:
            at = s["at_ms"]
            if not start + pause <= at < min(start + period, r["input_ms"]):
                continue
            w = s["workers"][1]
            pending = w["client_sent"] - w["results"]
            if pending > threshold or w["active_rpc"] != r["capacities"][1]:
                streak = None
            elif streak is None:
                streak = at
            elif at - streak >= 1000:
                confirmed = at
                break
        rows.append({"start_ms": start, "recovered_ms_after_pause_end": None if confirmed is None else streak - start - pause,
                     "confirmed_at_ms": confirmed, "threshold_chunks": threshold})
    return rows


def describe(r):
    workers = [worker_view(r, i) for i in range(2)]
    resource = r["series"]
    return {"workers": workers, "recoveries": recoveries(r), "normal_complete": r["normal_complete"],
            "all_complete_and_window_p95_within_1s": r["normal_complete"] == sum(r["capacities"]) and all(w["max_window_p95_ms"] <= 1000 for w in workers),
            "rejected": len(r["rejections"] or []), "rejection_ms": [x["ms"] for x in r["rejections"] or []],
            "queue_peak_bytes_session": r["queue"]["peak_bytes_session"],
            "cpu_mean_cores": r["cpu_mean_cores"], "heap_idle": r["heap_idle"],
            "heap_peak_sampled": max(s["heap_alloc"] for s in resource), "heap_after_gc": r["heap_after_gc"],
            "goroutines_idle": r["goroutines_idle"], "goroutines_peak": max(s["goroutines"] for s in resource),
            "goroutines_after": r["goroutines_after"], "probe_sessions": r["probe_sessions"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--exclude-timing-gaps", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    grouped, quality = defaultdict(list), []
    for path in args.inputs:
        meta = json.loads(path.with_suffix(path.suffix + ".meta.json").read_text())
        if meta["experiment"] != "multi" or meta["race"] or meta["exit_code"] != 0:
            raise ValueError(f"not a formal multi capacity run: {path}")
        for relative, sha in meta["source_sha256"].items():
            data = subprocess.check_output(["git", "show", f"{meta['commit']}:{relative}"], cwd=root)
            if hashlib.sha256(data).hexdigest() != sha:
                raise ValueError(f"uncommitted experiment source: {relative}")
        rows = records(path, strict_timing=not args.exclude_timing_gaps)
        events = [json.loads(line) for line in path.read_text().splitlines()]
        gaps = timing_gaps(events)
        bad = {(g["test"].rsplit("/", 1)[-1], g["round"]) for g in gaps}
        q = {"file": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "commit": meta["commit"],
             "source_matches_commit": True, "timing_gaps": gaps, "accepted": [], "excluded": []}
        rounds = Counter()
        for r in rows:
            validate(r)
            if r["input_ms"] < 60_000:
                raise ValueError("short smoke samples cannot establish capacity")
            rounds[r["case"]] += 1
            key = (r["case"], rounds[r["case"]])
            if key in bad:
                q["excluded"].append(key)
            else:
                q["accepted"].append(key)
                row = describe(r)
                row["source"] = {"file": path.name, "round": rounds[r["case"]]}
                grouped[f"{r['case']}_{r['input_ms']}ms"].append(row)
        if any(n != meta["count"] for n in rounds.values()):
            raise ValueError("record count mismatch")
        quality.append(q)
    if not grouped:
        raise ValueError("no samples passed timing checks")
    with args.output.open("x") as stream:
        json.dump({"quality": quality, "scenarios": grouped}, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
