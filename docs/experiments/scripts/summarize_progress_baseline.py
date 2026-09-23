"""Extract auditable summaries and sample CSV from the Go experiment log.

Usage: python3 docs/experiments/scripts/summarize_progress_baseline.py LOG_PATH
Uses only the standard library. Raw logs remain the authoritative full record.
"""

import collections
import csv
import json
import pathlib
import sys


def summarize(log_path):
    reports = []
    marker = "PROGRESS_BASELINE_JSON "
    for line in log_path.read_text().splitlines():
        if marker in line:
            report = json.loads(line.split(marker, 1)[1])
            if report["case"] != "probe_only":
                reports.append(report)
    counts = collections.Counter(r["case"] for r in reports)
    expected = {"normal": 3, "pause_500ms": 3, "slow_200ms": 3}
    if dict(counts) != expected:
        raise ValueError(f"expected three runs per case, found {dict(counts)}")

    groups = collections.defaultdict(list)
    runs = []
    for report in reports:
        if report["outcome"] != "completed":
            raise ValueError(f"incomplete experiment: {report['case']} {report.get('error')}")
        samples = report["samples"]
        if not samples:
            raise ValueError("missing samples")
        final = report["final_progress"]
        if (final["received_bytes"], final["processed_bytes"], final["pending_bytes"]) != (320000, 320000, 0):
            raise ValueError(f"unexpected final counters: {final}")
        if report["close_code"] != 1000 or report["active_after_cleanup"] != 0:
            raise ValueError("session did not close and clean up normally")
        if report["worker_chunks"] != 100 or report["worker_bytes"] != 320000:
            raise ValueError("audio integrity assertion missing")
        peak = max(s["pending_bytes"] for s in samples)
        if peak != report["observed_peak_pending_bytes"]:
            raise ValueError("peak does not match raw samples")
        if len(report["writes"]) != 100 or len(report["sends"]) != 100 or len(report["partials"]) != 20:
            raise ValueError("unexpected input/output record count")
        row = {k: v for k, v in report.items() if k not in {"samples", "writes", "sends", "partials"}}
        row["run"] = len(groups[report["case"]]) + 1
        row["sample_count"] = len(samples)
        row["audio_send_ms"] = report["writes"][-1]["returned_ms"]
        row["end_observed_pending_bytes"] = report["at_end_write_return"]["pending_bytes"]
        row["end_observed_pending_audio_ms"] = row["end_observed_pending_bytes"] / 32
        end = report["at_end_write_return"]
        row["end_client_written_not_gateway_counted_bytes"] = 320000 - end["received_bytes"]
        row["end_client_written_unconfirmed_bytes"] = 320000 - end["processed_bytes"]
        row["end_client_written_unconfirmed_audio_ms"] = row["end_client_written_unconfirmed_bytes"] / 32
        row["max_partial_wait_ms"] = max(p["wait_ms"] for p in report["partials"])
        groups[report["case"]].append(row)
        runs.append(row)

    metrics = [
        "observed_peak_pending_bytes", "observed_peak_audio_ms", "end_observed_pending_bytes",
        "end_observed_pending_audio_ms", "tail_ms", "max_send_ms", "max_write_ms",
        "audio_send_ms", "max_input_schedule_lag_ms", "max_sample_gap_ms",
        "max_snapshot_call_ms", "first_low_delay_ms", "max_partial_wait_ms", "sample_count",
        "end_client_written_not_gateway_counted_bytes", "end_client_written_unconfirmed_bytes",
        "end_client_written_unconfirmed_audio_ms",
    ]
    cases = {}
    for name, rows in groups.items():
        ranges = {}
        for metric in metrics:
            values = [r[metric] for r in rows if r[metric] is not None]
            ranges[metric] = [min(values), max(values)] if values else None
        cases[name] = {"completed": len(rows), "ranges": ranges}
    summary = {
        "source_log": log_path.name,
        "counts": dict(counts),
        "totals": {"completed": len(runs), "audio_chunks": sum(r["worker_chunks"] for r in runs),
                   "audio_bytes": sum(r["worker_bytes"] for r in runs),
                   "active_after_cleanup_each": [r["active_after_cleanup"] for r in runs]},
        "cases": cases,
        "runs": runs,
        "measurement_limits": [
            "Sampled peak is an observed lower bound on the true instantaneous maximum.",
            "Audio duration is bytes / 32000 seconds, not wall-clock waiting latency.",
            "End sample follows client end Write return, not Gateway end recognition.",
            "First low after resume means first sample <= one 3200-byte chunk; no sustained recovery guarantee.",
            "Instrumented HTTP wrapper calls production session.run and tracker; not Gateway.ServeHTTP.",
            "Client-written bytes not yet gateway-counted may be in upstream buffers; no buffer location attribution.",
        ],
    }
    out = log_path.with_name(log_path.stem + "-summary.json")
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    csv_path = log_path.with_name(log_path.stem + "-samples.csv")
    with csv_path.open("w", newline="") as f:
        fields = ["case", "run", "begin_ms", "at_ms", "received_bytes", "processed_bytes", "pending_bytes", "pending_audio_ms"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        run_numbers = collections.Counter()
        for report in reports:
            name = report["case"]
            run_numbers[name] += 1
            for sample in report["samples"]:
                writer.writerow({"case": name, "run": run_numbers[name], **sample,
                                 "pending_audio_ms": sample["pending_bytes"] / 32})
    print(json.dumps({"totals": summary["totals"], "cases": cases}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    summarize(pathlib.Path(sys.argv[1]))
