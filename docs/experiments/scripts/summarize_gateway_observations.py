#!/usr/bin/env python3
"""复核独立采样文件与负载窗口；离散观测不替代真实峰值或精确清理时间。"""
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

from run_loadgen_baseline import check_report, digest, distribution, now, save

MAX_GAP_NS = 500_000_000


def timestamp_ns(value):
    """解析同机记录的 UTC 时间，保留 Go 的九位小数，也接受 Python 的 +00:00。"""
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)", value)
    if not match:
        raise ValueError(f"not a UTC timestamp: {value!r}")
    whole, fraction = match.groups()
    seconds = int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp())
    return seconds * 1_000_000_000 + int((fraction or "").ljust(9, "0"))


def require(condition, message):
    """输入损坏时明确拒绝，不补零或丢弃失败行。"""
    if not condition:
        raise ValueError(message)


def nonnegative_int(value):
    return type(value) is int and 0 <= value <= 2**63 - 1


def read_complete_rows(path, live=False):
    """运行中仅读取已换行的完整记录；最终读取拒绝截断末行。"""
    data = path.read_bytes()
    if live:
        data = data[:data.rfind(b"\n") + 1]
    else:
        require(not data or data.endswith(b"\n"), "truncated JSONL")
    return [json.loads(line) for line in data.splitlines()]


def check_sampling_data(manifest, rows, load_start, load_finish, planned_sessions):
    """独立复算计数、窗口两端和观测间隔；查询时间区间不当成快照精确发生时刻。"""
    start, finish = timestamp_ns(load_start), timestamp_ns(load_finish)
    require(start < finish, "load window is not ordered")
    require(type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1, "manifest schema")
    require(manifest.get("output_path") == "samples.jsonl", "unexpected sample path")
    cfg = manifest["config"]
    require(cfg == dict(endpoint="http://127.0.0.1:8080/debug/gateway", interval_ns=100_000_000, request_timeout_ns=1_000_000_000), "sampling config differs")
    for key in ("samples_written", "successful_samples", "failed_samples"):
        require(nonnegative_int(manifest.get(key)), f"invalid {key}")
    require(manifest.get("output_error", "missing") is None and manifest.get("close_error", "missing") is None, "sample output/close error")
    require(manifest.get("stop_reason") == "deadline_exceeded" and isinstance(manifest.get("sampling_error"), str), "sampling did not reach deadline")
    recording_start = timestamp_ns(manifest["started_at"])
    recording_finish = timestamp_ns(manifest["finished_at"])
    require(recording_start <= recording_finish, "recording clock went backwards")
    successes, failures = [], []
    previous_finish = recording_start
    for index, row in enumerate(rows):
        require(type(row.get("schema_version")) is int and row["schema_version"] == 1, "sample schema")
        require(type(row.get("index")) is int and row["index"] == index, "sample index gap")
        require(nonnegative_int(row.get("duration_ns")), "invalid duration")
        begin, end = timestamp_ns(row["started_at"]), timestamp_ns(row["finished_at"])
        require(previous_finish <= begin <= end <= recording_finish, "sample wall clocks not ordered")
        previous_finish = end
        require("state" in row and "error" in row, "missing state/error")
        state, error = row["state"], row["error"]
        if error is not None:
            require(isinstance(error, str) and state is None, "invalid failed sample")
            failures.append((begin, end))
        else:
            require(isinstance(state, dict), "success without state")
            active, maximum, stopping = (state.get(k) for k in ("active_sessions", "max_sessions", "stopping"))
            require(nonnegative_int(active) and type(maximum) is int and maximum > 0 and active <= maximum and type(stopping) is bool, "invalid state")
            successes.append(dict(start=begin, finish=end, active=active, maximum=maximum, stopping=stopping, duration=row["duration_ns"]))
    require(manifest["samples_written"] == len(rows) and manifest["successful_samples"] == len(successes) and manifest["failed_samples"] == len(failures), "manifest/JSONL count mismatch")
    before = [r for r in successes if r["finish"] <= start]
    after = [r for r in successes if r["start"] >= finish]
    inside = [r for r in successes if r["start"] >= start and r["finish"] <= finish]
    bracket = [r for r in successes if before and after and before[-1]["finish"] <= r["finish"] <= after[0]["finish"]]
    gaps = [b["finish"] - a["finish"] for a, b in zip(bracket, bracket[1:])]
    peak = max((r["active"] for r in inside), default=None)
    zero_after = next((r for r in after if r["active"] == 0), None)
    window_failures = sum(a < finish and b > start for a, b in failures)
    checks = dict(
        idle_before=bool(before) and before[-1]["active"] == 0,
        zero_after=zero_after is not None,
        observed_planned_peak=peak == planned_sessions,
        state_matches_fixture=all(r["maximum"] == 64 and not r["stopping"] and r["active"] <= planned_sessions for r in successes),
        window_has_no_failed_queries=window_failures == 0,
        bracket_gap_within_budget=bool(gaps) and max(gaps) <= MAX_GAP_NS,
    )
    return dict(checks_passed=all(checks.values()), checks=checks, samples=len(rows), successful_samples=len(successes), failed_samples=len(failures),
                window_successful_samples=len(inside), window_failed_samples=window_failures,
                observed_peak_active=peak, max_bracketing_success_gap_ns=max(gaps) if gaps else None,
                first_post_exit_zero_sample_delay_ns=zero_after["finish"]-finish if zero_after else None,
                query_duration=distribution([r["duration"] for r in successes]),
                window_active_histogram={str(n): sum(r["active"] == n for r in inside) for n in sorted({r["active"] for r in inside})})


def check_sampling(directory, load_start, load_finish, planned_sessions):
    """从独占目录读取最终清单和全部 JSONL；不改动原始文件。"""
    manifest = json.loads((directory / "manifest.json").read_text())
    return check_sampling_data(manifest, read_complete_rows(directory / "samples.jsonl"), load_start, load_finish, planned_sessions)


def summarize(directory):
    """复读九批原始证据并记录分析输入哈希；派生文件只写入指定目录。"""
    manifest = json.loads((directory / "manifest.json").read_text())
    require(manifest["status"] == "passed", "experiment did not pass; inspect retained attempts")
    expected = [f"r{rep}-n{n}" for rep in (1, 2, 3) for n in (1, 2, 4)]
    formal = [b for b in manifest["batches"] if b["label"] != "warmup"]
    require([b["label"] for b in formal] == expected, "unexpected schedule")
    for name, expected_hash in manifest["artifact_sha256"].items():
        require(digest(directory / name) == expected_hash, f"artifact changed: {name}")
    batches = []
    for b in formal:
        n = b["planned_sessions"]
        load = check_report(directory / (b["label"] + ".json"), n, 160000)
        sampling = check_sampling(directory / b["sampling"]["directory"], b["started_at"], b["finished_at"], n)
        require(b["exit_code"] == 0 and b["sampling"]["exit_code"] == 0 and load["checks_passed"] and sampling["checks_passed"], f"batch failed: {b['label']}")
        batches.append(dict(label=b["label"], planned_sessions=n, load=load, gateway=sampling))
    groups = []
    for n in (1, 2, 4):
        selected = [b for b in batches if b["planned_sessions"] == n]
        sessions = [s for b in selected for s in b["load"]["sessions"]]
        groups.append(dict(planned_sessions=n, completed=len(sessions), observed_peaks=[b["gateway"]["observed_peak_active"] for b in selected],
                           samples=sum(b["gateway"]["samples"] for b in selected), failed_samples=sum(b["gateway"]["failed_samples"] for b in selected),
                           window_successful_samples=sum(b["gateway"]["window_successful_samples"] for b in selected),
                           max_success_gap_ns=max(b["gateway"]["max_bracketing_success_gap_ns"] for b in selected),
                           post_exit_zero_delay_ns=distribution([b["gateway"]["first_post_exit_zero_sample_delay_ns"] for b in selected]),
                           tail=distribution([s["tail_ns"] for s in sessions]),
                           audio_schedule_samples=sum(s["audio_schedule_samples"] for s in sessions),
                           max_audio_schedule_lag_ns=max(s["max_audio_schedule_lag_ns"] for s in sessions)))
    result = dict(formal_batches=9, completed_sessions=sum(g["completed"] for g in groups), groups=groups, batches=batches,
                  limits=["discrete sampled peaks, not continuous maxima", "zero observation delay is not exact cleanup latency", "same-host wall clock for cross-process alignment",
                          "500ms gap is an experiment coverage criterion, not a clinical SLA", "no queue/resource/stable-capacity result", "sampling overhead has no A/B measurement"])
    save(directory / "groups.json", result)
    save(directory / "analysis-manifest.json", dict(generated_at=now(), input_sha256={"manifest.json": digest(directory / "manifest.json"), **manifest["artifact_sha256"]},
         script_sha256={p.name: digest(p) for p in (Path(__file__), Path(__file__).with_name("run_loadgen_baseline.py"))}, groups_sha256=digest(directory / "groups.json")))
    print(json.dumps({"completed_sessions": result["completed_sessions"], "groups": groups}, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
