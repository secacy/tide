#!/usr/bin/env python3
"""复核联合观测原始证据；不把未采集当零，不把样本比例解释为时间利用率。"""
import json
from pathlib import Path
import sys

from run_loadgen_baseline import check_report, digest, distribution, now, save
from summarize_gateway_observations import (MAX_GAP_NS, check_sampling, nonnegative_int,
                                           read_complete_rows, require, timestamp_ns)


def check_worker_data(manifest, rows, load_start, load_finish, planned_sessions, endpoint):
    """检查文件/时间/状态契约，并只在完整落入负载窗口的有效计数上做统计。"""
    start, finish = timestamp_ns(load_start), timestamp_ns(load_finish)
    require(start < finish, "load window is not ordered")
    require(manifest.get("source_kind") == "worker" and type(manifest.get("schema_version")) is int and manifest["schema_version"] == 1, "manifest kind/schema")
    require(manifest.get("output_path") == "samples.jsonl", "unexpected sample path")
    require(manifest.get("config") == dict(endpoint=endpoint, interval_ns=20_000_000, request_timeout_ns=1_000_000_000), "sampling config differs")
    for key in ("samples_written", "successful_samples", "failed_samples"):
        require(nonnegative_int(manifest.get(key)), f"invalid {key}")
    require(manifest.get("output_error", "missing") is None and manifest.get("close_error", "missing") is None, "sample output/close error")
    require(manifest.get("stop_reason") == "deadline_exceeded" and isinstance(manifest.get("sampling_error"), str), "sampling did not reach deadline")
    recording_start, recording_finish = timestamp_ns(manifest["started_at"]), timestamp_ns(manifest["finished_at"])
    require(recording_start <= recording_finish, "recording clock went backwards")
    successes, failures, disabled = [], [], []
    previous_finish = recording_start
    for index, row in enumerate(rows):
        require(row.get("source_kind") == "worker" and type(row.get("schema_version")) is int and row["schema_version"] == 1, "sample kind/schema")
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
            continue
        require(isinstance(state, dict) and type(state.get("processing_limit_enabled")) is bool and "processing" in state, "invalid state")
        if not state["processing_limit_enabled"]:
            require(state["processing"] is None, "disabled with counts")
            disabled.append((begin, end))
            continue
        p = state["processing"]
        require(isinstance(p, dict), "enabled without counts")
        limit, in_use, waiting = (p.get(k) for k in ("limit", "in_use", "waiting"))
        require(nonnegative_int(limit) and limit > 0 and nonnegative_int(in_use) and in_use <= limit and nonnegative_int(waiting), "invalid counts")
        successes.append(dict(start=begin, finish=end, limit=limit, in_use=in_use, waiting=waiting, duration=row["duration_ns"]))
    require(manifest["samples_written"] == len(rows) and manifest["successful_samples"] == len(successes)+len(disabled) and manifest["failed_samples"] == len(failures), "manifest/JSONL count mismatch")
    before = [r for r in successes if r["finish"] <= start]
    after = [r for r in successes if r["start"] >= finish]
    inside = [r for r in successes if r["start"] >= start and r["finish"] <= finish]
    bracket = [r for r in successes if before and after and before[-1]["finish"] <= r["finish"] <= after[0]["finish"]]
    gaps = [b["finish"]-a["finish"] for a, b in zip(bracket, bracket[1:])]
    idle = lambda r: r["in_use"] == 0 and r["waiting"] == 0
    zero_after = next((r for r in after if idle(r)), None)
    window_failures = sum(a <= finish and b >= start for a, b in failures)
    checks = dict(idle_before=bool(before) and idle(before[-1]), zero_after=zero_after is not None,
                  enabled_throughout=len(disabled) == 0,
                  state_matches_fixture=all(r["limit"] == 1 and r["in_use"]+r["waiting"] <= planned_sessions for r in successes),
                  window_has_no_failed_queries=window_failures == 0,
                  window_has_observations=bool(inside),
                  bracket_gap_within_budget=bool(gaps) and max(gaps) <= MAX_GAP_NS)
    busy = sum(r["in_use"] > 0 for r in inside)
    waiting = sum(r["waiting"] > 0 for r in inside)
    return dict(checks_passed=all(checks.values()), checks=checks, samples=len(rows),
                successful_samples=len(successes)+len(disabled), failed_samples=len(failures), disabled_samples=len(disabled),
                window_successful_samples=len(inside), window_failed_samples=window_failures,
                observed_peak_in_use=max((r["in_use"] for r in inside), default=None),
                observed_peak_waiting=max((r["waiting"] for r in inside), default=None),
                window_busy_samples=busy, window_waiting_samples=waiting,
                busy_sample_fraction=busy/len(inside) if inside else None,
                waiting_sample_fraction=waiting/len(inside) if inside else None,
                window_state_histogram={f"{u},{w}": sum(r["in_use"] == u and r["waiting"] == w for r in inside)
                                        for u, w in sorted({(r["in_use"], r["waiting"]) for r in inside})},
                max_bracketing_success_gap_ns=max(gaps) if gaps else None,
                first_post_exit_idle_sample_delay_ns=zero_after["finish"]-finish if zero_after else None,
                query_duration=distribution([r["duration"] for r in successes]))


def check_worker_sampling(directory, start, finish, n, endpoint):
    """读取一次已结束的 Worker 采样；不写入或修复原始数据。"""
    return check_worker_data(json.loads((directory/"manifest.json").read_text()),
                             read_complete_rows(directory/"samples.jsonl"), start, finish, n, endpoint)


def summarize(directory):
    """复算九批负载与双路采样，核对归档哈希，再写独立的派生文件。"""
    manifest = json.loads((directory/"manifest.json").read_text())
    require(manifest["status"] == "passed", "experiment did not pass; retain and inspect attempts")
    formal = [b for b in manifest["batches"] if b["label"] != "warmup"]
    require([b["label"] for b in formal] == [f"r{r}-n{n}" for r in (1, 2, 3) for n in (1, 2, 4)], "unexpected schedule")
    for name, expected in manifest["artifact_sha256"].items():
        path = (directory/name).resolve()
        require(path.is_relative_to(directory.resolve()), "artifact path escapes directory")
        require(digest(path) == expected, f"artifact changed: {name}")
    batches = []
    for b in formal:
        n = b["planned_sessions"]
        load = check_report(directory/(b["label"]+".json"), n, 160000)
        gateway = check_sampling(directory/(b["label"]+"-gateway"), b["started_at"], b["finished_at"], n)
        worker = check_worker_sampling(directory/(b["label"]+"-worker"), b["started_at"], b["finished_at"], n, manifest["worker_endpoint"])
        require(b["exit_code"] == 0 and all(b["sampling"][k]["exit_code"] == 0 for k in ("gateway", "worker"))
                and all(x["checks_passed"] for x in (load, gateway, worker)), f"batch failed: {b['label']}")
        batches.append(dict(label=b["label"], planned_sessions=n, load=load, gateway=gateway, worker=worker))
    groups = []
    for n in (1, 2, 4):
        selected = [b for b in batches if b["planned_sessions"] == n]
        sessions = [s for b in selected for s in b["load"]["sessions"]]
        g = dict(planned_sessions=n, completed=len(sessions), tail=distribution([s["tail_ns"] for s in sessions]),
                 max_audio_schedule_lag_ns=max(s["max_audio_schedule_lag_ns"] for s in sessions))
        for kind in ("gateway", "worker"):
            states = [b[kind] for b in selected]
            g[kind] = {key: sum(s[key] for s in states) for key in ("samples", "successful_samples", "failed_samples", "window_successful_samples")}
            g[kind]["max_success_gap_ns"] = max(s["max_bracketing_success_gap_ns"] for s in states)
            g[kind]["query_duration_max_ns"] = max(s["query_duration"]["max_ns"] for s in states)
        g["gateway"]["observed_peaks"] = [b["gateway"]["observed_peak_active"] for b in selected]
        for key in ("window_busy_samples", "window_waiting_samples", "disabled_samples"):
            g["worker"][key] = sum(b["worker"][key] for b in selected)
        for name in ("busy", "waiting"):
            g["worker"][name+"_sample_fraction"] = g["worker"]["window_"+name+"_samples"]/g["worker"]["window_successful_samples"]
        g["worker"]["observed_peak_waiting"] = max(b["worker"]["observed_peak_waiting"] for b in selected)
        g["worker"]["observed_peak_in_use"] = max(b["worker"]["observed_peak_in_use"] for b in selected)
        groups.append(g)
    result = dict(formal_batches=len(batches), completed_sessions=sum(g["completed"] for g in groups), groups=groups, batches=batches,
                  limits=["discrete query samples, not continuous peaks or time utilization", "waiting count is not waiting latency or audio backlog",
                          "same-host wall clocks; query intervals differ across samplers", "no stable-capacity or overhead A/B conclusion"])
    save(directory/"groups.json", result)
    save(directory/"analysis-manifest.json", dict(generated_at=now(), input_sha256={"manifest.json": digest(directory/"manifest.json"), **manifest["artifact_sha256"]},
         script_sha256={p.name: digest(p) for p in (Path(__file__), Path(__file__).with_name("summarize_gateway_observations.py"), Path(__file__).with_name("run_loadgen_baseline.py"))},
         groups_sha256=digest(directory/"groups.json")))
    print(json.dumps(dict(completed_sessions=result["completed_sessions"], groups=groups), ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
