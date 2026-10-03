#!/usr/bin/env python3
"""有限批次加压的证据检查与业务判据分开；失败会话始终保留。"""
from collections import Counter
import json
from pathlib import Path
import sys

from run_loadgen_baseline import digest, distribution, now, read_schedule_metrics, save
from summarize_gateway_observations import check_sampling, nonnegative_int, read_complete_rows, require, timestamp_ns
from summarize_joint_observations import check_worker_sampling

COUNTS = (4, 8, 12)
AUDIO_BYTES = 640000
SESSION_TIMEOUT_S = 35
SAMPLE_DURATION_S = 40
TAIL_BUDGET_NS = 250_000_000
LAG_BUDGET_NS = 100_000_000


def pressure_profile(experiment="pressure_sweep", near_boundary=None):
    """仅允许预定义实验，避免分析时按结果放宽时长、档位或重复次数。"""
    if experiment == "pressure_sweep":
        return dict(experiment=experiment, counts=(4, 8, 12), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "dual_worker_weighted_overload":
        return dict(experiment=experiment, counts=(16,), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "dual_worker_weighted_extended":
        return dict(experiment=experiment, counts=(12,), repetitions=2, audio_bytes=3840000, timeout_s=140, duration_s=150)
    if experiment == "dual_worker_strategy":
        return dict(experiment=experiment, counts=(8, 10, 12), repetitions=1, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "dual_worker_heterogeneous":
        return dict(experiment=experiment, counts=(8, 10, 12), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "dual_worker_extended":
        return dict(experiment=experiment, counts=(16, 18), repetitions=2, audio_bytes=3840000, timeout_s=140, duration_s=150)
    if experiment == "dual_worker_boundary_short":
        return dict(experiment=experiment, counts=(19,), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "dual_worker_sweep":
        return dict(experiment=experiment, counts=(16, 18, 20), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "worker_comparison":
        return dict(experiment=experiment, counts=(8, 9, 10), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    if experiment == "boundary_short":
        return dict(experiment=experiment, counts=(9, 10, 11), repetitions=3, audio_bytes=640000, timeout_s=35, duration_s=40)
    require(experiment == "boundary_extended" and type(near_boundary) is int and 9 <= near_boundary <= 11, "unknown pressure profile")
    return dict(experiment=experiment, counts=(8, near_boundary), repetitions=2, audio_bytes=3840000, timeout_s=140, duration_s=150)


def check_pressure_data(doc, sessions, audio_bytes, timeout_s=SESSION_TIMEOUT_S):
    """严格复算 v2 报告；合法 failed/canceled/timed_out 不视为证据损坏。"""
    require(type(doc.get("schema_version")) is int and doc["schema_version"] == 2, "pressure requires v2 report")
    require(doc.get("tail_percentile_method") == "nearest_rank", "percentile method")
    cfg = dict(sessions=sessions, session=dict(url="ws://127.0.0.1:8080/v1/asr", audio_bytes=audio_bytes,
               chunk_bytes=3200, realtime=True, timeout_ns=timeout_s*1_000_000_000, expected_final_text="今天天气不错"))
    require(doc.get("config") == cfg, "load config differs")
    require(isinstance(doc.get("sessions"), list) and len(doc["sessions"]) == sessions, "missing session attempts")
    require("batch_error" in doc and (doc["batch_error"] is None or isinstance(doc["batch_error"], str)), "invalid batch error")
    begin, end = timestamp_ns(doc["started_at"]), timestamp_ns(doc["finished_at"])
    require(begin <= end, "batch clock order")
    rows, counts, errors, tails = [], Counter(), Counter(), []
    for i, s in enumerate(doc["sessions"]):
        require(type(s.get("index")) is int and s["index"] == i, "session index gap")
        a, b = timestamp_ns(s["started_at"]), timestamp_ns(s["finished_at"])
        require(begin <= a <= b <= end, "session clock order")
        outcome = s["outcome"]
        require(outcome in ("completed", "failed", "canceled", "timed_out"), "unknown outcome")
        counts[outcome] += 1
        require("error" in s and "tail_latency_ns" in s, "missing error/tail")
        tail, error, o = s["tail_latency_ns"], s["error"], s["observation"]
        keys = ("audio_bytes_written", "audio_chunks_written", "write_failures", "max_audio_write_duration_ns", "result_count", "final_result_count")
        require(all(nonnegative_int(o.get(k)) for k in keys), "invalid observation count")
        written, chunks = o["audio_bytes_written"], o["audio_chunks_written"]
        require(written <= audio_bytes and written == chunks*3200 and o["final_result_count"] <= o["result_count"], "invalid audio/result counts")
        schedule = read_schedule_metrics(2, o)
        require(chunks <= schedule["audio_schedule_samples"] <= audio_bytes//3200, "invalid attempted audio count")
        require(schedule["audio_schedule_samples"]-chunks <= o["write_failures"], "missing failed write count")
        for first, last, count in (("first_audio_started_at", "last_audio_finished_at", chunks), ("first_result_at", "last_result_at", o["result_count"])):
            require(first in o and last in o, "missing observation clocks")
            if count:
                require(o[first] is not None and o[last] is not None, "missing observed times")
                require(a <= timestamp_ns(o[first]) <= timestamp_ns(o[last]) <= b, "observation clock order")
            else:
                require(o[first] is None and o[last] is None, "zero count with observed times")
        require("last_final_at" in o, "missing final time")
        if o["final_result_count"]:
            require(o["last_final_at"] is not None and timestamp_ns(o["first_result_at"]) <= timestamp_ns(o["last_final_at"]) <= timestamp_ns(o["last_result_at"]), "final time order")
        else:
            require(o["last_final_at"] is None, "zero final count with timestamp")
        for kind in ("start", "end"):
            require(kind+"_write" in o, "missing control event")
            event = o[kind+"_write"]
            if event is not None:
                require(event.get("kind") == kind and type(event.get("audio_bytes")) is int and event["audio_bytes"] == 0, "control kind/bytes")
                require("error" in event and (event["error"] is None or isinstance(event["error"], str)), "control error")
                require(a <= timestamp_ns(event["started_at"]) <= timestamp_ns(event["finished_at"]) <= b, "control clock order")
        if outcome == "completed":
            require(error is None and nonnegative_int(tail), "completed error/tail")
            require(written == audio_bytes and o["write_failures"] == 0 and schedule["audio_schedule_samples"] == audio_bytes//3200, "incomplete completed audio")
            require(o["result_count"] == 5 and o["final_result_count"] == 1, "completed result count")
            require(all(o[k+"_write"] is not None and o[k+"_write"]["error"] is None for k in ("start", "end")), "completed control writes")
            require(timestamp_ns(o["first_result_at"]) < timestamp_ns(o["end_write"]["started_at"]) <= timestamp_ns(o["last_result_at"]), "completed result order")
            tails.append(tail)
        else:
            require(isinstance(error, str) and tail is None, "failed outcome error/tail")
            errors[error] += 1
        rows.append(dict(index=i, outcome=outcome, error=error, tail_ns=tail, audio_bytes_written=written,
                         audio_chunks_written=chunks, elapsed_wall_ns=b-a, **schedule))
    summary = doc["summary"]
    expected = dict(planned_sessions=sessions, **{k: counts[k] for k in ("completed", "failed", "canceled", "timed_out")},
                    planned_audio_bytes=sessions*audio_bytes, audio_bytes_written=sum(s["audio_bytes_written"] for s in rows))
    require(all(type(summary.get(k)) is int and summary[k] == v for k, v in expected.items()), "summary count mismatch")
    rate = counts["completed"]/sessions
    require(type(summary.get("completion_rate")) in (int, float) and summary["completion_rate"] == rate, "summary completion rate")
    require(nonnegative_int(summary.get("elapsed_ns")), "invalid elapsed")
    tail_summary = distribution(tails)
    require(summary.get("tail", "missing") == tail_summary, "summary tail mismatch")
    lags = [s["max_audio_schedule_lag_ns"] for s in rows if s["audio_schedule_samples"] > 0]
    max_lag = max(lags) if lags else None
    criteria = dict(all_completed=counts["completed"] == sessions and doc["batch_error"] is None,
                    tail_p95_within_budget=tail_summary is not None and tail_summary["p95_ns"] <= TAIL_BUDGET_NS,
                    sending_within_budget=len(lags) == sessions and max_lag <= LAG_BUDGET_NS)
    return dict(checks_passed=True, criteria_met=all(criteria.values()), criteria=criteria, summary=summary,
                batch_error=doc["batch_error"], sessions=rows, failure_errors=dict(sorted(errors.items())),
                audio_delivery_fraction=expected["audio_bytes_written"]/(sessions*audio_bytes),
                sessions_with_schedule_samples=len(lags), max_audio_schedule_lag_ns=max_lag,
                expected_exit_code=0 if counts["completed"] == sessions and doc["batch_error"] is None else 1)


def check_pressure_report(path, sessions, audio_bytes, timeout_s=SESSION_TIMEOUT_S):
    return check_pressure_data(json.loads(path.read_text()), sessions, audio_bytes, timeout_s)


def check_pressure_gateway(directory, start, finish, n):
    """过载后提前失败可能使峰值未被采到；保留该事实，但不混同采样证据无效。"""
    result = check_sampling(directory, start, finish, n)
    result["observed_planned_peak"] = result["checks"].pop("observed_planned_peak")
    result["checks_passed"] = all(result["checks"].values())
    return result


def phase_observations(directory, batch, audio_seconds=20, worker_ids=("worker",)):
    """按负载启动后五秒分段，保留活动会话变化，避免把失败后空闲视为持续承载。"""
    start, finish = timestamp_ns(batch["started_at"]), timestamp_ns(batch["finished_at"])
    data = {k: read_complete_rows(directory/(batch["label"]+"-"+k)/"samples.jsonl") for k in ("gateway", *worker_ids)}
    phases = []
    for second in range(0, audio_seconds, 5):
        left, right = start+second*1_000_000_000, min(finish, start+(second+5)*1_000_000_000)
        phase = dict(offset_s=second, covered_wall_ns=max(0, right-left))
        for kind, rows in data.items():
            valid = [r["state"] for r in rows if r["error"] is None and left <= timestamp_ns(r["started_at"]) < right
                     and timestamp_ns(r["started_at"]) <= timestamp_ns(r["finished_at"]) <= right]
            if kind == "gateway":
                phase[kind] = dict(samples=len(valid), active_histogram=dict(sorted(Counter(str(r["active_sessions"]) for r in valid).items())))
            else:
                values = [r["processing"] for r in valid if r["processing_limit_enabled"]]
                phase[kind] = dict(samples=len(values), busy_samples=sum(v["in_use"] > 0 for v in values), waiting_samples=sum(v["waiting"] > 0 for v in values),
                                   observed_peak_waiting=max((v["waiting"] for v in values), default=None))
        phases.append(phase)
    return phases


def summarize(directory):
    """采集通过不等于负载达标；全失败批次仍进入分组与总体分母。"""
    m = json.loads((directory/"manifest.json").read_text())
    require(m["status"] == "passed", "invalid/incomplete acquisition")
    profile = pressure_profile(m.get("experiment"), m.get("selection", {}).get("near_boundary"))
    counts, repetitions, audio_bytes, timeout_s = (profile[k] for k in ("counts", "repetitions", "audio_bytes", "timeout_s"))
    if profile["experiment"] == "boundary_extended":
        from run_boundary_study import choose_points
        decision = choose_points(m["selection"]["source_groups"])
        require(all(m["selection"][k] == v for k, v in decision.items()), "selection differs from predefined rule")
    if profile["experiment"] == "dual_worker_boundary_short":
        from run_dual_worker_boundary import decision_from_groups
        selection = m.get("selection", {})
        decision = decision_from_groups(selection.get("source_groups"))
        require(all(selection.get(k) == v for k, v in decision.items()), "refinement selection differs")
        require(m["source_sha256"] == selection.get("source_sha256"), "Go source differs from reference sweep")
    if profile["experiment"] == "dual_worker_extended":
        from run_dual_worker_extended import validate_selection
        validate_selection(m.get("selection", {}))
        require(m["source_sha256"] == m["selection"].get("source_sha256"), "Go source differs from short reference")
    if profile["experiment"] == "dual_worker_weighted_extended":
        from run_weighted_extended import validate_extended_conditions
        validate_extended_conditions(m)
    if profile["experiment"] == "dual_worker_weighted_overload":
        from run_weighted_overload import validate_overload_conditions
        validate_overload_conditions(m)
    require(m["criteria"] == dict(all_completed=True, tail_p95_ns=TAIL_BUDGET_NS, max_audio_schedule_lag_ns=LAG_BUDGET_NS), "criteria differ")
    require(m["schedule"]["session_counts"] == list(counts) and m["schedule"]["repetitions"] == repetitions and m["schedule"]["audio_bytes"] == audio_bytes and m["schedule"]["session_timeout_s"] == timeout_s, "schedule differs")
    for name, expected in m["artifact_sha256"].items():
        p = (directory/name).resolve()
        require(p.is_relative_to(directory.resolve()) and digest(p) == expected, f"artifact changed: {name}")
    formal = [b for b in m["batches"] if b["label"] != "warmup" and not b["label"].startswith("recovery-")]
    require([b["label"] for b in formal] == [f"r{r}-n{n}" for r in range(1, repetitions+1) for n in counts], "schedule incomplete")
    comparison = profile["experiment"] in ("worker_comparison", "dual_worker_sweep", "dual_worker_boundary_short", "dual_worker_extended", "dual_worker_heterogeneous", "dual_worker_strategy", "dual_worker_weighted_extended", "dual_worker_weighted_overload")
    if comparison:
        from worker_comparison_contract import validate_manifest
        validate_manifest(m)
        if profile["experiment"] in ("dual_worker_heterogeneous", "dual_worker_strategy", "dual_worker_weighted_extended", "dual_worker_weighted_overload"):
            from worker_comparison_contract import validate_startup_logs
            validate_startup_logs(directory, m)
    workers = m["workers"] if comparison else [dict(id="worker", endpoint=m["worker_endpoint"])]
    batches, recovery_checks = [], []
    # 新实验也重新检查预热，不只信任根清单中的通过标志。
    for b in m["batches"] if comparison else formal:
        n = b["planned_sessions"]
        recovery = b["label"].startswith("recovery-")
        size = 64000 if b["label"] == "warmup" or recovery else audio_bytes
        load = check_pressure_report(directory/(b["label"]+".json"), n, size, 5 if recovery else timeout_s)
        gateway = check_pressure_gateway(directory/(b["label"]+"-gateway"), b["started_at"], b["finished_at"], n)
        observed = {w["id"]: check_worker_sampling(directory/(b["label"]+"-"+w["id"]), b["started_at"], b["finished_at"], n, w["endpoint"]) for w in workers}
        keys = ["gateway", *observed]
        require(set(b["sampling"]) == set(keys), "missing/extra sampler")
        require(b["exit_code"] == load["expected_exit_code"] and all(b["sampling"][k]["exit_code"] == 0 and not b["sampling"][k].get("forced_cleanup", False) for k in keys)
                and gateway["checks_passed"] and all(w["checks_passed"] for w in observed.values()), "invalid acquisition: "+b["label"])
        if b["label"] == "warmup":
            require(load["criteria_met"], "warmup criteria failed")
            continue
        item = dict(label=b["label"], planned_sessions=n, load=load, gateway=gateway,
                    phases=phase_observations(directory, b, size//32000, tuple(observed)))
        item.update(workers=observed) if comparison else item.update(worker=observed["worker"])
        (recovery_checks if recovery else batches).append(item)
    groups = []
    for n in profile["counts"]:
        chosen = [b for b in batches if b["planned_sessions"] == n]
        rows = [s for b in chosen for s in b["load"]["sessions"]]
        counts = Counter(s["outcome"] for s in rows)
        errors = Counter(s["error"] for s in rows if s["outcome"] != "completed")
        lags = [s["max_audio_schedule_lag_ns"] for s in rows if s["audio_schedule_samples"]]
        g = dict(planned_sessions=n, attempts=len(rows), outcomes=dict(counts), completion_rate=counts["completed"]/len(rows),
                 batches_meeting_criteria=sum(b["load"]["criteria_met"] for b in chosen),
                 all_repetitions_meet_criteria=all(b["load"]["criteria_met"] for b in chosen),
                 tail_completed_only=distribution([s["tail_ns"] for s in rows if s["outcome"] == "completed"]),
                 audio_bytes_written=sum(s["audio_bytes_written"] for s in rows), planned_audio_bytes=len(rows)*audio_bytes,
                 failure_errors=dict(sorted(errors.items())), max_audio_schedule_lag_ns=max(lags) if lags else None,
                 gateway_observed_peaks=[b["gateway"]["observed_peak_active"] for b in chosen])
        if not comparison:
            g["worker_waiting_peaks"] = [b["worker"]["observed_peak_waiting"] for b in chosen]
        for kind in (("gateway",) if comparison else ("gateway", "worker")):
            g[kind] = {key: sum(b[kind][key] for b in chosen) for key in ("samples", "failed_samples", "window_successful_samples")}
            g[kind]["max_success_gap_ns"] = max(b[kind]["max_bracketing_success_gap_ns"] for b in chosen)
        if comparison:
            g["workers"] = {}
            for w in workers:
                states = [b["workers"][w["id"]] for b in chosen]
                totals = {k: sum(s[k] for s in states) for k in ("samples", "successful_samples", "failed_samples", "window_successful_samples", "window_busy_samples", "window_waiting_samples")}
                totals.update(waiting_peaks=[s["observed_peak_waiting"] for s in states],
                              in_use_peaks=[s["observed_peak_in_use"] for s in states],
                              max_success_gap_ns=max(s["max_bracketing_success_gap_ns"] for s in states))
                g["workers"][w["id"]] = totals
        groups.append(g)
    result = dict(groups=groups, batches=batches,
                  limits=[f"{audio_bytes//32000}s finite cohorts, not long-running stable capacity", "success-only tail percentiles must accompany failure counts",
                          "audio write success is not server processing acknowledgement", "discrete waiting counts not queue latency or utilization"])
    if comparison:
        result.update(worker_count=m["worker_count"], policy=m["policy"])
        result["limits"].append("per-worker peaks are asynchronous and must not be summed as a simultaneous peak")
    if profile["experiment"] in ("dual_worker_heterogeneous", "dual_worker_strategy", "dual_worker_weighted_extended", "dual_worker_weighted_overload"):
        result["worker_configs"] = m["worker_configs"]
    if profile["experiment"] in ("dual_worker_strategy", "dual_worker_weighted_extended", "dual_worker_weighted_overload"):
        result["weights"] = m["weights"]
    if profile["experiment"] == "dual_worker_weighted_overload":
        result.update(recovery_checks=recovery_checks,
                      all_recovery_meet_criteria=all(b["load"]["criteria_met"] for b in recovery_checks))
        result["limits"].append("two-session probes check new-session availability, not reconnect or failed-audio recovery")
    save(directory/"groups.json", result)
    scripts = ("summarize_pressure_sweep.py", "summarize_joint_observations.py", "summarize_gateway_observations.py", "run_loadgen_baseline.py", "run_boundary_study.py", "worker_comparison_contract.py", "run_dual_worker_boundary.py", "run_dual_worker_extended.py", "run_weighted_extended.py", "run_weighted_overload.py")
    save(directory/"analysis-manifest.json", dict(generated_at=now(), input_sha256={"manifest.json": digest(directory/"manifest.json"), **m["artifact_sha256"]},
         script_sha256={s: digest(Path(__file__).with_name(s)) for s in scripts}, groups_sha256=digest(directory/"groups.json")))
    print(json.dumps(groups, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
