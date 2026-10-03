#!/usr/bin/env python3
"""同版本异速 Worker 的三轮相邻策略对照；保留业务失败，拒绝条件漂移。"""
from collections import Counter
import argparse
import contextlib
import copy
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest, distribution, now, save
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize
from worker_comparison_contract import validate_manifest

POLICIES = ("round_robin", "weighted_round_robin")
ORDER = tuple(dict(pair=pair, policy=policy, directory=f"pair-{pair}-"+policy)
              for pair, policies in ((1, POLICIES), (2, POLICIES[::-1]), (3, POLICIES)) for policy in policies)
EXPERIMENT = "worker_strategy_comparison"


def validate_cohorts(order, manifests):
    """六组只允许策略与相应权重变化；动态端口不参与条件相等判断。"""
    require(order == list(ORDER) and len(manifests) == len(ORDER), "cohort order/count differs")
    reference = manifests[0]
    for item, m in zip(ORDER, manifests):
        validate_manifest(m)
        require(m["experiment"] == "dual_worker_strategy" and m["policy"] == item["policy"], "cohort policy differs")
        for key in ("source_commit", "source_sha256", "binary_sha256", "script_sha256", "worker_configs",
                    "gateway", "criteria", "sampling", "schedule", "go", "os", "cpu", "logical_cpus",
                    "memory_bytes", "environment", "race"):
            require(m[key] == reference[key], "cohort condition differs: "+key)


def aggregate(results, n):
    """合并同策略三批的原始会话重算分位数；不平均各批 p95，也不剔除失败。"""
    require(len(results) == 3, "need three policy repetitions")
    groups = [next(g for g in r["groups"] if g["planned_sessions"] == n) for r in results]
    batches = [next(b for b in r["batches"] if b["planned_sessions"] == n) for r in results]
    rows = [s for b in batches for s in b["load"]["sessions"]]
    require(len(rows) == 3*n, "session denominator differs")
    outcomes = Counter(s["outcome"] for s in rows)
    lags = [s["max_audio_schedule_lag_ns"] for s in rows if s["audio_schedule_samples"]]
    result = dict(planned_sessions=n, attempts=len(rows), outcomes=dict(outcomes),
                  completion_rate=outcomes["completed"]/len(rows),
                  batches_meeting_criteria=sum(b["load"]["criteria_met"] for b in batches),
                  all_repetitions_meet_criteria=all(b["load"]["criteria_met"] for b in batches),
                  tail_completed_only=distribution([s["tail_ns"] for s in rows if s["outcome"] == "completed"]),
                  failure_errors=dict(sorted(Counter(s["error"] for s in rows if s["outcome"] != "completed").items())),
                  audio_bytes_written=sum(s["audio_bytes_written"] for s in rows), planned_audio_bytes=len(rows)*640000,
                  max_audio_schedule_lag_ns=max(lags) if lags else None,
                  gateway_observed_peaks=[g["gateway_observed_peaks"][0] for g in groups])
    result["gateway"] = {k: sum(g["gateway"][k] for g in groups)
                         for k in ("samples", "failed_samples", "window_successful_samples")}
    result["gateway"]["max_success_gap_ns"] = max(g["gateway"]["max_success_gap_ns"] for g in groups)
    result["workers"] = {}
    for worker in ("worker1", "worker2"):
        values = [g["workers"][worker] for g in groups]
        totals = {k: sum(v[k] for v in values) for k in ("samples", "successful_samples", "failed_samples",
                  "window_successful_samples", "window_busy_samples", "window_waiting_samples")}
        totals.update(waiting_peaks=[v["waiting_peaks"][0] for v in values],
                      in_use_peaks=[v["in_use_peaks"][0] for v in values],
                      max_success_gap_ns=max(v["max_success_gap_ns"] for v in values))
        result["workers"][worker] = totals
    return result


def compare(directory):
    """校验原始哈希，在副本重算六组，再按策略与相邻轮次分别对照。"""
    m = json.loads((directory/"manifest.json").read_text())
    require(m.get("status") == "passed" and m.get("experiment") == EXPERIMENT, "incomplete strategy comparison")
    require(m.get("order") == list(ORDER), "cohort order differs")
    for name, expected in m["artifact_sha256"].items():
        path = (directory/name).resolve()
        require(path.is_relative_to(directory.resolve()) and digest(path) == expected, "artifact changed: "+name)
    results, manifests = [], []
    for item in ORDER:
        source = directory/item["directory"]
        require(item["directory"]+"/manifest.json" in m["artifact_sha256"], "unhashed child manifest")
        manifests.append(json.loads((source/"manifest.json").read_text()))
        with tempfile.TemporaryDirectory(prefix="tide-strategy-compare-") as temp:
            target = Path(temp)/"copy"
            shutil.copytree(source, target)
            with contextlib.redirect_stdout(io.StringIO()):
                result = summarize(target)
        require(result == json.loads((source/"groups.json").read_text()), "derived groups differ from evidence")
        results.append(result)
    validate_cohorts(m["order"], manifests)
    groups = []
    for n in (8, 10, 12):
        values = {policy: aggregate([r for r, item in zip(results, ORDER) if item["policy"] == policy], n) for policy in POLICIES}
        pairs = []
        for pair in (1, 2, 3):
            paired = {item["policy"]: next(g for g in r["groups"] if g["planned_sessions"] == n)
                      for r, item in zip(results, ORDER) if item["pair"] == pair}
            pairs.append(dict(pair=pair, **paired,
                         completion_rate_change_pp=(paired[POLICIES[1]]["completion_rate"]-paired[POLICIES[0]]["completion_rate"])*100))
        groups.append(dict(planned_sessions=n, **values, pairs=pairs,
                      completion_rate_change_pp=(values[POLICIES[1]]["completion_rate"]-values[POLICIES[0]]["completion_rate"])*100))
    result = dict(experiment=EXPERIMENT, groups=groups,
                  limits=["same-host 20s finite Mock cohorts; not real ASR, production or stable capacity",
                          "three adjacent pairs, RR/W then W/R then RR/W; order is not fully balanced or isolated",
                          "two warmup sessions and previous batches advance selector phase; no per-session assignment observations",
                          "weights change new selection proportions, not running-session migration or active ratios",
                          "completed-only tails have different populations when failures differ; no paired latency gain claim",
                          "asynchronous worker sample fractions are not CPU/time utilization; peaks cannot be summed"])
    save(directory/"comparison.json", result)
    save(directory/"analysis-manifest.json", dict(generated_at=now(),
         input_sha256={"manifest.json": digest(directory/"manifest.json"), **m["artifact_sha256"]},
         script_sha256={p.name: digest(p) for p in Path(__file__).parent.glob("*.py") if not p.name.startswith("test_")},
         comparison_sha256=digest(directory/"comparison.json")))
    for g in groups:
        print(json.dumps(dict(planned_sessions=g["planned_sessions"],
             completion_rate_change_pp=g["completion_rate_change_pp"],
             **{p: {k:g[p][k] for k in ("outcomes", "batches_meeting_criteria", "tail_completed_only")} for p in POLICIES}), ensure_ascii=False), flush=True)
    return result


def run_comparison(directory):
    """每组重启自有服务；业务失败继续保留，采集错误则保留整次尝试并退出。"""
    directory.mkdir(parents=True, exist_ok=False)
    manifest = dict(experiment=EXPERIMENT, status="running", started_at=now(), order=copy.deepcopy(list(ORDER)))
    save(directory/"manifest.json", manifest)
    try:
        for item in ORDER:
            child = directory/item["directory"]
            print("BEGIN", item["directory"], flush=True)
            run(child, pressure=True, profile=pressure_profile("dual_worker_strategy"), worker_count=2, policy=item["policy"])
            summarize(child)
        manifest["status"] = "passed"
    except BaseException as exc:
        manifest.update(status="error", error=repr(exc))
        raise
    finally:
        manifest["finished_at"] = now()
        manifest["artifact_sha256"] = {str(p.relative_to(directory)): digest(p) for p in directory.rglob("*") if p.is_file() and p != directory/"manifest.json"}
        save(directory/"manifest.json", manifest)
    compare(directory)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; existing directory only with --analyze-only")
    parser.add_argument("--analyze-only", action="store_true", help="recheck completed evidence without rerunning load")
    args = parser.parse_args()
    (compare if args.analyze_only else run_comparison)(args.output.resolve())
