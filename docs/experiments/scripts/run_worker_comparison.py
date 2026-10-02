#!/usr/bin/env python3
"""固定 8/9/10 场，先单 Worker 再双 Worker 各三批；完整保留证据与失败。"""
import argparse
import contextlib
import io
import json
from pathlib import Path
import shutil
import tempfile

from run_joint_observation_baseline import run
from run_loadgen_baseline import digest, now, save
from summarize_gateway_observations import require
from summarize_pressure_sweep import pressure_profile, summarize
from worker_comparison_contract import validate_pair

POINTS = Path(__file__).resolve().parent.parent/"results/boundary-study-operating-points-2026-10-02.json"


def compare(directory):
    """从双方原始副本重算，防止修改旧分析；成功率保留全分母，尾部分位数不作配对收益。"""
    m = json.loads((directory/"manifest.json").read_text())
    require(m.get("status") == "passed" and m.get("experiment") == "worker_count_comparison", "incomplete comparison")
    for name, expected in m["artifact_sha256"].items():
        path = (directory/name).resolve()
        require(path.is_relative_to(directory.resolve()) and digest(path) == expected, "artifact changed: "+name)
    results, manifests = [], []
    for count in (1, 2):
        source = directory/f"workers-{count}"
        require(f"workers-{count}/manifest.json" in m["artifact_sha256"], "unhashed child manifest")
        child = json.loads((source/"manifest.json").read_text())
        with tempfile.TemporaryDirectory(prefix="tide-worker-compare-") as temp:
            target = Path(temp)/"copy"
            shutil.copytree(source, target)
            with contextlib.redirect_stdout(io.StringIO()):
                result = summarize(target)
        require(result == json.loads((source/"groups.json").read_text()), "derived groups differ from evidence")
        results.append(result)
        manifests.append(child)
    points = json.loads((directory/"operating-points.json").read_text())
    require(digest(directory/"operating-points.json") == m["operating_points_sha256"], "operating points changed")
    validate_pair(*manifests, points)
    groups = []
    for a, b in zip(results[0]["groups"], results[1]["groups"]):
        require(a["planned_sessions"] == b["planned_sessions"], "unpaired workload")
        groups.append(dict(planned_sessions=a["planned_sessions"], single=a, double=b,
                           completion_rate_change_pp=(b["completion_rate"]-a["completion_rate"])*100))
    result = dict(experiment="worker_count_comparison", groups=groups,
                  limits=["20s fixed Mock finite cohorts on one host, not production or stable capacity",
                          "one processing slot per worker; total resources and sampler count increase",
                          "single cohort precedes double cohort; time-order and sampling overhead are not isolated",
                          "completed-only tail uses different successful populations when failures occur",
                          "per-worker asynchronous counts are not simultaneous totals or assignment counters",
                          "8/9/10 are single-worker reference points, not double-worker boundary estimates"])
    save(directory/"comparison.json", result)
    save(directory/"analysis-manifest.json", dict(generated_at=now(), input_sha256={"manifest.json": digest(directory/"manifest.json"), **m["artifact_sha256"]},
         script_sha256={p.name: digest(p) for p in Path(__file__).parent.glob("*.py") if not p.name.startswith("test_")},
         comparison_sha256=digest(directory/"comparison.json")))
    for group in groups:
        print(json.dumps({k: v for k, v in group.items() if k not in ("single", "double")}))
    return result


def run_comparison(directory):
    """每个组独立持有进程与目录；任一采集不合格就保留整体，不拼接好批次。"""
    directory.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(POINTS, directory/"operating-points.json")
    manifest = dict(experiment="worker_count_comparison", status="running", started_at=now(),
                    order=[1, 2], operating_points_sha256=digest(directory/"operating-points.json"))
    save(directory/"manifest.json", manifest)
    try:
        for count in (1, 2):
            child = directory/f"workers-{count}"
            print(f"BEGIN {count} worker(s)", flush=True)
            run(child, pressure=True, profile=pressure_profile("worker_comparison"), worker_count=count)
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
    parser.add_argument("--analyze-only", action="store_true", help="recheck completed raw evidence without rerunning load")
    args = parser.parse_args()
    (compare if args.analyze_only else run_comparison)(args.output.resolve())
