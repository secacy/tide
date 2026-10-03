#!/usr/bin/env python3
"""在 1/2/4 场负载中同时采集 Gateway 会话与 Worker 处理状态，保留全部尝试。"""
import argparse
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

from run_loadgen_baseline import ROOT, capture, check_report, digest, now, save
from summarize_gateway_observations import check_sampling, read_complete_rows
from summarize_joint_observations import check_worker_sampling, summarize


def wait_sampling_ready(process, directory, kind="gateway"):
    """等到完整且有效的零活动样本才启动负载；不以进程已创建代替就绪。"""
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("sampler exited before readiness")
        path = directory / "samples.jsonl"
        if path.exists():
            for row in read_complete_rows(path, live=True):
                state = row.get("state")
                if row.get("error") is None and state is not None:
                    expected = (dict(active_sessions=0, max_sessions=64, stopping=False) if kind == "gateway"
                                else dict(processing_limit_enabled=True, processing=dict(limit=1, in_use=0, waiting=0)))
                    if state != expected or (kind == "worker" and row.get("source_kind") != "worker"):
                        raise RuntimeError(f"{kind} not idle before load: {state}")
                    return row["index"]
        time.sleep(0.01)
    raise RuntimeError("sampler did not record an idle snapshot within 2s")


def check_gateway_port():
    """检查固定监听地址；允许已关闭连接的端口复用，不允许占用现有监听。"""
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", 8080))


def run(output, *, pressure=False, profile=None, selection=None, worker_count=1, policy="round_robin"):
    """每批先启动有限采样再运行负载；所有尝试独立保存，异常也回收自有进程。"""
    # 复用进程/采样/归档流程；不同实验的负载判据由各自分析器解释。
    from summarize_pressure_sweep import (AUDIO_BYTES, COUNTS, LAG_BUDGET_NS, SAMPLE_DURATION_S,
                                          SESSION_TIMEOUT_S, TAIL_BUDGET_NS,
                                          check_pressure_gateway, check_pressure_report, pressure_profile)
    if profile is not None:
        if not pressure or profile != pressure_profile(profile["experiment"], (selection or {}).get("near_boundary")):
            raise ValueError("invalid experiment profile")
    spec = profile or pressure_profile()
    comparison = pressure and spec["experiment"] in ("worker_comparison", "dual_worker_sweep", "dual_worker_boundary_short", "dual_worker_extended", "dual_worker_heterogeneous", "dual_worker_strategy", "dual_worker_weighted_extended")
    if type(worker_count) is not int or worker_count not in (1, 2) or (worker_count != 1 and not comparison):
        raise ValueError("multiple workers require a multi-worker profile")
    if spec["experiment"].startswith("dual_worker_") and worker_count != 2:
        raise ValueError("dual-worker experiments require exactly two workers")
    strategy = pressure and spec["experiment"] in ("dual_worker_strategy", "dual_worker_weighted_extended")
    if policy not in ("round_robin", "weighted_round_robin") or (policy != "round_robin" and not strategy):
        raise ValueError("policy requires a strategy-comparison profile")
    if spec["experiment"] == "dual_worker_weighted_extended" and policy != "weighted_round_robin":
        raise ValueError("weighted extended observation requires weighted policy")
    from worker_comparison_contract import worker_configs
    configs = worker_configs(spec["experiment"], worker_count)
    heterogeneous = pressure and spec["experiment"] in ("dual_worker_heterogeneous", "dual_worker_strategy", "dual_worker_weighted_extended")
    counts = spec["counts"] if pressure else (1, 2, 4)
    repetitions = spec["repetitions"] if pressure else 3
    audio_bytes = spec["audio_bytes"] if pressure else 160000
    timeout_s = spec["timeout_s"] if pressure else 20
    duration_s = spec["duration_s"] if pressure else 10
    read_load = (lambda path, n, size: check_pressure_report(path, n, size, timeout_s)) if pressure else check_report
    read_gateway = check_pressure_gateway if pressure else check_sampling
    output.mkdir(parents=True, exist_ok=False)
    manifest = dict(started_at=now(), status="running", source_commit=capture(["git", "rev-parse", "HEAD"]),
                    tracked_diff=capture(["git", "diff", "HEAD", "--", "cmd", "internal", "proto", "go.mod", "go.sum"]),
                    go=capture(["go", "version"]), os=capture(["sw_vers"]),
                    cpu=capture(["sysctl", "-n", "machdep.cpu.brand_string"]),
                    logical_cpus=capture(["sysctl", "-n", "hw.logicalcpu"]),
                    memory_bytes=capture(["sysctl", "-n", "hw.memsize"]),
                    environment={k: os.environ.get(k) for k in ("GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOFLAGS")},
                    race=False, commands=[], batches=[], shutdown=[],
                    worker=dict(processing_concurrency=1, processing_delay_ns=10_000_000, response_delay_ns=5_000_000,
                                partial_every_ns=500_000_000, partial_texts=["今", "今天", "今天天气", "今天天气不错"], final_text="今天天气不错"),
                    gateway=dict(max_sessions=64, max_message_bytes=1048576, max_pending_audio_bytes=32000,
                                 start_timeout_s=10, input_idle_timeout_s=30, worker_send_timeout_s=2, tail_timeout_s=15, result_write_timeout_s=2),
                    schedule=dict(session_counts=list(counts), repetitions=repetitions, audio_bytes=audio_bytes, chunk_bytes=3200,
                                  realtime=True, session_timeout_s=timeout_s, warmup_sessions=worker_count, warmup_audio_bytes=64000),
                    limits=["same-host independent processes, no isolation", "no waiting-duration, backlog, CPU or memory sampling", "sample fractions are not time utilization",
                            "client write success is not a server byte counter", "finite batches are not stable capacity",
                            "sampling overhead has no A/B measurement", "Mock emits only four partials then a final"])
    manifest["experiment"] = spec["experiment"] if pressure else "joint_baseline"
    if comparison:
        manifest.update(worker_count=worker_count, policy=policy)
    if strategy:
        manifest["weights"] = [2, 1] if policy == "weighted_round_robin" else None
    if heterogeneous:
        manifest.update(worker=None, worker_configs=configs)
    if selection is not None:
        manifest["selection"] = selection
    if pressure:
        manifest["criteria"] = dict(all_completed=True, tail_p95_ns=TAIL_BUDGET_NS, max_audio_schedule_lag_ns=LAG_BUDGET_NS)
    sources = capture(["git", "ls-files", "cmd", "internal", "proto", "go.mod", "go.sum"])
    manifest["source_sha256"] = {p: digest(ROOT/p) for p in sources.splitlines() if p.endswith((".go", ".proto")) or p in ("go.mod", "go.sum")}
    manifest["sampling"] = dict(gateway_interval_ns=100_000_000, worker_interval_ns=20_000_000, request_timeout_ns=1_000_000_000, duration_ns=duration_s*1_000_000_000, readiness_timeout_s=2, max_bracketing_success_gap_ns=500_000_000)
    manifest["script_sha256"] = {name: digest(Path(__file__).with_name(name)) for name in ("run_joint_observation_baseline.py", "summarize_joint_observations.py", "summarize_gateway_observations.py", "run_loadgen_baseline.py", "summarize_pressure_sweep.py", "run_pressure_sweep.py", "run_boundary_study.py", "run_worker_comparison.py", "worker_comparison_contract.py", "run_dual_worker_sweep.py", "run_dual_worker_boundary.py", "run_dual_worker_extended.py", "run_heterogeneous_workers.py", "run_strategy_comparison.py", "run_weighted_extended.py")}
    processes, handles, results, samplers = [], [], [], []
    save(output/"manifest.json", manifest)
    try:
        if spec["experiment"] == "dual_worker_weighted_extended":
            from run_weighted_extended import validate_extended_conditions
            validate_extended_conditions(manifest)
        if spec["experiment"] in ("dual_worker_boundary_short", "dual_worker_extended") and manifest["source_sha256"] != (selection or {}).get("source_sha256"):
            raise ValueError("Go source differs from reference sweep")
        # 当前 Gateway 固定监听 8080；占用则中止，不使用或停止已有服务。
        check_gateway_port()
        with tempfile.TemporaryDirectory(prefix="tide-load-baseline-") as temp:
            binaries = {}
            for name in ("asr-worker", "gateway", "loadgen", "gateway-sampler", "worker-sampler"):
                target = str(Path(temp)/name)
                command = ["go", "build", "-o", target, "./cmd/"+name]
                manifest["commands"].append(command)
                with (output/("build-"+name+".log")).open("w") as log:
                    subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
                binaries[name] = target
            manifest["binary_sha256"] = {name: digest(Path(path)) for name, path in binaries.items()}

            def start(name, args, label=None):
                label = label or name
                log = (output/(label+".log")).open("w")
                handles.append(log)
                command = [binaries[name], *args]
                manifest["commands"].append(command)
                p = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                processes.append((label, p))
                return p

            workers = []
            for index in range(worker_count):
                label = "asr-worker" if not comparison else f"asr-worker-{index+1}"
                delay_ms = configs[index]["processing_delay_ns"]//1_000_000
                worker = start("asr-worker", ["-listen=127.0.0.1:0", "-debug-listen=127.0.0.1:0", "-processing-concurrency=1", f"-processing-delay={delay_ms}ms", "-response-delay=5ms"], label)
                deadline = time.monotonic()+10
                address = None
                while time.monotonic() < deadline:
                    logs = (output/(label+".log")).read_text()
                    match = re.search(r"address=(127\.0\.0\.1:\d+)", logs)
                    debug_match = re.search(r"debug_address=(127\.0\.0\.1:\d+)", logs)
                    if match and debug_match:
                        address, debug_address = match[1], debug_match[1]
                        break
                    if worker.poll() is not None: raise RuntimeError("Worker exited before readiness")
                    time.sleep(0.05)
                if not address: raise RuntimeError("Worker did not announce address")
                workers.append(dict(id=f"worker{index+1}" if comparison else "worker", address=address,
                                    endpoint="http://"+debug_address+"/debug/worker"))
                if heterogeneous:
                    workers[-1]["config"] = configs[index]
            if comparison:
                manifest["workers"] = workers
            addresses = ",".join(w["address"] for w in workers)
            gateway_args = ["-workers="+addresses, "-max-pending-audio-bytes=32000"]
            if strategy:
                gateway_args.append("-worker-strategy="+policy)
                if policy == "weighted_round_robin":
                    gateway_args.append("-worker-weights=2,1")
            gateway = start("gateway", gateway_args)
            deadline = time.monotonic()+10
            while True:
                if gateway.poll() is not None: raise RuntimeError("Gateway exited before readiness")
                if "websocket gateway listening on :8080" in (output/"gateway.log").read_text():
                    with urllib.request.urlopen("http://127.0.0.1:8080/healthz", timeout=2) as response:
                        if response.status == 200: break
                if time.monotonic() >= deadline: raise RuntimeError("Gateway readiness timeout")
                time.sleep(0.05)
            if not comparison:
                manifest["worker_address"] = workers[0]["address"]
                manifest["worker_endpoint"] = workers[0]["endpoint"]
            schedule = [("warmup", worker_count, 64000)] + [(f"r{rep}-n{n}", n, audio_bytes) for rep in range(1, repetitions+1) for n in counts]
            for label, n, size in schedule:
                if any(p.poll() is not None for _, p in processes): raise RuntimeError("backend exited between batches")
                path = output/(label+".json")
                command = [binaries["loadgen"], "-url=ws://127.0.0.1:8080/v1/asr", f"-sessions={n}",
                           f"-audio-bytes={size}", "-chunk-bytes=3200", "-realtime=true", f"-session-timeout={timeout_s}s",
                           "-expected-final-text=今天天气不错", "-output="+str(path)]
                record = dict(label=label, planned_sessions=n, command=command, sampling={})
                manifest["batches"].append(record)
                manifest["commands"].append(command)
                active_samplers = []
                try:
                    # 每个后端独立采样；全部观测到空闲后才放行同批负载。
                    targets = [("gateway", "http://127.0.0.1:8080/debug/gateway", "100ms")] + [(w["id"], w["endpoint"], "20ms") for w in workers]
                    for key, url, interval in targets:
                        kind = "gateway" if key == "gateway" else "worker"
                        sample_name = label + "-" + key
                        sample_dir = output/sample_name
                        sample_command = [binaries[kind+"-sampler"], "-url="+url, "-interval="+interval,
                                          "-request-timeout=1s", f"-duration={10 if label == 'warmup' else duration_s}s", "-output-dir="+str(sample_dir)]
                        item = dict(directory=sample_name, started_at=now(), command=sample_command)
                        record["sampling"][key] = item
                        manifest["commands"].append(sample_command)
                        save(output/"manifest.json", manifest)
                        sample_log = (output/(sample_name+".log")).open("w")
                        handles.append(sample_log)
                        sampler = subprocess.Popen(sample_command, cwd=ROOT, stdout=sample_log, stderr=subprocess.STDOUT)
                        samplers.append(sampler)
                        active_samplers.append((kind, sampler, sample_log, sample_dir, item))
                    for kind, sampler, _, sample_dir, item in active_samplers:
                        item["ready_sample_index"] = wait_sampling_ready(sampler, sample_dir, kind)
                        item["ready_observed_at"] = now()
                    record["started_at"] = now()
                    save(output/"manifest.json", manifest)
                    with (output/(label+".log")).open("w") as log:
                        attempt = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=timeout_s+10)
                    record.update(finished_at=now(), exit_code=attempt.returncode)
                    # 正常运行等待预算结束，不用 SIGTERM 伪装完成。
                    for _, sampler, sample_log, _, item in active_samplers:
                        item.update(exit_code=sampler.wait(timeout=duration_s+5), finished_at=now())
                        sample_log.close()
                finally:
                    for _, sampler, _, _, item in active_samplers:
                        if sampler.poll() is None:
                            sampler.send_signal(signal.SIGTERM)
                            try: sampler.wait(timeout=3)
                            except subprocess.TimeoutExpired: sampler.kill(); sampler.wait()
                            item["forced_cleanup"] = True
                        item.setdefault("exit_code", sampler.returncode)
                        item.setdefault("finished_at", now())
                    save(output/"manifest.json", manifest)
                checked = read_load(path, n, size)
                gateway_observed = read_gateway(output/record["sampling"]["gateway"]["directory"], record["started_at"], record["finished_at"], n)
                worker_observed = {w["id"]: check_worker_sampling(output/record["sampling"][w["id"]]["directory"], record["started_at"], record["finished_at"], n, w["endpoint"]) for w in workers}
                record["checks_passed"] = (checked["checks_passed"] and attempt.returncode == checked.get("expected_exit_code", 0)
                    and all(item["exit_code"] == 0 and not item.get("forced_cleanup", False) for item in record["sampling"].values())
                    and gateway_observed["checks_passed"] and all(w["checks_passed"] for w in worker_observed.values()))
                observed = dict(gateway=gateway_observed)
                if comparison:
                    record.update(gateway_checks=gateway_observed, workers_checks=worker_observed)
                    observed["workers"] = worker_observed
                else:
                    record.update(gateway_checks=gateway_observed, worker_checks=worker_observed["worker"])
                    observed["worker"] = worker_observed["worker"]
                if pressure:
                    record["criteria_met"] = checked["criteria_met"]
                    record["criteria"] = checked["criteria"]
                if label != "warmup": results.append(dict(label=label, planned_sessions=n, **observed, **checked))
                save(output/"summary.json", dict(batches=results))
                save(output/"manifest.json", manifest)
                print(label, "exit", attempt.returncode, "evidence", record["checks_passed"], "criteria", record.get("criteria_met", "baseline"), flush=True)
                if label == "warmup" and (not record["checks_passed"] or not record.get("criteria_met", True)): raise RuntimeError("warmup failed; formal measurement not started")
            manifest["status"] = "passed" if all(r["checks_passed"] for r in manifest["batches"]) else "failed_checks"
            # 先停 Gateway，等待其正常清理，再停具有双服务收尾的 Worker。
            for name, p in reversed(processes):
                p.send_signal(signal.SIGTERM)
                try: code = p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill(); code = p.wait(); manifest["status"] = "shutdown_timeout"
                manifest["shutdown"].append(dict(process=name, exit_code=code))
            if any(item["exit_code"] != 0 for item in manifest["shutdown"]): manifest["status"] = "backend_shutdown_failed"
    except BaseException as exc:
        manifest.update(status="error", error=repr(exc))
        raise
    finally:
        for p in samplers:
            if p.poll() is None: p.kill(); p.wait()
        for _, p in reversed(processes):
            if p.poll() is None: p.kill(); p.wait()
        for log in handles: log.close()
        manifest["finished_at"] = now()
        manifest["artifact_sha256"] = {str(p.relative_to(output)): digest(p) for p in output.rglob("*") if p.is_file() and p != output/"manifest.json"}
        save(output/"manifest.json", manifest)
    if manifest["status"] != "passed": raise SystemExit("baseline checks failed; all evidence retained")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new directory; preserve every attempt")
    directory = parser.parse_args().output.resolve()
    run(directory)
    summarize(directory)
