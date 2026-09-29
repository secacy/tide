#!/usr/bin/env python3
"""运行单 Worker、1/2/4 场有限负载基线，保留所有尝试和环境，不修改业务代码。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[3]


def now():
    """用实际运行时钟记录 UTC 时间，不使用报告文件名推断测量时间。"""
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    """在独占实验目录内保存可读 JSON。"""
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def capture(args):
    """收集少量环境信息，失败时保留错误而不伪造值。"""
    p = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, timeout=10)
    return p.stdout.strip() if p.returncode == 0 else {"error": p.stderr.strip(), "exit": p.returncode}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def timestamp_ns(value):
    """精确保留 Go RFC3339Nano 的小数部分，避免 Python 微秒截断。"""
    whole, fraction = value.rstrip("Z").split(".") if "." in value else (value.rstrip("Z"), "")
    seconds = int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp())
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def distribution(values):
    """已完成会话的精确 nearest-rank；无样本输出 null。"""
    if not values:
        return None
    values = sorted(values)
    n = len(values)
    return dict(samples=n, min_ns=values[0], p50_ns=values[(n*50+99)//100-1],
                p95_ns=values[(n*95+99)//100-1], max_ns=values[-1])


def read_schedule_metrics(version, observation):
    """兼容旧版未知观测与 v2 显式空值；不把缺字段或布尔值当成合法计数。"""
    if type(version) is not int or version not in (1, 2):
        raise ValueError(f"unsupported report schema: {version!r}")
    if version == 1:
        return dict(audio_schedule_samples=None, max_audio_schedule_lag_ns=None)
    keys = ("audio_schedule_samples", "max_audio_schedule_lag_ns")
    if any(key not in observation for key in keys):
        raise ValueError("v2 observation is missing schedule fields")
    count, lag = (observation[key] for key in keys)
    if type(count) is not int or not 0 <= count <= 2**63-1:
        raise ValueError("schedule sample count must be a nonnegative int64")
    if count == 0:
        if lag is not None:
            raise ValueError("zero samples require null lag")
    elif type(lag) is not int or not 0 <= lag <= 2**63-1:
        raise ValueError("positive samples require a nonnegative int64 lag")
    return dict(zip(keys, (count, lag)))


def check_report(path, sessions, audio_bytes):
    """从原始报告复核输入、结果和摘要；保留失败事实，不能只挑成功样本。"""
    doc = json.loads(path.read_text())
    version = doc["schema_version"]
    if type(version) is not int or version not in (1, 2):
        raise ValueError(f"unsupported report schema: {version!r}")
    checks = []
    rows = []
    cfg = doc["config"]["session"]
    checks.append(doc["tail_percentile_method"] == "nearest_rank")
    checks.append(doc["config"]["sessions"] == sessions and cfg["audio_bytes"] == audio_bytes)
    checks.append(cfg["chunk_bytes"] == 3200 and cfg["realtime"] is True)
    checks.append(cfg["expected_final_text"] == "今天天气不错" and cfg["timeout_ns"] == 20_000_000_000)
    checks.append(len(doc["sessions"]) == sessions and doc["batch_error"] is None)
    for i, s in enumerate(doc["sessions"]):
        o = s["observation"]
        checks.append(s["index"] == i and s["outcome"] == "completed" and s["error"] is None)
        checks.append(o["audio_bytes_written"] == audio_bytes and o["audio_chunks_written"] == audio_bytes//3200)
        checks.append(o["write_failures"] == 0 and o["result_count"] == 5 and o["final_result_count"] == 1)
        for kind in ("start", "end"):
            event = o[kind+"_write"]
            checks.append(event is not None and event["kind"] == kind and event["error"] is None)
        row = dict(index=i, outcome=s["outcome"], tail_ns=s["tail_latency_ns"],
                   max_audio_write_ns=o["max_audio_write_duration_ns"])
        row.update(read_schedule_metrics(version, o))
        if version == 2:
            # 本夹具要求完整的实时输入；一般失败报告并无样本数等于成功块数的约束。
            checks.append(row["audio_schedule_samples"] == audio_bytes//3200)
        if o["first_audio_started_at"] and o["last_audio_finished_at"]:
            row["audio_send_span_ns"] = timestamp_ns(o["last_audio_finished_at"])-timestamp_ns(o["first_audio_started_at"])
        if o["first_result_at"] and o["end_write"]:
            row["first_result_before_end"] = timestamp_ns(o["first_result_at"]) < timestamp_ns(o["end_write"]["started_at"])
            checks.append(row["first_result_before_end"])
        else:
            checks.append(False)
        if s["tail_latency_ns"] is not None and o["end_write"] and o["last_result_at"]:
            # Go Sub 使用单调时钟，JSON 时间仅保留墙钟；不能要求两者逐纳秒相等。
            wall_tail = timestamp_ns(o["last_result_at"])-timestamp_ns(o["end_write"]["started_at"])
            row["wall_tail_minus_recorded_tail_ns"] = wall_tail-s["tail_latency_ns"]
            checks.append(s["tail_latency_ns"] >= 0)
        else:
            checks.append(False)
        rows.append(row)
    summary = doc["summary"]
    tails = [r["tail_ns"] for r in rows if r["outcome"] == "completed" and r["tail_ns"] is not None]
    checks.append(summary["tail"] == distribution(tails))
    checks.append(summary["planned_sessions"] == sessions and summary["completed"] == sessions)
    checks.append(all(summary[k] == 0 for k in ("failed", "canceled", "timed_out")))
    checks.append(summary["planned_audio_bytes"] == sessions*audio_bytes and summary["audio_bytes_written"] == sessions*audio_bytes)
    checks.append(summary["completion_rate"] == 1)
    return dict(schema_version=version, checks_passed=all(checks), summary=summary, sessions=rows)


def run(output):
    """启动自有进程，先预热再交替执行九批；任何异常也保存清单并回收子进程。"""
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
                    schedule=dict(session_counts=[1, 2, 4], repetitions=3, audio_bytes=160000, chunk_bytes=3200,
                                  realtime=True, session_timeout_s=20, warmup_sessions=1, warmup_audio_bytes=64000),
                    limits=["same-host independent processes, no isolation", "no server active/queue/progress/resource sampling",
                            "client write success is not a server byte counter", "5s finite batches are not stable capacity",
                            "send span hides transient pacing lag", "Mock emits only four partials then a final"])
    sources = capture(["git", "ls-files", "cmd", "internal", "proto", "go.mod", "go.sum"])
    manifest["source_sha256"] = {p: digest(ROOT/p) for p in sources.splitlines() if p.endswith((".go", ".proto")) or p in ("go.mod", "go.sum")}
    manifest["script_sha256"] = digest(Path(__file__))
    processes, handles, results = [], [], []
    save(output/"manifest.json", manifest)
    try:
        # 当前 Gateway 固定监听 8080；占用则中止，不使用或停止已有服务。
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 8080))
        with tempfile.TemporaryDirectory(prefix="tide-load-baseline-") as temp:
            binaries = {}
            for name in ("asr-worker", "gateway", "loadgen"):
                target = str(Path(temp)/name)
                command = ["go", "build", "-o", target, "./cmd/"+name]
                manifest["commands"].append(command)
                with (output/("build-"+name+".log")).open("w") as log:
                    subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
                binaries[name] = target
            manifest["binary_sha256"] = {name: digest(Path(path)) for name, path in binaries.items()}

            def start(name, args):
                log = (output/(name+".log")).open("w")
                handles.append(log)
                command = [binaries[name], *args]
                manifest["commands"].append(command)
                p = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
                processes.append((name, p))
                return p

            worker = start("asr-worker", ["-listen=127.0.0.1:0", "-processing-concurrency=1", "-processing-delay=10ms", "-response-delay=5ms"])
            deadline = time.monotonic()+10
            address = None
            while time.monotonic() < deadline:
                match = re.search(r"address=(127\.0\.0\.1:\d+)", (output/"asr-worker.log").read_text())
                if match: address = match[1]; break
                if worker.poll() is not None: raise RuntimeError("Worker exited before readiness")
                time.sleep(0.05)
            if not address: raise RuntimeError("Worker did not announce address")
            gateway = start("gateway", ["-workers="+address, "-max-pending-audio-bytes=32000"])
            deadline = time.monotonic()+10
            while True:
                if gateway.poll() is not None: raise RuntimeError("Gateway exited before readiness")
                if "websocket gateway listening on :8080" in (output/"gateway.log").read_text():
                    with urllib.request.urlopen("http://127.0.0.1:8080/healthz", timeout=2) as response:
                        if response.status == 200: break
                if time.monotonic() >= deadline: raise RuntimeError("Gateway readiness timeout")
                time.sleep(0.05)
            manifest["worker_address"] = address
            schedule = [("warmup", 1, 64000)] + [(f"r{rep}-n{n}", n, 160000) for rep in range(1, 4) for n in (1, 2, 4)]
            for label, n, size in schedule:
                if any(p.poll() is not None for _, p in processes): raise RuntimeError("backend exited between batches")
                path = output/(label+".json")
                command = [binaries["loadgen"], "-url=ws://127.0.0.1:8080/v1/asr", f"-sessions={n}",
                           f"-audio-bytes={size}", "-chunk-bytes=3200", "-realtime=true", "-session-timeout=20s",
                           "-expected-final-text=今天天气不错", "-output="+str(path)]
                record = dict(label=label, planned_sessions=n, started_at=now(), command=command)
                manifest["batches"].append(record)
                save(output/"manifest.json", manifest)
                with (output/(label+".log")).open("w") as log:
                    attempt = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=30)
                record.update(finished_at=now(), exit_code=attempt.returncode)
                checked = check_report(path, n, size)
                record["checks_passed"] = checked["checks_passed"] and attempt.returncode == 0
                if label != "warmup": results.append(dict(label=label, planned_sessions=n, **checked))
                save(output/"summary.json", dict(batches=results))
                save(output/"manifest.json", manifest)
                print(label, "exit", attempt.returncode, "checks", record["checks_passed"], flush=True)
                if label == "warmup" and not record["checks_passed"]: raise RuntimeError("warmup failed; formal measurement not started")
            manifest["status"] = "passed" if all(r["checks_passed"] for r in manifest["batches"]) else "failed_checks"
            # 先停 Gateway，等待其正常清理，再停没有信号收尾入口的 Mock。
            for name, p in reversed(processes):
                p.send_signal(signal.SIGTERM)
                try: code = p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill(); code = p.wait(); manifest["status"] = "shutdown_timeout"
                manifest["shutdown"].append(dict(process=name, exit_code=code))
            if manifest["shutdown"][0]["exit_code"] != 0: manifest["status"] = "gateway_shutdown_failed"
    except BaseException as exc:
        manifest.update(status="error", error=repr(exc))
        raise
    finally:
        for _, p in reversed(processes):
            if p.poll() is None: p.kill(); p.wait()
        for log in handles: log.close()
        manifest["finished_at"] = now()
        manifest["artifact_sha256"] = {p.name: digest(p) for p in output.iterdir() if p.is_file() and p.name != "manifest.json"}
        save(output/"manifest.json", manifest)
    if manifest["status"] != "passed": raise SystemExit("baseline checks failed; all evidence retained")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new result directory; existing directories are rejected")
    run(parser.parse_args().output.resolve())
