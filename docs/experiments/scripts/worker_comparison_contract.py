"""双 Worker 对照的固定条件校验；身份、采样来源与资源预算分别验证。"""
import re

from summarize_gateway_observations import require

WORKER = dict(processing_concurrency=1, processing_delay_ns=10_000_000, response_delay_ns=5_000_000,
              partial_every_ns=500_000_000, partial_texts=["今", "今天", "今天天气", "今天天气不错"], final_text="今天天气不错")
GATEWAY = dict(max_sessions=64, max_message_bytes=1048576, max_pending_audio_bytes=32000,
               start_timeout_s=10, input_idle_timeout_s=30, worker_send_timeout_s=2, tail_timeout_s=15, result_write_timeout_s=2)
CRITERIA = dict(all_completed=True, tail_p95_ns=250_000_000, max_audio_schedule_lag_ns=100_000_000)
SAMPLING = dict(gateway_interval_ns=100_000_000, worker_interval_ns=20_000_000,
                request_timeout_ns=1_000_000_000, duration_ns=40_000_000_000,
                readiness_timeout_s=2, max_bracketing_success_gap_ns=500_000_000)


def validate_manifest(m):
    """拒绝缺 Worker、重复 endpoint、条件漂移和未正常收尾；不要求业务全部成功。"""
    require(m.get("experiment") == "worker_comparison" and m.get("status") == "passed", "not completed worker comparison")
    count = m.get("worker_count")
    require(type(count) is int and count in (1, 2), "worker count")
    require(m.get("policy") == "round_robin", "policy differs")
    require(m.get("worker") == WORKER and m.get("gateway") == GATEWAY and m.get("criteria") == CRITERIA, "fixture differs")
    require(m.get("sampling") == SAMPLING, "sampling differs")
    require(m.get("schedule") == dict(session_counts=[8, 9, 10], repetitions=3, audio_bytes=640000,
            chunk_bytes=3200, realtime=True, session_timeout_s=35, warmup_sessions=count, warmup_audio_bytes=64000), "schedule differs")
    require(m.get("tracked_diff") == "" and m.get("race") is False and bool(m.get("source_sha256")), "source is not clean non-race fixture")
    workers = m.get("workers")
    require(isinstance(workers, list) and [w.get("id") for w in workers] == [f"worker{i+1}" for i in range(count)], "worker identity list")
    for w in workers:
        require(re.fullmatch(r"127\.0\.0\.1:\d+", w["address"]) is not None and
                re.fullmatch(r"http://127\.0\.0\.1:\d+/debug/worker", w["endpoint"]) is not None, "worker address format")
    require(len({w["address"] for w in workers}) == count and len({w["endpoint"] for w in workers}) == count, "duplicate worker address/endpoint")
    gateway_commands = [c for c in m["commands"] if c and c[0].endswith("/gateway")]
    require(len(gateway_commands) == 1 and gateway_commands[0][1:] == ["-workers="+",".join(w["address"] for w in workers), "-max-pending-audio-bytes=32000"], "gateway routing differs")
    worker_commands = [c for c in m["commands"] if c and c[0].endswith("/asr-worker")]
    require(len(worker_commands) == count and all(c[1:] == ["-listen=127.0.0.1:0", "-debug-listen=127.0.0.1:0", "-processing-concurrency=1", "-processing-delay=10ms", "-response-delay=5ms"] for c in worker_commands), "worker commands differ")
    expected = [("warmup", count)] + [(f"r{r}-n{n}", n) for r in (1, 2, 3) for n in (8, 9, 10)]
    require([(b["label"], b["planned_sessions"]) for b in m["batches"]] == expected, "batch schedule differs")
    required_files = set()
    for b in m["batches"]:
        label = b["label"]
        targets = {"gateway": "http://127.0.0.1:8080/debug/gateway", **{w["id"]: w["endpoint"] for w in workers}}
        require(set(b["sampling"]) == set(targets), "missing/extra sampler")
        required_files.update((label+".json", label+".log"))
        for key, endpoint in targets.items():
            item = b["sampling"][key]
            directory = label+"-"+key
            kind = "gateway" if key == "gateway" else "worker"
            require(item["directory"] == directory and not item.get("forced_cleanup", False), "sampler directory/cleanup")
            require(item["command"][0].endswith("/"+kind+"-sampler") and item["command"][1:-1] == ["-url="+endpoint, "-interval="+("100ms" if kind == "gateway" else "20ms"), "-request-timeout=1s", "-duration="+("10s" if label == "warmup" else "40s")], "sampler command differs")
            required_files.update((directory+"/samples.jsonl", directory+"/manifest.json", directory+".log"))
    require(required_files <= set(m["artifact_sha256"]), "unhashed evidence files")
    require(m.get("shutdown") == [dict(process="gateway", exit_code=0)] + [dict(process=f"asr-worker-{i}", exit_code=0) for i in range(count, 0, -1)], "backend shutdown differs")


def validate_pair(left, right, points):
    """保持同一输入、二进制、硬件和每实例配置；总 Worker 数明确从 1 变 2。"""
    validate_manifest(left)
    validate_manifest(right)
    require(left["worker_count"] == 1 and right["worker_count"] == 2, "comparison order differs")
    for key in ("source_commit", "source_sha256", "binary_sha256", "script_sha256", "worker", "gateway", "criteria", "sampling", "policy", "go", "os", "cpu", "logical_cpus", "memory_bytes", "environment", "race"):
        require(left[key] == right[key], "comparison differs: "+key)
    require(points.get("status") == "scoped_operating_points_established" and
            [(p["role"], p["sessions"]) for p in points["points"]] == [("normal_candidate", 8), ("near_boundary_candidate", 9), ("overload_candidate", 10)], "operating points differ")
    for key in ("worker", "gateway", "criteria"):
        require(points[key] == left[key], "operating point fixture differs: "+key)
    require(points["input"] == dict(bytes_per_second=32000, chunk_bytes=3200, realtime=True), "operating point input differs")
