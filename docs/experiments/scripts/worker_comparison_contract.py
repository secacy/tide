"""双 Worker 对照的固定条件校验；身份、采样来源与资源预算分别验证。"""
import copy
import re
import shlex

from summarize_gateway_observations import require

WORKER = dict(processing_concurrency=1, processing_delay_ns=10_000_000, response_delay_ns=5_000_000,
              partial_every_ns=500_000_000, partial_texts=["今", "今天", "今天天气", "今天天气不错"], final_text="今天天气不错")
GATEWAY = dict(max_sessions=64, max_message_bytes=1048576, max_pending_audio_bytes=32000,
               start_timeout_s=10, input_idle_timeout_s=30, worker_send_timeout_s=2, tail_timeout_s=15, result_write_timeout_s=2)
CRITERIA = dict(all_completed=True, tail_p95_ns=250_000_000, max_audio_schedule_lag_ns=100_000_000)
SAMPLING = dict(gateway_interval_ns=100_000_000, worker_interval_ns=20_000_000,
                request_timeout_ns=1_000_000_000, duration_ns=40_000_000_000,
                readiness_timeout_s=2, max_bracketing_success_gap_ns=500_000_000)


def worker_configs(experiment, count):
    """生成每实例固定配置；异速实验仅改变第二实例的逐块模拟耗时。"""
    require(type(count) is int and count in (1, 2), "worker count")
    if experiment in ("dual_worker_heterogeneous", "dual_worker_strategy"):
        require(count == 2, "heterogeneous experiment requires two workers")
        return [copy.deepcopy(WORKER), dict(copy.deepcopy(WORKER), processing_delay_ns=20_000_000)]
    return [copy.deepcopy(WORKER) for _ in range(count)]


def validate_startup_logs(directory, manifest):
    """将真实进程启动日志与实例身份、监听地址和配置相互核对。"""
    for i, w in enumerate(manifest["workers"], 1):
        text = (directory/f"asr-worker-{i}.log").read_text()
        rows = [line for line in text.splitlines() if ' INFO mock ASR worker started ' in line]
        require(len(rows) == 1, "missing/duplicate Worker startup")
        line = rows[0]
        cfg = w["config"]
        fields = {"address": w["address"], "debug_address": w["endpoint"].removeprefix("http://").removesuffix("/debug/worker"),
                  "processing_concurrency": str(cfg["processing_concurrency"]),
                  "processing_delay": str(cfg["processing_delay_ns"]//1_000_000)+"ms",
                  "response_delay": "5ms"}
        for key, value in fields.items():
            require(re.search(r"(?:^|\s)"+key+r"="+re.escape(value)+r"(?:\s|$)", line) is not None, "startup field differs: "+key)

    if manifest["experiment"] == "dual_worker_strategy":
        rows = [line for line in (directory/"gateway.log").read_text().splitlines()
                if ' INFO gateway backend configuration ' in line]
        require(len(rows) == 1, "missing/duplicate Gateway strategy startup")
        fields = {}
        for token in shlex.split(rows[0].split(' INFO gateway backend configuration ', 1)[1]):
            key, sep, value = token.partition("=")
            require(sep and key not in fields, "invalid/duplicate Gateway startup field")
            fields[key] = value
        expected = dict(strategy=manifest["policy"],
                        workers="["+" ".join(w["address"] for w in manifest["workers"])+"]",
                        weights="[2 1]" if manifest["weights"] else "[]")
        require(fields == expected, "Gateway startup strategy/address/weights differ")


def validate_manifest(m):
    """拒绝缺 Worker、重复 endpoint、条件漂移和未正常收尾；不要求业务全部成功。"""
    require(m.get("experiment") in ("worker_comparison", "dual_worker_sweep", "dual_worker_boundary_short", "dual_worker_extended", "dual_worker_heterogeneous", "dual_worker_strategy") and m.get("status") == "passed", "not completed multi-worker experiment")
    from summarize_pressure_sweep import pressure_profile
    profile = pressure_profile(m["experiment"])
    count = m.get("worker_count")
    require(type(count) is int and count in (1, 2), "worker count")
    require(not m["experiment"].startswith("dual_worker_") or count == 2, "dual-worker experiment requires two workers")
    strategy = m["experiment"] == "dual_worker_strategy"
    require(m.get("policy") in (("round_robin", "weighted_round_robin") if strategy else ("round_robin",)), "policy differs")
    if strategy:
        require("weights" in m and m["weights"] == ([2, 1] if m["policy"] == "weighted_round_robin" else None), "weights differ")
    configs = worker_configs(m["experiment"], count)
    heterogeneous = m["experiment"] in ("dual_worker_heterogeneous", "dual_worker_strategy")
    require(m.get("worker") == (None if heterogeneous else WORKER) and m.get("gateway") == GATEWAY and m.get("criteria") == CRITERIA, "fixture differs")
    if heterogeneous:
        require(m.get("worker_configs") == configs, "per-worker configs differ")
    expected_sampling = dict(SAMPLING, duration_ns=profile["duration_s"]*1_000_000_000)
    require(m.get("sampling") == expected_sampling, "sampling differs")
    require(m.get("schedule") == dict(session_counts=list(profile["counts"]), repetitions=profile["repetitions"], audio_bytes=profile["audio_bytes"],
            chunk_bytes=3200, realtime=True, session_timeout_s=profile["timeout_s"], warmup_sessions=count, warmup_audio_bytes=64000), "schedule differs")
    require(m.get("tracked_diff") == "" and m.get("race") is False and bool(m.get("source_sha256")), "source is not clean non-race fixture")
    workers = m.get("workers")
    require(isinstance(workers, list) and [w.get("id") for w in workers] == [f"worker{i+1}" for i in range(count)], "worker identity list")
    for index, w in enumerate(workers):
        if heterogeneous:
            require(w.get("config") == configs[index], "worker identity/config differs")
        require(re.fullmatch(r"127\.0\.0\.1:\d+", w["address"]) is not None and
                re.fullmatch(r"http://127\.0\.0\.1:\d+/debug/worker", w["endpoint"]) is not None, "worker address format")
    require(len({w["address"] for w in workers}) == count and len({w["endpoint"] for w in workers}) == count, "duplicate worker address/endpoint")
    gateway_commands = [c for c in m["commands"] if c and c[0].endswith("/gateway")]
    gateway_args = ["-workers="+",".join(w["address"] for w in workers), "-max-pending-audio-bytes=32000"]
    if strategy:
        gateway_args.append("-worker-strategy="+m["policy"])
        if m["policy"] == "weighted_round_robin":
            gateway_args.append("-worker-weights=2,1")
    require(len(gateway_commands) == 1 and gateway_commands[0][1:] == gateway_args, "gateway routing differs")
    worker_commands = [c for c in m["commands"] if c and c[0].endswith("/asr-worker")]
    require(len(worker_commands) == count and all(c[1:] == ["-listen=127.0.0.1:0", "-debug-listen=127.0.0.1:0", "-processing-concurrency=1", "-processing-delay="+str(cfg["processing_delay_ns"]//1_000_000)+"ms", "-response-delay=5ms"] for c, cfg in zip(worker_commands, configs)), "worker commands differ")
    expected = [("warmup", count)] + [(f"r{r}-n{n}", n) for r in range(1, profile["repetitions"]+1) for n in profile["counts"]]
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
            require(item["command"][0].endswith("/"+kind+"-sampler") and item["command"][1:-1] == ["-url="+endpoint, "-interval="+("100ms" if kind == "gateway" else "20ms"), "-request-timeout=1s", "-duration="+("10s" if label == "warmup" else str(profile["duration_s"])+"s")], "sampler command differs")
            required_files.update((directory+"/samples.jsonl", directory+"/manifest.json", directory+".log"))
    if heterogeneous:
        required_files.update(f"asr-worker-{i}.log" for i in range(1, count+1))
    if strategy:
        required_files.add("gateway.log")
    require(required_files <= set(m["artifact_sha256"]), "unhashed evidence files")
    require(m.get("shutdown") == [dict(process="gateway", exit_code=0)] + [dict(process=f"asr-worker-{i}", exit_code=0) for i in range(count, 0, -1)], "backend shutdown differs")


def validate_pair(left, right, points):
    """保持同一输入、二进制、硬件和每实例配置；总 Worker 数明确从 1 变 2。"""
    validate_manifest(left)
    validate_manifest(right)
    require(left["experiment"] == right["experiment"] == "worker_comparison", "pair experiment differs")
    require(left["worker_count"] == 1 and right["worker_count"] == 2, "comparison order differs")
    for key in ("source_commit", "source_sha256", "binary_sha256", "script_sha256", "worker", "gateway", "criteria", "sampling", "policy", "go", "os", "cpu", "logical_cpus", "memory_bytes", "environment", "race"):
        require(left[key] == right[key], "comparison differs: "+key)
    require(points.get("status") == "scoped_operating_points_established" and
            [(p["role"], p["sessions"]) for p in points["points"]] == [("normal_candidate", 8), ("near_boundary_candidate", 9), ("overload_candidate", 10)], "operating points differ")
    for key in ("worker", "gateway", "criteria"):
        require(points[key] == left[key], "operating point fixture differs: "+key)
    require(points["input"] == dict(bytes_per_second=32000, chunk_bytes=3200, realtime=True), "operating point input differs")
