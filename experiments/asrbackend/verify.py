"""Reconcile repeated backend evidence without importing the Go driver or models."""
import argparse
import hashlib
import json
import math
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def lines(path):
    raw = path.read_bytes()
    require(not raw or raw.endswith(b"\n"), f"incomplete evidence line: {path.name}")
    return [json.loads(line) for line in raw.splitlines()]


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def span(values):
    return {"min": min(values), "max": max(values)} if values else None


def reconcile(collection):
    manifest = load(collection / "manifest.json")
    require(manifest["schema"] == "tide.real-backend-freeze.v1", "wrong freeze schema")
    for name, expected in manifest["source"].items():
        require(not Path(name).is_absolute() and ".." not in Path(name).parts, "unsafe source path")
        require(hashlib.sha256((collection / "source" / name).read_bytes()).hexdigest() == expected,
                f"frozen source digest mismatch: {name}")
    require(manifest["source"], "missing source freeze")
    require("-race=true" not in manifest["worker_build_info"], "race Worker cannot be a runtime baseline")
    data = collection / "data"
    cfg = load(data / "conditions.json")
    require(cfg["schema"] == "tide.real-backend-baseline.v1", "wrong runtime schema")
    for key, expected in manifest["assets"].items():
        require(cfg[key] == expected, f"asset mismatch: {key}")
    expected_cfg = {"rounds": 5, "warmup": 1, "threads": 2, "decoding": "greedy_search",
                    "max_sessions": 1, "max_pending_audio_bytes": 0, "resume_window_ns": 10_000_000_000,
                    "max_audio_bytes": 960000, "max_chunk_bytes": 3200, "max_text_bytes": 4096,
                    "sample_interval_ns": 500_000_000, "max_sample_gap_ns": 3_000_000_000,
                    "normal_cleanup_budget_ns": 5_000_000_000, "cancel_cleanup_budget_ns": 15_000_000_000,
                    "warmup_idle_ns": 5_000_000_000, "case_idle_ns": 2_000_000_000, "final_idle_ns": 5_000_000_000,
                    "realtime": True, "client_resume": False, "forced_gc": False, "gateway_in_driver": True,
                    "sdk": "sherpa-onnx-go-macos@v1.13.8", "os": "darwin", "arch": "arm64"}
    for key, expected in expected_cfg.items():
        require(type(cfg[key]) is type(expected) and cfg[key] == expected, f"changed condition: {key}")
    slots = load(data / "slots.json")
    require(len(slots) == 11, "wrong slot denominator")
    events = lines(data / "model-events.jsonl")
    server = []
    raw_log = (data / "server.log").read_bytes()
    require(raw_log.endswith(b"\n"), "incomplete Worker log")
    for line in raw_log.splitlines():
        try:
            server.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # Native diagnostics remain in server.log, not lifecycle JSON.
    native = [e for e in server if e.get("msg") in ("model_created", "engine_closed")]
    require(not any(e.get("msg") == "model_creation_failed" for e in server), "model creation failed")
    require(len(events) == len(native) == 22, "native lifecycle event count mismatch")
    previous_event = -1
    for i, (observed, raw) in enumerate(zip(events, native)):
        kind = "model_created" if i % 2 == 0 else "engine_closed"
        require(observed["kind"] == raw["msg"] == kind and observed["model_id"] == raw["model_id"] == i // 2 + 1,
                "native lifecycle order mismatch")
        key = "load_ms" if kind == "model_created" else "close_ms"
        require(finite(raw[key]) and raw[key] >= 0 and observed[key] == raw[key], "native duration mismatch")
        require(observed["observed_ns"] >= previous_event, "native observation time reversed")
        previous_event = observed["observed_ns"]
    start = load(data / "process-start.json")
    end = load(data / "process.json")
    require(end["process_exit"] == 0 and end["gateway_active_before_fixture_cleanup"] == 0
            and end["models_created"] == end["models_closed"] == 11, "final process cleanup mismatch")
    require(start["worker_pid"] == end["worker_pid"] > 0 and start["driver_pid"] == end["driver_pid"] > 0
            and start["worker_pid"] != start["driver_pid"], "process identity mismatch")
    samples = lines(data / "samples.jsonl")
    require(len(samples) >= 2, "missing runtime samples")
    gaps, rss_failures, mono_gaps, wall_gaps = [], [], [], []
    for i, sample in enumerate(samples):
        require(sample["elapsed_ns"] >= 0 and sample["duration_ns"] >= 0, "invalid sample time")
        require(sample["active_sessions"] in (0, 1) and 0 <= sample["models_closed"] <= sample["models_created"] <= 11,
                "invalid sampled lifecycle counts")
        require(sample["driver_goroutines"] > 0 and sample["driver_heap_bytes"] > 0, "invalid driver sample")
        rss = sample["worker_rss_bytes"]
        if rss is None:
            require(sample.get("worker_rss_error"), "missing RSS failure reason")
            rss_failures.append(i)
        else:
            require(isinstance(rss, int) and rss > 0 and not sample.get("worker_rss_error"), "invalid RSS value")
        if i:
            mono = sample["elapsed_ns"] - samples[i-1]["elapsed_ns"]
            wall = sample["wall_ns"] - samples[i-1]["wall_ns"]
            require(mono > 0, "sample order reversed")
            mono_gaps.append(mono)
            wall_gaps.append(wall)
            if max(mono, wall) > cfg["max_sample_gap_ns"] or wall < 0:
                gaps.append({"after_index": i-1, "monotonic_ns": mono, "wall_ns": wall})
    rows, idle_rows = [], []
    for index, slot in enumerate(slots):
        cancel = index > 0 and index % 2 == 1
        round_no = (index+1)//2
        name = "warmup" if index == 0 else f"r{round_no}_{'cancel' if cancel else 'normal'}"
        require(slot["name"] == name and slot["round"] == round_no and slot["cancel"] is cancel, "slot plan changed")
        require(slot["state"] == "passed" and not slot.get("failure"), f"slot not passed: {name}")
        case = load(data / f"{name}-case.json")
        returned = load(data / f"{name}-client-return.json")
        rpc = load(data / f"{name}-grpc.json")
        callbacks = lines(data / f"{name}.jsonl")
        require(case["grpc"] == rpc, "RPC snapshot changed after archival")
        for key in returned:
            if key not in ("grpc", "gateway_active_after_cleanup", "run_return_to_observed_cleanup_ns", "native_closed_after_cleanup"):
                require(case[key] == returned[key], f"client-return evidence changed: {key}")
        require(case["name"] == name and case["input_sha256"] == cfg["input_sha256"]
                and case["input_bytes"] == cfg["input_bytes"], "case input identity mismatch")
        require(case["processed_audio_bytes"] is None and case["cer"] is None, "fabricated processing/quality metric")
        require(case["source_closes"] == 1 and case["segments"] == 1 and case["gateway_active_after_cleanup"] == 0
                and case["native_closed_after_cleanup"] == index+1, "per-case cleanup mismatch")
        require(len(callbacks) == case["callback_updates"] and callbacks, "callback count mismatch")
        results = rpc["results"]
        require(len(callbacks) <= len(results), "extra callback not from Worker")
        require(all(not r["has_progress"] and r["segment_id"] == "1" for r in results), "Worker result contract changed")
        previous_time = previous_read = 0
        for seq, (callback, result) in enumerate(zip(callbacks, results), 1):
            msg = callback["message"]
            require(msg["type"] == "result" and msg["seq"] == str(seq) and msg["segmentId"] == result["segment_id"]
                    and msg["text"] == result["text"] and msg["isFinal"] == result["final"], "callback changed or reordered")
            require(previous_time <= callback["received_ns"] <= case["run_ns"]
                    and previous_read <= callback["source_read_bytes"] <= case["source_read_bytes"],
                    "invalid callback time/input offset")
            previous_time, previous_read = callback["received_ns"], callback["source_read_bytes"]
        require(case["finals"] == sum(c["message"]["isFinal"] for c in callbacks), "final count mismatch")
        require(case["transcript"] == callbacks[-1]["message"]["text"], "snapshot was appended instead of replaced")
        first = next((c for c in callbacks if c["message"]["text"].strip()), None)
        require(first is not None and case["run_start_to_first_text_ns"] == first["received_ns"]
                and case["first_text_before_all_source_read"] and first["source_read_bytes"] < cfg["input_bytes"], "first-text evidence mismatch")
        require(sum(rpc["chunks"]) == rpc["audio_bytes"] and all(0 < n <= 3200 and n % 2 == 0 for n in rpc["chunks"]), "audio chunk mismatch")
        require(rpc["audio_bytes"] <= case["source_read_bytes"] <= cfg["input_bytes"], "forwarded bytes exceed source")
        require(not case["finals"] or callbacks[-1]["message"]["isFinal"], "final was not the last callback")
        client = case["client"]
        require(client["ResumeAttempts"] == client["ResumedConnections"] == 0, "unexpected client recovery")
        require(slot["return_ns"] - slot["start_ns"] == case["run_ns"] and slot["gateway_idle_observed_ns"] >= slot["return_ns"], "case clock mismatch")
        require(slot["model_events"] == events[index*2:index*2+2], "slot native events mismatch")
        require(slot["idle_start_ns"] >= max(slot["gateway_idle_observed_ns"], slot["model_events"][1]["observed_ns"]), "idle before cleanup")
        require(slot["idle_end_ns"]-slot["idle_start_ns"] >= (5_000_000_000 if index == 0 else 2_000_000_000), "idle window shortened")
        if index:
            require(slot["start_ns"] >= slots[index-1]["idle_end_ns"], "overlapping/restarted slots")
        if cancel:
            require(case["canceled"] and case.get("error") and not client["Completed"] and not client["CompletionAckWritten"]
                    and case["finals"] == 0 and case["source_eof_ns"] is None and not rpc["half_closed"]
                    and case["gateway_active_at_run_return"] == 1 and case["source_read_bytes"] < cfg["input_bytes"], "cancel outcome mismatch")
            require(slot["start_ns"] <= slot["cancel_ns"] <= slot["return_ns"] and slot["callback_seq_at_cancel"] == int(first["message"]["seq"])
                    and first["source_read_bytes"] <= slot["source_read_at_cancel"] <= case["source_read_bytes"], "cancel trigger mismatch")
        else:
            require(not case.get("error") and not case["canceled"] and client["Completed"] and client["CompletionAckWritten"]
                    and client["FinalOffset"] == cfg["input_bytes"] and client["AppliedSeq"] == len(callbacks)
                    and case["finals"] == 1 and len(results) == len(callbacks) and rpc["half_closed"] and rpc["terminal"] == "EOF"
                    and rpc["audio_bytes"] == cfg["input_bytes"] and rpc["audio_sha256"] == cfg["input_sha256"]
                    and case["source_read_bytes"] == cfg["input_bytes"] and rpc["chunks"][-1] == 1700
                    and case["source_eof_to_run_return_ns"] == case["run_ns"]-case["source_eof_ns"], "normal outcome mismatch")
            require(slot["cancel_ns"] is None and slot["source_read_at_cancel"] is None and slot["callback_seq_at_cancel"] is None, "normal slot was canceled")
        budget = cfg["cancel_cleanup_budget_ns"] if cancel else cfg["normal_cleanup_budget_ns"]
        require(0 <= case["run_return_to_observed_cleanup_ns"] <= budget, "cleanup budget exceeded")
        model = slot["model_events"]
        row = {"name": name, "warmup": index == 0, "cancel": cancel,
               "load_ms": model[0]["load_ms"], "run_ns": case["run_ns"],
               "first_text_ns": case["run_start_to_first_text_ns"], "tail_ns": case["source_eof_to_run_return_ns"],
               "return_to_cleanup_ns": case["run_return_to_observed_cleanup_ns"]}
        if cancel:
            row.update(cancel_to_return_ns=slot["return_ns"]-slot["cancel_ns"],
                       cancel_to_native_close_observed_ns=model[1]["observed_ns"]-slot["cancel_ns"],
                       cancel_to_gateway_idle_observed_ns=slot["gateway_idle_observed_ns"]-slot["cancel_ns"])
        rows.append(row)
        idle = [s for s in samples if slot["idle_start_ns"] <= s["elapsed_ns"]
                and s["elapsed_ns"]+s["duration_ns"] <= slot["idle_end_ns"]]
        require(idle and all(s["active_sessions"] == 0 and s["models_created"] == s["models_closed"] == index+1 for s in idle), "missing/unclean idle samples")
        idle_rows.append({"name": name, "samples": len(idle),
                          "worker_rss_bytes": span([s["worker_rss_bytes"] for s in idle if s["worker_rss_bytes"] is not None]),
                          "driver_heap_bytes": span([s["driver_heap_bytes"] for s in idle]),
                          "driver_goroutines": span([s["driver_goroutines"] for s in idle])})
    require(samples[0]["elapsed_ns"] <= cfg["max_sample_gap_ns"], "sampling started too late")
    final = [s for s in samples if s["phase"] == "final_idle"]
    require(final and final[-1]["elapsed_ns"] >= slots[-1]["idle_end_ns"]+4_000_000_000, "missing final idle observation")
    normal = [r for r in rows if not r["warmup"] and not r["cancel"]]
    return {"schema": "tide.real-backend-reconciled.v1", "normal_completed": len(normal), "normal_planned": 5,
            "cancel_expected": 5, "cancel_planned": 5, "warmup_completed": 1,
            "models_created": 11, "models_closed": 11, "cases": rows, "idle": idle_rows,
            "samples": len(samples), "sampling_gaps": gaps, "rss_unavailable_indices": rss_failures,
            "sampling_monotonic_max_gap_ns": max(mono_gaps), "sampling_wall_max_gap_ns": max(wall_gaps),
            "sample_duration_max_ns": max(s["duration_ns"] for s in samples),
            "resource_sampling_quality_ok": not gaps and not rss_failures,
            "worker_rss_sampled_max_bytes": max((s["worker_rss_bytes"] for s in samples if s["worker_rss_bytes"] is not None), default=None),
            "normal_first_text_ns": span([r["first_text_ns"] for r in normal]),
            "normal_tail_ns": span([r["tail_ns"] for r in normal]),
            "limitations": ["five rounds; not long-dialogue or stable capacity validation",
                            "driver Go heap/goroutines include Gateway, client and test tool",
                            "RSS includes native allocator/cache/mappings; sampled maximum is not exact peak",
                            "cancel cleanup includes default 10-second resume policy"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reconcile(args.collection)
    text = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")
