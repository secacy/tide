"""Independently reconcile the fixed paired admission experiment."""
import argparse
import hashlib
import json
import math
from pathlib import Path


def require(ok, message):
    if not ok:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def lines(path, native=False):
    raw = path.read_bytes()
    require(not raw or raw.endswith(b"\n"), f"incomplete JSONL: {path.name}")
    rows = []
    for line in raw.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            require(native, f"invalid JSONL: {path.name}")
    return rows


def plan():
    result = [("warmup", "capacity", "warmup", ["WA", "WB"])]
    for pair, strategies in enumerate((("old", "capacity"), ("capacity", "old"), ("old", "capacity")), 1):
        result += [(f"p{pair}_{s}", s, "staggered", ["L", "S", "N"]) for s in strategies]
    result += [(f"overload{i}", "capacity", "overload", ["L", "S", "N"]) for i in range(1, 4)]
    return result


def span(values):
    return {"min": min(values), "max": max(values)} if values else None


def verify_case(folder, name, reference, outcome):
    c = load(folder / f"{name}-case.json")
    returned = load(folder / f"{name}-return.json")
    r = c["report"]
    for key in ("name", "start_ns", "return_ns", "entry_code"):
        require(c[key] == returned[key], "return evidence changed")
    for key in r:
        if key != "grpc":
            require(r[key] == returned["report"][key], f"report changed: {key}")
    require(c["name"] == r["name"] == name and c["return_ns"]-c["start_ns"] == r["run_ns"], "case clock/name mismatch")
    require(r["input_bytes"] == reference["bytes"] and r["input_sha256"] == reference["sha256"], "input identity mismatch")
    require(0 <= r["source_read_bytes"] <= r["input_bytes"] and r["source_closes"] == 1, "source lifetime mismatch")
    client = r["client"]
    require(client["ResumeAttempts"] == client["ResumedConnections"] == 0, "unexpected client retry")
    require(r["processed_audio_bytes"] is None and r["cer"] is None, "fabricated model progress/quality")
    callbacks = lines(folder / f"{name}.jsonl")
    require(len(callbacks) == r["callback_updates"] and sum(e["message"]["isFinal"] for e in callbacks) == r["finals"], "callback/final count mismatch")
    rpc = r["grpc"]
    # encoding/json encodes nil Go slices as null. In a busy RPC no Send/Recv
    # data may exist; absence is still distinct from a fabricated completion.
    results = [] if rpc["results"] is None else rpc["results"]
    chunks = [] if rpc["chunks"] is None else rpc["chunks"]
    require(isinstance(results, list) and isinstance(chunks, list), "invalid RPC evidence arrays")
    if outcome == "session_limit":
        require(c["assignment"] is None and c["model_id"] is None and c["entry_code"] == "session_limit"
                and r.get("error") and not client["Completed"] and not client["CompletionAckWritten"]
                and not callbacks, "entry rejection mismatch")
        return c
    a = c["assignment"]
    require(a and c["start_ns"] <= a["selected_ns"] <= a["rpc_start_ns"] <= a["rpc_end_ns"], "RPC assignment clock mismatch")
    require(a["rpc_end_ns"] <= c["return_ns"]+5_000_000_000, "RPC did not terminate within cleanup budget")
    require(sum(chunks) == rpc["audio_bytes"] <= r["source_read_bytes"]
            and all(0 < n <= 3200 and n % 2 == 0 for n in chunks), "audio forwarding mismatch")
    require(all(not x["has_progress"] and x["segment_id"] == "1" for x in results), "unexpected Worker contract")
    require(len(callbacks) <= len(results), "extra callback")
    previous_time = previous_read = 0
    for seq, (event, original) in enumerate(zip(callbacks, results), 1):
        msg = event["message"]
        require(msg["type"] == "result" and msg["seq"] == str(seq) and msg["segmentId"] == original["segment_id"]
                and msg["text"] == original["text"] and msg["isFinal"] == original["final"], "snapshot changed or reordered")
        require(previous_time <= event["received_ns"] <= r["run_ns"]
                and previous_read <= event["source_read_bytes"] <= r["source_read_bytes"], "callback observation mismatch")
        previous_time, previous_read = event["received_ns"], event["source_read_bytes"]
    if outcome == "busy":
        require(rpc["terminal"] == "ResourceExhausted" and a["terminal_message"] == "worker is busy"
                and r.get("error") and not client["Completed"] and not client["CompletionAckWritten"]
                and c["model_id"] is None and not callbacks and r["finals"] == 0, "busy rejection mismatch")
        return c
    require(not c["entry_code"] and not r.get("error") and client["Completed"] and client["CompletionAckWritten"]
            and client["FinalOffset"] == r["input_bytes"] and client["AppliedSeq"] == len(callbacks), "completion mismatch")
    require(r["source_read_bytes"] == r["input_bytes"] and rpc["audio_bytes"] == r["input_bytes"]
            and rpc["audio_sha256"] == r["input_sha256"] and rpc["terminal"] == "EOF" and rpc["half_closed"]
            and rpc["chunks"][-1] == (r["input_bytes"]-1) % 3200+1 and c["model_id"] is not None, "normal audio/RPC mismatch")
    require(r["segments"] == 1 and r["finals"] == 1 and len(callbacks) == len(results)
            and callbacks[-1]["message"]["isFinal"] and r["transcript"] == callbacks[-1]["message"]["text"], "normal final snapshot mismatch")
    first = next((e for e in callbacks if e["message"]["text"].strip()), None)
    require(first and r["run_start_to_first_text_ns"] == first["received_ns"]
            and r["first_text_before_all_source_read"] and first["source_read_bytes"] < r["input_bytes"], "streaming first text mismatch")
    require(r["source_eof_ns"] is not None and 0 <= r["source_eof_ns"] <= r["run_ns"]
            and r["source_eof_to_run_return_ns"] == r["run_ns"]-r["source_eof_ns"], "tail time mismatch")
    return c


def reconcile(collection):
    manifest = load(collection / "manifest.json")
    require(manifest["schema"] == "tide.admission-freeze.v1" and manifest["source"], "missing freeze identity")
    for path, expected in manifest["source"].items():
        require(not Path(path).is_absolute() and ".." not in Path(path).parts, "unsafe frozen path")
        require(hashlib.sha256((collection / "source" / path).read_bytes()).hexdigest() == expected, "frozen source mismatch")
    require("-race=true" not in manifest["worker_build_info"] and manifest["native_libraries"], "invalid runtime preparation")
    data = collection / "data"
    cfg = load(data / "conditions.json")
    require(cfg["schema"] == "tide.admission-comparison.v1" and cfg["worker_sha256"] == manifest["worker_sha256"]
            and cfg["model_files"] == manifest["model_files"], "runtime identity mismatch")
    for name in ("long", "short"):
        require(cfg[f"input_{name}_bytes"] == manifest["inputs"][name]["bytes"]
                and cfg[f"input_{name}_sha256"] == manifest["inputs"][name]["sha256"], "input freeze mismatch")
    for key, expected in {"threads_per_worker": 2, "worker_limit": 1, "sample_interval_ns": 500_000_000,
                          "max_sample_gap_ns": 3_000_000_000, "between_idle_ns": 2_000_000_000,
                          "client_realtime": True, "client_resume": False, "forced_gc": False,
                          "sdk": "sherpa-onnx-go-macos@v1.13.8", "decoding": "greedy_search",
                          "arm_budget_ns": 45_000_000_000, "cleanup_budget_ns": 5_000_000_000}.items():
        require(type(cfg[key]) is type(expected) and cfg[key] == expected, f"changed condition: {key}")
    gateway = cfg["gateway"]
    expected_g = {"MaxSessions": 2, "MaxMessageBytes": 3208, "StartTimeout": 10_000_000_000,
                  "InputIdleTimeout": 30_000_000_000, "WorkerSendTimeout": 2_000_000_000,
                  "TailTimeout": 15_000_000_000, "ResultWriteTimeout": 2_000_000_000, "MaxPendingAudioBytes": 0}
    require({k: v for k, v in gateway.items() if k != "V2"} == expected_g, "Gateway protection changed")
    require(gateway["V2"] == {"MaxHandshakes": 64, "EntryTimeout": 10_000_000_000, "InputProgressTimeout": 30_000_000_000,
                             "ResumeWindow": 10_000_000_000, "ResultRetentionTimeout": 30_000_000_000,
                             "WorkerStatusTimeout": 2_000_000_000, "MaxAudioBytes": 1048576,
                             "MaxAudioChunks": 256, "MaxResultBytes": 1048576, "MaxResults": 256}, "v2 protection changed")
    start, end = load(data / "process-start.json"), load(data / "process.json")
    require(start["worker_pids"] == end["worker_pids"] and len(set(start["worker_pids"])) == 2
            and start["driver_pid"] == end["driver_pid"] and start["driver_pid"] not in start["worker_pids"]
            and end["process_exit"] == [0, 0], "process identity/exit mismatch")
    native = {}
    for worker, count in (("A", 10), ("B", 13)):
        raw = lines(data / f"worker{worker}" / "server.log", native=True)
        require(not any(r.get("msg") == "model_creation_failed" for r in raw), "model creation failed")
        raw = [r for r in raw if r.get("msg") in ("model_created", "engine_closed")]
        observed = lines(data / f"worker{worker}" / "model-events.jsonl")
        require(len(raw) == len(observed) == count*2, "native event denominator mismatch")
        for i, (r, e) in enumerate(zip(raw, observed)):
            kind = "model_created" if i % 2 == 0 else "engine_closed"
            duration = "load_ms" if i % 2 == 0 else "close_ms"
            require(r["msg"] == e["kind"] == kind and r["model_id"] == e["model_id"] == i//2+1
                    and math.isfinite(r[duration]) and r[duration] >= 0 and r[duration] == e[duration], "native event identity mismatch")
            require(e["observed_ns"] >= (observed[i-1]["observed_ns"] if i else 0), "native observation time reversed")
        native[worker] = observed
    require(end["models_created"] == end["models_closed"] == [10, 13], "model cleanup mismatch")
    arms = load(data / "arms.json")
    require(len(arms) == len(plan()), "arm denominator mismatch")
    previous = [0, 0]
    rows, all_cases, total_rpcs = [], {}, 0
    for arm, (name, strategy, kind, names) in zip(arms, plan()):
        require((arm["name"], arm["strategy"], arm["kind"], arm["cases"]) == (name, strategy, kind, names), "paired plan changed")
        require(arm["state"] == "passed" and not arm.get("failure"), f"arm did not pass: {name}")
        require(arm["before_models"] == previous, "process model counts reset between arms")
        require(arm["local_pool_reusable"] is (None if strategy == "old" else True), "local reservation cleanup missing")
        routing = lines(data / name / "routing.jsonl")
        require(all(e["type"] in ("selected", "rpc_open", "rpc_end") for e in routing), "unexpected allocator failure/retry")
        next_ids, cases = previous.copy(), {}
        for case_name in names:
            outcome = "normal"
            if case_name == "N":
                outcome = "session_limit" if kind == "overload" else "busy" if strategy == "old" else "normal"
            input_name = "long" if kind == "warmup" or case_name == "L" or (kind == "overload" and case_name == "S") else "short"
            case = verify_case(data / name, case_name, manifest["inputs"][input_name], outcome)
            relevant = [e for e in routing if e["case"] == case_name]
            require(len(relevant) == (0 if outcome == "session_limit" else 3), "RPC attempts changed")
            if outcome != "session_limit":
                a = case["assignment"]
                worker = "A" if case_name in ("L", "WA") or (case_name == "N" and strategy == "old") else "B"
                require(a["worker"] == worker and [e["type"] for e in relevant] == ["selected", "rpc_open", "rpc_end"]
                        and all(e["worker"] == worker for e in relevant), "selected wrong Worker")
                require([e["at_ns"] for e in relevant] == [a["selected_ns"], a["rpc_start_ns"], a["rpc_end_ns"]]
                        and relevant[-1]["message"] == a["terminal_message"] and relevant[-1]["code"] == case["report"]["grpc"]["terminal"], "RPC routing event mismatch")
                total_rpcs += 1
                if outcome == "normal":
                    index = ord(worker)-ord("A")
                    next_ids[index] += 1
                    require(case["model_id"] == next_ids[index], "case/model mapping mismatch")
                    model = native[worker][2*(case["model_id"]-1):2*case["model_id"]]
                    require(a["rpc_start_ns"] <= model[0]["observed_ns"] <= case["return_ns"]
                            and model[1]["observed_ns"] <= arm["cleanup_ns"], "model lifecycle clock mismatch")
                    case["load_ms"] = model[0]["load_ms"]
            cases[case_name] = case
            rows.append({"arm": name, "strategy": strategy, "kind": kind, "case": case_name, "outcome": outcome,
                         "worker": case["assignment"]["worker"] if case["assignment"] else None,
                         "run_ns": case["report"]["run_ns"], "first_text_ns": case["report"]["run_start_to_first_text_ns"],
                         "tail_ns": case["report"]["source_eof_to_run_return_ns"], "load_ms": case.get("load_ms")})
        require(arm["after_models"] == next_ids, "unexpected model creation in an arm")
        previous = next_ids
        if kind != "warmup":
            require(arm["trigger_snapshot"]["ActiveSessions"] == (1 if kind == "staggered" else 2), "trigger Gateway occupancy mismatch")
            require(arm["trigger_created"] == [x+1 for x in arm["before_models"]]
                    and arm["trigger_closed"] == ([arm["before_models"][0], arm["before_models"][1]+1] if kind == "staggered" else arm["before_models"]), "trigger native occupancy mismatch")
            require(cases["L"]["assignment"]["rpc_start_ns"] <= cases["S"]["start_ns"], "second launch preceded first RPC assignment")
            require(arm["trigger_ns"] <= cases["N"]["start_ns"] < cases["L"]["return_ns"], "long session did not overlap third attempt")
            if kind == "staggered":
                short_id = cases["S"]["model_id"]
                require(cases["S"]["return_ns"] <= arm["trigger_ns"]
                        and native["B"][2*short_id-1]["observed_ns"] <= arm["trigger_ns"], "short model was not released before N")
            else:
                require(cases["N"]["start_ns"] < cases["S"]["return_ns"], "second accepted call did not overlap overload probe")
        require(arm["cleanup_ns"] >= max(c["return_ns"] for c in cases.values()), "cleanup preceded client return")
        all_cases[name] = cases
    require(total_rpcs == 26 and len(rows) == 29, "attempt/RPC denominator mismatch")
    samples = lines(data / "samples.jsonl")
    require(len(samples) > 1, "missing resource sampling")
    gaps, unavailable = [], []
    maximum = {"A": 0, "B": 0}
    for i, sample in enumerate(samples):
        require(sample["active_sessions"] in (0, 1, 2) and sample["driver_goroutines"] > 0 and sample["driver_heap_bytes"] > 0, "invalid Gateway/driver sample")
        for j, worker in enumerate(("A", "B")):
            w = sample["workers"][worker]
            require(w["pid"] == start["worker_pids"][j] and 0 <= w["closed"] <= w["created"] <= end["models_created"][j]
                    and not w.get("log_error"), "invalid sampled model state")
            if w["rss_bytes"] is None:
                require(w.get("rss_error"), "missing RSS error reason")
                unavailable.append({"index": i, "worker": worker})
            else:
                require(w["rss_bytes"] > 0 and not w.get("rss_error"), "invalid RSS sample")
                maximum[worker] = max(maximum[worker], w["rss_bytes"])
        if i:
            mono = sample["elapsed_ns"]-samples[i-1]["elapsed_ns"]
            wall = sample["wall_ns"]-samples[i-1]["wall_ns"]
            require(mono > 0, "reversed sampling clock")
            if max(mono, wall) > cfg["max_sample_gap_ns"] or wall < 0:
                gaps.append({"after_index": i-1, "monotonic_ns": mono, "wall_ns": wall})
    for index, arm in enumerate(arms):
        following = arms[index+1]["name"] if index+1 < len(arms) else None
        idle = [s for s in samples if s["arm"] == "between" and s["elapsed_ns"] >= arm["cleanup_ns"]
                and (following is None or s["elapsed_ns"] < min(c["start_ns"] for c in all_cases[following].values()))]
        require(idle and all(s["active_sessions"] == 0 for s in idle), "missing clean between-arm observation")
        if following:
            require(min(c["start_ns"] for c in all_cases[following].values())-arm["cleanup_ns"] >= cfg["between_idle_ns"], "between-arm idle shortened")
    old_n = [r for r in rows if r["kind"] == "staggered" and r["strategy"] == "old" and r["case"] == "N"]
    new_n = [r for r in rows if r["kind"] == "staggered" and r["strategy"] == "capacity" and r["case"] == "N"]
    return {"schema": "tide.admission-reconciled.v1", "attempts": 29, "rpcs": total_rpcs,
            "warmup_completed": 2, "old_newcomer_completed": sum(r["outcome"] == "normal" for r in old_n),
            "new_newcomer_completed": sum(r["outcome"] == "normal" for r in new_n), "newcomer_planned_per_strategy": 3,
            "original_completed_per_strategy": {"old": 6, "capacity": 6}, "overload_accepted_completed": 6,
            "overload_entry_rejected": 3, "models_created": [10, 13], "models_closed": [10, 13],
            "cases": rows, "samples": len(samples), "sampling_gaps": gaps, "rss_unavailable": unavailable,
            "sampling_quality_ok": not gaps and not unavailable, "worker_rss_sampled_max_bytes": maximum,
            "sample_max_gap_ns": max(b["elapsed_ns"]-a["elapsed_ns"] for a, b in zip(samples, samples[1:])),
            "limits": ["paired engineered overlap; not stable capacity or long-dialogue validation",
                       "local capacity checked at arm boundaries; no in-run probing of TryAcquire",
                       "resource samples include driver/tool overhead; RSS maximum is sampled, not exact peak"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reconcile(args.collection)
    text = json.dumps(result, indent=2, ensure_ascii=False)+"\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")
