"""Reconcile frozen input, per-stream sample digests, callbacks and resource samples."""
import argparse
import json
from pathlib import Path

from prepare import digest, sample_digest


def require(ok, message):
    if not ok:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_text())


def lines(path, native=False):
    raw = path.read_bytes()
    require(raw.endswith(b"\n"), f"incomplete or empty JSONL: {path.name}")
    rows = []
    for line in raw.splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            require(native, f"invalid JSONL: {path.name}")
    return rows


def span(values):
    return {"min": min(values), "max": max(values)} if values else None


def check_callbacks(rows, rpc, expected_ids):
    require(isinstance(rpc, list) and len(rows) == len(rpc), "callback/RPC count mismatch")
    finals, closed, current = [], set(), None
    for index, (row, original) in enumerate(zip(rows, rpc), 1):
        msg = row["message"]
        segment = msg["segmentId"]
        require(msg["type"] == "result" and msg["seq"] == str(index), "noncontinuous result sequence")
        require(segment in expected_ids and segment not in closed, "invalid segment or revision after final")
        if segment != current:
            require(current is None or current in closed, "segment changed before old final")
            current = segment
        require(not original["has_progress"] and (segment, msg["text"], msg["isFinal"]) ==
                (original["segment_id"], original["text"], original["final"]), "Worker snapshot changed")
        require(row["current_text_bytes"] == (0 if msg["isFinal"] else len(msg["text"].encode())), "consumer kept historical text")
        if msg["isFinal"]:
            finals.append(segment)
            closed.add(segment)
    require(finals == expected_ids, "missing or duplicated segment final")


def check_native(rows, inputs):
    result = {}
    model, stream = None, None
    count = 0
    for row in rows:
        kind = row.get("msg")
        require(kind not in ("model_creation_failed", "stream_creation_failed"), "native creation failed")
        if kind == "model_created":
            require(model is None and stream is None and row["model_id"] == count+1 and count < len(inputs), "overlapping or repeated model")
            count += 1
            model = count
            result[count] = {"created": [], "closed": [], "load_ms": row["load_ms"]}
        elif kind == "stream_created":
            require(model == row["model_id"] and stream is None, "overlapping native stream")
            stream = row["segment_id"]
            require(stream == len(result[model]["created"])+1 and stream <= len(inputs[model-1]["segments"]) and row["initial"] == (stream == 1) and row["create_ns"] >= 0, "invalid stream creation")
            result[model]["created"].append(row)
        elif kind == "stream_closed":
            require(model == row["model_id"] and stream == row["segment_id"] and row["reason"] == "segment_release" and row["release_ns"] >= 0, "stream close mismatch")
            expected = inputs[model-1]["segments"][stream-1]
            require(row["samples"] == expected["samples"] and row["sample_f32le_sha256"] == expected["sample_f32le_sha256"], "native submitted sample digest/count mismatch")
            result[model]["closed"].append(row)
            stream = None
        elif kind == "engine_closed":
            require(model == row["model_id"] and stream is None, "model closed before stream release")
            require(len(result[model]["created"]) == len(result[model]["closed"]) == len(inputs[model-1]["segments"]), "missing stream lifecycle")
            result[model]["close_ms"] = row["close_ms"]
            model = None
    require(model is None and stream is None and count == len(inputs) and all("close_ms" in r for r in result.values()), "missing model close or case")
    return result


def check_samples(rows, formal):
    require(len(rows) >= 240, "insufficient resource samples")
    require(all(r["duration_ns"] >= 0 and r["active_sessions"] in (0, 1) for r in rows), "invalid runtime sample")
    gaps = [b["elapsed_ns"]-a["elapsed_ns"] for a, b in zip(rows, rows[1:])]
    walls = [b["wall_ns"]-a["wall_ns"] for a, b in zip(rows, rows[1:])]
    require(all(x > 0 for x in gaps) and all(x > 0 for x in walls), "nonmonotonic sample clocks")
    quality = max(gaps) <= 3000000000 and max(walls) <= 3000000000 and all(r["worker_rss_bytes"] and not r.get("worker_rss_error") for r in rows)
    require(rows[0]["elapsed_ns"] <= formal["StartNS"] and rows[-1]["elapsed_ns"] >= formal["ReturnNS"]+9000000000, "missing formal window or final idle")
    phases = {}
    for phase in sorted({r["phase"] for r in rows}):
        selected = [r for r in rows if r["phase"] == phase]
        phases[phase] = {"n": len(selected), "rss_bytes": span([r["worker_rss_bytes"] for r in selected if r["worker_rss_bytes"]]),
                         "driver_heap_bytes": span([r["driver_heap_bytes"] for r in selected]), "driver_goroutines": span([r["driver_goroutines"] for r in selected])}
    require({"warmup", "warmup_idle", "formal", "formal_idle", "probe", "probe_idle"}.issubset(phases), "missing experiment phase")
    return {"quality_passed": quality, "n": len(rows), "max_monotonic_gap_ns": max(gaps), "max_wall_gap_ns": max(walls),
            "rss_missing": sum(not r["worker_rss_bytes"] for r in rows), "max_sample_duration_ns": max(r["duration_ns"] for r in rows), "phases": phases}


def verify(root):
    m = load(root / "manifest.json")
    require(m["schema"] == "tide.segment120.v1" and m["plan"] == ["warmup", "formal", "probe"] and m["segment_bytes"] == 960000, "experiment condition mismatch")
    require(bool(m["source"]) and bool(m["native_libraries"]), "missing source or native identities")
    for name, expected in m["source"].items():
        require(digest(root / "source" / name) == expected, f"source digest mismatch: {name}")
    base, formal = (root / "base.pcm").read_bytes(), (root / "input.pcm").read_bytes()
    require(digest(root / "base.pcm") == "237ee4752b8a1fb41c6284ae94f2d7baf9a1fc8f28c26d3409c69e86cecbade1", "original PCM identity mismatch")
    require(len(base) == 321700 and len(formal) == 3840000 and formal == (base*12)[:3840000], "frozen PCM construction mismatch")
    data = {"warmup": base, "formal": formal, "probe": base[:96000]}
    for name, pcm in data.items():
        reference = m["inputs"][name]
        import hashlib
        require(reference["sha256"] == hashlib.sha256(pcm).hexdigest() and reference["bytes"] == len(pcm), "input digest mismatch")
        expected = [{"samples": len(pcm[i:i+960000])//2, "sample_f32le_sha256": sample_digest(pcm[i:i+960000])} for i in range(0, len(pcm), 960000)]
        require(reference["segments"] == expected, "input sample reference mismatch")
    native = lines(root / "data/server.log", native=True)
    listening = [r for r in native if r.get("msg") == "listening"]
    require(len(listening) == 1 and listening[0]["max_audio_bytes"] == 3840000 and listening[0]["segment_audio_bytes"] == 960000 and listening[0]["observe_segments"] and not listening[0]["progress_available"] and listening[0]["threads"] == 2, "Worker runtime configuration mismatch")
    lifecycles = check_native(native, [m["inputs"][name] for name in m["plan"]])
    summaries, cases = {}, []
    for index, name in enumerate(m["plan"], 1):
        folder = root / "data"
        c = load(folder / f"{name}.json")
        cases.append(c)
        reference = m["inputs"][name]
        ids = [str(i+1) for i in range(len(reference["segments"]))]
        require(c["name"] == name and c["state"] == "passed" and not c["Failure"], "failed or missing planned case")
        require(c["InputBytes"] == reference["bytes"] and c["SourceCloses"] == 1 and c["AcquireCalls"] == index and c["LeaseReusable"], "source/allocation cleanup mismatch")
        client, rpc = c["Client"], c["RPC"]
        require(client["Completed"] and client["CompletionAckWritten"] and client["FinalOffset"] == reference["bytes"] and client["ResumeAttempts"] == client["ResumedConnections"] == 0, "client completion mismatch")
        require(rpc["audio_bytes"] == reference["bytes"] and rpc["audio_sha256"] == reference["sha256"] and rpc["half_closed"] and rpc["terminal"] == "EOF", "RPC audio/termination mismatch")
        callbacks = lines(folder / f"{name}-results.jsonl")
        check_callbacks(callbacks, rpc["results"], ids)
        require(c["Updates"] == client["AppliedSeq"] == len(callbacks) and c["Finals"] == ids, "callback summary mismatch")
        reads = lines(folder / f"{name}-reads.jsonl")
        source, offset = [], 0
        for r in reads:
            require(r["offset"] == offset and 0 <= r["bytes"] <= 3200, "source read position mismatch")
            offset += r["bytes"]
            if r["bytes"]:
                source.append(r)
        require(offset == reference["bytes"] and reads[-1]["eof"] and reads[-1]["at_ns"] == c["SourceEOFNS"] and source[0]["at_ns"] == c["FirstReadNS"], "source EOF or clock mismatch")
        events = lines(folder / f"{name}-rpc.jsonl")
        sends = [r for r in events if r["event"] == "send"]
        require([r["bytes"] for r in sends] == rpc["chunks"] and len(sends) == len(source), "send/read count mismatch")
        offset = 0
        for r in sends:
            require(r["success"] and r["offset"] == offset and r["end_ns"] >= r["start_ns"], "send failed or reordered")
            offset += r["bytes"]
        require(offset == reference["bytes"] and [r["error"] for r in events if r["event"] == "terminal"] == ["EOF"], "RPC event terminal mismatch")
        finals = [r for r in callbacks if r["message"]["isFinal"]]
        timing = []
        for i, final in enumerate(finals, 1):
            boundary = min(i*960000, reference["bytes"])
            read_at = next(r["at_ns"] for r in source if r["offset"]+r["bytes"] == boundary)
            send_at = next(r["end_ns"] for r in sends if r["offset"]+r["bytes"] == boundary)
            require(final["at_ns"] >= read_at and final["at_ns"] >= send_at, "final before original input boundary")
            timing.append({"segment": i, "source_read_to_final_ns": final["at_ns"]-read_at, "grpc_send_to_final_ns": final["at_ns"]-send_at})
        require(c["StartNS"] <= c["FirstReadNS"] <= c["SourceEOFNS"] <= c["ReturnNS"] <= c["IdleNS"] <= c["ReturnNS"]+5000000000, "case cleanup clock mismatch")
        model = lifecycles[index]
        summaries[name] = {"completed": True, "input_bytes": reference["bytes"], "finals": ids, "updates": len(callbacks), "timing": timing,
                           "max_source_lag_ns": max(r["at_ns"]-c["FirstReadNS"]-r["offset"]*1000000000//32000 for r in source),
                           "max_grpc_send_lag_ns": max(r["end_ns"]-c["FirstReadNS"]-r["offset"]*1000000000//32000 for r in sends),
                           "source_eof_to_return_ns": c["ReturnNS"]-c["SourceEOFNS"], "cleanup_after_return_ns": c["IdleNS"]-c["ReturnNS"],
                           "new_stream_create_ns": [r["create_ns"] for r in model["created"] if not r["initial"]],
                           "stream_release_ns": [r["release_ns"] for r in model["closed"]], "model_load_ms": model["load_ms"]}
    require(load(root / "data/cases.json") == cases, "aggregate cases differ")
    samples = check_samples(lines(root / "data/samples.jsonl"), cases[1])
    return {"schema": "tide.segment120-reconciled.v1", "cases": summaries, "resources": samples,
            "models_created_and_closed": len(lifecycles), "streams_created_and_closed": sum(len(r["closed"]) for r in lifecycles.values())}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    result = verify(args.root)
    text = json.dumps(result, indent=2, ensure_ascii=False)+"\n"
    if args.output:
        args.output.write_text(text)
    print(text)
