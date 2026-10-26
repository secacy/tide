#!/usr/bin/env python3
"""Reconcile raw Gateway/v2 evidence and compute character errors, without ASR."""
import argparse
import hashlib
import json
from pathlib import Path
import unicodedata

IDS = [f"q{i:02}" for i in range(1, 7)]
NORMALIZATION = "nfc-ascii-lower-no-space-sentence-punctuation-v1"
PUNCTUATION = frozenset('，,。！？!?、；;：“”"‘’')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def normalize(text):
    """Preserve decimals/signs and numeric spelling; lowercase ASCII letters only."""
    text = unicodedata.normalize("NFC", text)
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in text
                   if not c.isspace() and c not in PUNCTUATION)


def character_errors(reference, hypothesis):
    """Unit-cost Levenshtein, ties: match/substitute > delete > insert.

    Alignment records original direction (reference -> hypothesis); NFC Unicode
    code points, not UTF-8 bytes. Empty reference has no CER denominator.
    """
    a, b = normalize(reference), normalize(hypothesis)
    dp = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        dp[i][0] = i
    for j in range(len(b) + 1):
        dp[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            dp[i][j] = min(dp[i-1][j-1] + (a[i-1] != b[j-1]), dp[i-1][j] + 1, dp[i][j-1] + 1)
    alignment = []
    counts = {"S": 0, "D": 0, "I": 0}
    i, j = len(a), len(b)
    while i or j:
        if i and j and dp[i][j] == dp[i-1][j-1] + (a[i-1] != b[j-1]):
            kind, left, right = ("=" if a[i-1] == b[j-1] else "S"), a[i-1], b[j-1]
            i, j = i-1, j-1
        elif i and dp[i][j] == dp[i-1][j] + 1:
            kind, left, right = "D", a[i-1], ""
            i -= 1
        else:
            kind, left, right = "I", "", b[j-1]
            j -= 1
        if kind != "=":
            counts[kind] += 1
        alignment.append({"op": kind, "reference": left, "hypothesis": right})
    errors = sum(counts.values())
    return {"reference_normalized": a, "hypothesis_normalized": b, "N": len(a), **counts,
            "errors": errors, "cer": errors / len(a) if a else None, "alignment": list(reversed(alignment))}


def reconcile_completed(folder, item, case, index):
    """Only score validated final snapshots; protocol success is separate from CER."""
    r = item["report"]
    name = case["id"]
    require(r["name"] == name and not r.get("error") and not r["canceled"], "invalid successful report")
    require(r["input_bytes"] == r["source_read_bytes"] == case["pcm_bytes"] and r["input_sha256"] == case["pcm_sha256"], "input identity mismatch")
    require(r["source_closes"] == 1 and r["gateway_active_after_cleanup"] == 0 and r["native_closed_after_cleanup"] == index+1, "cleanup mismatch")
    require(r["processed_audio_bytes"] is None and r["cer"] is None, "fabricated processing or embedded quality")
    require(r["segments"] == 1 and 0 <= r["run_return_to_observed_cleanup_ns"] <= 5_000_000_000, "invalid cleanup or segment count")
    require(r["source_eof_ns"] is not None and r["source_eof_to_run_return_ns"] == r["run_ns"] - r["source_eof_ns"] >= 0, "tail timing mismatch")
    events = [json.loads(s) for s in (folder / (name + ".jsonl")).read_text().splitlines()]
    require(len(events) == r["callback_updates"] > 0, "missing callback evidence")
    messages = [e["message"] for e in events]
    require([int(m["seq"]) for m in messages] == list(range(1, len(messages)+1)), "noncontinuous application sequence")
    require(all(m["type"] == "result" and m["segmentId"] == "1" for m in messages), "snapshot identity mismatch")
    require(sum(m["isFinal"] for m in messages) == r["finals"] == 1 and messages[-1]["isFinal"], "missing or premature final")
    require(r["transcript"] == messages[-1]["text"], "summary rewrote final text")
    previous_time = previous_read = 0
    for e in events:
        require(previous_time <= e["received_ns"] <= r["run_ns"], "event time mismatch")
        require(previous_read <= e["source_read_bytes"] <= r["source_read_bytes"], "source offset mismatch")
        previous_time, previous_read = e["received_ns"], e["source_read_bytes"]
    first = next((e for e in events if e["message"]["text"].strip()), None)
    require(r["run_start_to_first_text_ns"] == (first["received_ns"] if first else None), "first text timing mismatch")
    require(r["first_text_before_all_source_read"] == (first is not None and first["source_read_bytes"] < r["input_bytes"]), "first text ordering mismatch")
    rpc = json.loads((folder / (name + "-grpc.json")).read_text())
    require(rpc == r["grpc"] and rpc["audio_bytes"] == case["pcm_bytes"] and rpc["audio_sha256"] == case["pcm_sha256"], "forwarded PCM mismatch")
    require(rpc["half_closed"] and rpc["terminal"] == "EOF", "missing normal RPC finish")
    expected_chunks = [min(3200, case["pcm_bytes"]-p) for p in range(0, case["pcm_bytes"], 3200)]
    require(rpc["chunks"] == expected_chunks, "missing or altered input chunk")
    require(len(rpc["results"]) == len(messages), "Worker/application update mismatch")
    for raw, applied in zip(rpc["results"], messages):
        require(not raw["has_progress"] and raw["segment_id"] == applied["segmentId"] and (raw["text"], raw["final"]) == (applied["text"], applied["isFinal"]), "changed Worker snapshot")
    client = r["client"]
    require(client["Completed"] and client["CompletionAckWritten"] and client["FinalOffset"] == case["pcm_bytes"] and client["AppliedSeq"] == len(messages), "invalid completed/ack facts")
    require(client["ResumeAttempts"] == client["ResumedConnections"] == 0, "unexpected recovery")
    returned = json.loads((folder / (name + "-client-return.json")).read_text())
    for key in ["client", "source_read_bytes", "source_closes", "run_ns", "source_eof_ns", "callback_updates", "finals", "transcript", "gateway_active_at_run_return"]:
        require(returned[key] == r[key], "rewritten intermediate report")
    return r["transcript"]


def score(folder):
    """Keep the six-slot denominator, including failures and damaged evidence."""
    manifest_bytes = (folder / "input-manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    require(manifest["schema"] == "tide.asr-quality-input.v1" and [c["id"] for c in manifest["cases"]] == IDS, "invalid frozen inputs")
    conditions = json.loads((folder / "conditions.json").read_text())
    require(conditions["input_manifest_sha256"] == digest(manifest_bytes), "manifest changed after preflight")
    require(conditions["max_pending_audio_bytes"] == 0 and conditions["max_sessions"] == 1 and conditions["realtime"] and conditions["chunk_bytes"] == 3200 and not conditions["client_resume_enabled"], "unexpected run configuration")
    require(conditions["resume_window_ns"] == 10_000_000_000 and conditions["result_retention_ns"] == 30_000_000_000, "unexpected lifecycle budgets")
    reports = json.loads((folder / "summary.json").read_text())
    require([r["id"] for r in reports] == IDS, "missing planned case slots")
    scored = []
    for index, (item, case) in enumerate(zip(reports, manifest["cases"])):
        require(item["outcome"] in ("completed", "failed", "unknown"), "unknown outcome category")
        require(case["reference_sha256"] == digest((case["reference"]+"\n").encode()), "reference text/hash mismatch")
        row = {"id": case["id"], "outcome": item["outcome"], "issue": item.get("issue"), "quality": None,
               "facts": [{"fact": f, "judgment": "pending_human_review", "evidence": None} for f in case["facts"]]}
        if item["outcome"] == "completed":
            try:
                hypothesis = reconcile_completed(folder, item, case, index)
                row["hypothesis"] = hypothesis
                row["reference"] = case["reference"]
                row["quality"] = character_errors(case["reference"], hypothesis)
                require(row["quality"]["N"] > 0, "empty reference cannot be scored")
            except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError) as error:
                row["outcome"], row["issue"], row["quality"] = "unknown", str(error), None
        if row["outcome"] != "completed":
            for f in row["facts"]:
                f["judgment"] = "unable_to_judge"
        scored.append(row)
    totals = {k: sum(row["quality"][k] for row in scored if row["quality"]) for k in ("N", "S", "D", "I", "errors")}
    totals["cer"] = totals["errors"] / totals["N"] if totals["N"] else None
    outcomes = {kind: sum(row["outcome"] == kind for row in scored) for kind in ("completed", "failed", "unknown")}
    # A serial model creation/release event pair per completed run is separate evidence.
    events = []
    for line in (folder / "server.log").read_text().splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("msg") in ("model_created", "engine_closed"):
            events.append([e["msg"], e["model_id"]])
    expected = [[kind, i] for i in range(1, 7) for kind in ("model_created", "engine_closed")]
    process = json.loads((folder / "process.json").read_text()) if (folder / "process.json").exists() else None
    lifecycle = events == expected and process == {"process_exit": 0, "models_closed": 6, "gateway_active_before_fixture_cleanup": 0}
    return {"schema": "tide.asr-quality-score.v1", "normalization": NORMALIZATION,
            "alignment_tie_order": ["match/substitute", "delete", "insert"], "planned": 6, "outcomes": outcomes,
            "scored_cases": outcomes["completed"], "completed_subset": totals,
            "six_models_created_and_closed": lifecycle, "manual_fact_evaluation": "pending", "cases": scored}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = score(args.folder)
    data = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(data)
    else:
        print(data, end="")
