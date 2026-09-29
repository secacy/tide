#!/usr/bin/env python3
"""从正式九批原始 JSON 复算分组结果；预热不进入统计，失败数据不静默过滤。"""
import json
from pathlib import Path
import runpy
import sys


def summarize(directory):
    helpers = runpy.run_path(str(Path(__file__).with_name("run_loadgen_baseline.py")))
    manifest = json.loads((directory/"manifest.json").read_text())
    if manifest["status"] != "passed":
        raise ValueError("experiment incomplete or failed; inspect retained evidence first")
    expected = [f"r{rep}-n{n}" for rep in range(1, 4) for n in (1, 2, 4)]
    actual = [r["label"] for r in manifest["batches"] if r["label"] != "warmup"]
    if actual != expected:
        raise ValueError("unexpected formal batch schedule")
    groups = []
    for n in (1, 2, 4):
        batches = [helpers["check_report"](directory/f"r{rep}-n{n}.json", n, 160000) for rep in range(1, 4)]
        if not all(b["checks_passed"] for b in batches):
            raise ValueError("raw report recheck failed")
        rows = [row for b in batches for row in b["sessions"]]
        spans = [r["audio_send_span_ns"] for r in rows]
        clock_diffs = [r["wall_tail_minus_recorded_tail_ns"] for r in rows]
        # 含旧版未知数据时不补造零值；明确版本，避免输出不完整的混合汇总。
        schedule = None
        if all(r["audio_schedule_samples"] is not None for r in rows):
            maxima = [r["max_audio_schedule_lag_ns"] for r in rows if r["max_audio_schedule_lag_ns"] is not None]
            schedule = dict(samples=sum(r["audio_schedule_samples"] for r in rows),
                            sessions_with_samples=sum(r["audio_schedule_samples"] > 0 for r in rows),
                            max_lag_ns=max(maxima) if maxima else None)
        groups.append(dict(planned_sessions_per_batch=n, batches=3, completed=len(rows),
                           schema_versions=sorted({b["schema_version"] for b in batches}), audio_schedule=schedule,
                           successful_audio_bytes=sum(b["summary"]["audio_bytes_written"] for b in batches),
                           audio_chunks=len(rows)*50, results=len(rows)*5,
                           first_result_before_end=sum(r["first_result_before_end"] for r in rows),
                           tail=helpers["distribution"]([r["tail_ns"] for r in rows]),
                           audio_send_span_ns=dict(min=min(spans), max=max(spans)),
                           max_audio_write_ns=max(r["max_audio_write_ns"] for r in rows),
                           wall_tail_minus_recorded_tail_ns=dict(min=min(clock_diffs), max=max(clock_diffs))))
    result = dict(formal_batches=9, planned_sessions=21, completed_sessions=sum(g["completed"] for g in groups),
                  warmup_excluded=True, groups=groups,
                  limits=["tail percentiles pool three batches per N; each p95 equals the observed maximum",
                          "no server independent byte/active/queue/resource measurement",
                          "audio_schedule null means unknown legacy data; no per-chunk lag percentile can be reconstructed",
                          "send span uses serialized wall clock; recorded tail uses process monotonic clock"])
    helpers["save"](directory/"groups.json", result)
    inputs = [directory/"manifest.json"] + [directory/(label+".json") for label in expected]
    helpers["save"](directory/"analysis-manifest.json", dict(
        generated_at=helpers["now"](),
        input_sha256={p.name: helpers["digest"](p) for p in inputs},
        script_sha256={name: helpers["digest"](Path(__file__).with_name(name)) for name in
                       ("summarize_loadgen_baseline.py", "run_loadgen_baseline.py")},
        groups_sha256=helpers["digest"](directory/"groups.json")))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
