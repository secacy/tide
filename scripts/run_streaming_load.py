#!/usr/bin/env python3
"""Run EXP-003, EXP-006, EXP-007 or EXP-008 with test-only observation; never rewrite production files."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment", choices=("streaming", "capacity", "admission", "selection"), default="streaming")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--case", default=".*", help="Go subtest name regular expression")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--duration-ms", type=int, help="Override input duration for smoke checks")
    parser.add_argument("--race", action="store_true", help="Correctness check; do not use for samples")
    args = parser.parse_args()
    if args.count < 1 or (args.duration_ms is not None and args.duration_ms < 1):
        parser.error("count and duration must be positive")
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    meta_path = output.with_suffix(output.suffix + ".meta.json")
    if output.exists() or meta_path.exists():
        parser.error("output already exists; choose a new path to preserve previous samples")

    # Match exact sites and fail on source drift instead of silently losing observations.
    replacements = {
        "internal/gateway/audio_queue.go": [
            ("\tq.bytes += len(chunk)\n", "\tq.bytes += len(chunk)\n\tloadQueuePush(q)\n"),
            ("\t\t\tchunk := q.slots[q.head]\n", "\t\t\tloadQueuePop(q)\n\t\t\tchunk := q.slots[q.head]\n"),
        ],
        "internal/gateway/session.go": [
            ("\tif err := readStart(ctx, s.ws); err != nil {", "\tdefer loadQueueRelease(queue)\n\tif err := readStart(ctx, s.ws); err != nil {"),
        ],
    }
    with tempfile.TemporaryDirectory(prefix="tide-load-") as temp:
        overlay, overlay_hashes = {}, {}
        for relative, edits in replacements.items():
            source = root / relative
            content = source.read_text()
            for before, after in edits:
                if content.count(before) != 1:
                    raise RuntimeError(f"observation site changed: {relative}: {before!r}")
                content = content.replace(before, after)
            if relative.endswith("audio_queue.go"):
                content += "\nfunc init() { loadOverlayEnabled = true }\n"
            target = Path(temp) / source.name
            target.write_text(content)
            overlay[str(source)] = str(target)
            overlay_hashes[relative] = hashlib.sha256(content.encode()).hexdigest()
        overlay_file = Path(temp) / "overlay.json"
        overlay_file.write_text(json.dumps({"Replace": overlay}))
        env = os.environ.copy()
        env["TIDE_RUN_EXPERIMENTS"] = "1"
        if args.duration_ms is not None:
            env["TIDE_LOAD_DURATION_MS"] = str(args.duration_ms)
        else:
            env.pop("TIDE_LOAD_DURATION_MS", None)
        test_name = {"streaming": "TestExperimentStreamingLoad", "capacity": "TestExperimentSharedCapacity", "admission": "TestExperimentAdmissionProtection", "selection": "TestExperimentWorkerSelection"}[args.experiment]
        command = ["go", "test", "-mod=readonly", "-tags=tide_load", "-overlay", str(overlay_file),
                   "./internal/gateway", "-run", f"^{test_name}$/^{args.case}$",
                   f"-count={args.count}", "-timeout=15m", "-json"]
        if args.experiment == "selection":
            command += ["-bench=^BenchmarkWorkerSelection$", "-benchtime=100000x", "-benchmem"]
        if args.race:
            command.insert(2, "-race")
        sources = [root / "go.mod", root / "go.sum", Path(__file__).resolve()]
        sources += sorted((root / "internal").rglob("*.go"))
        sources += sorted((root / "proto").rglob("*.go"))
        metadata = {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
            "source_sha256": {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
            "overlay_sha256": overlay_hashes,
            "go_version": subprocess.check_output(["go", "version"], cwd=root, env=env, text=True).strip(),
            "platform": platform.platform(), "logical_cpus": os.cpu_count(),
            "experiment": args.experiment, "case": args.case, "count": args.count, "duration_override_ms": args.duration_ms,
            "race": args.race, "command": command,
            "environment": {key: env.get(key) for key in ("GOCACHE", "GOPROXY", "GOSUMDB", "GOGC", "GOMEMLIMIT", "GOMAXPROCS")},
        }
        meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
        started = time.monotonic()
        with output.open("x") as stream:
            completed = subprocess.run(command, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        metadata["command_elapsed_seconds"] = time.monotonic() - started
        metadata["exit_code"] = completed.returncode
        meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
        print(f"exit={completed.returncode}; results={output}; metadata={meta_path}")
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
