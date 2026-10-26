"""Freeze code, configuration and asset identity before a backend runtime run."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import subprocess
import wave

PCM_SHA = "237ee4752b8a1fb41c6284ae94f2d7baf9a1fc8f28c26d3409c69e86cecbade1"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(root, output, worker, model):
    lock_path = root / "experiments/asrprobe/model-assets.json"
    models = json.loads(lock_path.read_text())["profiles"]["zipformer-bilingual-2023"]["files"]
    for name, expected in models.items():
        if digest(model / name) != expected:
            raise ValueError(f"model identity mismatch: {name}")
    with wave.open(str(model / "test_wavs/0.wav"), "rb") as wav:
        if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, 16000):
            raise ValueError("unexpected PCM format")
        pcm = wav.readframes(wav.getnframes())
    if len(pcm) != 321700 or hashlib.sha256(pcm).hexdigest() != PCM_SHA:
        raise ValueError("unexpected baseline input")
    paths = {root / "go.mod", root / "go.sum", lock_path,
             root / "docs/worklog/2026-10-26-real-asr-backend-baseline-design.md",
             root / "docs/experiments/real-asr-backend-baseline.md"}
    for name in ("internal", "proto", "workers/sherpa", "experiments/asrbackend"):
        paths.update(p for p in (root / name).rglob("*")
                     if p.is_file() and p.suffix in (".go", ".mod", ".sum", ".proto", ".py"))
    worker_sha = digest(worker)
    hardware = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize", "hw.logicalcpu"],
                              text=True, capture_output=True, timeout=5)
    output.mkdir()  # Evidence collection never overwrites an earlier run.
    files = {}
    for path in sorted(paths):
        relative = path.relative_to(root)
        destination = output / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        data = path.read_bytes()
        destination.write_bytes(data)
        files[str(relative)] = hashlib.sha256(data).hexdigest()
    manifest = {
        "schema": "tide.real-backend-freeze.v1", "source": files,
        "assets": {"model_files": models, "input_bytes": len(pcm), "input_sha256": PCM_SHA,
                   "worker_sha256": worker_sha},
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=root, text=True),
        "worker_path": str(worker), "model_path": str(model),
        "platform": platform.platform(),
        "hardware": {"values": hardware.stdout.splitlines(), "query_exit": hardware.returncode,
                     "query_error": hardware.stderr.strip()},
        "worker_build_info": subprocess.check_output(["go", "version", "-m", str(worker)], cwd=root, text=True),
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--worker-bin", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = prepare(Path(__file__).resolve().parents[2], args.output,
                       args.worker_bin.resolve(), args.model_dir.resolve())
    print(json.dumps({"source_files": len(manifest["source"]), "worker_sha256": manifest["assets"]["worker_sha256"]}))
