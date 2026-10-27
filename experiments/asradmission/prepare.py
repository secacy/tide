"""Freeze the paired two-Worker experiment before any recognition run."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import wave

INPUTS = {
    "long": {"bytes": 321700, "sha256": "237ee4752b8a1fb41c6284ae94f2d7baf9a1fc8f28c26d3409c69e86cecbade1"},
    "short": {"bytes": 163200, "sha256": "35d512d4a5d5a3a705f53361d4edffc0e0f49edb54a42a5ba6fe591e42019e2a"},
}


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def prepare(root, output, worker, model):
    lock_path = root / "experiments/asrprobe/model-assets.json"
    models = json.loads(lock_path.read_text())["profiles"]["zipformer-bilingual-2023"]["files"]
    for name, expected in models.items():
        if digest(model / name) != expected:
            raise ValueError(f"model mismatch: {name}")
    for index, name in enumerate(("long", "short")):
        with wave.open(str(model / f"test_wavs/{index}.wav"), "rb") as wav:
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate()) != (1, 2, 16000):
                raise ValueError("PCM format mismatch")
            pcm = wav.readframes(wav.getnframes())
        if len(pcm) != INPUTS[name]["bytes"] or hashlib.sha256(pcm).hexdigest() != INPUTS[name]["sha256"]:
            raise ValueError(f"PCM identity mismatch: {name}")
    paths = {root / "go.mod", root / "go.sum", lock_path,
             root / "docs/experiments/real-asr-admission-comparison.md"}
    for name in ("internal", "proto", "workers/sherpa", "experiments/asradmission"):
        paths.update(p for p in (root / name).rglob("*") if p.is_file()
                     and p.suffix in (".go", ".mod", ".sum", ".proto", ".py"))
    hardware = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize", "hw.logicalcpu"],
                              text=True, capture_output=True, timeout=5)
    module = json.loads(subprocess.check_output(["go", "list", "-m", "-json", "github.com/k2-fsa/sherpa-onnx-go-macos"],
                                               cwd=root / "workers/sherpa", text=True))
    libraries = {str(p): digest(p) for p in sorted(Path(module["Dir"]).rglob("*.dylib"))}
    if not libraries:
        raise ValueError("native library identity missing")
    manifest = {"schema": "tide.admission-freeze.v1", "inputs": INPUTS, "model_files": models,
                "worker_sha256": digest(worker), "worker_path": str(worker), "model_path": str(model),
                "native_libraries": libraries, "source": {}, "platform": platform.platform(),
                "hardware": {"values": hardware.stdout.splitlines(), "exit": hardware.returncode, "error": hardware.stderr.strip()},
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "git_status": subprocess.check_output(["git", "status", "--short"], cwd=root, text=True),
                "worker_build_info": subprocess.check_output(["go", "version", "-m", str(worker)], text=True)}
    output.mkdir()
    for path in sorted(paths):
        relative = str(path.relative_to(root))
        destination = output / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
        manifest["source"][relative] = digest(destination)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False)+"\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--worker-bin", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = prepare(Path(__file__).resolve().parents[2], args.output, args.worker_bin.resolve(), args.model_dir.resolve())
    print(json.dumps({"source_files": len(manifest["source"]), "native_libraries": len(manifest["native_libraries"]),
                      "worker_sha256": manifest["worker_sha256"]}))
