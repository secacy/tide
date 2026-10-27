"""Freeze source and repeated PCM before the single 120-second experiment."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import struct
import subprocess
import wave


def digest(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def sample_digest(pcm):
    h = hashlib.sha256()
    for (sample,) in struct.iter_unpack("<h", pcm):
        h.update(struct.pack("<f", sample / 32768))
    return h.hexdigest()


def prepare(root, output, binary, driver, model):
    lock = root / "experiments/asrprobe/model-assets.json"
    models = json.loads(lock.read_text())["profiles"]["zipformer-bilingual-2023"]["files"]
    for name, expected in models.items():
        if digest(model / name) != expected:
            raise ValueError(f"model mismatch: {name}")
    with wave.open(str(model / "test_wavs/0.wav"), "rb") as f:
        if (f.getnchannels(), f.getsampwidth(), f.getframerate()) != (1, 2, 16000):
            raise ValueError("input format mismatch")
        base = f.readframes(f.getnframes())
    if len(base) != 321700 or hashlib.sha256(base).hexdigest() != "237ee4752b8a1fb41c6284ae94f2d7baf9a1fc8f28c26d3409c69e86cecbade1":
        raise ValueError("input identity mismatch")
    pcm = (base * 12)[:3840000]
    module = json.loads(subprocess.check_output(["go", "list", "-m", "-json", "github.com/k2-fsa/sherpa-onnx-go-macos"], cwd=root / "workers/sherpa", text=True))
    libraries = {str(p): digest(p) for p in sorted(Path(module["Dir"]).rglob("*.dylib"))}
    if not libraries:
        raise ValueError("missing native libraries")
    paths = {root / "go.mod", root / "go.sum", lock, root / "docs/experiments/real-asr-segmented-120s.md"}
    for directory in ("internal", "proto", "workers/sherpa", "experiments/asrsegment"):
        paths.update(p for p in (root / directory).rglob("*") if p.is_file() and p.suffix in (".go", ".mod", ".sum", ".proto", ".py"))
    hardware = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize", "hw.logicalcpu"], text=True).splitlines()
    manifest = {"schema": "tide.segment120.v1", "worker_sha256": digest(binary), "worker_path": str(binary), "driver_sha256": digest(driver), "model_path": str(model),
                "model_sha256": models, "native_libraries": libraries, "source": {}, "hardware": hardware, "platform": platform.platform(),
                "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
                "build_info": subprocess.check_output(["go", "version", "-m", str(binary)], text=True),
                "inputs": {}, "plan": ["warmup", "formal", "probe"], "segment_bytes": 960000,
                "idle_seconds": [5, 10, 5], "sample_gap_limit_ns": 3000000000}
    for name, data in (("warmup", base), ("formal", pcm), ("probe", base[:96000])):
        manifest["inputs"][name] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                    "segments": [{"samples": len(data[i:i+960000])//2, "sample_f32le_sha256": sample_digest(data[i:i+960000])}
                                                 for i in range(0, len(data), 960000)]}
    output.mkdir()
    (output / "input.pcm").write_bytes(pcm)
    (output / "base.pcm").write_bytes(base)
    for path in sorted(paths):
        relative = str(path.relative_to(root))
        destination = output / "source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
        manifest["source"][relative] = digest(destination)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False)+"\n")
    return manifest


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("output", type=Path)
    p.add_argument("--worker-bin", type=Path, required=True)
    p.add_argument("--driver-bin", type=Path, required=True)
    p.add_argument("--model-dir", type=Path, required=True)
    args = p.parse_args()
    result = prepare(Path(__file__).resolve().parents[2], args.output, args.worker_bin.resolve(), args.driver_bin.resolve(), args.model_dir.resolve())
    print(json.dumps({"source_files": len(result["source"]), "input_bytes": result["inputs"]["formal"]["bytes"], "worker_sha256": result["worker_sha256"]}))
