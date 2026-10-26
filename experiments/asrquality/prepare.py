#!/usr/bin/env python3
"""Freeze reviewed references and convert local m4a to short PCM16 WAV; no ASR."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import wave

SCHEMA = "tide.asr-quality-input.v1"
IDS = [f"q{i:02}" for i in range(1, 7)]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_pcm(path):
    """Return validated raw samples; WAV headers never enter the Worker."""
    with wave.open(str(path), "rb") as audio:
        if (audio.getnchannels(), audio.getsampwidth(), audio.getframerate(), audio.getcomptype()) != (1, 2, 16000, "NONE"):
            raise ValueError("expected mono PCM16 16kHz WAV")
        frames = audio.getnframes()
        pcm = audio.readframes(frames)
    if not 0 < len(pcm) <= 960000 or len(pcm) % 2 or len(pcm) != frames * 2:
        raise ValueError("empty, truncated, unaligned or over-30s PCM")
    return pcm


def prepare(source, output, cases, reviewer):
    """Require explicit human review; preserve source and reject output overwrite."""
    specification = json.loads(cases.read_text(encoding="utf-8"))
    if specification.get("schema") != "tide.asr-quality-cases.v1" or [c["id"] for c in specification["cases"]] != IDS:
        raise ValueError("expected six ordered reviewed cases")
    if not reviewer.strip() or any(not (source / (i + ".m4a")).is_file() for i in IDS):
        raise ValueError("reviewer and all six original recordings are required")
    output.mkdir(parents=True)  # Create parents; reject an existing leaf, even after failure.
    (output / "pcm").mkdir()
    (output / "references").mkdir()
    (output / "conversion").mkdir()
    manifest = {"schema": SCHEMA, "created_utc": datetime.now(timezone.utc).isoformat(),
                "review": {"reviewer": reviewer, "basis": "recorder confirmed by replay that all six recordings match the prompts before ASR", "independent_reviewers": 1},
                "cases_sha256": digest(cases.read_bytes()), "cases": []}
    (output / "cases.json").write_bytes(cases.read_bytes())
    (output / "conversion" / "ffmpeg-version.txt").write_bytes(subprocess.check_output(["ffmpeg", "-version"]))
    for case in specification["cases"]:
        name = case["id"]
        original = source / (name + ".m4a")
        original_sha = digest(original.read_bytes())
        probe = subprocess.check_output(["ffprobe", "-v", "error", "-show_format", "-show_streams", "-of", "json", str(original)])
        (output / "conversion" / (name + "-source.json")).write_bytes(probe)
        target = output / "pcm" / (name + ".wav")
        args = ["ffmpeg", "-nostdin", "-v", "error", "-n", "-i", str(original), "-map", "0:a:0", "-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-map_metadata", "-1", str(target)]
        result = subprocess.run(args, capture_output=True)
        (output / "conversion" / (name + "-ffmpeg.log")).write_bytes(result.stderr)
        result.check_returncode()
        pcm = read_pcm(target)
        if digest(original.read_bytes()) != original_sha:
            raise ValueError("source changed during conversion")
        reference = (case["reference"] + "\n").encode("utf-8")
        reference_path = "references/" + name + ".txt"
        (output / reference_path).write_bytes(reference)
        manifest["cases"].append({"id": name, "source_file": str(original.resolve()), "source_sha256": original_sha,
            "wav_file": "pcm/" + name + ".wav", "wav_sha256": digest(target.read_bytes()),
            "pcm_bytes": len(pcm), "pcm_sha256": digest(pcm), "duration_seconds": len(pcm) / 32000,
            "reference_file": reference_path, "reference_sha256": digest(reference), "reference": case["reference"],
            "facts": case["facts"], "conversion_command": args})
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--cases", type=Path, default=Path(__file__).with_name("cases.json"))
    parser.add_argument("--reviewer", required=True, help="only after human replay confirmation; no model-generated references")
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.output, args.cases, args.reviewer), ensure_ascii=False, indent=2))
