#!/usr/bin/env python3
"""Download the pinned public probe assets; reject existing target directories."""
import argparse
import hashlib
from pathlib import Path
import urllib.request

REVISION = "98590b7ed6443e77b714204da2757d75e1a642f4"
BASE = ("https://huggingface.co/csukuangfj/"
        "sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20/resolve/"
        + REVISION + "/")
ASSETS = {
    "encoder-epoch-99-avg-1.int8.onnx": "8fa764187a261844f859d7143ebaa563af5d10adfece4c18a8f414c88cba2a9b",
    "decoder-epoch-99-avg-1.int8.onnx": "1a70c593d71e53f023f5f55b0b4cfff5055abb786ee3992e5f63dc2e273cc4fa",
    "joiner-epoch-99-avg-1.int8.onnx": "1ed689c5ed19dbaa725d9d191bb4822b5f4855a39e1ffd28cbc1f340d25b2ee0",
    "tokens.txt": "a8e0e4ec53810e433789b54a5c0134a7eaa2ffca595a6334d54c00da858841d3",
    "test_wavs/0.wav": "7d93384ca14702cc584a7a33fe2fed92e89e708549161cb12ea38c916882103b",
    "test_wavs/1.wav": "8bfb42c963e623ebab31b81ff4404867d07d3102507c87ac14577c4c61663b8c",
    "README.md": "b6f9458f4208ae821beaaf11dc983486916a4089a2f3677b40f9ff06ec4e6440",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir()  # No overwrite, including after an incomplete download.
    for name, expected in ASSETS.items():
        path = args.output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        with urllib.request.urlopen(BASE + name, timeout=120) as source, path.open("xb") as target:
            while block := source.read(1024 * 1024):
                digest.update(block)
                target.write(block)
        if digest.hexdigest() != expected:
            raise ValueError(f"SHA256 mismatch: {name}; directory kept for diagnosis")
        print(name, path.stat().st_size, expected, flush=True)


if __name__ == "__main__":
    main()
