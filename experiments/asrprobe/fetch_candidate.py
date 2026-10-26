#!/usr/bin/env python3
"""Download the one pinned candidate release, verify its archive, then extract.

The output directory must be new. Incomplete downloads remain for diagnosis.
No license is inferred from sherpa's source-code license.
"""
import argparse
import hashlib
import json
from pathlib import Path
import tarfile
import urllib.request

from model_assets import CANDIDATE, LOCK_PATH


def extract_verified(archive, output, entry):
    """Check the release archive before reading members; copy only pinned files."""
    source = entry["source"]
    with archive.open("rb") as f:
        actual = hashlib.file_digest(f, "sha256").hexdigest()
    if actual != source["sha256"] or archive.stat().st_size != source["size"]:
        raise ValueError("candidate release archive size/SHA256 mismatch")
    expected = {**entry["files"], **entry["auxiliary_files"]}
    prefix = archive.name.removesuffix(".tar.bz2") + "/"
    with tarfile.open(archive, "r:bz2") as bundle:
        for name, digest in expected.items():
            member = bundle.getmember(prefix + name)
            if not member.isfile():
                raise ValueError("expected regular asset: " + name)
            data = bundle.extractfile(member).read()
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("candidate file SHA256 mismatch: " + name)
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as target:
                target.write(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new directory; model inputs will be under model/")
    args = parser.parse_args()
    entry = json.loads(LOCK_PATH.read_bytes())["profiles"][CANDIDATE]
    source = entry["source"]
    args.output.mkdir()
    archive = args.output / source["url"].rsplit("/", 1)[1]
    req = urllib.request.Request(source["url"], headers={"User-Agent": "Tide-ASR-probe"})
    with urllib.request.urlopen(req, timeout=120) as incoming, archive.open("xb") as target:
        while block := incoming.read(1024 * 1024):
            target.write(block)
    extract_verified(archive, args.output / "model", entry)
    print(args.output / "model")
    print("Weight license status:", source["license_status"])


if __name__ == "__main__":
    main()
