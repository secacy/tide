"""Reject corrupted copies of a completed real collection without changing originals."""
import argparse
import json
from pathlib import Path
import shutil
import tempfile

from verify import verify


def check(root):
    result = []
    for kind in ("missing_stream_close", "sample_digest", "duplicate_final", "completion", "sample_gap"):
        with tempfile.TemporaryDirectory(prefix="tide-segment-counter-") as temporary:
            target = Path(temporary) / "collection"
            shutil.copytree(root, target)
            data = target / "data"
            if kind in ("missing_stream_close", "sample_digest"):
                path = data / "server.log"
                rows = path.read_text().splitlines()
                for index, text in enumerate(rows):
                    try:
                        row = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if row.get("msg") == "stream_closed" and row["model_id"] == 2:
                        if kind == "missing_stream_close":
                            del rows[index]
                        else:
                            row["sample_f32le_sha256"] = "0"*64
                            rows[index] = json.dumps(row)
                        break
                path.write_text("\n".join(rows)+"\n")
            elif kind == "duplicate_final":
                path = data / "formal-results.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                row = next(r for r in rows if r["message"]["isFinal"])
                rows.append(row)
                path.write_text("".join(json.dumps(r)+"\n" for r in rows))
            elif kind == "completion":
                path = data / "formal.json"
                row = json.loads(path.read_text())
                row["Client"]["CompletionAckWritten"] = False
                path.write_text(json.dumps(row))
            else:
                path = data / "samples.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                for row in rows[len(rows)//2:]:
                    row["elapsed_ns"] += 4000000000
                    row["wall_ns"] += 4000000000
                path.write_text("".join(json.dumps(r)+"\n" for r in rows))
            try:
                summary = verify(target)
                rejected = kind == "sample_gap" and not summary["resources"]["quality_passed"]
            except ValueError:
                rejected = True
            if not rejected:
                raise ValueError(f"corruption accepted: {kind}")
            result.append({"kind": kind, "rejected": True})
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--output", type=Path)
    args = p.parse_args()
    result = check(args.root)
    text = json.dumps(result, indent=2)+"\n"
    if args.output:
        args.output.write_text(text)
    print(text)
