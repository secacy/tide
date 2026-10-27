"""Check that corrupted copies of real evidence are rejected; never edit the original."""
import argparse
import copy
import json
from pathlib import Path
import shutil
import tempfile

from verify import reconcile


def run(collection):
    # First require an intact baseline, so missing files cannot count as a successful countercheck.
    reconcile(collection)
    mutations = [
        ("completion_ack", "p1_capacity/N-case.json", lambda c: c["report"]["client"].update(CompletionAckWritten=False), False),
        ("wrong_worker", "p1_capacity/N-case.json", lambda c: c["assignment"].update(worker="A"), False),
        ("not_busy_error", "p1_old/N-case.json", lambda c: c["assignment"].update(terminal_message="audio stream limit exceeded"), False),
        ("missing_close", "workerB/model-events.jsonl", lambda rows: rows.pop(), True),
        ("trigger_occupancy", "arms.json", lambda rows: rows[1]["trigger_snapshot"].update(ActiveSessions=2), False),
        ("zero_rss", "samples.jsonl", lambda rows: rows[0]["workers"]["A"].update(rss_bytes=0), True),
    ]
    results = []
    with tempfile.TemporaryDirectory(prefix="tide-admission-counterchecks-") as temporary:
        for name, relative, mutate, jsonl in mutations:
            target = Path(temporary) / name
            shutil.copytree(collection, target)
            path = target / "data" / relative
            data = [json.loads(x) for x in path.read_text().splitlines()] if jsonl else json.loads(path.read_text())
            mutate(data)
            if name == "completion_ack":
                returned = target / "data/p1_capacity/N-return.json"
                value = json.loads(returned.read_text())
                value["report"]["client"]["CompletionAckWritten"] = False
                returned.write_text(json.dumps(value)+"\n")
            path.write_text("".join(json.dumps(x)+"\n" for x in data) if jsonl else json.dumps(data)+"\n")
            try:
                reconcile(target)
            except ValueError as error:
                results.append({"mutation": name, "rejected": True, "reason": str(error)})
            else:
                raise ValueError(f"corruption accepted: {name}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.collection), ensure_ascii=False, indent=2))
