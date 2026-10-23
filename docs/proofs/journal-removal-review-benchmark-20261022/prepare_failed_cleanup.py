#!/usr/bin/env python3
"""Prepare cleanup-only readiness without changing a failed product capture."""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    folder = ROOT / args.run
    if folder.parent != ROOT or folder.is_symlink() or not folder.is_dir():
        raise ValueError("Require one exact ordinary capture folder")
    summary = json.loads((folder / "summary.json").read_text())
    if summary.get("complete") is not False or summary.get("all_owned_processes_stopped") is not True:
        raise ValueError("Keep the incomplete product result and stop all owned processes first")
    source = folder / "receipts.jsonl"
    if source.is_symlink() or source.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Require the exact bounded original receipt")
    raw = source.read_bytes()
    events = [line for line in raw.splitlines(keepends=True)
              if json.loads(line).get("operation") in ("owned-node-start", "owned-node-stop")]
    readiness = folder / "failed-cleanup-readiness"
    readiness.mkdir(mode=0o700)
    (readiness / "ownership.json").write_bytes((folder / "ownership.json").read_bytes())
    (readiness / "receipts.jsonl").write_bytes(b"".join(events))
    record = {"complete": True, "all_owned_processes_stopped": True,
              "scope": "Cleanup readiness only after diagnosed helper error; parent product capture remains complete=false",
              "reason": args.reason, "original_summary": str((folder / "summary.json").relative_to(ROOT)),
              "source_receipts_sha256": hashlib.sha256(raw).hexdigest()}
    (readiness / "summary.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    print(str(readiness))


if __name__ == "__main__":
    main()
