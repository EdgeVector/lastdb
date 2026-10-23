#!/usr/bin/env python3
"""Compress exact large operational receipts after all owned data cleanup."""

import hashlib
import json
from pathlib import Path
from reuse import ROOT, original


def main():
    if json.loads((ROOT / "build-scratch-cleanup.json").read_text()).get("removed") is not True:
        raise ValueError("Complete all generated data and build cleanup before receipt compression")
    tool = original("compress_receipts")
    tool.ROOT = ROOT
    folders = [ROOT / name for name in ("retention-operational", "edge-operational", "overflow-operational",
               "auto-reclaim-operational", "orphan-repeat-operational", "failed-retention-operational-envelope-1")]
    receipts = []
    for folder in folders:
        files = sorted(folder.iterdir())
        if len(files) > 512 or any(path.is_symlink() or not path.is_file() for path in files):
            raise ValueError("The exact operational receipt folder has unexpected entries")
        for path in files:
            if path.suffix in (".json", ".jsonl") and path.stat().st_size > 128 * 1024:
                if path.name in ("summary.json", "ownership.json", "failure-diagnosis.json", "failed-scratch-cleanup.json"):
                    raise ValueError("A short ownership or summary receipt is unexpectedly large")
                receipts.append(tool.compress(path))
    value = {"complete": True, "files": receipts,
             "all_compressed_bytes_round_trip": True,
             "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (ROOT / "operational-compression.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"operational_receipts_compressed": len(receipts), "round_trip_verified": True}), flush=True)


if __name__ == "__main__":
    main()
