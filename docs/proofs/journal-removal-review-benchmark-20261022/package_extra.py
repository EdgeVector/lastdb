#!/usr/bin/env python3
"""Compress the exact extra-phase file inventories after their completed capture."""

import hashlib
import json
from reuse import ROOT, original


def main():
    manifest = json.loads((ROOT / "pair-runs.json").read_text())
    if manifest.get("complete") is not True or len(manifest["runs"]) != 6:
        raise ValueError("All six captures must complete before inventory compression")
    tool = original("compress_receipts")
    tool.ROOT = ROOT
    receipts = []
    for run in manifest["runs"]:
        if run.get("exit_code") != 0 or run.get("scratch_removed") is not True:
            raise ValueError("The owned capture or its cleanup is incomplete")
        for name in ("focused-checkpoints.json", "erase-checkpoints.json"):
            receipts.append(tool.compress(ROOT / run["run"] / name))
    value = {"all_complete": len(receipts) == 12, "files": receipts,
             "driver_sha256": hashlib.sha256(__import__("pathlib").Path(__file__).read_bytes()).hexdigest()}
    (ROOT / "extra-compression.json").write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"files": len(receipts), "round_trip_bytes_match": True}))


if __name__ == "__main__":
    main()
