#!/usr/bin/env python3
"""Package the complete intermediate round with exact byte round trips."""

import hashlib
import json
from pathlib import Path
from reuse import ROOT, original


def main():
    target = ROOT / "intermediate-fa06955"
    archive = json.loads((ROOT / "intermediate-fa06955-archive.json").read_text())
    if archive.get("all_bytes_unchanged") is not True:
        raise ValueError("The intermediate round lacks its exact unchanged archive")
    (target / ".gitignore").write_text("*/receipts.full.local.gz\n")
    for name in ("compress_receipts", "compact_receipts"):
        tool = original(name)
        tool.ROOT = target
        tool.main()
    tool = original("compress_receipts")
    tool.ROOT = target
    compressed = []
    for label in ("before", "after"):
        for pair in (1, 2, 3):
            for name in ("focused-checkpoints.json", "erase-checkpoints.json"):
                compressed.append(tool.compress(target / f"{label}-{pair}" / name))
    for name in ("retention-operational", "edge-operational", "overflow-operational", "auto-reclaim-operational"):
        folder = target / name
        paths = sorted(folder.iterdir())
        if len(paths) > 512 or any(path.is_symlink() or not path.is_file() for path in paths):
            raise ValueError("The intermediate operational folder contains unexpected entries")
        for path in paths:
            if path.suffix in (".json", ".jsonl") and path.stat().st_size > 128 * 1024:
                compressed.append(tool.compress(path))
    receipt = {"complete": True, "files": compressed, "all_round_trip_bytes_match": True,
               "original_archive": "intermediate-fa06955-archive.json",
               "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (target / "additional-compression.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"intermediate_packaged": True, "additional_files": len(compressed)}))


if __name__ == "__main__":
    main()
