#!/usr/bin/env python3
"""Compress exact saved receipts, verify their bytes, then remove the raw copy."""

import gzip
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LIMIT = 64 * 1024 * 1024


def checksum(path, compressed=False):
    digest, size = hashlib.sha256(), 0
    opener = gzip.open if compressed else open
    with opener(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            size += len(block)
            if size > LIMIT:
                raise ValueError("A saved receipt exceeds 64 MiB")
            digest.update(block)
    return {"bytes": size, "sha256": digest.hexdigest()}


def compress(path):
    if path.is_symlink() or not path.is_file() or path.parent.parent != ROOT:
        raise ValueError("Compress only an exact saved run receipt")
    original = checksum(path)
    target = path.with_name(path.name + ".gz")
    if target.exists():
        raise ValueError("The compressed receipt already exists")
    with path.open("rb") as source, target.open("xb") as output:
        with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                compressed.write(block)
    if checksum(target, compressed=True) != original:
        raise RuntimeError("The compressed receipt does not reproduce its original bytes")
    receipt = {"path": str(path.relative_to(ROOT)), "compressed_path": str(target.relative_to(ROOT)),
               "uncompressed": original, "compressed": checksum(target), "round_trip_bytes_match": True}
    path.unlink()
    return receipt


def main():
    manifest = json.loads((ROOT / "pair-runs.json").read_text())
    if manifest.get("complete") is not True or len(manifest["runs"]) != 6:
        raise ValueError("Retain raw files until all six captures and cleanup finish")
    receipts = []
    try:
        for item in manifest["runs"]:
            if item["exit_code"] != 0 or not item["scratch_removed"]:
                raise ValueError("A run is incomplete or still owns scratch data")
            folder = ROOT / item["run"]
            for name in ("receipts.jsonl", "checkpoints.json"):
                receipts.append(compress(folder / name))
    finally:
        (ROOT / "compression.json").write_text(json.dumps({"files": receipts,
            "all_complete": len(receipts) == 12}, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"files": len(receipts), "uncompressed_bytes": sum(r["uncompressed"]["bytes"] for r in receipts),
                      "compressed_bytes": sum(r["compressed"]["bytes"] for r in receipts), "round_trip_verified": True}))


if __name__ == "__main__":
    main()
