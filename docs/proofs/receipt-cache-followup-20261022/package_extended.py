#!/usr/bin/env python3
"""Package exact operational, profile, and default-maintenance evidence."""

import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from reuse import ROOT, original


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def folders():
    names = ["retention-operational", "edge-operational", "overflow-operational",
             "auto-reclaim-operational", "orphan-repeat-operational"]
    names += [variant + "-profile-1" for variant in ("before", "draft", "after")]
    names += ["auto-" + variant + "-1" for variant in ("before", "draft", "after")]
    result = [ROOT / name for name in names]
    for folder in result:
        summary = json.loads((folder / "summary.json").read_text())
        cleanup = json.loads((folder / "cleanup.json").read_text())
        if (folder.is_symlink() or folder.resolve(strict=True).parent != ROOT
                or summary.get("complete") is not True or cleanup.get("removed") is not True):
            raise ValueError("All extended captures require successful exact owned cleanup")
    return result


def native_sample(folder):
    source, target = folder / "sample.txt", folder / "native-sample.txt.gz"
    summary = json.loads((folder / "summary.json").read_text())
    expected = summary["sample_sha256"]
    if source.is_symlink() or target.exists() or source.stat().st_size > 16 * 1024 * 1024 or digest(source) != expected:
        raise ValueError("The original native sample differs from its exact diagnostic receipt")
    with source.open("rb") as raw, target.open("xb") as output:
        with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as packed:
            for block in iter(lambda: raw.read(1024 * 1024), b""):
                packed.write(block)
    with gzip.open(target, "rb") as packed:
        roundtrip = hashlib.sha256(packed.read(16 * 1024 * 1024 + 1)).hexdigest()
    if roundtrip != expected or target.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("The native sample does not reproduce its original bytes within the blob limit")
    return {"original": str(source.relative_to(ROOT)), "original_sha256": expected,
            "compressed": str(target.relative_to(ROOT)), "compressed_sha256": digest(target),
            "original_bytes": source.stat().st_size, "compressed_bytes": target.stat().st_size,
            "round_trip_bytes_match": True, "original_ignored_sample_retained_locally": True}


def main():
    target = ROOT / "extended-compression.json"
    if target.exists() or json.loads((ROOT / "build-scratch-cleanup.json").read_text()).get("removed") is not True:
        raise ValueError("Finish all owned binary cleanup; do not replace an earlier package index")
    compress, compact = original("compress_receipts"), original("compact_receipts")
    compress.ROOT = compact.ROOT = ROOT
    result = {"complete": False, "compressed_files": [], "compact_requests": [], "native_samples": [],
              "original_summaries_sha256": {}, "driver_sha256": digest(Path(__file__))}
    try:
        for folder in folders():
            summary = folder / "summary.json"
            result["original_summaries_sha256"][str(summary.relative_to(ROOT))] = digest(summary)
            files = sorted(folder.iterdir())
            if len(files) > 512 or any(path.is_symlink() or not path.is_file() for path in files):
                raise ValueError("A finite capture folder contains unexpected entries")
            extended = "-profile-" in folder.name or folder.name.startswith("auto-") and folder.name.endswith("-1")
            if extended:
                result["compressed_files"].append(compress.compress(folder / "receipts.jsonl"))
                result["compact_requests"].append(compact.package(folder))
            if "-profile-" in folder.name:
                result["native_samples"].append(native_sample(folder))
            for path in files:
                if path.name == "receipts.jsonl" and extended:
                    continue
                if path.name == "node.log":
                    result["compressed_files"].append(compress.compress(path))
                    continue
                if path.suffix in (".json", ".jsonl") and path.stat().st_size > 128 * 1024:
                    if path.name in ("summary.json", "ownership.json", "profile-provenance.json"):
                        if path.stat().st_size > 2 * 1024 * 1024:
                            raise ValueError("An unchanged summary or ownership receipt exceeds the blob limit")
                        continue
                    result["compressed_files"].append(compress.compress(path))
            if digest(summary) != result["original_summaries_sha256"][str(summary.relative_to(ROOT))]:
                raise ValueError("An original summary changed during evidence compression")
        for row in result["compressed_files"]:
            moved = next((entry for entry in result["compact_requests"]
                          if row["compressed_path"] == entry["run"] + "/receipts.jsonl.gz"), None)
            if moved:
                row["compressed_path"] = moved["retained_local_full_path"]
            elif row["compressed"]["bytes"] > 2 * 1024 * 1024:
                raise ValueError("A compressed receipt exceeds the repository blob limit")
        result["complete"] = True
    finally:
        target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"extended_capture_folders": 11, "complete": True}))


if __name__ == "__main__":
    main()
