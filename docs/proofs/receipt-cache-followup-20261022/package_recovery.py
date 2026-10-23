#!/usr/bin/env python3
"""Keep exact historical, rejected, and writer-gate bytes in bounded packages."""

import gzip
import hashlib
import json
import os
from pathlib import Path

from reuse import ROOT, original

LIMIT = 64 * 1024 * 1024


def checksum(path, compressed=False):
    value, size = hashlib.sha256(), 0
    opener = gzip.open if compressed else open
    with opener(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            size += len(block)
            if size > LIMIT:
                raise ValueError("An exact recovery receipt exceeds sixty-four MiB")
            value.update(block)
    return {"sha256": value.hexdigest(), "bytes": size}


def compress(path):
    if (path.is_symlink() or not path.is_file() or path.stat().st_uid != os.getuid()
            or not path.resolve(strict=True).is_relative_to(ROOT) or len(path.relative_to(ROOT).parts) > 3):
        raise ValueError("Compress only an exact owned evidence file inside the finite proof root")
    target = path.with_name(path.name + ".gz")
    if target.exists():
        raise ValueError("Do not replace an original compressed recovery receipt")
    expected = checksum(path)
    with path.open("rb") as source, target.open("xb") as sink:
        with gzip.GzipFile(filename="", fileobj=sink, mode="wb", mtime=0) as packed:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                packed.write(block)
    if checksum(target, compressed=True) != expected:
        raise ValueError("The recovery receipt does not reproduce its original bytes")
    value = {"original_path": str(path.relative_to(ROOT)), "compressed_path": str(target.relative_to(ROOT)),
             "original": expected, "compressed": checksum(target), "round_trip_bytes_match": True}
    path.unlink()
    return value


def folder_package(folder):
    summary = folder / "summary.json"
    original_summary = checksum(summary)
    files = sorted(folder.iterdir())
    if len(files) > 512 or any(path.is_symlink() or not path.is_file() for path in files):
        raise ValueError("The finite exact recovery folder has unexpected entries")
    values, compacted = [], None
    profile = "profile-1" in folder.name
    for path in files:
        if path.name == "receipts.jsonl" and profile:
            row = compress(path)
            values.append(row)
            tool = original("compact_receipts")
            tool.ROOT = folder.parent
            compacted = tool.package(folder)
            for field in ("compact_path", "retained_local_full_path"):
                compacted[field] = str((folder.parent / compacted[field]).relative_to(ROOT))
            row["compressed_path"] = str((folder / "receipts.full.local.gz").relative_to(ROOT))
        elif path.name in ("sample.txt", "node.log"):
            row = compress(path)
            if path.name == "sample.txt":
                source, destination = folder / "sample.txt.gz", folder / "native-sample.txt.gz"
                if destination.exists():
                    raise ValueError("Do not replace an exact native sample package")
                source.rename(destination)
                row["compressed_path"] = str(destination.relative_to(ROOT))
            values.append(row)
        elif path.suffix in (".json", ".jsonl") and path.stat().st_size > 128 * 1024 and path != summary:
            values.append(compress(path))
    if checksum(summary) != original_summary:
        raise ValueError("A preserved original recovery summary changed bytes")
    for row in values:
        if not row["compressed_path"].endswith("receipts.full.local.gz") and row["compressed"]["bytes"] > 2 * 1024 * 1024:
            raise ValueError("A recovery package exceeds the repository blob bound")
    return {"folder": str(folder.relative_to(ROOT)), "original_summary": original_summary,
            "files": values, "compact_requests": compacted}


def main():
    target = ROOT / "recovery-compression.json"
    if target.exists() or json.loads((ROOT / "build-scratch-cleanup.json").read_text()).get("removed") is not True:
        raise ValueError("Complete all owned cleanup; do not replace a recovery package receipt")
    folders = [ROOT / name for name in ("before-writer-capability", "after-writer-capability", "historical-before-profile-1")]
    folders += [ROOT / "rejected-build-1" / name for name in ("after-profile-1", "retention-operational")]
    result = {"complete": False, "folders": [], "compiler_logs": [], "driver_sha256": checksum(Path(__file__))["sha256"]}
    try:
        for folder in folders:
            result["folders"].append(folder_package(folder))
        logs = [ROOT / (variant + "-isolated-" + step + "." + stream + ".log")
                for variant in ("before", "after") for step in ("node", "physical") for stream in ("stdout", "stderr")]
        for path in logs:
            row = compress(path)
            if row["compressed"]["bytes"] > 2 * 1024 * 1024:
                raise ValueError("An exact compiler log exceeds the blob bound")
            result["compiler_logs"].append(row)
        result["complete"] = True
    finally:
        target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"recovery_evidence_packaged": True, "folders": len(folders)}))


if __name__ == "__main__":
    main()
