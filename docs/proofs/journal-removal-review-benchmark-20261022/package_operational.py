#!/usr/bin/env python3
"""Package three exact generated operational captures with the existing tools."""

import importlib.util
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
ORIGINAL = ROOT.parent / "journal-removal-benchmark-20261022"


def module(name):
    path = ORIGINAL / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"operational_{name}", path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    result.ROOT = ROOT
    return result


def load(path):
    if path.is_symlink() or path.stat().st_size > 4 * 1024 * 1024:
        raise ValueError("An operational index exceeds its exact ordinary-file bound")
    return json.loads(path.read_text())


def save(path, value):
    if path.exists():
        raise ValueError("Keep the existing operational evidence index")
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    folders = [ROOT / name for name in ("erase-operational", "erase-operational-2", "erase-operational-3")]
    summaries = [load(folder / "summary.json") for folder in folders]
    failed_ready = [load(folder / "failed-cleanup-readiness" / "summary.json") for folder in folders[:2]]
    diagnoses = [load(folder / "failure-diagnosis.json") for folder in folders[:2]]
    cleanups = [load(folder / "failed-cleanup-readiness" / "cleanup.json") for folder in folders[:2]]
    cleanups.append(load(folders[2] / "cleanup.json"))
    if (any(row.get("complete") is not False for row in summaries[:2])
            or summaries[2].get("complete") is not True
            or any(row.get("product_failure_established") is not False for row in diagnoses)
            or any(row.get("complete") is not True for row in failed_ready)
            or not all(row.get("removed") is True for row in cleanups)
            or not all(row.get("all_owned_processes_stopped") is True for row in summaries)):
        raise ValueError("Preserve the failed run distinction and finish both owned cleanups")
    compress, compact = module("compress_receipts"), module("compact_receipts")
    compression, entries = [], []
    for folder in folders:
        receipt = compress.compress(folder / "receipts.jsonl")
        entry = compact.package(folder)
        receipt["compressed_path"] = entry["retained_local_full_path"]
        compression.append(receipt)
        entries.append(entry)
        checkpoints = folder / "checkpoints.json"
        if checkpoints.exists():
            compression.append(compress.compress(checkpoints))
    save(ROOT / "operational-compression.json", {"files": compression, "round_trip_verified": True})
    save(ROOT / "operational-evidence-index.json", {
        "format": "lastdb-request-samples-v1", "full_generated_payload_captures_retained_locally": True,
        "scope": "First two captures contain diagnosed helper errors; third capture is the corrected product operation",
        "failed_summaries_preserved": [str((folder / "summary.json").relative_to(ROOT)) for folder in folders[:2]],
        "failed_cleanup_readiness_scope": "Cleanup only; does not change the failed product summary",
        "failed_input_seed_erratum": "First two inputs inherit a benchmark seed description; their raw requests use one durable batch of 256",
        "successful_summary": "erase-operational-3/summary.json", "runs": entries})
    print(json.dumps({"packaged_runs": len(entries), "compact_bytes": sum(e["compact_bytes"] for e in entries)}))


if __name__ == "__main__":
    main()
