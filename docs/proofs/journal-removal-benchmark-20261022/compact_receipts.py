#!/usr/bin/env python3
"""Keep compact request timings in Git and full generated payloads locally."""

import gzip
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
HEADER = {"operation": "evidence-format", "format": "lastdb-request-samples-v1", "compact": True}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def file_digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def compact_row(raw):
    row = json.loads(raw)
    if row.get("operation") != "request":
        return row
    request, response = row.pop("request"), row.pop("response")
    data = response.get("data", response)
    row.update(request_sha256=digest(request), response_sha256=digest(response),
               original_receipt_line_sha256=hashlib.sha256(raw).hexdigest(),
               response_summary={key: data[key] for key in
                   ("ok", "count", "durability", "returned_count", "has_more") if key in data})
    return row


def package(folder):
    source = folder / "receipts.jsonl.gz"
    target = folder / "request-samples.json.gz"
    retained = folder / "receipts.full.local.gz"
    if source.is_symlink() or not source.is_file() or target.exists() or retained.exists():
        raise ValueError("The exact completed receipt paths differ or already exist")
    lines, raw_digest, raw_bytes = 0, hashlib.sha256(), 0
    with gzip.open(source, "rb") as original, target.open("xb") as output:
        with gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=0) as compressed:
            compressed.write((json.dumps(HEADER, sort_keys=True) + "\n").encode())
            for raw in original:
                lines += 1
                raw_bytes += len(raw)
                if lines > 10000 or raw_bytes > 64 * 1024 * 1024 or len(raw) > 8 * 1024 * 1024:
                    raise ValueError("The exact generated receipt exceeds its finite bounds")
                raw_digest.update(raw)
                compressed.write((json.dumps(compact_row(raw), sort_keys=True) + "\n").encode())
    if target.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("Compact evidence exceeds the existing repository blob limit")
    source.rename(retained)
    return {"run": folder.name, "compact_path": str(target.relative_to(ROOT)),
            "compact_sha256": file_digest(target), "compact_bytes": target.stat().st_size,
            "retained_local_full_path": str(retained.relative_to(ROOT)),
            "retained_local_compressed_sha256": file_digest(retained),
            "retained_local_compressed_bytes": retained.stat().st_size,
            "full_uncompressed_sha256": raw_digest.hexdigest(), "full_uncompressed_bytes": raw_bytes,
            "original_lines": lines, "compact_payload_fields_replaced_with_hashes": True}


def main():
    pairs = json.loads((ROOT / "pair-runs.json").read_text())
    if not pairs.get("complete") or len(pairs["runs"]) != 6:
        raise ValueError("Wait for all six complete captures")
    entries = [package(ROOT / item["run"]) for item in pairs["runs"]]
    compression = json.loads((ROOT / "compression.json").read_text())
    locations = {f'{entry["run"]}/receipts.jsonl.gz': entry["retained_local_full_path"] for entry in entries}
    for entry in compression["files"]:
        entry["compressed_path"] = locations.get(entry["compressed_path"], entry["compressed_path"])
    (ROOT / "compression.json").write_text(json.dumps(compression, indent=2, sort_keys=True) + "\n")
    (ROOT / "evidence-index.json").write_text(json.dumps({"format": "lastdb-request-samples-v1",
        "full_generated_payload_captures_retained_locally": True, "runs": entries}, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"compact_runs": len(entries), "compact_total_bytes": sum(e["compact_bytes"] for e in entries)}))


if __name__ == "__main__":
    main()
