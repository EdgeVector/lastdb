"""Retain current request phases after a timed case, with exact coverage."""

import gzip
import http.client
import json
import time

from node import save
from tail_report import update_tail_lines


def capture_native_boundary(node):
    """Read cumulative maintenance counters outside the measured case."""
    started = time.time_ns() // 1000
    try:
        response, _, _ = node.call("/api/db/native-status")
        native = response.get("native_disk", {})
        parts = native.get("parts")
        if not isinstance(parts, dict):
            return {"available": False, "reason": "native parts status is unreported",
                    "started_unix_us": started, "ended_unix_us": time.time_ns() // 1000}
        return {"available": True, "started_unix_us": started,
                "ended_unix_us": time.time_ns() // 1000,
                "parts": {name: value for name, value in parts.items()
                          if name.startswith(("maintenance_", "reclaim_", "compaction_"))},
                "generation": native.get("generation"),
                "completed_sequence": native.get("completed_sequence")}
    except (OSError, RuntimeError, ValueError, http.client.HTTPException) as failure:
        return {"available": False, "reason": type(failure).__name__,
                "started_unix_us": started, "ended_unix_us": time.time_ns() // 1000}


def capture_phases(node, output, case, sequence, samples):
    response, _, _ = node.call("/api/status?recent=1")
    operations = response.get("status", {}).get("request_ops", {})
    recent = operations.get("recent", [])
    requested = {sample["request_id"] for sample in samples}
    if len(requested) != len(samples):
        raise RuntimeError("Measured request IDs are not unique within the case")
    matched = {}
    for operation in recent:
        request_id = operation.get("request_id")
        if request_id not in requested or operation.get("client") != "github-benchmark":
            continue
        if request_id in matched:
            raise RuntimeError("The request ring repeats a measured request ID")
        matched[request_id] = operation
    for sample in samples:
        if sample["request_id"] in matched:
            sample["server_request_ops"] = matched[sample["request_id"]]
    coverage = {"requests": len(samples), "matched": len(matched),
                "with_phases": sum(bool(row.get("phases")) for row in matched.values()),
                "ring_capacity": operations.get("ring_capacity"),
                "complete": len(matched) == len(samples),
                "scope": "Absent samples or phase fields are unreported, not measured zero"}
    destination = output / "request-phases"
    destination.mkdir(exist_ok=True)
    save(destination / f"{sequence:02d}.json",
         {"case": case, "coverage": coverage, "request_ops": operations})
    return coverage


def phase_lines(output, manifest):
    lines = ["", "## Slow four-client writes and request phases", "",
             "These are actual measured calls joined to server records by request ID.",
             "Phase capture runs after each timed case. It is excluded from latency and throughput.",
             "The request ring is finite. Missing coverage and omitted phases mean unreported, not zero.",
             "Socket queue wait precedes server duration; it is not part of that duration.",
             "Phase totals can overlap; do not add a parent phase to its children.", "",
             "| Run | Matching requests | Slow write RPC ms | Server ms | Largest reported phases ms |",
             "|---|---:|---:|---:|---|"]
    for run in manifest["runs"]:
        path = output / run["directory"] / "samples.jsonl.gz"
        if not path.exists():
            continue
        with gzip.open(path, "rt") as source:
            samples = [json.loads(line) for line in source]
        writes = [sample for sample in samples
                  if sample["case"] == "concurrent-write/4" and "error" not in sample]
        if not writes:
            continue
        matched = sum("server_request_ops" in sample for sample in writes)
        for sample in sorted(writes, key=lambda row: row["elapsed_ms"], reverse=True)[:4]:
            operation = sample.get("server_request_ops", {})
            phases = sorted(operation.get("phases", {}).items(),
                            key=lambda item: item[1], reverse=True)[:5]
            detail = ", ".join(f"{name}={value / 1000:.3f}" for name, value in phases) or "unreported"
            server = operation.get("duration_ms", "unreported")
            lines.append(f"| {run['directory']} | {matched}/{len(writes)} | "
                         f"{sample['elapsed_ms']:.3f} | {server} | {detail} |")
    return lines + update_tail_lines(output, manifest)
