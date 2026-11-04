"""Retain current request phases after a timed case, with exact coverage."""

import gzip
import json

from node import save
from tail_report import update_tail_lines


def capture_phases(node, output, case, sequence, samples):
    response, _, _ = node.call("/api/status?recent=1")
    operations = response.get("status", {}).get("request_ops", {})
    recent = operations.get("recent", [])
    requested = {item["request_id"] for sample in samples
                 for item in sample.get("physical_requests", [{"request_id": sample["request_id"]}])}
    matched = {}
    for operation in recent:
        request_id = operation.get("request_id")
        if request_id not in requested or operation.get("client") != "github-benchmark":
            continue
        if request_id in matched:
            raise RuntimeError("The request ring repeats a measured physical request ID")
        matched[request_id] = operation
    complete = 0
    for sample in samples:
        physical = sample.get("physical_requests", [{"request_id": sample["request_id"]}])
        ids = [item["request_id"] for item in physical]
        sample["server_physical_ops"] = [matched[key] for key in ids if key in matched]
        sample["response_request_id"] = ids[-1]
        if ids[-1] in matched:
            sample["server_request_ops"] = matched[ids[-1]]
        complete += all(key in matched for key in ids)
    coverage = {"requests": len(samples), "matched": complete,
                "physical_requests": len(requested), "physical_matched": len(matched),
                "with_phases": sum(bool(row.get("phases")) for row in matched.values()),
                "ring_capacity": operations.get("ring_capacity"), "complete": complete == len(samples),
                "scope": "Every physical RPC is retained; server_request_ops is the final response RPC. Missing phases are unreported."}
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
             "| Run | Matching requests | Slow logical write ms | Server ms | Largest reported phases ms |",
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
