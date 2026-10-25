"""Keep initial mixed reads visible alongside later reads, without trimming cases."""

import gzip
import json
import math

from node import save


def distribution(values):
    ordered = sorted(values)
    return {"reported": len(ordered), **{
        name: ordered[math.ceil(len(ordered) * fraction) - 1] if ordered else None
        for name, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99))}}


def cohort(samples):
    receipts = [row["read_receipt"] for row in samples if row.get("read_receipt")]
    payloads = [row["payload_bytes"] for row in receipts if row.get("payload_bytes") is not None]
    versions = [row for row in samples if row.get("read_current_case_records") is not None]
    return {
        "requests": len(samples),
        "rpc": distribution([row["elapsed_ms"] for row in samples]),
        "socket_queue": distribution([row["socket_queue_wait_nanos"] / 1e6 for row in receipts
                                      if row.get("socket_queue_wait_nanos") is not None]),
        "storage": distribution([row["query_storage_elapsed_nanos"] / 1e6 for row in receipts
                                 if row.get("query_storage_elapsed_nanos") is not None]),
        "payload_reported": len(payloads),
        "payload_fetches": sum(value > 0 for value in payloads),
        "current_case_records": sum(row["read_current_case_records"] for row in versions),
        "version_reported_records": sum(row["records"] for row in versions),
    }


def timing(value):
    return (f"{value['p95_ms']:.3f} ({value['reported']})"
            if value["p95_ms"] is not None else "unreported (0)")


def mixed_read_lines(output, manifest):
    if "mixed" not in manifest["recipe"] or manifest["recipe"].get("insert_only"):
        return []
    lines = ["", "## Initial and later mixed reads", "",
             "All reads remain in the aggregate mixed metrics and slowdown advisories.",
             "Groups use each client's request order, not elapsed time or a warm-up cutoff.",
             "Read-first means turn 0; write-first means the first read at turn 1; later means turn 2 or higher.",
             "A group name does not prove a cache hit or miss. Payload receipts show actual fetches.",
             "Current-case values have titles written during this case; older values can be valid captured snapshots.",
             "Queue and storage columns show their own p95 and reported count; missing means unreported.",
             "Their percentiles cannot be added or subtracted to explain an RPC percentile.",
             "Small initial groups can make p95 equal the maximum; these groups are descriptive, not regression gates.", "",
             "| Run / clients | Read group | Calls | RPC p50 / p95 ms | Socket queue p95 ms (reported) | Storage p95 ms (reported) | Payload fetches / reported | Current-case values / reported records |",
             "|---|---|---:|---:|---:|---:|---:|---:|"]
    results = []
    for run in manifest["runs"]:
        folder = output / run["directory"]
        with gzip.open(folder / "samples.jsonl.gz", "rt") as source:
            reads = [row for line in source if (row := json.loads(line))["case"].startswith("concurrent-mixed/")
                     and row["kind"] == "read"]
        for case in sorted({row["case"] for row in reads}):
            samples = [row for row in reads if row["case"] == case]
            groups = {"read-first": [], "write-first": [], "later": []}
            for row in samples:
                turn = row["client_request"]
                groups["read-first" if turn == 0 else "write-first" if turn == 1 else "later"].append(row)
            for name, rows in groups.items():
                value = cohort(rows)
                results.append({"run": run["directory"], "case": case, "group": name, **value})
                rpc = (f"{value['rpc']['p50_ms']:.3f} / {value['rpc']['p95_ms']:.3f}"
                       if rows else "unreported")
                lines.append(f"| {run['directory']} / {case.rsplit('/', 1)[-1]} | {name} | {len(rows)} | "
                             f"{rpc} | {timing(value['socket_queue'])} | {timing(value['storage'])} | "
                             f"{value['payload_fetches']}/{value['payload_reported']} | "
                             f"{value['current_case_records']}/{value['version_reported_records']} |")
    save(output / "mixed-read-cohorts.json", {"groups": results,
         "scope": "Diagnostic partitions only; all samples remain in aggregate metrics"})
    return lines
