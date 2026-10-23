#!/usr/bin/env python3
"""Summarize all three matched pairs without pooling refused copies as successes."""
import argparse
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics


def load(path):
    return json.loads(path.read_text())


def resource_peak(rows, phase, key):
    selected = [row[key] for row in rows if row["phase"] == phase
                and row["ps_exit"] == 0 and row["lsof_exit"] == 0]
    return max(selected) if selected else None


def client_delay_plus_rpc_p95(rows):
    values = sorted(row["elapsed_ms"] + row["dispatch_lag_ms"] for row in rows)
    return values[math.ceil(len(values) * 0.95) - 1] if values else None


def run_metrics(run):
    summary, inputs = load(run / "summary.json"), load(run / "inputs.json")
    if not summary["complete"] or summary["error"] or not summary["all_owned_processes_stopped"]:
        raise ValueError(f"Incomplete run: {run}")
    identity = load(run / "implementation-identification.json")
    if (identity["new_compaction_status_present"] != (inputs["label"] == "after")
            or identity["expected_new_compaction"] != (inputs["label"] == "after")):
        raise ValueError("The actual storage implementation differs")
    if not load(run / "cleanup.json")["removed"]:
        raise ValueError(f"Owned home remains: {run}")
    archive = (run / "receipts.jsonl.gz").read_bytes()
    raw = gzip.decompress(archive)
    receipt = load(run / "receipt-archive.json")
    if (hashlib.sha256(raw).hexdigest() != receipt["raw_sha256"]
            or hashlib.sha256(archive).hexdigest() != receipt["archive_sha256"]):
        raise ValueError("Receipt archive differs")
    receipts = [json.loads(line) for line in raw.decode().splitlines()]
    checked = [row for row in receipts if row["operation"] == "all-known-keys-correct"]
    seeded = [row for row in receipts if row["operation"] == "seed-complete"]
    tallies = [row["report"] for row in receipts
               if row["operation"] == "counter-verifier-complete"]
    if len(checked) != 1 or len(seeded) != 1 or not tallies or any(not row["done"] or not row["tally_complete"]
                                             or row["mismatches_total"] for row in tallies):
        raise ValueError("Final exact-key or counter evidence is incomplete")
    data = load(run / "metrics.json")
    phases = data["phases"]
    resources = load(run / "resources.json")
    writer_samples = load(run / "writer-samples.json")
    memory = resource_peak(resources, "two-copies", "rss_bytes")
    result = {"name": run.name, "pair": inputs["pair"], "label": inputs["label"],
        "source_revision": inputs["source_revision"], "binary_sha256": inputs["binary_sha256"],
        "driver_sha256": inputs["driver_sha256"], "workload_sha256": inputs["workload_sha256"],
        "scale_support_sha256": inputs["scale_support_sha256"],
        "product_support_sha256": inputs["product_support_sha256"],
        "snapshot_support_sha256": inputs["snapshot_support_sha256"],
        "records": summary["records"], "value_digest": checked[0]["sha256"],
        "seed_seconds": seeded[0]["seconds"],
        "capacity_refusals": summary["capacity_refusals"],
        "idle_copy_ms": phases["idle-full"]["copy"]["wall_ms"],
        "native_entries": sum(phases["idle-full"]["copy"]["report"][key]
                              for key in ("records_copied", "tombstones_copied")),
        "two_copy_ms": phases["two-copies"]["wall_ms"],
        "two_copy_reported_jobs": phases["two-copies"]["max_reported_jobs"],
        "two_copy_memory_mib": memory / 1048576 if memory is not None else None,
        "two_copy_file_handles": resource_peak(resources, "two-copies", "open_fds"),
        "reported_publication_max_ms": phases["two-copies"]["status"]["parts"]
            ["compaction_swap_max_millis"],
        "resource_samples": len(resources), "monitor_errors": data["monitor_errors"]}
    for mode in ("queued", "durable"):
        active, baseline = phases[f"{mode}-active"], phases[f"{mode}-baseline"]
        result[f"{mode}_published"] = active["copy"]["published"]
        result[f"{mode}_copy_ms"] = active["copy"]["wall_ms"] if active["copy"]["published"] else None
        result[f"{mode}_cleanup_ms"] = active["cleanup"]["wall_ms"]
        result[f"{mode}_cleanup_finished_during_copy"] = (
            active["cleanup"]["ended_mono_ns"] < active["copy"]["ended_mono_ns"])
        for field in ("files_removed", "bytes_removed", "reclaim_deferred"):
            result[f"{mode}_cleanup_{field}"] = active["cleanup"]["report"][field]
        result[f"{mode}_maintenance_pair_ms"] = active["maintenance_pair_wall_ms"]
        result[f"{mode}_overlap_samples"] = active["writes_during_copy"]["requests"]
        result[f"{mode}_max_dispatch_lag_ms"] = active["max_dispatch_lag_ms"]
        result[f"{mode}_client_delay_plus_rpc_p95_ms"] = client_delay_plus_rpc_p95(
            writer_samples[f"{mode}-active"])
        report = active["copy"].get("report", {})
        for field in ("catch_up_records", "catch_up_rounds", "publication_retries"):
            result[f"{mode}_{field}"] = report.get(field)
        for percentile in ("p50_ms", "p95_ms", "p99_ms", "max_ms", "largest_completion_gap_ms"):
            result[f"{mode}_write_{percentile}"] = active["writes_during_copy"].get(percentile)
            result[f"{mode}_baseline_{percentile}"] = baseline.get(percentile)
        active_p95 = active["writes_during_copy"].get("p95_ms")
        result[f"{mode}_p95_slowdown"] = active_p95 / baseline["p95_ms"] if active_p95 else None
    return result


def aggregate(rows, key):
    values = [row[key] for row in rows if row.get(key) is not None]
    if not values:
        return {"n": 0, "median": None, "min": None, "max": None}
    return {"n": len(values), "median": statistics.median(values),
            "min": min(values), "max": max(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for pair in range(3):
        index = load(args.measurements / f"pair-{pair}.json")
        if not index["complete"] or len(index["runs"]) != 2:
            raise ValueError("All three complete matched pairs are required")
        rows.extend(run_metrics(args.measurements / item["name"]) for item in index["runs"])
    for key in ("value_digest", "driver_sha256", "workload_sha256", "records",
                "scale_support_sha256", "product_support_sha256", "snapshot_support_sha256"):
        if len({row[key] for row in rows}) != 1:
            raise ValueError(f"The paired workload differs: {key}")
    numeric = sorted({key for row in rows for key, value in row.items()
                      if (value is None or isinstance(value, (int, float))) and not isinstance(value, bool)
                      and key not in ("pair", "records", "capacity_refusals")})
    grouped = {label: {key: aggregate([row for row in rows if row["label"] == label], key)
                      for key in numeric} for label in ("before", "after")}
    result = {"pairs": 3, "runs": rows, "summary": grouped,
        "capacity_refusals": {label: sum(row["capacity_refusals"] for row in rows
                                        if row["label"] == label) for label in grouped},
        "limits": ["three local pairs", "alternating order; shared host and uncontrolled filesystem cache",
                   "median of per-run measures; no universal latency guarantee",
                   "old and new publication timers have different scope",
                   "refused copies excluded from successful-copy latency"]}
    (args.output / "comparison.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    with (args.output / "per-run.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    write_report(args.output, result)


def write_report(output, result):
    after_rows = [row for row in result["runs"] if row["label"] == "after"]
    successful_hold_max = max(row["reported_publication_max_ms"] for row in after_rows)
    overlapping_cleanups = sum(row[f"{mode}_cleanup_finished_during_copy"]
                               for row in after_rows for mode in ("queued", "durable"))
    overlap_removed = sum(row[f"{mode}_cleanup_files_removed"] for row in after_rows
                          for mode in ("queued", "durable"))
    columns = [
        ("Durable seed of 100,000 records", "seed_seconds", "s"),
        ("Quiet full compaction", "idle_copy_ms", "ms"),
        ("Normal write p95 without compaction", "queued_baseline_p95_ms", "ms"),
        ("Successful copy beside normal writes", "queued_copy_ms", "ms"),
        ("Cleanup response beside normal writes and a copy", "queued_cleanup_ms", "ms"),
        ("Normal write p95 during compaction", "queued_write_p95_ms", "ms"),
        ("Normal write p99 during compaction", "queued_write_p99_ms", "ms"),
        ("Normal client delay plus RPC p95 (all 400 writes)", "queued_client_delay_plus_rpc_p95_ms", "ms"),
        ("Worst normal write during compaction", "queued_write_max_ms", "ms"),
        ("Successful copy beside durable writes", "durable_copy_ms", "ms"),
        ("Durable write p95 without compaction", "durable_baseline_p95_ms", "ms"),
        ("Cleanup response beside durable writes and a copy", "durable_cleanup_ms", "ms"),
        ("Durable write p95 during compaction", "durable_write_p95_ms", "ms"),
        ("Durable client delay plus RPC p95 (all 200 writes)", "durable_client_delay_plus_rpc_p95_ms", "ms"),
        ("Worst durable write during compaction", "durable_write_max_ms", "ms"),
        ("Two simultaneous compaction callers", "two_copy_ms", "ms"),
        ("Sampled memory size during two copies", "two_copy_memory_mib", "MiB"),
        ("Sampled file handles during two copies", "two_copy_file_handles", "handles")]
    lines = ["# Compaction benchmark: PR #275", "",
        "The report compares three matched pairs on fresh generated homes with 100,000 records each.",
        "The baseline is c6b4f7ed9. The candidate is c94ec7760.",
        "",
        "CAUTION: This comparison describes those historical revisions, not the current default write behavior.",
        "The normal-write rows use a memory ack. The durable-write rows require completion on disk.",
        "Later commit 0deeafc5e requires disk completion before every native write ack on main.",
        "These results do not measure that later change.",
        "",
        "Values below are medians of the three per-run measures. Brackets show the per-run range.", "",
        "| Measure | Before | After | Change |", "|---|---:|---:|---:|"]
    for title, key, unit in columns:
        before, after = (result["summary"][label][key] for label in ("before", "after"))
        def cell(value):
            if value["median"] is None:
                return "No observation"
            return f'{value["median"]:.2f} {unit} [{value["min"]:.2f}, {value["max"]:.2f}]'
        change = "—"
        if before["n"] == after["n"] == 3 and before["median"]:
            change = f'{(after["median"] / before["median"] - 1) * 100:+.1f}%'
        lines.append(f"| {title} | {cell(before)} | {cell(after)} | {change} |")
    lines.extend(["", f'Capacity refusals: {result["capacity_refusals"]}.', "",
        f"The new version completed {overlapping_cleanups} of six cleanup calls before its concurrent copy response.",
        f"Those six calls removed {overlap_removed} files in total.",
        f"The largest reported successful publication hold across the new runs was {successful_hold_max} milliseconds.",
        "That timer uses whole milliseconds and excludes unsuccessful publication attempts.", "",
        "Every run checks all known record values and completes the atom-counter diagnostic with zero mismatches.",
        "Each owned node stops, and the supported cleanup command removes its exact generated home.", "",
        "The baseline and candidate publication timers have different scope. Their values are retained in per-run.csv without a speed ratio.",
        "Memory and file-handle samples do not establish strict peaks between samples.",
        "The workload has a fixed offered rate. It measures response latency, not maximum write throughput.",
        "Cleanup figures measure response latency. Per-run.csv also retains removed-file counts and each deferral flag.",
        "RPC percentiles exclude delay before dispatch. Separate percentiles retain the sum of client dispatch delay and RPC duration.",
        "That sum excludes client payload construction and receipt bookkeeping; it is not an exact scheduled-arrival response timer.",
        "The fixed write window can end before publication. It does not force write contention at every publication boundary.",
        "Three local pairs on a shared host establish no general latency guarantee.",
        "See METHODS.md, builds.json, comparison.json, and the archived request receipts for complete provenance.", ""])
    (output / "REPORT.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
