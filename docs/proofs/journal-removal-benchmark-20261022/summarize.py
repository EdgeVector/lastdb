#!/usr/bin/env python3
"""Summarize three completed product pairs without another node or workload."""

import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics
import time


LABELS = ("before", "after")
PAIRS = (1, 2, 3)
PHASE_NAMES = ("durable-single-64", "durable-batch32-16", "queued-single-256",
               "queued-batch32-8", "compaction", "durable-single-after-compact-24")
IDENTITY = ("pid", "ri_proc_start_abstime", "executable_uuid")
COUNTERS = ("ri_diskio_bytesread", "ri_diskio_byteswritten", "ri_logical_writes",
            "ri_user_time", "ri_system_time")


def open_input(path, limit):
    if not path.exists():
        path = path.with_name(path.name + ".gz")
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError(f"Missing, linked, or oversized input: {path}")
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def load(path, limit=64 * 1024 * 1024):
    with open_input(path, limit) as stream:
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise ValueError("JSON input exceeds its decompressed byte bound")
    return json.loads(value)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def distribution(values):
    values = list(values)
    if any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in values):
        raise ValueError("A numeric summary contains an invalid value")
    return {"values": values, "median": statistics.median(values),
            "min": min(values), "max": max(values)}


def paired(values):
    before, after = [item[0] for item in values], [item[1] for item in values]
    result = {"before": distribution(before), "after": distribution(after),
              "after_minus_before": distribution(a - b for b, a in values)}
    positive = all(b > 0 and a >= 0 for b, a in values)
    if positive:
        result["reduction_percent"] = distribution(100 * (1 - a / b) for b, a in values)
    if positive and all(a > 0 for a in after):
        result["before_over_after_ratio"] = distribution(b / a for b, a in values)
    return result


def request_acks(path):
    limit = 128 * 1024 * 1024
    compressed = path.with_name(path.name + ".gz")
    compact = not (path.exists() or path.is_symlink() or compressed.exists() or compressed.is_symlink())
    if compact:
        path = path.with_name("request-samples.json.gz")
    elif not path.exists() and not path.is_symlink():
        path = compressed
    result, size, deadline = {}, 0, time.monotonic() + 30
    with open_input(path, limit) as stream:
        for number in range(10001):
            line = stream.readline(8 * 1024 * 1024 + 1)
            if not line:
                if compact and number == 0:
                    raise ValueError("Compact timing evidence lacks its format header")
                return result, {"file": path.name, "compact": compact,
                                "format": "lastdb-request-samples-v1" if compact else "full-receipts-jsonl"}
            size += len(line)
            if (number == 10000 or len(line) > 8 * 1024 * 1024
                    or size > limit or time.monotonic() > deadline):
                raise ValueError("Request receipts exceed their line, count, or time bound")
            row = json.loads(line)
            if compact and number == 0:
                if (row.get("operation") != "evidence-format" or row.get("compact") is not True
                        or row.get("format") != "lastdb-request-samples-v1"):
                    raise ValueError("Compact timing evidence has an unsupported or absent format header")
                continue
            if row.get("operation") != "request":
                continue
            if row.get("http_status") != 200:
                raise ValueError("A captured product request failed")
            if row["path"] == "/api/mutations/batch":
                result.setdefault(row["phase"], []).append(row["elapsed_seconds"])
    raise ValueError("Request receipt read did not finish")


def validate_boundary(before, after, phase):
    first, last = before["native"], after["native"]
    for checkpoint in (before, after):
        native = checkpoint["native"]
        if (native["writer_failed"] or native["accepted_batches"] or native["core_warm_dirty"]
                or native["accepted_sequence"] != native["completed_sequence"]
                or native["parts"]["auto_maintenance"]):
            raise ValueError("A measured checkpoint is failed, undrained, or automatic maintenance is enabled")
    start, end = before["process"], after["process"]
    if any(start[key] != end[key] for key in IDENTITY):
        raise ValueError("A measured phase spans different process identities")
    for key in COUNTERS:
        delta = end[key] - start[key]
        if delta < 0 or delta != phase["process"]["cumulative_counter_delta"][key]:
            raise ValueError("A measured process counter delta differs")
    for key in ("generation", "frontier", "accepted_sequence", "completed_sequence"):
        if last[key] - first[key] != phase[f"{key}_delta"]:
            raise ValueError("A measured native boundary delta differs")


def physical_cost(before, after, phase):
    first, last = before["inventory"], after["inventory"]
    old, new = first["_files"], last["_files"]
    cost = {"logical_bytes_net": last["bytes"] - first["bytes"],
            "allocated_bytes_net": last["allocated_bytes"] - first["allocated_bytes"],
            "files_net": last["files"] - first["files"],
            "new_path_files": len(set(new) - set(old)),
            "new_or_grown_logical_bytes": sum(max(0, value[0] - old.get(name, [0, 0])[0])
                                               for name, value in new.items()),
            "new_or_grown_allocated_bytes": sum(max(0, value[1] - old.get(name, [0, 0])[1])
                                                 for name, value in new.items())}
    if (cost["logical_bytes_net"] != phase["growth"]["bytes_net"]
            or cost["allocated_bytes_net"] != phase["growth"]["allocated_bytes_net"]):
        raise ValueError("The phase and checkpoint file growth differ")
    return cost


def phase_metrics(phase, checkpoints, acks):
    label = phase["label"]
    stem = "compact" if phase["kind"] == "compaction-phase" else label
    before, after = checkpoints[stem + "-before"], checkpoints[stem + "-after"]
    validate_boundary(before, after, phase)
    metrics = physical_cost(before, after, phase)
    metrics.update({key: phase[key] for key in ("generation_delta", "frontier_delta",
                   "accepted_sequence_delta", "completed_sequence_delta")})
    metrics["durable_completion_wall_seconds"] = phase["wall_through_final_flush_seconds"]
    metrics["final_explicit_flush_seconds"] = phase["final_explicit_flush_seconds"]
    metrics.update({f"process_{key}": value for key, value in
                    phase["process"]["cumulative_counter_delta"].items()})
    metrics["process_lifetime_peak_memory_bytes"] = phase["process"]["process_lifetime_max_memory_bytes"]
    for when in ("before", "after"):
        for key, value in phase["process"][f"memory_{when}_bytes"].items():
            metrics[f"memory_{when}_{key}"] = value
    if phase["kind"] == "compaction-phase":
        report = phase["report"]["compact"]["native_disk"]
        metrics.update(compact_wall_seconds=phase["compact_wall_seconds"],
                       pack_millis=report["pack_millis"], reclaim_millis=report["reclaim_millis"],
                       records_copied=report["records_copied"], tombstones_copied=report["tombstones_copied"],
                       bytes_removed=report["bytes_removed"], files_removed=report["files_removed"],
                       warm_read_baseline_p50_ms=phase["warm_read_baseline"]["median_ms"],
                       warm_read_baseline_p95_ms=phase["warm_read_baseline"]["p95_ms"])
        for name, probe in phase["probes"].items():
            metrics[f"{name}_probe_ack_seconds"] = probe["ack_seconds"]
        return {"metrics": metrics, "probes": phase["probes"], "report": report,
                "normalization": "Whole compaction phase, including concurrent probes; no per-update normalization"}
    inputs, samples = phase["inputs"], acks[label]
    if len(samples) != inputs["requests"] or len(samples) != phase["request_ack"]["samples"]:
        raise ValueError("The captured mutation request count differs")
    ordered = sorted(samples)
    metrics.update(ack_mean_ms=1000 * statistics.mean(samples), ack_p50_ms=1000 * statistics.median(samples),
                   ack_p95_ms=1000 * ordered[math.ceil(0.95 * len(ordered)) - 1],
                   ack_max_ms=1000 * max(samples), ack_sequence_seconds=phase["ack_sequence_seconds"])
    if not math.isclose(metrics["ack_p50_ms"], phase["request_ack"]["median_ms"], rel_tol=1e-9):
        raise ValueError("The raw request median differs from the phase summary")
    records, requests, commits = inputs["logical_mutations"], inputs["requests"], phase["frontier_delta"]
    if records < 1 or requests < 1 or commits < 1:
        raise ValueError("A write phase lacks records, requests, or complete commits")
    metrics.update(logical_mutations=records, requests=requests, requests_per_commit=requests / commits,
                   accepted_native_requests_per_commit=phase["accepted_sequence_delta"] / commits,
                   generations_per_commit=phase["generation_delta"] / commits,
                   logical_mutations_per_commit=records / commits,
                   durable_completion_seconds_per_record=metrics["durable_completion_wall_seconds"] / records,
                   process_write_bytes_per_commit=metrics["process_ri_diskio_byteswritten"] / commits)
    for key in ("logical_bytes_net", "allocated_bytes_net", "new_path_files", "files_net",
                "new_or_grown_logical_bytes", "new_or_grown_allocated_bytes",
                "process_ri_diskio_byteswritten", "process_ri_diskio_bytesread", "process_ri_logical_writes"):
        metrics[f"{key}_per_record"] = metrics[key] / records
    return {"metrics": metrics, "inputs": inputs, "requested_durability": phase["requested_durability"],
            "width": phase["width"], "new_or_grown_by_family": phase["growth"]["new_or_grown"],
            "inventory_families_before": before["inventory"]["families"],
            "inventory_families_after": after["inventory"]["families"],
            "normalization": "One logical updated record is one supplied mutation; repeated keys count again"}


def read_run(root, label, pair, build):
    folder = root / f"{label}-{pair}"
    if folder.is_symlink() or folder.resolve(strict=True).parent != root:
        raise ValueError("The run folder leaves the exact proof root")
    summary, inputs = load(folder / "summary.json"), load(folder / "inputs.json")
    required = ("complete", "all_owned_processes_stopped", "all_phase_values_verified", "graceful_reopen_verified")
    if not all(summary.get(key) is True for key in required):
        raise ValueError(f"The run is incomplete, active, or lacks readback: {folder.name}")
    if (inputs["source_revision"] != build["revision"] or inputs["binary_sha256"] != build["binary_sha256"]
            or inputs["automatic_maintenance"] is not False or inputs["settings"]["LASTDB_RESIDENT_PERSIST_MS"] != "500"):
        raise ValueError("The run source, binary, or controlled settings differ")
    checkpoints_list = load(folder / "checkpoints.json")
    if (len(checkpoints_list) > 64 or len(summary["phases"]) != 6
            or {phase["label"] for phase in summary["phases"]} != set(PHASE_NAMES)):
        raise ValueError("The run phase or checkpoint count exceeds the expected bound")
    checkpoints = {item["label"]: item for item in checkpoints_list}
    acks, timing_evidence = request_acks(folder / "receipts.jsonl")
    phases = {phase["label"]: phase_metrics(phase, checkpoints, acks) for phase in summary["phases"]}
    peaks = {}
    for item in checkpoints_list:
        process = item["process"]
        identity = f'{process["pid"]}:{process["ri_proc_start_abstime"]}'
        peaks[identity] = max(peaks.get(identity, 0), process["ri_lifetime_max_phys_footprint"])
    cleanup = load(folder / "cleanup.json")
    if cleanup.get("removed") is not True:
        raise ValueError("An owned run lacks a completed cleanup receipt")
    return {"name": folder.name, "source_revision": inputs["source_revision"],
            "binary_sha256": inputs["binary_sha256"], "settings": inputs["settings"],
            "helpers_sha256": inputs["helpers_sha256"], "expected_values_sha256": summary["expected_values_sha256"],
            "request_timing_evidence": timing_evidence,
            "all_values_and_reopen_verified": True, "owned_scratch_removed": True,
            "process_lifetime_peaks_by_identity": peaks, "peak_process_memory_bytes": max(peaks.values()),
            "phases": phases}


def compare_runs(runs):
    labels = list(runs["before-1"]["phases"])
    result = {}
    for phase in labels:
        pairs = []
        for number in PAIRS:
            before, after = runs[f"before-{number}"], runs[f"after-{number}"]
            if (before["expected_values_sha256"] != after["expected_values_sha256"]
                    or before["settings"] != after["settings"] or before["helpers_sha256"] != after["helpers_sha256"]):
                raise ValueError("The paired expected state, settings, or driver versions differ")
            first, last = before["phases"][phase], after["phases"][phase]
            if first.get("inputs") != last.get("inputs"):
                raise ValueError("The paired ordered write inputs differ")
            pairs.append((first["metrics"], last["metrics"]))
        metrics = {key: paired([(first[key], last[key]) for first, last in pairs]) for key in pairs[0][0]}
        result[phase] = {"independent_pairs": 3, "metrics": metrics}
        if phase == "compaction":
            result[phase]["probe_request_overlap_counts"] = {
                role: {probe: sum(runs[f"{role}-{number}"]["phases"][phase]["probes"][probe]
                                  ["request_intervals_overlap"] for number in PAIRS)
                       for probe in ("warm_read", "durable_write")} for role in LABELS}
        else:
            result[phase]["frontier_deltas_match_in_all_pairs"] = all(
                first["frontier_delta"] == last["frontier_delta"] for first, last in pairs)
    return result


def formatted(value, decimals=2):
    return f'{value["median"]:.{decimals}f} [{value["min"]:.{decimals}f}, {value["max"]:.{decimals}f}]'


def report(summary):
    lines = ["Controlled journal removal comparison: three fresh-home pairs; n=3 per variant.",
             "Automatic maintenance is off; flush is 500 ms. This is not default-product performance.",
             "Numbers show medians [minimum, maximum] across the three runs or matched pair changes.",
             "Ack p50/p95 describe calls within each run. Calls are not pooled as independent repeats.",
             "Darwin process I/O is process accounting, not physical SSD writes. OS caches are uncontrolled.", ""]
    lines.insert(5, "Full generated payload captures stay local under a hash index; committed evidence retains request timings and payload hashes.")
    for phase, value in summary["comparisons"].items():
        metrics = value["metrics"]
        lines.append(phase)
        keys = ("compact_wall_seconds", "pack_millis", "reclaim_millis", "warm_read_probe_ack_seconds",
                "durable_write_probe_ack_seconds") if phase == "compaction" else (
                "ack_mean_ms", "ack_p50_ms", "ack_p95_ms", "durable_completion_wall_seconds",
                "process_ri_diskio_byteswritten_per_record", "new_or_grown_logical_bytes_per_record",
                "new_or_grown_allocated_bytes_per_record", "logical_bytes_net_per_record",
                "allocated_bytes_net_per_record", "new_path_files_per_record", "frontier_delta",
                "requests_per_commit", "accepted_native_requests_per_commit", "generations_per_commit")
        for key in keys:
            metric = metrics[key]
            change = metric.get("reduction_percent")
            suffix = f'; paired reduction % {formatted(change)}' if change else "; signed net metric; no ratio"
            precision = 6 if "probe_ack_seconds" in key else 2
            lines.append(f'  {key}: before {formatted(metric["before"], precision)}; after {formatted(metric["after"], precision)}{suffix}')
        if phase == "compaction":
            lines.append(f'  Actual request overlaps, out of three: {json.dumps(value["probe_request_overlap_counts"], sort_keys=True)}')
        elif not value["frontier_deltas_match_in_all_pairs"]:
            lines.append("  Commit counts differ. Product cost includes internal native work; no fixed per-commit cost claim.")
        lines.append("")
    memory = summary["peak_process_memory_bytes"]
    lines.append(f'Peak process memory bytes: before {formatted(memory["before"], 0)}; after {formatted(memory["after"], 0)}.')
    lines.append("Observed lifetime peaks at checkpoints include seed and compaction; final shutdown is outside the sample scope.")
    lines.append("requests_per_commit counts HTTP mutation calls; accepted_native_requests_per_commit measures native queue coalescence.")
    lines.append("All six runs confirm complete values, graceful reopen, stopped owned nodes, and exact owned cleanup.")
    lines.append("No power-loss, cloud, primary, significance, or default-maintenance claim.")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    if {".lastdb", ".folddb", ".primary-lastdb-archive-20261014"}.intersection(root.parts):
        raise ValueError("The summarizer accepts proof files outside primary and archived data")
    manifest = load(root / "pair-runs.json")
    if manifest.get("complete") is not True or len(manifest["runs"]) != 6:
        raise ValueError("Wait for all six complete run and cleanup receipts")
    expected = {(label, number) for label in LABELS for number in PAIRS}
    actual = {(item["label"], item["pair"]) for item in manifest["runs"]}
    if actual != expected or any(item["exit_code"] != 0 or not item["scratch_removed"] for item in manifest["runs"]):
        raise ValueError("The pair manifest lacks the six exact successful owned runs")
    builds = manifest["builds"]
    if any(builds["before"][key] != builds["after"][key] for key in ("cargo", "rustc", "build_command")):
        raise ValueError("The matched build toolchain or command differs")
    runs = {f"{label}-{number}": read_run(root, label, number, builds[label]) for number in PAIRS for label in LABELS}
    summary = {"complete": True, "method": "Three matched pairs of fresh generated homes; one independent run per arm per pair",
               "independent_pairs": 3, "runs_per_variant": 3, "request_samples_pooled": False,
               "automatic_maintenance": False, "flush_ms": 500, "order": manifest["order"], "builds": builds,
               "driver_sha256": digest(Path(__file__)), "comparisons": compare_runs(runs), "runs": runs,
               "peak_process_memory_bytes": paired([(runs[f"before-{number}"]["peak_process_memory_bytes"],
                                                       runs[f"after-{number}"]["peak_process_memory_bytes"])
                                                      for number in PAIRS]),
               "scope": ["Controlled automatic-maintenance-off product comparison", "Operating system cache uncontrolled",
                         "Darwin process I/O counters are not physical SSD writes", "No significance claim from three pairs",
                         "Net growth differs from new-file bytes and from process I/O", "Compaction includes concurrent probes",
                         "Full generated payload captures remain local under a hashed evidence index",
                         "Committed summaries and compact per-request timing evidence retain payload SHA-256 hashes"]}
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (root / "summary.txt").write_text(report(summary))
    print(json.dumps({"complete": True, "pairs": 3, "output": str(root / "summary.json")}))


if __name__ == "__main__":
    main()
