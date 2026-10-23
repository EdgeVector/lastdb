#!/usr/bin/env python3
"""Audit actual default-maintenance captures; preserve each descriptive n=1 run."""

import argparse
import datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

REVISIONS = {"before": "08d8436ccce9e2493e187a6b0133dc93f47c410f",
             "draft": "a1941e3a8742d4da4743a02db99153119bdf034b",
             "after": "e79b711517519c9ce27d0f6f1b405a364be3ba91"}
LIMIT = 64 * 1024 * 1024


def read_bytes(path):
    if not path.exists():
        path = path.with_name(path.name + ".gz")
    if path.is_symlink() or path.stat().st_size > LIMIT:
        raise ValueError("An audit input is linked or exceeds its finite byte bound")
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as stream:
        raw = stream.read(LIMIT + 1)
    if len(raw) > LIMIT:
        raise ValueError("An audit input exceeds its decompressed byte bound")
    return raw


def load(path):
    return json.loads(read_bytes(path))


def receipts(folder):
    path = folder / "receipts.jsonl"
    if not path.exists() and not path.with_name(path.name + ".gz").exists():
        path = folder / "receipts.full.local.gz"
    lines = read_bytes(path).splitlines()
    if len(lines) > 10000 or any(len(line) > 8 * 1024 * 1024 for line in lines):
        raise ValueError("Raw request receipts exceed their line bounds")
    return [json.loads(line) for line in lines]


def equal(actual, expected):
    if isinstance(expected, dict):
        for name, value in expected.items():
            equal(actual[name], value)
    elif isinstance(expected, float):
        assert math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-8), (actual, expected)
    else:
        assert actual == expected, (actual, expected)


def stats(values):
    ordered = sorted(values)
    return {"samples": len(values), "median_ms": statistics.median(values) * 1000,
            "p95_ms": ordered[math.ceil(.95 * len(values)) - 1] * 1000,
            "max_ms": ordered[-1] * 1000}


def expected_request(ordinal):
    first, revision = ordinal * 32 % 64, 100000 + ordinal
    changes = []
    for number in range(first, first + 32):
        body = "".join(hashlib.sha256(f"owned-disk:{number}:{revision}:{part}".encode()).hexdigest()
                       for part in range(16))
        changes.append({"type": "mutation", "schema": "DiskGrowthMeasure", "mutation_type": "update",
                        "durability": "durable", "key_value": {"hash": "measure", "range": f"key-{number:06d}"},
                        "fields_and_values": {"title": f"title-{number}-revision-{revision}", "body": body}})
    return {"mutations": changes}


def audit_requests(folder, summary, raw):
    requests = [row for row in raw if row.get("operation") == "request" and row.get("phase") == "active"
                and row.get("path") == "/api/mutations/batch"]
    intervals = load(folder / "request-intervals.json")
    assert len(requests) == len(intervals) == summary["fixed_requests"] == 330
    digest = hashlib.sha256()
    dimensions = {name: 0 for name in ("requests", "logical_mutations", "changed_field_values",
                  "request_json_bytes", "fields_json_bytes", "body_value_bytes", "title_value_bytes")}
    previous, origin = 0, intervals[0]["nominal_start_monotonic_ns"] - 2_000_000_000
    for ordinal, (row, interval) in enumerate(zip(requests, intervals)):
        request = row["request"]
        equal(request, expected_request(ordinal))
        equal(row["request_bytes"], len(json.dumps(request, separators=(",", ":")).encode()))
        equal(interval, {name: row[name] for name in interval if name in row})
        start, end = row["started_monotonic_ns"], row["ended_monotonic_ns"]
        nominal = origin + (ordinal + 1) * 2_000_000_000
        assert previous <= start <= end and start >= nominal
        equal(row["elapsed_seconds"], (end - start) / 1e9)
        equal(interval["start_lateness_seconds"], (start - nominal) / 1e9)
        equal(interval["nominal_start_monotonic_ns"], nominal)
        equal(row["http_status"], 200)
        equal(row["response"].get("data", row["response"]), {"count": 32, "durability": "durable"})
        digest.update((json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n").encode())
        dimensions["requests"] += 1
        dimensions["request_json_bytes"] += row["request_bytes"]
        for mutation in request["mutations"]:
            fields = mutation["fields_and_values"]
            dimensions["logical_mutations"] += 1
            dimensions["changed_field_values"] += len(fields)
            dimensions["fields_json_bytes"] += len(json.dumps(fields, separators=(",", ":")).encode())
            dimensions["body_value_bytes"] += len(fields["body"].encode())
            dimensions["title_value_bytes"] += len(fields["title"].encode())
        previous = end
    dimensions["ordered_input_sha256"] = digest.hexdigest()
    equal(summary["write_inputs"], dimensions)
    equal(summary["ack"], stats([row["elapsed_seconds"] for row in requests]))
    equal(summary["actual_active_seconds"], (previous - origin) / 1e9)
    equal(summary["max_start_lateness_seconds"], max(row["start_lateness_seconds"] for row in intervals))
    windows = load(folder / "mutation-windows.json")
    assert len(windows) == 22
    for ordinal, window in enumerate(windows):
        batch = requests[ordinal * 15:(ordinal + 1) * 15]
        equal(window["ack"], stats([row["elapsed_seconds"] for row in batch]))
        equal(window["last_request_ordinal"], (ordinal + 1) * 15)
        equal(window["started_monotonic_ns"], batch[0]["started_monotonic_ns"])
        equal(window["ended_monotonic_ns"], batch[-1]["ended_monotonic_ns"])
        equal(window["max_start_lateness_seconds"], max(row["start_lateness_seconds"]
              for row in intervals[ordinal * 15:(ordinal + 1) * 15]))
    return requests, intervals, dimensions


def audit_samples(folder, summary, intervals):
    rows = load(folder / "size-samples.json")
    equal(summary["size_samples_sha256"], hashlib.sha256(read_bytes(folder / "size-samples.json")).hexdigest())
    assert 4 <= len(rows) <= 64 and len(rows) == summary["sample_count"]
    equal(rows[0]["native"]["parts"]["dependency_trigger"], 4096)
    identity, previous_counters = None, None
    for row in sorted(rows, key=lambda item: item["process"]["sampled_monotonic_ns"]):
        listing, process = row["inventory"], row["process"]
        equal(listing["atomic_inventory"], False)
        assert listing["started_monotonic_ns"] <= listing["ended_monotonic_ns"]
        for name in ("files", "bytes", "allocated_bytes"):
            equal(listing[name], sum(family[name] for family in listing["families"].values()))
        current = tuple(process[name] for name in ("pid", "ri_proc_start_abstime", "executable_uuid"))
        identity = current if identity is None else identity
        equal(current, identity)
        counters = [process[name] for name in ("ri_diskio_bytesread", "ri_diskio_byteswritten",
                    "ri_logical_writes", "ri_user_time", "ri_system_time", "ri_lifetime_max_phys_footprint")]
        if previous_counters is not None:
            assert all(last <= now for last, now in zip(previous_counters, counters))
        previous_counters = counters
        assert row["native"]["writer_failed"] is False
        equal(row["native"]["parts"]["auto_maintenance"], True)
        equal(row["native"]["parts"]["dependency_high_water"], 4096)
    for label, name in (("seeded-before-active", "seeded"), ("after-last-active-ack", "after_active"),
                        ("fixed-quiet-tail-end", "after_quiet_tail")):
        selected = [row for row in rows if row["label"] == label]
        assert len(selected) == 1
        equal(summary[name], selected[0])
    final = summary["after_quiet_tail"]
    final_ack = intervals[-1]["ended_monotonic_ns"]
    assert final["started_monotonic_ns"] >= final_ack + 60_000_000_000
    equal(summary["observed_quiet_tail_seconds"], (final["ended_monotonic_ns"] - final_ack) / 1e9)
    equal(summary["stable_final"], rows[-1])
    markers = []
    for row in rows[-2:]:
        native, listing = row["native"], row["inventory"]
        assert not native["parts"]["maintenance_running"] and not native["parts"]["reclaim_pending"]
        assert not native["accepted_batches"] and not native["core_warm_dirty"]
        assert native["accepted_sequence"] == native["completed_sequence"] and listing["disappeared_entries"] == 0
        markers.append((listing["inventory_sha256"], native["generation"], native["frontier"],
                        native["accepted_sequence"], native["completed_sequence"]))
    equal(markers[0], markers[1])
    peaks = {name: max(row["process"][name] for row in rows) for name in
             ("ri_resident_size", "ri_phys_footprint", "ri_lifetime_max_phys_footprint")}
    failures = {name: max(row["native"]["parts"][name] for row in rows) for name in
                ("maintenance_failures", "reclaim_failures", "maintenance_worker_restarts")}
    return rows, {"scope": "All captured size samples in the original node, before reopen",
                  "observed_process_peaks_bytes": peaks, "native_counter_maxima": failures,
                  "warm_bytes": {name: max(row["native"]["core_warm_bytes"][name] for row in rows)
                                 for name in ("used_bytes", "peak_bytes", "limit_bytes")},
                  "warm_record_limit_values": sorted({row["native"]["warm_record_limit"] for row in rows})}


def unix_ns(text):
    whole, fraction = text.rstrip("Z").split(".") if "." in text else (text.rstrip("Z"), "0")
    instant = datetime.datetime.fromisoformat(whole).replace(tzinfo=datetime.timezone.utc)
    return int(instant.timestamp()) * 1_000_000_000 + int(fraction.ljust(9, "0")[:9])


def worker_events(folder, requests, raw, samples):
    events, orphan, reopened_events = [], [], []
    lines = read_bytes(folder / "node.log").decode().splitlines()
    assert len(lines) <= 100000
    original_stop = next(row["at_unix_ns"] for row in raw if row.get("operation") == "owned-node-stop")
    native = [row for row in raw if row.get("operation") == "request" and row.get("phase") != "reopen"
              and row.get("path") == "/api/db/native-status"]
    for number, original in enumerate(lines, 1):
        line = re.sub(r"\x1b\[[0-9;]*m", "", original)
        if "native storage maintenance reached the dependency high-water mark" in line:
            timestamp = unix_ns(line.split()[0])
            fields = {name: int(value) for name, value in re.findall(
                r"\b(parts_before|parts_after|files_removed|pack_millis|reclaim_millis)=(\d+)", line)}
            assert len(fields) == 5
            end = timestamp - fields["reclaim_millis"] * 1_000_000
            start = end - fields["pack_millis"] * 1_000_000
            overlap = [index + 1 for index, row in enumerate(requests)
                       if row["started_unix_ns"] < end and row["ended_unix_ns"] > start]
            event = {"line": number, "completion_log_unix_ns": timestamp, **fields,
                     "approximate_pack_interval_unix_ns": [start, end],
                     "requests_overlapping_approximate_interval": overlap}
            (events if timestamp <= original_stop else reopened_events).append(event)
        elif "native storage removed unreferenced part files" in line:
            timestamp = unix_ns(line.split()[0])
            event = {"line": number, "at_unix_ns": timestamp,
                     "fields": {name: int(value) for name, value in re.findall(r"\b(files|bytes)=(\d+)", line)}}
            (orphan if timestamp <= original_stop else reopened_events).append(event)
    first = samples[0]["native"]["parts"]["maintenance_runs"]
    last = samples[-1]["native"]["parts"]["maintenance_runs"]
    equal(len(events), last)
    bounds = []
    for count in range(first + 1, last + 1):
        lower, upper = [], []
        for row in native:
            body = row["response"].get("data", row["response"])
            observed = body["native_disk"]["parts"]["maintenance_runs"]
            (lower if observed < count else upper).append(row)
        assert lower and upper
        bounds.append({"maintenance_run": count,
                       "completion_observation_bound_unix_ns": [max(row["started_unix_ns"] for row in lower),
                                                                 min(row["ended_unix_ns"] for row in upper)]})
    return {"completed_pack_count": last - first, "pack_log_events": events,
            "pack_completion_status_bounds": bounds, "orphan_removal_log_events": orphan,
            "reopened_node_events": reopened_events,
            "pack_or_orphan_removal_observed": bool(last - first or orphan),
            "interval_limit": "Reconstructed pack windows are approximate: reclaim capture, dispatch, and log delay lie outside the section timers. Status bounds cover completion, not exact gate ownership."}


def audit_run(root, variant):
    folder = root / ("auto-" + variant + "-1")
    summary, inputs, cleanup = load(folder / "summary.json"), load(folder / "inputs.json"), load(folder / "cleanup.json")
    equal(inputs["source_revision"], REVISIONS[variant])
    equal(inputs["settings"]["LASTDB_AUTO_MAINTENANCE"], "1")
    equal(inputs["unset_default_maintenance_knobs"], {"dependency_high_water": 4096,
          "start_delay_seconds": 30, "grace_ms": 0, "orphan_min_age_ms": 600000})
    equal(summary, {"complete": True, "all_owned_processes_stopped": True,
                    "all_values_and_reopen_verified": True, "dropped_requests": 0,
                    "known_keys": 64, "fixed_requests": 330, "batch_width": 32,
                    "nominal_active_seconds": 660, "fixed_quiet_tail_seconds": 60})
    equal(cleanup["removed"], True)
    raw = receipts(folder)
    requests, intervals, dimensions = audit_requests(folder, summary, raw)
    samples, memory = audit_samples(folder, summary, intervals)
    events = worker_events(folder, requests, raw, samples)
    snapshots = {name: {"inventory": summary[name]["inventory"], "parts": summary[name]["native"]["parts"]}
                 for name in ("seeded", "after_active", "after_quiet_tail", "stable_final")}
    seed = snapshots["seeded"]["inventory"]
    growth = {}
    for name, snapshot in snapshots.items():
        listing = snapshot["inventory"]
        growth[name] = {field: listing[field] - seed[field] for field in ("files", "bytes", "allocated_bytes")}
        growth[name]["families"] = {
            family: {field: listing["families"].get(family, {}).get(field, 0)
                     - seed["families"].get(family, {}).get(field, 0)
                     for field in ("files", "bytes", "allocated_bytes")}
            for family in sorted(set(seed["families"]) | set(listing["families"]))}
    counters = {name: samples[-1]["process"][name] - samples[0]["process"][name] for name in
                ("ri_diskio_bytesread", "ri_diskio_byteswritten", "ri_logical_writes")}
    return {"variant": variant, "source_revision": REVISIONS[variant], "independent_audit_passed": True,
            "n": 1, "scope": "One descriptive AUTO-on run: 660 nominal active seconds and 60 quiet seconds after the final ack; no steady-state or significance claim",
            "write_inputs": dimensions, "ack": stats([row["elapsed_seconds"] for row in requests]),
            "tail_counts_strictly_greater_than_ms": {str(ms): sum(row["elapsed_seconds"] * 1000 > ms for row in requests)
                                                    for ms in (100, 500, 1000)},
            "actual_active_seconds": summary["actual_active_seconds"],
            "max_start_lateness_seconds": summary["max_start_lateness_seconds"],
            "retained_samples": snapshots, "growth_from_seed": growth, "sample_count": len(samples),
            "sample_span_process_io_deltas": counters, "memory": memory, "worker": events,
            "input_sha256": {name: hashlib.sha256(read_bytes(folder / name)).hexdigest() for name in
                             ("summary.json", "inputs.json", "request-intervals.json", "mutation-windows.json", "size-samples.json", "node.log")}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--variant", choices=tuple(REVISIONS), action="append")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Write a new analysis artifact; never replace raw evidence")
    results = [audit_run(args.root, name) for name in args.variant or REVISIONS]
    if len(results) > 1:
        assert all(row["write_inputs"] == results[0]["write_inputs"] for row in results)
    output = {"runs": results, "all_audits_passed": True,
              "analysis_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    args.output.write_text(json.dumps(output, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"variants": [row["variant"] for row in results], "all_audits_passed": True}))


if __name__ == "__main__":
    main()
