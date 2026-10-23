"""A separate fixed probe sweep after all first-round matched measurements."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import statistics
import time

from reuse import original
from benchmark_support import phase_cost, save, stats

COUNT = 64
FIRST = 0.02
INTERVAL = 0.05


def timing_stats(values):
    ordered = sorted(values)
    return {"samples": len(ordered), "mean_ms": statistics.mean(ordered) * 1000,
            "median_ms": statistics.median(ordered) * 1000,
            "p95_ms": ordered[math.ceil(0.95 * len(ordered)) - 1] * 1000,
            "max_ms": max(ordered) * 1000, "min_ms": min(ordered) * 1000}


def stream(capture, role, dispatched):
    results = []
    for ordinal in range(COUNT):
        nominal = dispatched + int((FIRST + INTERVAL * ordinal) * 1e9)
        remaining = (nominal - time.monotonic_ns()) / 1e9
        if remaining > 0:
            time.sleep(min(remaining, capture.remaining()))
        if role == "warm_read":
            response, receipt = capture.call_with_receipt("/api/query", original("benchmark").query())
            if capture.rows(response) != {"key-000001": capture.expected["key-000001"]}:
                raise RuntimeError("A focused warm read returns an unexpected value")
        else:
            _, receipt = capture.write([400], 7000 + ordinal, "durable")
        timing = {"request_id": receipt["request_id"], "role": role, "ordinal": ordinal,
                  "nominal_start_monotonic_ns": nominal,
                  "started_monotonic_ns": receipt["started_monotonic_ns"],
                  "ended_monotonic_ns": receipt["ended_monotonic_ns"],
                  "start_lateness_seconds": (receipt["started_monotonic_ns"] - nominal) / 1e9,
                  "elapsed_seconds": receipt["elapsed_seconds"]}
        capture.remember("focused-probe", **timing)
        results.append((timing, receipt))
    return results


def describe(probes, compact):
    values = []
    for timing, _ in probes:
        timing["request_intervals_overlap"] = max(timing["started_monotonic_ns"],
            compact["started_monotonic_ns"]) < min(timing["ended_monotonic_ns"], compact["ended_monotonic_ns"])
        values.append(timing)
    overlapping = [row for row in values if row["request_intervals_overlap"]]
    return {"requests": COUNT, "overlap_count": len(overlapping),
            "all_ack": timing_stats([row["elapsed_seconds"] for row in values]),
            "overlap_ack": timing_stats([row["elapsed_seconds"] for row in overlapping]) if overlapping else None,
            "start_lateness": timing_stats([row["start_lateness_seconds"] for row in values]),
            "requests_timeline": values}


def run(capture):
    capture.phase = "focused-compaction"
    baseline = [capture.call_with_receipt("/api/query", original("benchmark").query())[1]["elapsed_seconds"]
                for _ in range(8)]
    before = capture.checkpoint("focused-compact-before")
    dispatched_unix, dispatched = time.time_ns(), time.monotonic_ns()
    with ThreadPoolExecutor(max_workers=3) as pool:
        compact = pool.submit(capture.call_with_receipt, "/api/db/compact", {"dry_run": False, "grace_ms": 0})
        read = pool.submit(stream, capture, "warm_read", dispatched)
        write = pool.submit(stream, capture, "durable_write", dispatched)
        report, compact_receipt = compact.result()
        reads, writes = read.result(), write.result()
    flush, flush_receipt = capture.flush("focused-timed-final-flush")
    ended = time.monotonic_ns()
    after = capture.checkpoint("focused-compact-after")
    result = {"label": "focused-compaction", "kind": "fixed-probe-sweep", "grace_ms": 0,
              "nominal_first_delay_seconds": FIRST, "nominal_interval_seconds": INTERVAL,
              "dispatched_unix_ns": dispatched_unix, "dispatched_monotonic_ns": dispatched,
              "ended_monotonic_ns": ended, "wall_through_final_flush_seconds": (ended - dispatched) / 1e9,
              "compact_wall_seconds": compact_receipt["elapsed_seconds"],
              "compact_started_monotonic_ns": compact_receipt["started_monotonic_ns"],
              "compact_ended_monotonic_ns": compact_receipt["ended_monotonic_ns"],
              "warm_read_baseline": stats(baseline), "report": report,
              "percentile_rule": "Nearest rank; overlap percentiles can have few requests, so retain the sample count",
              "probes": {"warm_read": describe(reads, compact_receipt),
                         "durable_write": describe(writes, compact_receipt)},
              "write_inputs": original("benchmark").dimensions([row[1] for row in writes]),
              "final_explicit_flush": flush, "final_explicit_flush_seconds": flush_receipt["elapsed_seconds"],
              "scope": "Separate compaction plus fixed probe sweep; overlap refers to request intervals",
              **phase_cost(before, after)}
    capture.readback("focused-compaction")
    result["all_values_verified"] = True
    result["expected_values_sha256"] = hashlib.sha256(json.dumps(capture.expected, sort_keys=True).encode()).hexdigest()
    capture.remember("focused-summary", summary=result)
    capture.save_receipts()
    save(capture.output / "focused-compaction.json", result)
    return result
