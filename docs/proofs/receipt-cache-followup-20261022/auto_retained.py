#!/usr/bin/env python3
"""Measure one active default-maintenance cycle and a fixed quiet tail."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "receipt-part-pruning-benchmark-20261022"))
from reuse import original
import benchmark_support as support
from benchmark_support import FIELDS, SCHEMA, file_hash, save, stats
from io_probe import sample_process
from long_sampling import Samples
from retained_inventory import LIMITS

REQUESTS, WIDTH, KEYS = 330, 32, 64
CADENCE_SECONDS, QUIET_SECONDS = 2, 60


class AutoCapture(support.Benchmark):
    def start(self):
        previous = support.SETTINGS
        support.SETTINGS = {**previous, "LASTDB_AUTO_MAINTENANCE": "1",
                            "RUST_LOG": "warn,fold_db::storage::disk_pack::parts=info"}
        try:
            super().start()
        finally:
            support.SETTINGS = previous


def readback(capture, label):
    request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 128,
               "filter": {"HashRangeKeys": [["measure", key] for key in sorted(capture.expected)]}}
    exact = capture.call("/api/query", request)
    if capture.rows(exact) != capture.expected or exact.get("has_more") is not False:
        raise RuntimeError("The active exact read differs from the sixty-four known keys")
    actual, order, cursor = {}, [], None
    for _ in range(4):
        request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 37,
                   "filter": {"HashRangeRange": {"hash": "measure", "start": "key-000000", "end": "key-000064"}}}
        if cursor is not None:
            request["cursor"] = cursor
        result = capture.call("/api/query", request)
        rows = capture.rows(result)
        if len(rows) > 37 or set(rows).intersection(actual):
            raise RuntimeError("The active range repeats a key or exceeds its page bound")
        actual.update(rows)
        order.extend(row["key"]["range"] for row in result["results"])
        if result.get("has_more") is False:
            break
        next_cursor = result.get("next_cursor")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("The active range cursor cannot advance")
        cursor = next_cursor
    else:
        raise RuntimeError("The active range exceeds four pages")
    if actual != capture.expected or order != sorted(capture.expected):
        raise RuntimeError("The active range values or order differ")
    capture.remember("known-key-readback", label=label, keys=KEYS, range_page_size=37,
                     expected_sha256=hashlib.sha256(json.dumps(capture.expected, sort_keys=True).encode()).hexdigest())


def wait_until(capture, deadline, samples):
    while time.monotonic_ns() < deadline:
        capture.remaining()
        if samples.errors:
            raise RuntimeError("The active size sampler failed: " + repr(samples.errors))
        time.sleep(min(0.25, max(0, (deadline - time.monotonic_ns()) / 1e9)))


def active(capture, samples):
    receipts, windows = [], []
    origin = samples.origin
    for ordinal in range(REQUESTS):
        nominal = origin + (ordinal + 1) * CADENCE_SECONDS * 1_000_000_000
        wait_until(capture, nominal, samples)
        first = (ordinal * WIDTH) % KEYS
        _, receipt = capture.write(list(range(first, first + WIDTH)), 100000 + ordinal)
        receipt["nominal_start_monotonic_ns"] = nominal
        receipt["start_lateness_seconds"] = (receipt["started_monotonic_ns"] - nominal) / 1e9
        receipts.append(receipt)
        if (ordinal + 1) % 15 == 0:
            readback(capture, "active-batch-" + str(ordinal + 1))
            batch = receipts[-15:]
            windows.append({"last_request_ordinal": ordinal + 1,
                            "ack": stats([row["elapsed_seconds"] for row in batch]),
                            "max_start_lateness_seconds": max(row["start_lateness_seconds"] for row in batch),
                            "started_monotonic_ns": batch[0]["started_monotonic_ns"],
                            "ended_monotonic_ns": batch[-1]["ended_monotonic_ns"]})
            capture.save_receipts()
    return receipts, windows


def run(capture):
    capture.start()
    capture.phase = "seed"
    capture.install()
    capture.flush("schema-ready")
    for first in (0, 32):
        capture.write(list(range(first, first + WIDTH)), 0, create=True)
    capture.flush("seed-ready")
    readback(capture, "seed-ready")
    samples = Samples(capture, sample_process)
    seeded = samples.take("seeded-before-active")
    parts = seeded["native"]["parts"]
    if (parts["auto_maintenance"] is not True or parts["dependency_high_water"] != 4096
            or parts["dependency_trigger"] != 4096):
        raise RuntimeError("The active run does not use automatic maintenance and its default trigger")
    capture.phase = "active"
    samples.origin = time.monotonic_ns()
    samples.start()
    try:
        receipts, windows = active(capture, samples)
        final_ack = receipts[-1]["ended_monotonic_ns"]
        after_active = samples.take("after-last-active-ack")
        capture.phase = "quiet-tail"
        wait_until(capture, final_ack + QUIET_SECONDS * 1_000_000_000, samples)
        tail_end = samples.take("fixed-quiet-tail-end")
    finally:
        samples.stop()
    capture.flush("quiet-tail-final-fence")
    stable = samples.quiet()
    readback(capture, "quiet-tail")
    capture.stop()
    capture.phase = "reopen"
    capture.start()
    readback(capture, "reopened")
    capture.flush("reopen-durable-fence")
    dimensions = original("benchmark").dimensions(receipts)
    save(capture.output / "size-samples.json", samples.rows)
    save(capture.output / "mutation-windows.json", windows)
    intervals = [{name: row[name] for name in ("started_unix_ns", "ended_unix_ns", "started_monotonic_ns",
                 "ended_monotonic_ns", "elapsed_seconds", "nominal_start_monotonic_ns", "start_lateness_seconds")}
                 for row in receipts]
    save(capture.output / "request-intervals.json", intervals)
    return {"complete": True, "known_keys": KEYS, "fixed_requests": REQUESTS, "batch_width": WIDTH,
            "write_inputs": dimensions, "all_values_and_reopen_verified": True,
            "nominal_active_seconds": REQUESTS * CADENCE_SECONDS,
            "actual_active_seconds": (final_ack - samples.origin) / 1e9,
            "fixed_quiet_tail_seconds": QUIET_SECONDS,
            "observed_quiet_tail_seconds": (tail_end["ended_monotonic_ns"] - final_ack) / 1e9,
            "dropped_requests": 0, "ack": stats([row["elapsed_seconds"] for row in receipts]),
            "max_start_lateness_seconds": max(row["start_lateness_seconds"] for row in receipts),
            "seeded": seeded, "after_active": after_active, "after_quiet_tail": tail_end, "stable_final": stable,
            "sample_count": len(samples.rows), "size_samples_sha256": file_hash(capture.output / "size-samples.json"),
            "scope": "One descriptive first-cycle run; active writes continue through the default ten-minute repeat. No steady-state or significance claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    capture = AutoCapture(args)
    capture.deadline = time.monotonic() + 1080
    helper_names = ("auto_retained.py", "long_sampling.py", "retained_inventory.py")
    helpers = [ROOT / name for name in helper_names]
    helpers += [Path(sys.modules["reuse"].__file__), Path(support.__file__), Path(support.measure.__file__),
                Path(sys.modules[support.Capture.__module__].__file__), Path(sample_process.__code__.co_filename),
                Path(original("benchmark").__file__)]
    save(capture.output / "inputs.json", {"source_revision": args.source_revision, "label": args.label,
         "binary": str(capture.binary), "binary_sha256": file_hash(capture.binary),
         "helpers_sha256": {str(path): file_hash(path) for path in helpers},
         "automatic_maintenance": True, "records": KEYS, "schema_fields": FIELDS, "body_bytes": 1024,
         "seed": {"requests": 2, "width": WIDTH, "durability": "durable"},
         "active": {"requests": REQUESTS, "width": WIDTH, "cadence_seconds": CADENCE_SECONDS,
                    "nominal_first_request_seconds": 2, "nominal_last_request_seconds": 660,
                    "late_requests_skipped": False, "durability": "durable"},
         "quiet_tail_seconds_after_actual_final_ack": QUIET_SECONDS,
         "settings": {**support.SETTINGS, "LASTDB_AUTO_MAINTENANCE": "1",
                      "RUST_LOG": "warn,fold_db::storage::disk_pack::parts=info"},
         "unset_default_maintenance_knobs": {"dependency_high_water": 4096, "start_delay_seconds": 30,
                                             "grace_ms": 0, "orphan_min_age_ms": 600000},
         "limits": {**LIMITS, "run_seconds": 1080, "request_seconds": 120, "max_size_samples": 64},
         "claim_limit": "One descriptive first default-maintenance cycle per variant; no steady-state claim"})
    summary = {"complete": False}
    try:
        summary.update(run(capture))
    except BaseException as failure:
        summary["error"] = str(failure)
        raise
    finally:
        try:
            capture.stop()
        except BaseException as failure:
            summary.update(complete=False, shutdown_error=str(failure))
            raise
        finally:
            capture.save_receipts()
            summary.update(all_owned_processes_stopped=capture.process is None,
                           scratch=str(capture.root), home=str(capture.home), scratch_retained=True)
            save(capture.output / "summary.json", summary)
            print(json.dumps({name: value for name, value in summary.items() if name not in
                             ("seeded", "after_active", "after_quiet_tail", "stable_final")}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
