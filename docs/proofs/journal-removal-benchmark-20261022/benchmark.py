#!/usr/bin/env python3
"""Measure fixed product workloads on one new owned LastDB home; retain receipts."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from benchmark_support import Benchmark, FIELDS, SCHEMA, phase_cost, save, stats


PHASES = [("durable-single-64", 64, 1, 0, "durable", 1000),
          ("durable-batch32-16", 16, 32, 0, "durable", 2000),
          ("queued-single-256", 256, 1, 128, "queued", 3000),
          ("queued-batch32-8", 8, 32, 0, "queued", 4000)]


def dimensions(receipts):
    requests = [row["request"] for row in receipts]
    mutations = [mutation for request in requests for mutation in request["mutations"]]
    values = [mutation["fields_and_values"] for mutation in mutations]
    digest = hashlib.sha256()
    for request in requests:
        digest.update((json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return {"requests": len(requests), "logical_mutations": len(mutations),
            "changed_field_values": sum(len(value) for value in values),
            "request_json_bytes": sum(row["request_bytes"] for row in receipts),
            "fields_json_bytes": sum(len(json.dumps(value, separators=(",", ":")).encode()) for value in values),
            "body_value_bytes": sum(len(value.get("body", "").encode()) for value in values),
            "title_value_bytes": sum(len(value.get("title", "").encode()) for value in values),
            "ordered_input_sha256": digest.hexdigest()}


def seed(capture):
    capture.phase = "seed"
    capture.install()
    capture.flush("schema-ready")
    receipts = []
    for first in range(0, 512, 32):
        _, receipt = capture.write(list(range(first, first + 32)), 0, create=True)
        receipts.append(receipt)
    capture.checkpoint("seeded-512")
    capture.readback("seeded-512")
    capture.remember("seed-input-dimensions", **dimensions(receipts))
    capture.save_receipts()


def write_phase(capture, spec):
    label, count, width, offset, mode, revision = spec
    capture.phase = label
    before = capture.checkpoint(label + "-before")
    started_unix, started = time.time_ns(), time.monotonic_ns()
    receipts = []
    for step in range(count):
        numbers = [(offset + step * width + item) % 512 for item in range(width)]
        _, receipt = capture.write(numbers, revision + step, mode)
        receipts.append(receipt)
    final_ack = time.monotonic_ns()
    flush, flush_receipt = capture.flush(label + "-timed-final-flush")
    ended, ended_unix = time.monotonic_ns(), time.time_ns()
    after = capture.checkpoint(label + "-after")
    result = {"kind": "write-phase", "label": label, "requested_durability": mode,
              "width": width, "inputs": dimensions(receipts),
              "started_unix_ns": started_unix, "ended_unix_ns": ended_unix,
              "started_monotonic_ns": started, "ended_monotonic_ns": ended,
              "ack_sequence_seconds": (final_ack - started) / 1e9,
              "wall_through_final_flush_seconds": (ended - started) / 1e9,
              "request_ack": stats([row["elapsed_seconds"] for row in receipts]),
              "final_explicit_flush_seconds": flush_receipt["elapsed_seconds"],
              "final_explicit_flush": flush, **phase_cost(before, after)}
    capture.readback(label)
    capture.remember("phase-summary", summary=result)
    capture.save_receipts()
    save(capture.output / f"{label}.json", result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return result


def query():
    return {"schema_name": SCHEMA, "fields": FIELDS,
            "filter": {"HashRangeKeys": [["measure", "key-000001"]]}, "limit": 1}


def compact_phase(capture):
    capture.phase = "compaction"
    baseline = [capture.call_with_receipt("/api/query", query())[1]["elapsed_seconds"]
                for _ in range(8)]
    before = capture.checkpoint("compact-before")
    started_unix, started = time.time_ns(), time.monotonic_ns()
    with ThreadPoolExecutor(max_workers=3) as pool:
        compact = pool.submit(capture.call_with_receipt, "/api/db/compact",
                              {"dry_run": False, "grace_ms": 0})
        time.sleep(0.02)
        read = pool.submit(capture.call_with_receipt, "/api/query", query())
        write = pool.submit(capture.write, [400], 5000, "durable")
        compact_result, compact_receipt = compact.result()
        read_result, read_receipt = read.result()
        write_result, write_receipt = write.result()
    flush, flush_receipt = capture.flush("compact-timed-final-flush")
    ended, ended_unix = time.monotonic_ns(), time.time_ns()
    after = capture.checkpoint("compact-after")
    if capture.rows(read_result) != {"key-000001": capture.expected["key-000001"]}:
        raise RuntimeError("The concurrent warm read returns an unexpected value")
    probes = {}
    for name, receipt in (("warm_read", read_receipt), ("durable_write", write_receipt)):
        probes[name] = {"ack_seconds": receipt["elapsed_seconds"],
                        "started_monotonic_ns": receipt["started_monotonic_ns"],
                        "ended_monotonic_ns": receipt["ended_monotonic_ns"],
                        "start_after_dispatch_seconds": (receipt["started_monotonic_ns"] - started) / 1e9,
                        "request_intervals_overlap": max(receipt["started_monotonic_ns"],
                            compact_receipt["started_monotonic_ns"]) < min(receipt["ended_monotonic_ns"],
                            compact_receipt["ended_monotonic_ns"])}
    result = {"kind": "compaction-phase", "label": "compaction", "grace_ms": 0,
              "scheduled_probe_delay_seconds": 0.02, "warm_read_baseline": stats(baseline),
              "started_unix_ns": started_unix, "ended_unix_ns": ended_unix,
              "started_monotonic_ns": started, "ended_monotonic_ns": ended,
              "wall_through_final_flush_seconds": (ended - started) / 1e9,
              "compact_wall_seconds": compact_receipt["elapsed_seconds"],
              "compact_request_started_monotonic_ns": compact_receipt["started_monotonic_ns"],
              "compact_request_ended_monotonic_ns": compact_receipt["ended_monotonic_ns"],
              "probes": probes, "report": compact_result, "write_reply": write_result,
              "probe_write_inputs": dimensions([write_receipt]),
              "final_explicit_flush": flush, "final_explicit_flush_seconds": flush_receipt["elapsed_seconds"],
              **phase_cost(before, after)}
    capture.readback("compaction")
    capture.remember("phase-summary", summary=result)
    capture.save_receipts()
    save(capture.output / "compaction.json", result)
    print(json.dumps(result, sort_keys=True), flush=True)
    return result


def run(capture):
    capture.start()
    seed(capture)
    results = [write_phase(capture, spec) for spec in PHASES]
    results.append(compact_phase(capture))
    results.append(write_phase(capture, ("durable-single-after-compact-24", 24, 1, 450, "durable", 6000)))
    capture.phase = "final-verification"
    capture.readback("all-phases")
    capture.checkpoint("final-before-reopen")
    capture.save_receipts()
    capture.stop()
    capture.save_receipts()
    capture.phase = "reopen-verification"
    capture.start()
    capture.readback("graceful-reopen")
    capture.checkpoint("reopened")
    capture.save_receipts()
    save(capture.output / "checkpoints.json", capture.checkpoints)
    return {"complete": True, "records": 512, "phases": results,
            "all_phase_values_verified": True, "graceful_reopen_verified": True,
            "expected_values_sha256": hashlib.sha256(json.dumps(capture.expected, sort_keys=True).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    capture = Benchmark(args)
    summary = {"complete": False, "label": args.label, "source_revision": args.source_revision}
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
            summary.update(scratch=str(capture.root), home=str(capture.home),
                           all_owned_processes_stopped=capture.process is None, scratch_retained=True)
            save(capture.output / "summary.json", summary)
            print(json.dumps({key: value for key, value in summary.items() if key != "phases"},
                             sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
