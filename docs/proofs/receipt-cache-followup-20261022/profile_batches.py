#!/usr/bin/env python3
"""Capture real release-node call stacks during a fixed generated batch workload."""

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
PRIOR = ROOT.parent / "receipt-part-pruning-benchmark-20261022"
sys.path.insert(0, str(PRIOR))
from reuse import original
from benchmark_support import file_hash, save
from io_probe import sample_process
from profile_sample import Sampler

REQUESTS = 256
WIDTH = 32


def inventory(capture):
    home = capture.home / "data" / "disk-pack"
    pending, result, entries, total, deadline = [(home, 0)], {}, 0, 0, time.monotonic() + 20
    while pending:
        parent, depth = pending.pop()
        if depth > 8:
            raise RuntimeError("The diagnostic inventory exceeds depth eight")
        for path in parent.iterdir():
            entries += 1
            if path.is_symlink() or entries > 40000 or time.monotonic() > deadline:
                raise RuntimeError("The diagnostic inventory is linked or exceeds its finite bound")
            if path.is_dir():
                pending.append((path, depth + 1))
            elif path.is_file():
                info = path.stat()
                total += info.st_size
                if len(result) >= 20000 or total > 4 * 1024 ** 3:
                    raise RuntimeError("The diagnostic home exceeds twenty thousand files or four GiB")
                result[str(path.relative_to(home))] = [info.st_size, info.st_blocks * 512]
    return {"files": len(result), "bytes": total, "allocated_bytes": sum(row[1] for row in result.values()),
            "paths": result}


def timebase():
    class Info(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]
    info = Info()
    result = ctypes.CDLL("/usr/lib/libSystem.B.dylib").mach_timebase_info(ctypes.byref(info))
    if result != 0 or info.numer == 0 or info.denom == 0:
        raise RuntimeError("The host does not return a valid Mach timebase")
    return {"numer": info.numer, "denom": info.denom}


def run(capture):
    capture.start()
    original("benchmark").seed(capture)
    capture.phase = "fixed-batch-profile"
    start_inventory = inventory(capture)
    before = sample_process(capture.process.pid)
    cpu_started = time.monotonic_ns()
    conversion = timebase()
    profiler = Sampler(capture.process.pid, capture.output)
    capture.remember("owned-profiler-start", pid=profiler.process.pid, target_pid=capture.process.pid,
                     argv=profiler.argv, started_monotonic_ns=profiler.dispatch_monotonic_ns)
    receipts = []
    try:
        for step in range(REQUESTS):
            first = (step * WIDTH) % 512
            _, receipt = capture.write(list(range(first, first + WIDTH)), 10000 + step)
            receipts.append(receipt)
            if step % 16 == 15:
                inventory(capture)
        capture.flush("fixed-profile-work-complete")
        work_ended = time.monotonic_ns()
        after = sample_process(capture.process.pid)
        cpu_ended = time.monotonic_ns()
        sampler = profiler.finish(timeout=min(60, capture.remaining()))
    finally:
        profiler.stop()
    code = sampler["actual_child_exit"]["exit_code"]
    capture.remember("owned-profiler-stop", **sampler)
    if code != 0 or not (capture.output / "sample.txt").is_file():
        raise RuntimeError("The owned-process sample failed; preserve its exact error and home")
    raw = {name: after[name] - before[name] for name in ("ri_user_time", "ri_system_time")}
    cpu = {name: ticks * conversion["numer"] / conversion["denom"] / 1e9 for name, ticks in raw.items()}
    end_inventory = inventory(capture)
    native = capture.call("/api/db/native-status")["native_disk"]
    if native.get("writer_failed") is not False:
        raise RuntimeError("The profiled native writer reports failure")
    capture.readback("profile-fixed-work")
    capture.stop()
    capture.start()
    capture.readback("profile-reopened")
    return {"complete": True, "fixed_batch_requests": REQUESTS, "batch_width": WIDTH,
            "offered_logical_mutations": REQUESTS * WIDTH, "sample_seconds_requested": 20,
            "sample_interval_ms": 5, "sample_exit_code": code, "profiler_stopped": True,
            "sample_sha256": file_hash(capture.output / "sample.txt"), "mach_timebase": conversion,
            "raw_cpu_counter_deltas": raw, "converted_cpu_seconds": cpu,
            "cpu_formula": "raw Mach ticks * numer / denom / 1000000000",
            "sampler": sampler, "capture_overlap": profiler.overlaps(receipts),
            "cpu_counter_interval_monotonic_ns": [cpu_started, cpu_ended],
            "cpu_scope": "Whole fixed 256-request workload and final flush; separate from the sampled interval",
            "work_ended_monotonic_ns": work_ended, "write_inputs": original("benchmark").dimensions(receipts),
            "request_intervals": [{name: receipt[name] for name in
                ("started_monotonic_ns", "ended_monotonic_ns", "started_unix_ns", "ended_unix_ns",
                 "elapsed_seconds", "request_bytes")} for receipt in receipts],
            "start_inventory": start_inventory, "end_inventory": end_inventory,
            "exact_range_reopen_verified": True, "native_at_work_end": native,
            "scope": "Diagnostic wall-stack samples during real generated batch writes; not benchmark latency or CPU percentages"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    capture = original("benchmark").Benchmark(args)
    save(capture.output / "diagnostic-inputs.json", {"source_revision": args.source_revision,
         "binary": str(capture.binary), "binary_sha256": file_hash(capture.binary),
         "driver_sha256": file_hash(Path(__file__)), "profile_binary": "/usr/bin/sample",
         "profile_binary_sha256": file_hash(Path("/usr/bin/sample")), "fixed_batch_requests": REQUESTS,
         "batch_width": WIDTH, "keys": 512, "body_bytes": 1024, "automatic_maintenance": False,
         "limits": {"run_seconds": 300, "native_bytes": 4 * 1024 ** 3, "native_files": 20000,
                    "inventory_entries": 40000, "inventory_depth": 8, "inventory_seconds": 20},
         "supersedes_inherited_limits": "The inherited seed inputs retain their original 1 GiB cap; this separate diagnostic uses 4 GiB for 8192 offered updates",
         "claim_limit": "Diagnostic profile; instrumentation changes timing"})
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
            print(json.dumps({key: value for key, value in summary.items() if key not in
                             ("start_inventory", "end_inventory", "request_intervals")}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
