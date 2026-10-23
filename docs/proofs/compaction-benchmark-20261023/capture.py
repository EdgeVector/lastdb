#!/usr/bin/env python3
"""Benchmark real compaction and caller latency on one fresh generated home."""
import argparse
import importlib.util
import json
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import resource
import signal
import subprocess
import threading
import time

from workload import Workload

HERE = Path(__file__).resolve().parent
SUPPORT = HERE.parent / "atom-scale-review-20261022" / "measure.py"
spec = importlib.util.spec_from_file_location("scale_measure", SUPPORT)
scale = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scale)
base = scale.base


class Benchmark(Workload, scale.Measure):
    def __init__(self, args):
        args.automatic_worker, args.baseline_seconds, args.seed_batch = False, 10, 128
        super().__init__(args)
        self.protocol = base
        self.label, self.pair = args.label, args.pair
        self.samples, self.phases, self.outcomes, self.resources = {}, {}, [], []
        self.monitor_errors = []
        self.monitor_stop = threading.Event()
        self.stage = "seed"
        inputs = {"source_revision": args.source_revision, "binary": str(self.binary),
            "binary_sha256": base.file_hash(self.binary), "label": self.label, "pair": self.pair,
            "records": self.records, "body_bytes": 1024, "seed_batch": 128,
            "seed_durability": "durable", "auto_maintenance": False,
            "clock": "perf_counter_ns for RPC; time_ns for interval intersection",
            "filesystem_cache": "uncontrolled; fresh independent homes; alternating pair order",
            "scope": "actual owned daemon; generated data only; no primary or copied home",
            "driver_sha256": base.file_hash(Path(__file__)),
            "workload_sha256": base.file_hash(HERE / "workload.py"),
            "scale_support_sha256": base.file_hash(SUPPORT),
            "product_support_sha256": base.file_hash(Path(scale.product.__file__)),
            "snapshot_support_sha256": base.file_hash(Path(base.__file__)),
            "limits": "three pairs; shared host; sampled resources; no fixed latency guarantee"}
        base.save(self.output / "inputs.json", inputs)
        base.save(self.output / "recipe.json", {"records": self.records,
            "baseline": {"queued": {"batches": 100, "rate": 20},
                         "durable": {"batches": 100, "rate": 10}},
            "active": {"queued": {"batches": 400, "rate": 20},
                       "durable": {"batches": 200, "rate": 10}},
            "batch_records": 16, "hot_records": 64, "copy_start_after_acks": 5,
            "cleanup_start": "observed copy phase", "full_copy": True,
            "two_copy_start": "shared barrier; two simultaneous HTTP callers",
            "final_validation": "all seeded keys by exact key; complete counter tally",
            "unexpected_extra_membership_checked": False,
            "resource_sample_interval_seconds": 1,
            "timer_limit": "old swap timer excludes gate wait, queue drain and view handoff; new timer includes the full acquired gate"})

    def remember(self, operation, **values):
        phase = getattr(getattr(self, "local", None), "phase", "startup")
        if operation == "request" and values.get("path") == "/api/mutations/batch":
            ended = time.time_ns()
            elapsed = values["elapsed_nanos"]
            row = {"ended_ns": ended, "started_ns": ended - elapsed,
                   "ended_mono_ns": time.perf_counter_ns(),
                   "elapsed_ms": elapsed / 1e6, "http_status": values["http_status"],
                   "index": getattr(self.local, "writer_index", None),
                   "dispatch_lag_ms": getattr(self.local, "dispatch_lag_ms", 0)}
            self.samples.setdefault(phase, []).append(row)
        values.setdefault("phase", phase)
        scale.product.ProductRun.remember(self, operation, **values)

    def monitor(self):
        while not self.monitor_stop.wait(1):
            process = self.process
            if process is None or process.poll() is not None:
                continue
            try:
                rss = subprocess.run(["/bin/ps", "-p", str(process.pid), "-o", "rss="],
                                     capture_output=True, text=True, timeout=5)
                files = subprocess.run(["/usr/sbin/lsof", "-p", str(process.pid), "-Ff"],
                                       capture_output=True, text=True, timeout=5)
                self.resources.append({"at_ns": time.time_ns(), "phase": self.stage,
                    "pid": process.pid, "rss_bytes": int(rss.stdout.strip() or 0) * 1024,
                    "open_fds": sum(line.startswith("f") and line[1:].isdigit()
                                    for line in files.stdout.splitlines()),
                    "ps_exit": rss.returncode, "lsof_exit": files.returncode})
            except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                self.monitor_errors.append({"at_ns": time.time_ns(), "error": str(error)})

    def run(self):
        self.start()
        base.save(self.output / "version.json", self.call("/api/version"))
        status = self.call("/api/db/native-status")["native_disk"]["parts"]
        identified = "compaction_jobs" in status
        base.save(self.output / "implementation-identification.json", {
            "expected_new_compaction": self.label == "after",
            "new_compaction_status_present": identified, "parts": status})
        if identified != (self.label == "after"):
            raise RuntimeError("The running storage library differs from the benchmark revision")
        self.install()
        self.local.phase = "seed"
        started = time.monotonic()
        for first in range(0, self.records, 128):
            self.mutate("create", range(first, min(first + 128, self.records)), 0,
                        durability="durable")
            if first % 12800 == 0:
                print(json.dumps({"pair": self.pair, "label": self.label,
                    "seeded": min(first + 128, self.records)}), flush=True)
        self.flush("seed")
        self.remember("seed-complete", seconds=time.monotonic() - started,
                      records=self.records)
        monitor = threading.Thread(target=self.monitor, daemon=True)
        monitor.start()
        try:
            self.quiet("idle-full")
            self.verify_samples("initial-warmup")
            self.baseline("queued", 100, 20, 100000)
            self.active("queued", 400, 20, 200000)
            self.quiet("reset-after-queued")
            self.baseline("durable", 100, 10, 300000)
            self.active("durable", 200, 10, 400000)
            self.dual()
        finally:
            self.monitor_stop.set()
            monitor.join(timeout=15)
            if monitor.is_alive():
                raise RuntimeError("Resource monitor remains active")
            base.save(self.output / "metrics.json", {"phases": self.phases,
                "outcomes": self.outcomes, "monitor_errors": self.monitor_errors})
            base.save(self.output / "writer-samples.json", self.samples)
            base.save(self.output / "resources.json", self.resources)
        self.local.phase = "final-validation"
        self.flush("before-validation")
        self.verify_all()
        self.verify_counters()
        self.flush("final")
        status = self.call("/api/db/native-status")["native_disk"]
        if status.get("writer_failed") is not False:
            raise RuntimeError("The final writer is unhealthy")
        base.save(self.output / "status-final.json", status)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt("Benchmark received an interruption")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=default_output('compaction-benchmark-20261023'))
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--label", choices=("before", "after"), required=True)
    parser.add_argument("--pair", type=int, required=True)
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    if not 4096 <= args.records <= 100000 or not 0 <= args.pair < 3:
        parser.error("records must be 4096..100000 and pair must be 0..2")
    signal.signal(signal.SIGTERM, interrupted)
    capture, complete, error = Benchmark(args), False, None
    try:
        capture.run()
        complete = True
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        try:
            capture.stop()
        except BaseException as failure:
            complete, error = False, f"{error or ''}; shutdown: {failure}"
        base.save(capture.output / "child-memory.json", {
            "maxrss_raw": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
            "scope": "child peak includes seed, final validation, and resource helpers"})
        base.save(capture.output / "summary.json", {"complete": complete, "error": error,
            "scratch": str(capture.root), "home": str(capture.home),
            "all_owned_processes_stopped": capture.process is None,
            "records": len(capture.expected), "pair": args.pair, "label": args.label,
            "capacity_refusals": sum(not row["published"] for row in capture.outcomes),
            "data_retained_for_review": True})
    print(json.dumps({"complete": complete, "error": error, "label": args.label,
                      "pair": args.pair, "output": str(capture.output)}), flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
