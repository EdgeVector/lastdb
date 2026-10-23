#!/usr/bin/env python3
"""Measure actual node writes and compaction on one fresh generated owned home."""
import argparse
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import resource
import statistics
import subprocess
import sys
import threading
import time

SUPPORT = Path(__file__).resolve().parents[1] / "atom-lifecycle-completion-20261022"
sys.path.insert(0, str(SUPPORT))
spec = importlib.util.spec_from_file_location("lifecycle_product", SUPPORT / "product_run.py")
product = importlib.util.module_from_spec(spec)
spec.loader.exec_module(product)
base = product.base


class Measure(product.ProductRun):
    def __init__(self, args):
        args.recovery = args.binary
        super().__init__(args)
        self.local = threading.local()
        self.samples = {}
        self.stopped = threading.Event()
        self.accepted = 0
        self.phase_compactions = []
        self.automatic_worker = args.automatic_worker
        self.local.phase = "startup"
        inputs = json.loads((self.output / "inputs.json").read_text())
        for name in ("recovery_sha256", "deleted", "recreated", "update_rounds"):
            inputs.pop(name, None)
        inputs.update(driver_sha256=base.file_hash(Path(__file__)), records=args.records,
                      benchmark="same fresh home; adjacent sequential phases; no cache reset",
                      baseline_seconds=args.baseline_seconds, seed_batch=args.seed_batch,
                      active_writer_batch=16, expected_native_entries="measured by full-copy receipt",
                      auto_maintenance=args.automatic_worker,
                      auto_high_water=128 if args.automatic_worker else None,
                      scope="generated data only; no primary or copied home")
        base.save(self.output / "inputs.json", inputs)
        base.save(self.output / "recipe.json", {"user_records": args.records,
            "native_entries": "measured full-copy records plus tombstones",
            "seed_batch": args.seed_batch, "body_bytes": 1024,
            "active_writer_batch": 16, "active_writer_keys": 64,
            "baseline_seconds": args.baseline_seconds, "client_pause_seconds": 0.01,
            "recent_update_keys_each_round": 64, "recent_rounds": 3,
            "readback": "all changed keys plus a fixed sample distributed across the seeded keys",
            "source_revision": args.source_revision, "driver_sha256": base.file_hash(Path(__file__)),
            "scope": "actual local node; generated data only; no primary or cloud access"})
        self.baseline_seconds, self.seed_batch = args.baseline_seconds, args.seed_batch

    def start(self):
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
            LASTDB_AUTO_MAINTENANCE="1" if self.automatic_worker else "0",
            LASTDB_SELF_METRICS_SAMPLE_SECS="1", RUST_MIN_STACK="16777216")
        if self.automatic_worker:
            environment.update(LASTDB_DEPENDENCY_HIGH_WATER="128",
                               LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="0")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember("owned-node-start", pid=self.process.pid, home=str(self.home))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned node exited before health")
            if self.socket.exists():
                try:
                    health = self.call("/health")
                    if health.get("ok") or health.get("status") == "ok":
                        return
                except (OSError, product.http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("The owned node missed its health deadline")

    def remember(self, operation, **values):
        phase = getattr(getattr(self, "local", None), "phase", "startup")
        if operation == "request" and values.get("path") == "/api/mutations/batch":
            sample = {"elapsed_ms": values["elapsed_nanos"] / 1e6,
                      "completed_unix_ns": time.time_ns(), "http_status": values["http_status"]}
            self.samples.setdefault(phase, []).append(sample)
        values.setdefault("phase", phase)
        super().remember(operation, **values)

    def compact_phase(self, name, full_copy):
        self.local.phase = name
        started = time.time_ns()
        report = self.compact(full_copy)["compact"]["native_disk"]
        result = {"phase": name, "start_unix_ns": started,
                  "end_unix_ns": time.time_ns(), "report": report}
        self.phase_compactions.append(result)
        self.remember("measured-compaction", **result)
        print(json.dumps({"phase": name, "report": report}), flush=True)
        return report

    def writer(self, phase, seconds=None):
        self.local.phase = phase
        started, count = time.monotonic(), 0
        while not self.stopped.is_set():
            if seconds is not None and time.monotonic() - started >= seconds:
                break
            if time.monotonic() - started > 600:
                raise RuntimeError("The finite writer window exceeded ten minutes")
            first = (count % 4) * 16
            self.mutate("update", range(first, first + 16), self.accepted + 100000,
                        durability="durable")
            self.accepted += 16
            count += 1
            time.sleep(0.01)
        self.remember("writer-window", requests=count, elapsed_seconds=time.monotonic() - started)

    def verify_samples(self, name):
        self.local.phase = name
        numbers = sorted(set(range(64)) | set(range(128, 192))
                         | set(range(0, self.records, max(1, self.records // 128)))
                         | {self.records - 1})
        seen = {}
        for first in range(0, len(numbers), 64):
            selected = numbers[first:first + 64]
            response = self.call("/api/query", {"schema_name": base.SCHEMA, "fields": base.FIELDS,
                "filter": {"HashRangeKeys": [[base.HASH, base.key(number)] for number in selected]},
                "limit": 64})
            seen.update(self.rows(response))
        expected = {base.key(number): self.expected[base.key(number)] for number in numbers}
        if seen != expected:
            raise RuntimeError("Accepted values differ on the complete updated set or distributed sample")
        self.remember("accepted-value-samples", keys=len(numbers), all_updated_keys=True,
                      expected_sha256=base.hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest())

    def run(self):
        self.start()
        self.install()
        self.local.phase = "seed"
        for first in range(0, self.records, self.seed_batch):
            self.mutate("create", range(first, min(first + self.seed_batch, self.records)),
                        0, durability="durable")
            if first % (self.seed_batch * 20) == 0:
                print(json.dumps({"seeded": first + min(self.seed_batch, self.records - first),
                                  "total": self.records}), flush=True)
        self.flush("seed")
        if self.automatic_worker:
            return self.finish_automatic_run()
        self.compact_phase("initial-full", True)
        self.verify_samples("initial-readback")
        self.local.phase = "baseline"
        self.writer("baseline", self.baseline_seconds)
        self.flush("baseline")
        self.verify_samples("baseline-readback")
        self.stopped.clear()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(self.writer, "concurrent-full")
            while len(self.samples.get("concurrent-full", [])) < 5:
                if writer.done():
                    writer.result()
                    raise RuntimeError("Writer ended before five concurrent acknowledgements")
                time.sleep(0.01)
            try:
                self.compact_phase("concurrent-full", True)
            finally:
                self.stopped.set()
            writer.result()
        self.flush("concurrent-full")
        self.verify_samples("concurrent-readback")
        for revision in range(3):
            self.local.phase = f"recent-writes-{revision}"
            self.mutate("update", range(128, 192), 200000 + revision, durability="durable")
            self.compact_phase(f"recent-tier-{revision}", False)
            self.verify_samples(f"tier-readback-{revision}")
        self.local.phase = "final"
        self.verify_counters()
        self.flush("final")
        status = self.call("/api/db/native-status")["native_disk"]
        base.save(self.output / "status-final.json", status)
        base.save(self.output / "compactions.json", self.phase_compactions)
        base.save(self.output / "writer-samples.json", self.samples)
        result = {}
        for name in ("baseline", "concurrent-full"):
            samples = self.samples[name]
            values = sorted(item["elapsed_ms"] for item in samples)
            times = [item["completed_unix_ns"] for item in samples]
            result[name] = {"requests": len(samples), "p50_ms": statistics.median(values),
                "p95_ms": values[min(len(values) - 1, int(len(values) * 0.95))],
                "p99_ms": values[min(len(values) - 1, int(len(values) * 0.99))],
                "max_ms": max(values),
                "largest_completion_gap_ms": max((b - a) / 1e6 for a, b in zip(times, times[1:]))}
        base.save(self.output / "comparison.json", result)
        print(json.dumps({"comparison": result}), flush=True)

    def finish_automatic_run(self):
        self.local.phase = "automatic-worker"
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            status = self.call("/api/db/native-status")["native_disk"]
            parts = status["parts"]
            if (parts["maintenance_runs"] >= 1 and not parts["maintenance_running"]
                    and parts["dependency_parts"] < parts["dependency_parts_peak"]):
                break
            if status.get("writer_failed") is not False:
                raise RuntimeError("The automatic worker left a failed writer")
            time.sleep(0.1)
        else:
            raise RuntimeError("The automatic worker did not complete and reduce the part count")
        self.mutate("update", range(64), 300000, durability="durable")
        self.verify_samples("automatic-worker-readback")
        self.verify_counters()
        self.flush("automatic-worker-final")
        status = self.call("/api/db/native-status")["native_disk"]
        base.save(self.output / "status-final.json", status)
        self.remember("automatic-worker-complete", status=status,
                      soft_limit_exercised=False,
                      operator_settings={"LASTDB_DEPENDENCY_HIGH_WATER": 128,
                                         "LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS": 0})
        print(json.dumps({"automatic_worker": status}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=default_output('atom-scale-review-20261022'))
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--seed-batch", type=int, default=128)
    parser.add_argument("--baseline-seconds", type=float, default=20)
    parser.add_argument("--automatic-worker", action="store_true")
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    if not 4096 <= args.records <= 100000 or not 1 <= args.seed_batch <= 256:
        parser.error("records must be 4096..100000, seed batch 1..256")
    capture, complete, error = Measure(args), False, None
    try:
        capture.run()
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        capture.stopped.set()
        try:
            capture.stop()
        except Exception as failure:
            complete, error = False, f"{error or ''}; shutdown: {failure}"
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        base.save(capture.output / "child-memory.json", {"maxrss_raw": usage.ru_maxrss,
            "raw_unit": "bytes" if sys.platform == "darwin" else "KiB",
            "child_processes": "one owned node", "platform": sys.platform,
            "scope": "observed node peak over seed and all later phases; no process-memory cap claim"})
        base.save(capture.output / "summary.json", {"complete": complete, "error": error,
            "scratch": str(capture.root), "home": str(capture.home),
            "all_owned_processes_stopped": capture.process is None,
            "generated_user_records": args.records,
            "data_retained_for_review": True})
    print(json.dumps({"complete": complete, "error": error, "output": str(capture.output)}), flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
