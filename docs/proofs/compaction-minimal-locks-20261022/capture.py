#!/usr/bin/env python3
"""Capture actual daemon compaction, writes, and cleanup on fresh generated data."""
import argparse
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
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


class Capture(product.ProductRun):
    def __init__(self, args):
        super().__init__(args)
        self.mode = args.mode
        self.worker = False
        self.phase = "startup"
        self.samples = []
        self.resources = []
        self.monitor_stop = threading.Event()
        self.orphans = []
        inputs = json.loads((self.output / "inputs.json").read_text())
        inputs.update(driver_sha256=base.file_hash(Path(__file__)), mode=self.mode,
            support_sha256=base.file_hash(SUPPORT / "product_run.py"),
            maintenance_support_sha256=base.file_hash(SUPPORT / "product_maintenance.py"),
            snapshot_support_sha256=base.file_hash(product.SUPPORT),
            claim_limit="Generated product operations; process interruption only; no power-loss result")
        base.save(self.output / "inputs.json", inputs)

    def remember(self, operation, **values):
        phase = getattr(self, "phase", "startup")
        if operation == "request" and values.get("path") == "/api/mutations/batch":
            getattr(self, "samples", []).append({"phase": phase,
                "elapsed_ms": values["elapsed_nanos"] / 1e6,
                "completed_ns": time.time_ns(), "http_status": values["http_status"]})
        values.setdefault("capture_phase", phase)
        super().remember(operation, **values)

    def start(self):
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
            LASTDB_AUTO_MAINTENANCE="1" if self.worker else "0",
            LASTDB_SELF_METRICS_SAMPLE_SECS="1", LASTDB_MOLECULE_DRAIN_MS="1000",
            RUST_MIN_STACK="16777216",
            RUST_LOG="warn,fold_db::storage::disk_pack::parts::worker=info")
        if self.worker:
            environment.update(LASTDB_DEPENDENCY_HIGH_WATER="65536",
                LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="5", LASTDB_ORPHAN_MIN_AGE_MS="0")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember("owned-node-start", pid=self.process.pid, home=str(self.home), worker=self.worker)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Owned node exited before health")
            if self.socket.exists():
                try:
                    health = self.call("/health")
                    if health.get("ok") or health.get("status") == "ok":
                        return
                except (OSError, product.http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("Owned node missed its health deadline")

    def monitor(self):
        while not self.monitor_stop.wait(1):
            process = self.process
            if process is None or process.poll() is not None:
                continue
            rss = subprocess.run(["/bin/ps", "-p", str(process.pid), "-o", "rss="],
                                 capture_output=True, text=True, timeout=5)
            descriptors = subprocess.run(["/usr/sbin/lsof", "-p", str(process.pid), "-Ff"],
                                        capture_output=True, text=True, timeout=5)
            self.resources.append({"at_ns": time.time_ns(), "phase": self.phase, "pid": process.pid,
                "rss_bytes": int(rss.stdout.strip() or 0) * 1024,
                "open_fds": sum(line.startswith("f") and line[1:].isdigit()
                                for line in descriptors.stdout.splitlines()),
                "ps_exit": rss.returncode, "lsof_exit": descriptors.returncode})

    def finite_writer(self, phase, seconds):
        self.phase = phase
        deadline, count = time.monotonic() + seconds, 0
        while time.monotonic() < deadline:
            first = count % 4 * 16
            self.mutate("update", range(first, first + 16), 100000 + count, durability="durable")
            count += 1
            time.sleep(0.01)
        self.remember("finite-writer", requests=count, seconds=seconds, phase=phase)

    def compact_outcome(self, full=True):
        started = time.time_ns()
        try:
            report = self.compact(full)["compact"]["native_disk"]
            result = {"published": True, "report": report}
        except RuntimeError as error:
            message = str(error)
            if "HTTP 503" not in message or not any(name in message for name in
                    ("compaction_behind", "compaction_busy")):
                raise
            result = {"published": False, "capacity_refusal": message}
        self.remember("compaction-outcome", started_ns=started, ended_ns=time.time_ns(), **result)
        return result

    def overlap(self, writes=False):
        self.phase = "overlap-writes" if writes else "overlap-unchanged"
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            copies = [pool.submit(self.compact_outcome) for _ in range(2)]
            deadline, observed = time.monotonic() + 30, None
            while time.monotonic() < deadline:
                state = self.call("/api/db/native-status")["native_disk"]["parts"]
                if state.get("compaction_jobs") == 2:
                    observed = state
                    break
                if all(copy.done() for copy in copies):
                    break
                time.sleep(0.02)
            if observed is None:
                raise RuntimeError("Two active compaction owners were not observed")
            self.remember("two-active-copies", state=observed)
            refused = self.compact_outcome()
            if refused["published"] or "compaction_busy" not in refused["capacity_refusal"]:
                raise RuntimeError("The third copy did not receive the resource refusal")
            writer = pool.submit(self.finite_writer, "overlap-writes", 20) if writes else None
            cleanups, orphan_observed = [], False
            while not all(copy.done() for copy in copies):
                before = self.call("/api/db/native-status")["native_disk"]["parts"]
                report = self.call("/api/db/compact", {"dry_run": False,
                    "reclaim_only": True, "grace_ms": 0})["compact"]["native_disk"]
                after = self.call("/api/db/native-status")["native_disk"]["parts"]
                cleanups.append({"before": before, "after": after, "report": report})
                self.remember("cleanup-during-copy", before=before, after=after, report=report)
                if self.orphans and not orphan_observed and all(not path.exists() for path in self.orphans):
                    orphan_observed = before.get("compaction_jobs", 0) > 0
                    self.remember("interrupted-output-swept", during_copy=orphan_observed,
                                  orphan_files=len(self.orphans))
                if time.monotonic() > deadline + 180:
                    raise RuntimeError("The finite copy window exceeded its deadline")
                time.sleep(0.05)
            results = [copy.result() for copy in copies]
            if writer:
                writer.result()
        if not any(not item["report"]["reclaim_deferred"] and item["before"].get("compaction_jobs", 0)
                   for item in cleanups):
            raise RuntimeError("No cleanup completed during an active copy")
        self.remember("overlap-complete", writes=writes, results=results,
            cleanups=len(cleanups), orphan_swept_during_copy=orphan_observed,
            invalidated_after_removal=sum(item["report"]["reclaim_deferred"]
                and item["report"]["files_removed"] > 0 for item in cleanups))
        if not writes and not all(result["published"] for result in results):
            raise RuntimeError("An unchanged-value overlapping copy failed to publish")
        self.phase = "verify-overlap"
        self.flush("overlap")
        self.verify("overlap")
        self.verify_counters()

    def run(self):
        monitor = threading.Thread(target=self.monitor, daemon=True)
        monitor.start()
        try:
            if self.mode == "lifecycle":
                super().run()
                self.overlap()
            else:
                self.start()
                self.install()
                self.phase = "seed"
                for first in range(0, self.records, 128):
                    self.mutate("create", range(first, min(first + 128, self.records)), 0,
                                durability="durable")
                    if first % 12800 == 0:
                        print(json.dumps({"seeded": min(first + 128, self.records)}), flush=True)
                self.write_long(0, 0)
                self.flush("seed")
                self.compact(True)
                self.stop()
                self.start()
                packs = self.home / "data" / "disk-pack" / "packs"
                previous = {path.name for path in packs.iterdir()}
                self.interrupt_copy()
                for path in packs.iterdir():
                    if path.name not in previous:
                        self.orphans.extend(path.glob("part-*"))
                        self.orphans.extend((path / "map").glob("part-*"))
                if not self.orphans:
                    raise RuntimeError("No interrupted private payload files remain")
                self.finite_writer("baseline", 20)
                self.stop()
                self.worker = True
                self.start()
                self.phase = "worker-start"
                self.mutate("update", range(64, 80), 300000, durability="durable")
                self.mutate("update", range(96, 112), 300001, durability="durable")
                self.overlap(writes=True)
                self.overlap()
                self.compact(True)
                self.stop()
                self.start()
                self.verify("final-reopen")
                self.verify_counters()
            self.flush("final")
            base.save(self.output / "status-final.json", self.call("/api/db/native-status")["native_disk"])
            base.save(self.output / "compactions.json", self.compactions)
        finally:
            self.monitor_stop.set()
            monitor.join(timeout=15)
            if monitor.is_alive():
                raise RuntimeError("Resource monitor did not stop")
            base.save(self.output / "resources.json", self.resources)
            base.save(self.output / "writer-samples.json", self.samples)
            comparison = {}
            for phase in ("baseline", "overlap-writes"):
                samples = [item for item in self.samples if item["phase"] == phase]
                values = sorted(item["elapsed_ms"] for item in samples)
                if values:
                    comparison[phase] = {"requests": len(values), "p50_ms": statistics.median(values),
                        "p95_ms": values[min(len(values) - 1, int(len(values) * 0.95))],
                        "max_ms": max(values), "scope": "adjacent phases; shared host; no paired speed claim"}
            base.save(self.output / "comparison.json", comparison)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--recovery", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--records", type=int, default=4096)
    parser.add_argument("--mode", choices=("lifecycle", "overlap"), default="lifecycle")
    args = parser.parse_args()
    if not 128 <= args.records <= 100000:
        parser.error("records must be between 128 and 100000")
    capture, complete, error = Capture(args), False, None
    try:
        capture.run()
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        try:
            capture.stop()
        except Exception as failure:
            complete, error = False, f"{error or ''}; shutdown: {failure}"
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        base.save(capture.output / "child-memory.json", {"maxrss_raw": usage.ru_maxrss,
            "scope": "observed child peak, including helpers; no memory-cap claim"})
        base.save(capture.output / "summary.json", {"complete": complete, "error": error,
            "scratch": str(capture.root), "home": str(capture.home),
            "all_owned_processes_stopped": capture.process is None,
            "expected_live_records": len(capture.expected), "data_retained_for_review": True})
    print(json.dumps({"complete": complete, "error": error, "output": str(capture.output)}), flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
