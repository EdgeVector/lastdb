"""Real queued and durable writes beside compaction and exact-file cleanup."""
import concurrent.futures
import hashlib
import json
import math
import statistics
import threading
import time


def describe(samples):
    if not samples:
        return {"requests": 0}
    values = sorted(row["elapsed_ms"] for row in samples)
    ends = sorted(row["ended_mono_ns"] for row in samples)
    result = {"requests": len(values), "p50_ms": statistics.median(values),
              "max_ms": max(values), "largest_completion_gap_ms":
              max(((b - a) / 1e6 for a, b in zip(ends, ends[1:])), default=0)}
    for percentile in (95, 99):
        result[f"p{percentile}_ms"] = values[math.ceil(percentile / 100 * len(values)) - 1]
    return result


class Workload:
    def writer(self, name, count, rate, durability, revision, ready=None):
        self.local.phase = name
        started = time.monotonic()
        for number in range(count):
            target = started + number / rate
            time.sleep(max(0, target - time.monotonic()))
            self.local.writer_index = number
            self.local.dispatch_lag_ms = max(0, time.monotonic() - target) * 1000
            first = number % 4 * 16
            self.mutate("update", range(first, first + 16), revision + number,
                        durability=durability)
            if ready is not None and number == 4:
                ready.set()
        self.remember("writer-complete", name=name, batches=count, rate=rate,
                      durability=durability, wall_seconds=time.monotonic() - started)
        return describe(self.samples[name])

    def copy(self, name, barrier=None):
        self.local.phase = name
        if barrier is not None:
            barrier.wait(timeout=30)
        started = time.time_ns()
        started_mono = time.perf_counter_ns()
        try:
            report = self.compact(True)["compact"]["native_disk"]
            outcome = {"published": True, "report": report}
        except RuntimeError as error:
            detail = str(error)
            if "HTTP 503" not in detail or not any(reason in detail for reason in
                    ("compaction_behind", "compaction_busy")):
                raise
            outcome = {"published": False, "capacity_refusal": detail}
        ended = time.time_ns()
        ended_mono = time.perf_counter_ns()
        result = {"name": name, "started_ns": started, "ended_ns": ended,
                  "started_mono_ns": started_mono, "ended_mono_ns": ended_mono,
                  "wall_ms": (ended_mono - started_mono) / 1e6, **outcome}
        self.outcomes.append(result)
        self.remember("copy-outcome", **result)
        return result

    def cleanup(self, name, before):
        self.local.phase = name
        started = time.time_ns()
        started_mono = time.perf_counter_ns()
        report = self.call("/api/db/compact", {"dry_run": False, "reclaim_only": True,
                           "grace_ms": 0})["compact"]["native_disk"]
        ended = time.time_ns()
        ended_mono = time.perf_counter_ns()
        after = self.call("/api/db/native-status")["native_disk"]["parts"]
        result = {"started_ns": started, "ended_ns": ended,
                  "started_mono_ns": started_mono, "ended_mono_ns": ended_mono,
                  "wall_ms": (ended_mono - started_mono) / 1e6, "report": report,
                  "before": before, "after": after}
        self.remember("cleanup-response", name=name, **result)
        return result

    def observe_copy(self, future):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = self.call("/api/db/native-status")["native_disk"]["parts"]
            if state.get("compaction_phase") in ("copy", "multiple"):
                return state
            if future.done():
                future.result()
                break
            time.sleep(0.02)
        raise RuntimeError("A copy phase was not observed before cleanup")

    def active(self, durability, count, rate, revision):
        name = f"{durability}-active"
        self.stage = name
        self.local.phase = name
        ready = threading.Event()
        started = time.time_ns()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            writer = pool.submit(self.writer, name, count, rate, durability, revision, ready)
            while not ready.wait(0.02):
                if writer.done():
                    writer.result()
                    raise RuntimeError("The scheduled writer ended before five acknowledgements")
                if (time.time_ns() - started) / 1e9 > 60:
                    raise RuntimeError("The scheduled writer missed its start deadline")
            copy = pool.submit(self.copy, f"{name}-copy")
            observed = self.observe_copy(copy)
            cleanup = pool.submit(self.cleanup, f"{name}-cleanup", observed)
            copied, reclaimed = copy.result(), cleanup.result()
            writer.result()
        samples = self.samples[name]
        overlap = [row for row in samples if row["ended_ns"] >= copied["started_ns"]
                   and row["started_ns"] <= copied["ended_ns"]]
        result = {"name": name, "copy": copied, "cleanup": reclaimed,
                  "all_writes": describe(samples), "writes_during_copy": describe(overlap),
                  "maintenance_pair_wall_ms":
                  (max(copied["ended_mono_ns"], reclaimed["ended_mono_ns"])
                   - copied["started_mono_ns"]) / 1e6,
                  "late_dispatches": sum(row["dispatch_lag_ms"] > 10 for row in samples),
                  "max_dispatch_lag_ms": max(row["dispatch_lag_ms"] for row in samples)}
        self.phases[name] = result
        self.local.phase = f"{name}-verify"
        self.flush(name)
        self.verify_samples(f"{name}-readback")
        self.remember("active-phase-complete", result=result)

    def quiet(self, name):
        self.stage = name
        self.local.phase = name
        copied = self.copy(name)
        if not copied["published"]:
            raise RuntimeError(f"Quiet compaction refused: {name}")
        status = self.call("/api/db/native-status")["native_disk"]
        self.phases[name] = {"copy": copied, "status": status}

    def baseline(self, durability, count, rate, revision):
        name = f"{durability}-baseline"
        self.stage = name
        self.phases[name] = self.writer(name, count, rate, durability, revision)
        self.local.phase = f"{name}-verify"
        self.flush(name)
        self.verify_samples(f"{name}-readback")

    def dual(self):
        self.stage = "two-copies"
        self.local.phase = "two-copies-observer"
        barrier = threading.Barrier(3)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(self.copy, f"two-copies-{number}", barrier)
                       for number in range(2)]
            barrier.wait(timeout=30)
            observed = self.observe_copy(futures[0])
            max_jobs = observed.get("compaction_jobs")
            deadline = time.monotonic() + 180
            while not all(future.done() for future in futures):
                state = self.call("/api/db/native-status")["native_disk"]["parts"]
                if "compaction_jobs" in state:
                    max_jobs = max(max_jobs or 0, state["compaction_jobs"])
                if time.monotonic() > deadline:
                    raise RuntimeError("Two-copy measurement exceeded three minutes")
                time.sleep(0.1)
            copies = [future.result() for future in futures]
        if not all(copied["published"] for copied in copies):
            raise RuntimeError("A quiet two-copy operation refused publication")
        result = {"copies": copies, "max_reported_jobs": max_jobs,
                  "wall_ms": (max(row["ended_mono_ns"] for row in copies)
                              - min(row["started_mono_ns"] for row in copies)) / 1e6,
                  "status": self.call("/api/db/native-status")["native_disk"]}
        self.phases["two-copies"] = result
        self.remember("two-copy-complete", result=result)

    def verify_all(self):
        base = self.protocol
        digest = hashlib.sha256()
        for first in range(0, self.records, 64):
            numbers = range(first, min(first + 64, self.records))
            result = self.call("/api/query", {"schema_name": base.SCHEMA,
                "fields": base.FIELDS, "filter": {"HashRangeKeys":
                [[base.HASH, base.key(number)] for number in numbers]}, "limit": 64})
            actual = self.rows(result)
            expected = {base.key(number): self.expected[base.key(number)] for number in numbers}
            if actual != expected:
                raise RuntimeError("Final exact values differ from accepted writes")
            digest.update(json.dumps(actual, sort_keys=True).encode())
        self.remember("all-known-keys-correct", records=self.records, sha256=digest.hexdigest(),
                      unexpected_extra_membership_checked=False)
