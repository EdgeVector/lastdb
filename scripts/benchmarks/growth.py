"""Measure resource growth on generated data through actual keyed requests."""

from contextlib import contextmanager
import json
import statistics
import threading
import time

from node import save
from workload import FIELDS, SCHEMA, Workload, read_request, validate_read, write_request

PROFILES = {
    "standard": {"records": 512, "rounds": 3, "idle_seconds": 1},
    "nightly": {"records": 2048, "rounds": 8, "idle_seconds": 3},
    "batch-insert": {"records": 512, "rounds": 3, "idle_seconds": 1},
}
CASES = ("fixed-read", "fixed-update", "insert")


def slope(points, name):
    center = (len(points) - 1) / 2
    return sum((i - center) * point[name] for i, point in enumerate(points)) / sum(
        (i - center) ** 2 for i in range(len(points)))


class GrowthWorkload:
    def __init__(self, node, recipe, output):
        self.node, self.output = node, output
        self.recipe = recipe["growth"]
        self.live_records = self.recipe["records"]
        self.expected = {}
        self.samples, self.cases, self.phase = [], {}, "seed"
        self.started = time.monotonic()

    def memory(self):
        value = self.node.memory_bytes()
        if value is None or value <= 0:
            raise RuntimeError("The growth benchmark lacks daemon memory samples")
        return value

    @contextmanager
    def monitor(self):
        stopped, failures = threading.Event(), []

        def sample():
            try:
                while not stopped.is_set():
                    self.samples.append({"elapsed_seconds": time.monotonic() - self.started,
                                         "phase": self.phase, "memory_bytes": self.memory()})
                    stopped.wait(0.2)
            except Exception as failure:
                failures.append(failure)

        thread = threading.Thread(target=sample, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stopped.set()
            thread.join(timeout=5)
        if thread.is_alive() or failures:
            raise RuntimeError(f"The growth memory monitor failed: {failures}")

    def read(self, first, count):
        for start in range(first, first + count, 64):
            numbers = list(range(start, min(start + 64, first + count)))
            result, _, _ = self.node.call("/api/query", read_request(numbers))
            validate_read(result, numbers)
            for record in result["results"]:
                number = int(record["key"]["range"].removeprefix("key-"))
                if record["fields"]["title"] != f"record-{number}-{self.expected[number]}":
                    raise RuntimeError("A growth read returned an older write")

    def write(self, first, count, revision, insert):
        for start in range(first, first + count, 128):
            numbers = list(range(start, min(start + 128, first + count)))
            result, _, _ = self.node.call(
                "/api/mutations/batch", write_request(numbers, revision, insert))
            Workload.validate_write(result, len(numbers))
            self.expected.update({number: revision for number in numbers})
        self.read(first, count)

    def settle_disk(self, idle, sample=None):
        """Wait until the disk size stays constant for three seconds, within 90 seconds."""
        samples, deadline, stable_since, previous = [], time.monotonic() + 90, None, None
        while time.monotonic() < deadline:
            current = (sample or self.node.disk_usage)()
            now = time.monotonic()
            idle.append(self.memory())
            samples.append({"elapsed_seconds": now - self.started, **current})
            if current != previous:
                stable_since = now
            if stable_since is not None and now - stable_since >= 3:
                return current, samples, now - stable_since
            previous = current
            time.sleep(1)
        raise ValueError("The v2 disk size did not stay constant for three seconds within 90 seconds")

    def compact(self):
        # A v2 run home has no explicit compaction; its run merges are automatic.
        self.phase = "settle"
        _, _, settled = self.settle_disk([])
        return {"format": 2, "automatic_merges": True, "settled_seconds": settled}

    def checkpoint(self, number):
        result, _, _ = self.node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise RuntimeError("A growth checkpoint lacks a durable flush")
        before = self.node.disk_breakdown()
        receipt = self.compact()
        self.phase = "verify-compaction"
        self.read(0, self.live_records)
        self.phase = "idle"
        idle, deadline = [], time.monotonic() + self.recipe["idle_seconds"]
        while time.monotonic() < deadline:
            idle.append(self.memory())
            time.sleep(0.2)
        after = self.node.disk_breakdown()
        return {"round": number, "live_records": self.live_records,
                "elapsed_seconds": time.monotonic() - self.started,
                "idle_memory_bytes": statistics.median(idle), "idle_memory_samples": idle,
                "disk_breakdown_before": before, "disk_breakdown": after,
                "disk_before_compaction_bytes": before["database"]["logical_bytes"],
                "compacted_disk_bytes": after["database"]["logical_bytes"],
                "search_queue_bytes": max(after["search_queue"].values()),
                "whole_home_disk_bytes": max(after["whole_home"].values()),
                "other_home_bytes": max(after["other_home"].values()),
                "allocated_disk_before_compaction_bytes": before["database"]["allocated_bytes"],
                "compacted_allocated_disk_bytes": after["database"]["allocated_bytes"], "compaction": receipt}

    def seed(self):
        schema = {"source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
                  "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
                  "fields": FIELDS, "field_descriptions": {f: f"generated benchmark {f}" for f in FIELDS}}
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})
        self.write(0, self.live_records, "growth-seed", True)

    def measure(self, case):
        points = [self.checkpoint(0)]
        for number in range(1, self.recipe["rounds"] + 1):
            self.phase = case
            count = self.recipe["records"]
            if case == "fixed-read":
                self.read(0, count)
            else:
                insert = case == "insert"
                self.write(self.live_records if insert else 0, count,
                           f"growth-{case}-{number:03d}", insert)
                if insert:
                    self.live_records += count
            points.append(self.checkpoint(number))
            save(self.output / "growth-progress.json", {"case": case, "checkpoints": points})
        first, last = points[0], points[-1]
        return {"checkpoints": points, "metrics": {
            "live_records": self.live_records,
            "idle_memory_bytes": last["idle_memory_bytes"],
            "memory_growth_bytes": last["idle_memory_bytes"] - first["idle_memory_bytes"],
            "memory_growth_bytes_per_round": slope(points, "idle_memory_bytes"),
            "disk_before_compaction_bytes": last["disk_before_compaction_bytes"],
            "compacted_disk_bytes": last["compacted_disk_bytes"],
            **{name: last[name] for name in ("search_queue_bytes", "whole_home_disk_bytes", "other_home_bytes")},
            "disk_growth_bytes": last["compacted_disk_bytes"] - first["compacted_disk_bytes"],
            "disk_growth_bytes_per_round": slope(points, "compacted_disk_bytes"),
            "compacted_disk_bytes_per_record": last["compacted_disk_bytes"] / self.live_records,
            "allocated_disk_before_compaction_bytes": last["allocated_disk_before_compaction_bytes"],
            "compacted_allocated_disk_bytes": last["compacted_allocated_disk_bytes"],
            "allocated_disk_growth_bytes": last["compacted_allocated_disk_bytes"] - first["compacted_allocated_disk_bytes"],
            "allocated_disk_growth_bytes_per_round": slope(points, "compacted_allocated_disk_bytes"),
            "compacted_allocated_disk_bytes_per_record": last["compacted_allocated_disk_bytes"] / self.live_records}}

    def run(self):
        self.seed()
        with (self.output / "growth-memory.jsonl").open("w") as raw:
            try:
                for case in CASES:
                    self.samples = []
                    try:
                        with self.monitor():
                            self.cases[case] = self.measure(case)
                    finally:
                        for sample in self.samples:
                            raw.write(json.dumps({"case": case, **sample}) + "\n")
                        raw.flush()
                    idle = [value for p in self.cases[case]["checkpoints"] for value in p["idle_memory_samples"]]
                    self.cases[case]["metrics"]["peak_memory_bytes"] = max(
                        [s["memory_bytes"] for s in self.samples] + idle)
                    save(self.output / "growth.json", self.cases)
                    print(json.dumps({"growth_case": case, **self.cases[case]["metrics"]}), flush=True)
                native, _, _ = self.node.call("/api/db/native-status")
                save(self.output / "native-status.json", native)
                if native["native_disk"].get("writer_failed") is not False:
                    raise RuntimeError("The growth benchmark ended with an unhealthy writer")
            finally:
                save(self.output / "growth.json", self.cases)
