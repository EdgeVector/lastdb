"""A fixed recipe of keyed reads, durable writes, and closed-loop concurrency."""

from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import math
import threading
import time

from node import save
from phases import capture_phases
from range_continuation.process_counters import counter_delta, read_counters

SCHEMA, HASH = "GitHubBenchmark", "generated"
FIELDS = ["bucket", "id", "title", "body"]
PROFILES = {
    "standard": {"records": 10000, "samples": 256, "single_write_samples": 256, "write_samples": 32,
                 "batch_update_samples": 128, "pairs": 2},
    "nightly": {"records": 100000, "samples": 2048, "single_write_samples": 512, "write_samples": 128,
                "batch_update_samples": 128, "pairs": 3},
    "batch-insert": {"records": 10000, "samples": 256, "single_write_samples": 256, "write_samples": 256,
                     "pairs": 3, "insert_only": True},
}


def key(number):
    return f"key-{number:09d}"


def fields(number, revision):
    title = f"record-{number}-{revision}"
    return {"bucket": HASH, "id": key(number), "title": title,
            "body": hashlib.shake_256(title.encode()).hexdigest(512)}


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def read_request(numbers):
    return encoded({"schema_name": SCHEMA, "fields": FIELDS,
                    "filter": {"HashRangeKeys": [[HASH, key(n)] for n in numbers]},
                    "limit": len(numbers)})


def write_request(numbers, revision, insert):
    mutations = []
    for number in numbers:
        value = fields(number, revision)
        if not insert:
            value = {name: value[name] for name in ("title", "body")}
        mutations.append({"type": "mutation", "schema": SCHEMA,
                          "mutation_type": "create" if insert else "update",
                          "durability": "durable",
                          "key_value": {"hash": HASH, "range": key(number)},
                          "fields_and_values": value})
    return encoded({"mutations": mutations})


def validate_read(result, numbers):
    records = result.get("results", [])
    if result.get("returned_count") != len(numbers) or len(records) != len(numbers):
        raise RuntimeError("A keyed read returned an incomplete result")
    found = {}
    expected = {key(n): n for n in numbers}
    for record in records:
        coordinate, value = record["key"], record["fields"]
        number = expected.get(coordinate["range"])
        if coordinate["hash"] != HASH or coordinate["range"] in found or number is None:
            raise RuntimeError("A keyed read returned an unexpected or duplicate key")
        title = value["title"]
        if (value["bucket"] != HASH or value["id"] != coordinate["range"]
                or not title.startswith(f"record-{number}-")
                or value["body"] != hashlib.shake_256(title.encode()).hexdigest(512)):
            raise RuntimeError("A keyed read returned an invalid generated record")
        found[coordinate["range"]] = title
    if set(found) != set(expected):
        raise RuntimeError("A keyed read returned different keys")
    return found


def describe(samples, elapsed):
    good = [sample for sample in samples if "error" not in sample]
    ordered = sorted(sample["elapsed_ms"] for sample in good)

    def percentile(fraction):
        return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] if ordered else None

    return {"requests": len(samples), "errors": len(samples) - len(good),
            "records": sum(sample["records"] for sample in good), "elapsed_seconds": elapsed,
            "requests_per_second": len(good) / elapsed,
            "records_per_second": sum(sample["records"] for sample in good) / elapsed,
            "p50_ms": percentile(0.5), "p95_ms": percentile(0.95), "p99_ms": percentile(0.99)}


class Workload:
    def __init__(self, node, recipe, output, upgrade_target=None, minimum_retired=0):
        self.node, self.recipe, self.output = node, recipe, output
        self.upgrade_target, self.minimum_retired = upgrade_target, minimum_retired
        self.cleanup_progress = []
        self.metrics, self.next_insert = {}, recipe["records"]
        self.raw = gzip.open(output / "samples.jsonl.gz", "wt")

    def seed(self):
        schema = {"source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
                  "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
                  "fields": FIELDS, "field_types": {field: "String" for field in FIELDS},
                  "field_descriptions": {f: f"generated benchmark {f}" for f in FIELDS}}
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})
        for start in range(0, self.recipe["records"], 128):
            numbers = list(range(start, min(start + 128, self.recipe["records"])))
            result, _, _ = self.node.call("/api/mutations/batch", write_request(numbers, "seed", True))
            self.validate_write(result, len(numbers))
        result, _, _ = self.node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise RuntimeError("The generated seed lacks a durable flush")
        # Reopen the same owned home. No seeded logical record remains warm.
        if self.upgrade_target:
            save(self.output / "seed-native-status.json", self.node.call("/api/db/native-status")[0])
            save(self.output / "seed-version.json", json.loads((self.output / "version.json").read_text()))
        self.node.stop()
        if self.upgrade_target:
            save(self.output / "seed-shutdown.json", json.loads((self.output / "shutdown.json").read_text()))
            save(self.output / "upgrade-source.json", {
                "seed_revision": self.node.revision, "records": self.recipe["records"],
                "target_revision": self.upgrade_target[1], "generated_data_only": True})
            self.node.binary, self.node.revision, self.node.workers = self.upgrade_target
        self.node.start()
        result, _, _ = self.node.call("/api/admin/shed", {})
        boundary = result["shed"]
        save(self.output / "cold-boundary.json", boundary)
        if boundary.get("error") or boundary.get("target_met") is not True:
            raise RuntimeError("The cold-read boundary did not purge the logical warm records")

    @staticmethod
    def validate_write(result, count):
        if result.get("count") != count or result.get("durability") != "durable":
            raise RuntimeError("A write lacks a complete durable acknowledgement")

    def prepare(self, mode, batch, samples, clients):
        jobs = [[] for _ in range(clients)]
        for index in range(samples):
            client, turn = index % clients, index // clients
            # Every client alternates; adjacent clients start with opposite kinds.
            mixed_read = mode == "mixed" and (client + turn) % 2 == 0
            kind = "read" if mode in ("cold", "hot", "read") or mixed_read else "write"
            if mode == "cold":
                numbers = list(range(self.cold_next, self.cold_next + batch))
                self.cold_next += batch
            elif mode == "insert":
                numbers = list(range(self.next_insert, self.next_insert + batch))
                self.next_insert += batch
            else:
                offset = index // 2 if mode == "mixed" else index
                first = self.recipe["records"] - 256 + ((offset * batch) % 256)
                numbers = list(range(first, first + batch))
            body = (read_request(numbers) if kind == "read" else
                    write_request(numbers, f"{self.case}-{index}", mode == "insert"))
            request_id = f"bench-{len(self.metrics)}-{index:06d}"
            jobs[client].append((kind, numbers, body, request_id))
        return jobs

    def worker(self, jobs, barrier, stopped, mode, client):
        samples = []
        barrier.wait(timeout=30)
        for turn, (kind, numbers, body, request_id) in enumerate(jobs):
            if stopped.is_set():
                break
            sample = {"kind": kind, "records": len(numbers), "client": client, "client_request": turn,
                      "request_id": request_id,
                      "request_sha256": hashlib.sha256(body).hexdigest()}
            try:
                path = "/api/query" if kind == "read" else "/api/mutations/batch"
                observe = (self.case == "update/128"
                           or self.case == "update/1" and self.recipe["native_timing"])
                before = read_counters(self.node.process.pid) if observe else None
                result, elapsed, receipt = self.node.call(
                    path, body, receipt=kind == "read", request_id=request_id)
                if observe:
                    after = read_counters(self.node.process.pid)
                    sample.update(process_before=before, process_after=after,
                                  process_delta=counter_delta(before, after))
                sample.update(elapsed_ms=elapsed, read_receipt=receipt)
                if kind == "read":
                    titles = validate_read(result, numbers)
                    if mode == "mixed":
                        sample["read_current_case_records"] = sum(
                            titles[key(number)].startswith(f"record-{number}-{self.case}-")
                            for number in numbers)
                    if not receipt:
                        raise RuntimeError("The read lacks its storage receipt")
                    # Native unknown-path fallbacks do not increment the old
                    # logical miss counter. Use the purge + first-key boundary
                    # and actual disk bytes, rather than interpreting zero as hot.
                    if mode == "cold" and (receipt["read_operations"] == 0 or receipt["payload_bytes"] == 0):
                        raise RuntimeError("The cold read did not fetch payload bytes from disk")
                    needed = self.warm_hits_per_key * len(numbers)
                    if mode == "hot" and receipt["logical_warm_hits"] < needed:
                        raise RuntimeError(f"The hot read reported {receipt['logical_warm_hits']} warm hits; "
                                           f"its keys and fields need {needed}")
                else:
                    self.validate_write(result, len(numbers))
                    # Preserve reported zeros, but never invent them when an
                    # older response omits these existing diagnostic fields.
                    sample["resident_stages_us"] = result.get("resident_stages_us")
            except Exception as failure:
                sample["error"] = f"{type(failure).__name__}: {failure}"
                stopped.set()
            samples.append(sample)
        return samples

    def measure(self, name, mode, batch=1, clients=1, samples=None):
        self.case = name
        samples = samples or self.recipe["samples"]
        jobs = self.prepare(mode, batch, samples, clients)
        before = self.node.disk_bytes()
        sequence = len(self.metrics)
        barrier, stopped, monitor_stop = threading.Barrier(clients + 1), threading.Event(), threading.Event()
        memory = []

        def monitor():
            while not monitor_stop.is_set():
                value = self.node.memory_bytes()
                if value is not None:
                    memory.append(value)
                monitor_stop.wait(0.2)

        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        try:
            with ThreadPoolExecutor(max_workers=clients) as pool:
                futures = [pool.submit(self.worker, items, barrier, stopped, mode, client)
                           for client, items in enumerate(jobs)]
                case_started_us = time.time_ns() // 1000
                started = time.perf_counter()
                barrier.wait(timeout=30)
                rows = [sample for future in futures for sample in future.result()]
                elapsed = time.perf_counter() - started
                case_ended_us = time.time_ns() // 1000
        finally:
            monitor_stop.set()
            monitor_thread.join(timeout=5)
        metric = describe(rows, elapsed)
        metric.update(sequence=sequence, clients=clients, batch_records=batch,
                      case_started_unix_us=case_started_us, case_ended_unix_us=case_ended_us,
                      sampled_peak_memory_bytes=max(memory, default=None),
                      disk_before_bytes=before, disk_after_bytes=self.node.disk_bytes())
        metric["disk_growth_bytes"] = metric["disk_after_bytes"] - before
        # Capture after timed work and its resource observations. The diagnostic
        # call never enters latency, throughput, memory, or the disk-size window.
        if mode in ("insert", "update", "write", "mixed") and not metric["errors"]:
            metric["phase_coverage"] = capture_phases(self.node, self.output, name, sequence, rows)
        self.metrics[name] = metric
        if mode == "mixed":
            for kind in ("read", "write"):
                self.metrics[f"{name}/{kind}"] = describe([r for r in rows if r["kind"] == kind], elapsed)
        for row in rows:
            self.raw.write(json.dumps({"case": name, **row}) + "\n")
        self.raw.flush()
        save(self.output / "metrics.json", self.metrics)
        if self.upgrade_target:
            native, _, _ = self.node.call("/api/db/native-status")
            self.cleanup_progress.append({"case": name, "retry_cache": native.get("retry_cache")})
            save(self.output / "retry-cache-upgrade.json", self.cleanup_progress)
        print(json.dumps({"case": name, **metric}), flush=True)
        if metric["errors"] or len(rows) != samples:
            errors = [r["error"] for r in rows if "error" in r]
            raise RuntimeError(f"{name}: incomplete measurement: {errors[:3]}")
        if name == "update/128" and metric["phase_coverage"]["complete"] is not True:
            raise RuntimeError("The batch-update diagnostics lack complete exact request-ID phase coverage")

    def run(self):
        try:
            self.seed()
            # An atom home has one warm entry per requested field. A format 7
            # home has one per record key, whatever the fields.
            self.warm_hits_per_key = 1 if self.node.record_home() else len(FIELDS)
            self.cold_next = 0
            self.measure("cold-read/1", "cold")
            self.measure("cold-read/32", "cold", batch=32)
            hot = list(range(self.recipe["records"] - 256, self.recipe["records"]))
            for start in range(0, len(hot), 64):
                numbers = hot[start:start + 64]
                result, _, _ = self.node.call("/api/query", read_request(numbers))
                validate_read(result, numbers)
            self.measure("hot-read/1", "hot")
            self.measure("hot-read/32", "hot", batch=32)
            modes = ("insert",) if self.recipe.get("insert_only") else ("insert", "update")
            for mode in modes:
                for batch in (1, 128):
                    count = (self.recipe["single_write_samples"] if batch == 1 else
                             self.recipe["batch_update_samples"] if mode == "update" else
                             self.recipe["write_samples"])
                    self.measure(f"{mode}/{batch}", mode, batch=batch, samples=count)
            if not self.recipe.get("insert_only"):
                for clients in (1, 4, 16, 32):
                    for mode in ("read", "write", "mixed"):
                        self.measure(f"concurrent-{mode}/{clients}", mode, clients=clients)
            native, _, _ = self.node.call("/api/db/native-status")
            save(self.output / "native-status.json", native)
            if native["native_disk"].get("writer_failed") is not False:
                raise RuntimeError("The final native writer is unhealthy")
            if self.upgrade_target:
                self.verify_upgrade(native)
        finally:
            self.raw.close()

    def verify_upgrade(self, native):
        # Read every known original and inserted key after the upgrade workload.
        for first in range(0, self.next_insert, 64):
            numbers = list(range(first, min(first + 64, self.next_insert)))
            result, _, _ = self.node.call("/api/query", read_request(numbers))
            validate_read(result, numbers)
            for record in result["results"]:
                number = int(record["key"]["range"].removeprefix("key-"))
                if number < self.recipe["records"] - 256 and record["fields"] != fields(number, "seed"):
                    raise RuntimeError("The upgrade changed an untouched product record")
        cache = native.get("retry_cache", {})
        if self.minimum_retired and (
                cache.get("legacy_complete") is not True or cache.get("legacy_pending") is not False
                or (cache.get("legacy_retired") or 0) < self.minimum_retired):
            raise RuntimeError("The upgrade did not finish its old-receipt cleanup")
        body = write_request([self.next_insert], "upgrade-retry", True)
        first, _, _ = self.node.call("/api/mutations/batch", body)
        repeated, _, _ = self.node.call("/api/mutations/batch", body)
        self.validate_write(first, 1)
        self.validate_write(repeated, 1)
        ids = first.get("mutation_ids")
        if not ids or not repeated.get("mutation_ids"):
            raise RuntimeError("A repeated request lacks its mutation receipt")
        record, _, _ = self.node.call("/api/query", read_request([self.next_insert]))
        validate_read(record, [self.next_insert])
        if record["results"][0]["fields"] != fields(self.next_insert, "upgrade-retry"):
            raise RuntimeError("The unchanged retry changed its product record")
        save(self.output / "upgrade-complete.json", {
            "known_keys_read": self.next_insert, "unchanged_seed_keys": self.recipe["records"] - 256,
            "repeat_value_unchanged": True, "receipt_ids": [ids, repeated["mutation_ids"]],
            "receipt_identity": "Each HTTP request receives a new signed author clock",
            "retry_cache": cache})
