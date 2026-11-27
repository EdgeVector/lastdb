"""A fixed recipe of keyed reads, durable writes, and closed-loop concurrency."""

from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
import math
import random
import threading
import time

from node import save
from phases import capture_phases, read_ops
from process_counters import counter_delta, read_counters

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


# Spread records use numbers from SPREAD_BASE and 64 hashes. Their seed batches interleave the
# hashes, so each run file from those batches covers the whole spread key span.
SPREAD_BASE, SPREAD_HASHES = 500_000_000, 64


def key(number):
    return f"key-{number:09d}"


def bucket(number):
    return HASH if number < SPREAD_BASE else f"spread-{number % SPREAD_HASHES:02d}"


def benchmark_schema():
    return {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
        "schema_type": "HashRange",
        "record_keys": {"primary": {"kind": "hash_range", "range_type": "String"}, "alternates": {}},
        "fields": FIELDS, "field_types": dict.fromkeys(FIELDS, "String"),
        "field_descriptions": {f: f"generated benchmark {f}" for f in FIELDS},
    }


def fields(number, revision):
    title = f"record-{number}-{revision}"
    return {"bucket": bucket(number), "id": key(number), "title": title,
            "body": hashlib.shake_256(title.encode()).hexdigest(512)}


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def read_request(numbers):
    reads = [{"schema": SCHEMA, "key": {"hash": bucket(number), "range": key(number)}} for number in numbers]
    return encoded({"reads": reads})


def read_rows(result):
    pages = result.get("results") if isinstance(result, dict) else None
    if not isinstance(pages, list):
        raise RuntimeError("A keyed read returned an unexpected reply")
    rows = []
    for page in pages:
        if not isinstance(page, dict) or "error" in page or "records" not in page:
            raise RuntimeError("A keyed read returned an unexpected reply")
        rows.extend(page["records"])
    return rows


def write_request(numbers, revision, insert):
    mutations = []
    for number in numbers:
        value = fields(number, revision)
        if not insert:
            value = {name: value[name] for name in ("title", "body")}
        mutations.append({"type": "mutation", "schema": SCHEMA,
                          "mutation_type": "create" if insert else "update",
                          "durability": "durable",
                          "key_value": {"hash": bucket(number), "range": key(number)},
                          "fields_and_values": value})
    return encoded({"mutations": mutations})


def validate_read(result, numbers):
    records = read_rows(result)
    if len(records) != len(numbers):
        raise RuntimeError("A keyed read returned an incomplete result")
    found = {}
    expected = {key(n): n for n in numbers}
    for record in records:
        coordinate, value = record["key"], record["record"]
        number = expected.get(coordinate["range"])
        if number is None or coordinate["hash"] != bucket(number) or coordinate["range"] in found:
            raise RuntimeError("A keyed read returned an unexpected or duplicate key")
        title = value["title"]
        if (value["bucket"] != bucket(number) or value["id"] != coordinate["range"]
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
    def __init__(self, node, recipe, output):
        self.node, self.recipe, self.output = node, recipe, output
        self.metrics, self.next_insert = {}, recipe["records"]
        self.raw = gzip.open(output / "samples.jsonl.gz", "wt")

    def seed(self):
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": benchmark_schema()})
        for start in range(0, self.recipe["records"], 128):
            numbers = list(range(start, min(start + 128, self.recipe["records"])))
            result, _, _ = self.node.call("/v1/write", write_request(numbers, "seed", True))
            self.validate_write(result, len(numbers))
        spread = self.recipe.get("spread_records", 0)
        for start in range(0, spread, 128):
            numbers = list(range(SPREAD_BASE + start, SPREAD_BASE + min(start + 128, spread)))
            result, _, _ = self.node.call("/v1/write", write_request(numbers, "seed", True))
            self.validate_write(result, len(numbers))
        self.spread_order = list(range(SPREAD_BASE, SPREAD_BASE + spread))
        random.Random(spread).shuffle(self.spread_order)
        result, _, _ = self.node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise RuntimeError("The generated seed lacks a durable flush")
        # Reopen the same owned home. No seeded record remains warm.
        self.node.stop()
        self.node.start()
        result, _, _ = self.node.call("/api/admin/shed", {})
        boundary = result["shed"]
        save(self.output / "cold-boundary.json", boundary)
        if boundary.get("error") or boundary.get("target_met") is not True:
            raise RuntimeError("The cold-read boundary did not purge the logical warm records")

    @staticmethod
    def validate_write(result, count):
        revisions = result.get("revisions") if isinstance(result, dict) else None
        if (result.get("written") != count or not isinstance(revisions, list) or len(revisions) != count
                or any(not isinstance(value, str) or not value for value in revisions)):
            raise RuntimeError("A write lacks one revision for each record")

    def prepare(self, mode, batch, samples, clients):
        jobs = [[] for _ in range(clients)]
        for index in range(samples):
            client, turn = index % clients, index // clients
            # Every client alternates; adjacent clients start with opposite kinds.
            mixed_read = mode == "mixed" and (client + turn) % 2 == 0
            kind = "read" if mode in ("cold", "spread", "hot", "read") or mixed_read else "write"
            if mode == "spread":
                numbers = self.spread_order[self.spread_next:self.spread_next + batch]
                self.spread_next += batch
                if len(numbers) != batch:
                    raise RuntimeError("The spread seed has too few records for distinct cold keys")
            elif mode == "cold":
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
                path = "/v1/read" if kind == "read" else "/v1/write"
                observe = (self.case == "update/128"
                           or self.case == "update/1" and self.recipe["native_timing"])
                before = read_counters(self.node.process.pid) if observe else None
                transport = {}
                result, elapsed, receipt = self.node.call(
                    path, body, receipt=kind == "read", request_id=request_id, transport=transport)
                sample.update(transport)
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
                    if receipt:
                        # Native unknown-path fallbacks do not increment the old
                        # logical miss counter. Use the purge + first-key boundary
                        # and actual disk bytes, rather than interpreting zero as hot.
                        if mode in ("cold", "spread") and (receipt["read_operations"] == 0 or receipt["payload_bytes"] == 0):
                            raise RuntimeError("The cold read did not fetch payload bytes from disk")
                        # A home has one warm entry per record key, whatever the fields.
                        needed = len(numbers)
                        if mode == "hot" and receipt["logical_warm_hits"] < needed:
                            raise RuntimeError(f"The hot read reported {receipt['logical_warm_hits']} warm hits; "
                                               f"its keys need {needed}")
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
        ops_before = read_ops(self.node) if mode in ("insert", "update", "write", "mixed") else None
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
            metric["phase_coverage"] = capture_phases(self.node, self.output, name, sequence, rows, ops_before)
        self.metrics[name] = metric
        if mode == "mixed":
            for kind in ("read", "write"):
                self.metrics[f"{name}/{kind}"] = describe([r for r in rows if r["kind"] == kind], elapsed)
        for row in rows:
            self.raw.write(json.dumps({"case": name, **row}) + "\n")
        self.raw.flush()
        save(self.output / "metrics.json", self.metrics)
        print(json.dumps({"case": name, **metric}), flush=True)
        if metric["errors"] or len(rows) != samples:
            errors = [r["error"] for r in rows if "error" in r]
            raise RuntimeError(f"{name}: incomplete measurement: {errors[:3]}")
        if (name == "update/128"
                and metric["phase_coverage"].get("observed_writes") is not None
                and metric["phase_coverage"]["complete"] is not True):
            raise RuntimeError("The batch-update diagnostics lack complete /api/ops write_phases coverage")

    def run(self):
        try:
            self.seed()
            self.cold_next = 0
            self.measure("cold-read/1", "cold")
            self.measure("cold-read/32", "cold", batch=32)
            if self.recipe.get("spread_records"):
                self.spread_next = 0
                self.measure("cold-read-spread/1", "spread")
                self.measure("cold-read-spread/32", "spread", batch=32)
            hot = list(range(self.recipe["records"] - 256, self.recipe["records"]))
            for start in range(0, len(hot), 64):
                numbers = hot[start:start + 64]
                result, _, _ = self.node.call("/v1/read", read_request(numbers))
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
        finally:
            self.raw.close()
