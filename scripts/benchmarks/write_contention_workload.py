"""Actual keyed, durable requests for the isolated writer investigation."""

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import hashlib
import json
import os
import threading
import time

from node import save
from range_continuation.process_counters import read_counters, counter_delta
from workload import FIELDS, fields, key, encoded, describe, Workload
from phases import capture_phases
from disk_budget import require_search_off


RECORDS = 100000


class Writers:
    def __init__(self, node, layout):
        self.node, self.layout = node, layout
        self.versions = {}
        self.sequence = 0

    def target(self, number):
        owner = number % 16
        schema = f"WriteContention{owner:02d}" if self.layout == "schemas" else "GitHubBenchmark"
        bucket = f"generated-{owner:02d}" if self.layout == "hashes" else "generated"
        return schema, bucket

    def value(self, number, version):
        return {**fields(number, version), "bucket": self.target(number)[1]}

    def mutation(self, number, version, create=False):
        schema, bucket = self.target(number)
        value = self.value(number, version)
        return {"type": "mutation", "schema": schema, "mutation_type": "create" if create else "update",
                "durability": "durable", "key_value": {"hash": bucket, "range": key(number)},
                "fields_and_values": value if create else {n: value[n] for n in ("title", "body")}}

    def seed(self):
        schemas = sorted({self.target(n)[0] for n in range(16)})
        definitions, identities = {}, set()
        for schema in schemas:
            description = "generated benchmark" if schema == "GitHubBenchmark" else f"{schema} generated benchmark"
            definition = {"source": "system_seed", "name": schema, "descriptive_name": schema,
                          "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
                          "fields": FIELDS, "field_types": {f: "String" for f in FIELDS},
                          "field_descriptions": {f: f"{description} {f}" for f in FIELDS}}
            self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})
            actual = self.node.call(f"/api/schema/{schema}")[0]["schema"]
            hashes = set(actual["field_hashes"].values())
            if len(hashes) != len(FIELDS) or identities.intersection(hashes):
                raise ValueError("Investigation schemas must have distinct canonical fields")
            identities.update(hashes)
            definitions[schema] = actual
        save(self.node.output / "schema-identities.json", definitions)
        for first in range(0, RECORDS, 128):
            numbers = range(first, min(first + 128, RECORDS))
            response, _, _ = self.node.call("/api/mutations/batch", {
                "mutations": [self.mutation(n, "seed", True) for n in numbers]})
            Workload.validate_write(response, len(numbers))
        if self.node.call("/api/db/flush", {})[0].get("durable") is not True:
            raise ValueError("Generated seed lacks its durable flush")
        self.node.stop()
        self.node.start()
        self.verify()

    def verify(self):
        groups = defaultdict(list)
        for number in [*range(32), *range(RECORDS - 256, RECORDS)]:
            groups[self.target(number)[0]].append(number)
        for schema, numbers in groups.items():
            for first in range(0, len(numbers), 64):
                selected = numbers[first:first + 64]
                coordinates = [[self.target(n)[1], key(n)] for n in selected]
                result = self.node.call("/api/query", {"schema_name": schema, "fields": FIELDS,
                    "filter": {"HashRangeKeys": coordinates}, "limit": len(selected)})[0]
                expected = {(self.target(n)[1], key(n)): self.value(n, self.versions.get(n, "seed"))
                            for n in selected}
                actual = {(r["key"]["hash"], r["key"]["range"]): r["fields"] for r in result["results"]}
                if result["returned_count"] != len(selected) or len(result["results"]) != len(selected) or actual != expected:
                    raise ValueError("A known-key read differs from the last durable write")

    def jobs(self, name, clients, batch, requests):
        jobs = [[] for _ in range(clients)]
        owners = []
        for client in range(clients):
            owners.append({RECORDS - 256 + client + clients * n for n in range(256 // clients)})
        if sum(map(len, owners)) != len(set.union(*owners)):
            raise ValueError("Writers unexpectedly share keys")
        for index in range(requests):
            client, turn = index % clients, index // clients
            owned = sorted(owners[client])
            numbers = [owned[(turn * batch + n) % len(owned)] for n in range(batch)]
            version = f"{name}-{index}"
            body = encoded({"mutations": [self.mutation(n, version) for n in numbers]})
            jobs[client].append((body, f"wc-{name}-{index:06d}", numbers))
            self.versions.update({n: version for n in numbers})
        return jobs

    def measure(self, name, clients, batch=1, requests=2048):
        jobs = self.jobs(name, clients, batch, requests)
        barrier = threading.Barrier(clients + 1)

        def worker(client):
            rows = []
            barrier.wait(timeout=30)
            for body, request_id, numbers in jobs[client]:
                result, elapsed, _ = self.node.call("/api/mutations/batch", body, request_id=request_id)
                Workload.validate_write(result, len(numbers))
                rows.append({"case": name, "kind": "write", "client": client, "records": len(numbers),
                             "request_id": request_id, "request_sha256": hashlib.sha256(body).hexdigest(),
                             "elapsed_ms": elapsed, "resident_stages_us": result.get("resident_stages_us")})
            return rows

        status = self.node.call("/api/status")[0]["status"]
        require_search_off(self.node, self.node.output, name)
        with ThreadPoolExecutor(max_workers=clients) as pool:
            futures = [pool.submit(worker, client) for client in range(clients)]
            before = {"daemon": read_counters(self.node.process.pid), "client": read_counters(os.getpid())}
            started_us, started = time.time_ns() // 1000, time.monotonic_ns()
            barrier.wait(timeout=30)
            rows = [row for future in futures for row in future.result()]
            ended, ended_us = time.monotonic_ns(), time.time_ns() // 1000
            after = {"daemon": read_counters(self.node.process.pid), "client": read_counters(os.getpid())}
        if len(rows) != requests:
            raise ValueError("The write case did not complete all requests")
        result = {"name": name, "clients": clients, "batch_records": batch, "requests": requests,
                  "workers": status["uds"]["workers"], "native_timing": self.node.native_timing,
                  "case_started_unix_us": started_us, "case_ended_unix_us": ended_us,
                  "wall_ns": ended - started, "metrics": describe(rows, (ended - started) / 1e9),
                  "process": {label: counter_delta(before[label], after[label]) for label in before}}
        for label, observation in result["process"].items():
            cpu = observation["derived"]["process_cpu_ns"]
            if cpu is None or not observation["process_identity_same"]:
                raise ValueError(f"Missing stable {label} CPU observations")
            observation["average_cpu_cores"] = cpu / observation["elapsed_ns"]
        result["phase_coverage"] = capture_phases(self.node, self.node.output, name, self.sequence, rows)
        self.sequence += 1
        save(self.node.output / f"{name}-samples.json", rows)
        self.verify()
        save(self.node.output / f"{name}.json", result)
        print(json.dumps({"case": name, "metrics": result["metrics"]}), flush=True)
        return result
