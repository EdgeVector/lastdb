"""Owned-fixture preparation, bounded observations, and mixed-write receipts."""

import gzip
import hashlib
import json
import threading
import time

from cold_layout import snapshot
from node import save
from workload import PROFILES, Workload, fields, key, read_request, validate_read

CASE = "concurrent-mixed/4"
RECIPE = {**PROFILES["standard"], "native_timing": False}


def precondition(node, output):
    """Replay the standard history up to (but excluding) mixed/4."""
    output.mkdir()
    work = Workload(node, RECIPE, output)
    try:
        work.seed()
        work.cold_next = 0
        work.measure("cold-read/1", "cold")
        work.measure("cold-read/32", "cold", batch=32)
        warm(node)
        work.measure("hot-read/1", "hot")
        work.measure("hot-read/32", "hot", batch=32)
        for mode in ("insert", "update"):
            for batch in (1, 128):
                count = (RECIPE["single_write_samples"] if batch == 1 else
                         RECIPE["batch_update_samples"] if mode == "update" else RECIPE["write_samples"])
                work.measure(f"{mode}/{batch}", mode, batch=batch, samples=count)
        for clients, modes in ((1, ("read", "write", "mixed")), (4, ("read", "write"))):
            for mode in modes:
                work.measure(f"concurrent-{mode}/{clients}", mode, clients=clients)
    finally:
        work.raw.close()


def warm(node):
    titles = {}
    for start in range(RECIPE["records"] - 256, RECIPE["records"], 64):
        numbers = list(range(start, start + 64))
        result, _, _ = node.call("/api/query", read_request(numbers))
        titles.update(validate_read(result, numbers))
    return hashlib.sha256(json.dumps(titles, sort_keys=True).encode()).hexdigest()


def observe(node):
    # Status and filesystem observations are not an atomic physical snapshot.
    started = time.monotonic_ns()
    unix_started = time.time_ns() // 1000
    state = snapshot(node)
    state.update(observation_start_ns=started, observation_end_ns=time.monotonic_ns(),
                 observation_start_unix_us=unix_started, observation_end_unix_us=time.time_ns() // 1000)
    return state


class Trace:
    """Intrusive diagnostic only; never pooled into the unpolled timing arm."""

    def __init__(self, node, output):
        self.node, self.output = node, output
        self.stop, self.ready = threading.Event(), threading.Event()
        self.error, self.operations, self.observations = None, {}, []
        self.thread = threading.Thread(target=self.run, daemon=True)

    def collect(self):
        state = observe(self.node)
        recent = self.node.call("/api/status?recent=1")[0]["status"]["request_ops"]["recent"]
        for row in recent:
            if (row.get("client") == "github-benchmark"
                    and (row.get("request_id") or "").startswith("bench-0-")):
                self.operations[row["request_id"]] = row
        self.observations.append(state)

    def run(self):
        try:
            deadline = time.monotonic() + 60
            while not self.stop.is_set():
                if time.monotonic() >= deadline or len(self.observations) >= 600:
                    raise RuntimeError("The diagnostic observation exceeded its bound")
                self.collect()
                self.ready.set()
                self.stop.wait(0.1)
        except BaseException as failure:
            self.error = f"{type(failure).__name__}: {failure}"
            self.ready.set()

    def start(self):
        self.thread.start()
        if not self.ready.wait(10) or self.error:
            self.finish()
            raise RuntimeError(self.error or "Diagnostic observer did not start")

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=20)
        if self.thread.is_alive():
            raise RuntimeError("Diagnostic observer did not finish")
        if not self.error:
            try:
                self.collect()
            except BaseException as failure:
                self.error = f"{type(failure).__name__}: {failure}"
        save(self.output / "trace.json", {"error": self.error, "observations": self.observations,
                                         "request_ops": self.operations})
        if self.error:
            raise RuntimeError(self.error)


def audit(node, output, trace):
    rows = [json.loads(line) for line in gzip.decompress((output / "samples.jsonl.gz").read_bytes()).splitlines()]
    if len(rows) != 256 or any(row.get("error") or row["case"] != CASE for row in rows):
        raise RuntimeError("The target case lacks 256 successful samples")
    expected, identities = {}, []
    for client in range(4):
        selected = sorted((r for r in rows if r["client"] == client), key=lambda r: r["client_request"])
        if len(selected) != 64 or [r["client_request"] for r in selected] != list(range(64)):
            raise RuntimeError("A mixed client lacks its complete request sequence")
        for turn, row in enumerate(selected):
            if row["kind"] != ("read" if (client + turn) % 2 == 0 else "write"):
                raise RuntimeError("The mixed read/write alternation changed")
            identities.append([client, turn, row["kind"], row["request_sha256"]])
            if row["kind"] == "read":
                cost = row.get("read_receipt")
                if not cost or cost["logical_warm_hits"] != 4 or cost["read_operations"] != 0:
                    raise RuntimeError("The mixed read is not a four-field logical warm hit")
            else:
                if not isinstance(row.get("resident_stages_us", {}).get("ack"), (int, float)):
                    raise RuntimeError("A durable write lacks its native acknowledgment timing")
                index = turn * 4 + client
                number = RECIPE["records"] - 256 + index // 2
                expected[key(number)] = fields(number, f"{CASE}-{index}")["title"]
    operations = trace.operations if trace else {r["request_id"]: r.get("server_request_ops") for r in rows}
    if any(not operations.get(r["request_id"]) for r in rows):
        raise RuntimeError("The target case lacks exact server request-ID coverage")
    # This verification is after timing and tracing; every written key must survive.
    numbers = [int(name.removeprefix("key-")) for name in expected]
    for start in range(0, len(numbers), 64):
        batch = numbers[start:start + 64]
        result = node.call("/api/query", read_request(batch))[0]
        found = validate_read(result, batch)
        if any(title != expected[name] for name, title in found.items()):
            raise RuntimeError("A successful durable write is missing or has the wrong final value")
    save(output / "audit.json", {"requests": 256, "reads": 128, "writes": 128,
                                 "verified_written_keys": len(expected), "server_coverage": 256,
                                 "identities": identities})
    return identities
