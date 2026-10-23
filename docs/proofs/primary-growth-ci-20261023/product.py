#!/usr/bin/env python3
"""Operate generated native homes on the GitHub runner; never primary data."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time

SCHEMA = "UpgradeCompactionRecord"
FIELDS = ["bucket", "id", "value"]


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


class Connection(http.client.HTTPConnection):
    def __init__(self, path, timeout=60):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


def call(path, route, body=None, timeout=60):
    connection = Connection(path, timeout)
    started = time.perf_counter_ns()
    headers = {"Content-Type": "application/json", "X-LastDB-Client": "primary-upgrade-check"}
    try:
        connection.request("GET" if body is None else "POST", route,
                           None if body is None else json.dumps(body), headers)
        response = connection.getresponse()
        raw = response.read()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = {"body": raw.decode("utf-8", errors="replace")}
        elapsed = (time.perf_counter_ns() - started) / 1_000_000
        if response.status != 200:
            raise RuntimeError(f"{route}: HTTP {response.status}: {value}")
        return value.get("data", value), elapsed
    finally:
        connection.close()


class Node:
    def __init__(self, binary, home, sock, log):
        self.binary, self.home, self.sock, self.log = binary, home, sock, log
        self.process = None

    def start(self):
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_"))}
        environment.update(LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           RUST_MIN_STACK="16777216")
        self.home.mkdir(mode=0o700, exist_ok=True)
        self.stream = self.log.open("ab")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.sock), "--full-socket-path", str(self.sock) + "f"],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.stream,
            stderr=self.stream, start_new_session=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"Owned node exited: {self.log}")
            try:
                value, _ = call(self.sock, "/health", timeout=2)
                if value.get("status") == "ok":
                    print(f"owned_node_ready pid={self.process.pid}", flush=True)
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(0.1)
        raise RuntimeError("Owned node did not become ready")

    def stop(self):
        if self.process is None:
            return
        session = json.loads((self.home / "current-session.json").read_text())
        self.process.send_signal(signal.SIGTERM)
        code = self.process.wait(timeout=60)
        receipt = json.loads((self.home / ".shutdown_flush_ready").read_text())
        self.stream.close()
        self.process = None
        if code != 0 or receipt.get("flush_ok") is not True or receipt.get("pid") != session["pid"]:
            raise RuntimeError("Owned node lacks a clean shutdown flush receipt")
        return receipt

    def cleanup_process(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
            self.stream.close()


def rows(sock, expected, schema=SCHEMA):
    keys = [["upgrade", f"key-{number:03d}"] for number in range(65)]
    value, elapsed = call(sock, "/api/query", {"schema_name": schema,
        "fields": FIELDS, "filter": {"HashRangeKeys": keys}, "limit": 65})
    actual = {row["key"]["range"]: row["fields"] for row in value.get("results", [])}
    if actual != expected or value.get("unresolved_rows", 0) != 0:
        raise RuntimeError("Exact-key records differ from the acknowledged values")
    return {"records": len(actual), "milliseconds": elapsed, "unresolved_rows": 0}


def mutation(number, operation, value):
    return {"type": "mutation", "schema": SCHEMA, "mutation_type": operation,
        "durability": "durable", "key_value": {"hash": "upgrade", "range": f"key-{number:03d}"},
        "fields_and_values": {} if operation == "delete" else value}


def fields(number, revision=0):
    return {"bucket": "upgrade", "id": f"key-{number:03d}", "value": f"value-{number}-{revision}"}


def durable(sock, mutations):
    value, elapsed = call(sock, "/api/mutations/batch", {"mutations": mutations})
    if value.get("durability") != "durable" or value.get("count") != len(mutations):
        raise RuntimeError("A complete durable acknowledgement is absent")
    return {"count": value["count"], "durability": value["durability"], "milliseconds": elapsed}


def healthy(sock):
    value, _ = call(sock, "/api/db/native-status")
    native = value["native_disk"]
    if native["writer_failed"]:
        raise RuntimeError("Record admission failure mark is set")
    return {key: native.get(key) for key in (
        "generation", "frontier", "writer_failed", "accepted_sequence", "completed_sequence")}


def seed(args):
    root = args.scratch.resolve(strict=True)
    owner = json.loads((root / "owner.json").read_text())
    if owner["scratch"] != str(root) or owner["owner_uid"] != os.getuid() or owner["primary_data"]:
        raise RuntimeError("Owned scratch identity differs")
    if any(part in root.parts for part in (".lastdb", ".primary-lastdb-archive-20261014")):
        raise RuntimeError("Scratch names protected primary data")
    baseline = Node(args.baseline.resolve(), root / "baseline", root / "b.sock", root / "baseline.log")
    candidate = Node(args.candidate.resolve(), root / "candidate", root / "c.sock", root / "candidate.log")
    report = {"source": args.source_commit, "primary_copy": False,
              "baseline_sha256": digest(baseline.binary), "candidate_sha256": digest(candidate.binary)}
    expected = {f"key-{n:03d}": fields(n) for n in range(64)}
    try:
        baseline.start()
        call(baseline.sock, "/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": FIELDS, "field_descriptions": {name: "Upgrade product receipt " + name for name in FIELDS}}})
        report["baseline_write"] = durable(baseline.sock, [mutation(n, "create", fields(n)) for n in range(64)])
        report["baseline_stop"] = baseline.stop()
        baseline.start()
        report["baseline_read"] = rows(baseline.sock, expected)
        report["baseline_status"] = healthy(baseline.sock)
        report["baseline_reopen_stop"] = baseline.stop()
        shutil.copytree(baseline.home, candidate.home)
        candidate.start()
        print("candidate_reads_baseline_data", flush=True)
        report["candidate_read"] = rows(candidate.sock, expected)
        if report["candidate_read"]["milliseconds"] > 3 * max(250, report["baseline_read"]["milliseconds"]):
            raise RuntimeError("Candidate exact-key read exceeds the latency bound")
        report["candidate_write"] = durable(candidate.sock, [mutation(0, "update", fields(0, 1)),
            mutation(1, "delete", {}), mutation(64, "create", fields(64))])
        expected["key-000"] = fields(0, 1)
        expected.pop("key-001")
        expected["key-064"] = fields(64)
        report["candidate_read_after_write"] = rows(candidate.sock, expected)
        print("candidate_compaction_start", flush=True)
        compact, elapsed = call(candidate.sock, "/api/db/compact", {"dry_run": False, "grace_ms": 0}, timeout=600)
        report["candidate_compaction"] = compact["compact"]["native_disk"]
        report["candidate_compaction_milliseconds"] = elapsed
        if not report["candidate_compaction"].get("executed"):
            raise RuntimeError("Candidate compaction did not execute")
        report["candidate_read_after_compaction"] = rows(candidate.sock, expected)
        report["candidate_status"] = healthy(candidate.sock)
        report["candidate_memory_kib"] = int(subprocess.check_output([
            "ps", "-p", str(candidate.process.pid), "-o", "rss="], text=True).strip())
        if report["candidate_memory_kib"] >= 14 * 1024 * 1024:
            raise RuntimeError("Candidate exceeds the memory guard headroom")
        report["candidate_stop"] = candidate.stop()
        rollback = Node(baseline.binary, candidate.home, root / "r.sock", root / "rollback.log")
        try:
            rollback.start()
            report["old_binary_reads_candidate_compaction"] = rows(rollback.sock, expected)
            report["old_binary_stop"] = rollback.stop()
        finally:
            rollback.cleanup_process()
        report["passed"] = True
        save(args.output / "candidate-check.json", report)
        print("candidate_check_passed", flush=True)
    finally:
        baseline.cleanup_process()
        candidate.cleanup_process()
        if not report.get("passed"):
            report["passed"] = False
            save(args.output / "candidate-check.json", report)

