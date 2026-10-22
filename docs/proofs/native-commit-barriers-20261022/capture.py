#!/usr/bin/env python3
"""Compare real main/candidate daemon acknowledgments on fresh generated homes."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
import signal
import transport
import workload as schemas
save = schemas.save


class Node:
    def __init__(self, args):
        self.binary, self.home = args.binary.resolve(), args.home.resolve()
        self.high_water, self.workers = args.high_water, args.uds_workers
        self.socket = str(self.home / "data/folddb.sock")
        self.log = (args.output / "daemon.txt").open("ab")
        self.mode = args.mode
        self.process = None

    def call(self, route, payload=None):
        client = transport.UnixHTTP(self.socket)
        try:
            status, data, _ = transport.request(client, route, payload)
            if status != 200:
                raise RuntimeError(f"{route}: HTTP {status}: {data}")
            return data
        finally:
            client.close()

    def stop(self, sig=signal.SIGTERM):
        if self.process is None or self.process.poll() is not None:
            return None
        self.process.send_signal(sig)
        return self.process.wait(timeout=90)

    def start(self):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("LASTDB_", "FOLDDB_"))
               and key not in ("OBS_SENTRY_DSN", "SENTRY_DSN")}
        env.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                   FOLDDB_DISABLE_KEYCHAIN="1", LASTDB_DEPENDENCY_HIGH_WATER=str(self.high_water),
                   LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="0", LASTDB_UDS_WORKERS=str(self.workers),
                   LASTDB_RESIDENT_PERSIST_MS="500", RUST_MIN_STACK="67108864", RUST_LOG="warn")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home)],
                                        env=env, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned daemon exited {self.process.returncode}")
            try:
                status = self.call("/api/db/native-status")
                actual = status["native_disk"].get("journal_ack_experiment", False)
                if actual:
                    raise RuntimeError("the actual daemon ack mode differs from the requested mode")
                return
            except (OSError, TimeoutError):
                time.sleep(.1)
        raise RuntimeError("owned daemon did not become ready")


def measured_calls(node, args):
    start = threading.Barrier(args.clients)
    assignments = {str(client): [schemas.schema_name(s)
                   for s in range(client, args.schemas, args.clients)] for client in range(args.clients)}

    def worker(client):
        rows, expected = [], {}
        names = assignments[str(client)]
        connection = transport.UnixHTTP(node.socket)
        start.wait()
        try:
            for turn in range(args.count // args.clients):
                number = turn * args.clients + client
                name = names[turn % len(names)]
                operation = "create" if turn < len(names) else "update"
                body, values = schemas.payload(name, number, args.batch_size, operation)
                began = time.perf_counter()
                started_us = time.time_ns() // 1000
                try:
                    status, response, elapsed = transport.request(connection, "/api/mutations/batch", body)
                except Exception as error:
                    status, response = 0, {"client_error": type(error).__name__, "detail": str(error)}
                    elapsed = (time.perf_counter()-began)*1000
                    connection.last_window = (started_us, time.time_ns() // 1000)
                durability = response.get("durability")
                ok = (status == 200 and response.get("ok", response.get("success", False))
                      and durability in ("queued", "durable")
                      and len(response.get("mutation_ids", [])) == args.batch_size)
                rows.append({"number": number, "client": client, "schema": name,
                             "mutation_type": operation, "records": args.batch_size,
                             "start_unix_us": connection.last_window[0],
                             "end_unix_us": connection.last_window[1], "latency_ms": elapsed,
                             "status": status, "ok": ok, "receipt": response})
                if not ok:
                    break
                expected[name] = values
                if (turn+1) % 100 == 0:
                    print(json.dumps({"mode": args.mode, "client": client, "completed": turn+1}), flush=True)
        finally:
            connection.close()
        return rows, expected

    before = node.call("/api/db/native-status")
    schemas.save_worker_status(node, args.output/"worker-status-before.json")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        results = list(pool.map(worker, range(args.clients)))
    elapsed = time.perf_counter() - started
    rows = sorted([row for result in results for row in result[0]], key=lambda row: row["start_unix_us"])
    expected = {schema: values for result in results for schema, values in result[1].items()}
    measured = {"summary": schemas.summarize(rows, elapsed, args.batch_size),
                "durability_receipt_counts": dict(Counter(row["receipt"].get("durability") for row in rows)),
                "assignments": assignments, "before": before,
                "after": node.call("/api/db/native-status"), "expected_sha256": expected, "samples": rows}
    schemas.save_worker_status(node, args.output/"worker-status-after.json")
    save(args.output/"writes.json", measured)
    print(json.dumps({"mode": args.mode, **measured["summary"],
                      "receipts": measured["durability_receipt_counts"]}), flush=True)
    if len(rows) != args.count or measured["summary"]["errors"]:
        raise RuntimeError("the real workload contains refused or missing requests")
    return expected


def finish(node, args, expected):
    values = schemas.check_values(node, expected)
    save(args.output/"memory-read.json", values)
    if not values["all_values_match"]:
        raise RuntimeError("the known-key reads differ from the acknowledged values")
    # Explicit durable API completion must survive an immediate process stop.
    name = schemas.schema_name(0)
    sentinel = "explicit-durable-ack-before-forced-stop"
    receipt = node.call("/api/mutation", {"type": "mutation", "schema": name,
        "mutation_type": "create", "key_value": {"hash": name, "range": "sentinel"},
        "fields_and_values": {"bucket": name, "id": "sentinel", "value": sentinel},
        "durability": "durable"})
    if receipt.get("durability") != "durable":
        raise RuntimeError("the explicit durable request did not complete on disk")
    code = node.stop(signal.SIGKILL)
    save(args.output/"forced-stop.json", {"receipt": receipt, "exit_code": code,
         "pid": node.process.pid, "signal": "SIGKILL"})
    expected[name]["sentinel"] = hashlib.sha256(sentinel.encode()).hexdigest()
    node.start()
    result = schemas.check_values(node, expected)
    result["native_status"] = node.call("/api/db/native-status")
    save(args.output/"recovery.json", result)
    if not result["all_values_match"]:
        raise RuntimeError("the daemon did not recover every known final record")
    exit_code = node.stop()
    marker = json.loads((node.home/".shutdown_flush_ready").read_text())
    save(args.output/"shutdown.json", {"exit_code": exit_code})
    save(args.output/"native-shutdown.json", marker)
    if exit_code != 0 or not marker["flush_ok"]:
        raise RuntimeError("the final owned daemon shutdown did not complete its disk flush")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("main", "candidate"), required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--schemas", type=int, default=64)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--high-water", type=int, default=4096)
    parser.add_argument("--uds-workers", type=int, default=8)
    args = parser.parse_args()
    if (args.home.exists() or args.count <= 0 or args.clients <= 0 or args.schemas <= 1
            or args.schemas % args.clients or args.count % args.clients or args.count < args.schemas
            or args.batch_size <= 0 or args.batch_size > 128 or args.uds_workers < args.clients):
        raise RuntimeError("use a fresh home, positive counts, and equal independent schema sets")
    if sys.platform == "darwin" and len(str(args.home.resolve()/"data/folddb-full.sock").encode())+4 > 103:
        raise RuntimeError("use a shorter explicit home for the macOS control socket")
    args.output.mkdir(parents=True, exist_ok=False)
    args.home.mkdir()
    inputs = {"binary": str(args.binary.resolve()),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "source_commit": args.source_commit, "home": str(args.home.resolve()),
        "generated_data": True, "mode": args.mode, "count": args.count,
        "clients": args.clients, "schemas": args.schemas, "uds_workers": args.uds_workers,
        "records_per_batch": args.batch_size, "value_bytes": 64,
        "dependency_high_water": args.high_water, "maintenance_start_delay_seconds": 0,
        "memory_flush_interval_ms": 500, "disk_cap_bytes": None,
        "durability_parameter": "default; omitted", "numeric_latency_trace": False,
        "shared_schema_between_clients": False, "load_before": os.getloadavg(),
        "method": "Closed-loop real batch API calls; each client rotates through separate generated schemas. Every latency sample is retained."}
    save(args.output/"inputs.json", inputs)
    node = Node(args)
    try:
        node.start()
        schemas.seed(node, args.schemas, args.output)
        expected = measured_calls(node, args)
        finish(node, args, expected)
    except Exception as error:
        save(args.output/"failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()


if __name__ == "__main__":
    main()
