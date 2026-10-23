#!/usr/bin/env python3
"""Capture capped real-daemon writes, concurrent compaction, and recovery receipts."""

import argparse
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
SOURCE = Path(__file__).resolve().parent.parent / "native-log-schema-concurrency-20261021/capture.py"
SPEC = importlib.util.spec_from_file_location("independent_schemas", SOURCE)
schemas = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(schemas)
save = schemas.save


def compact_during_calls(node, args):
    stop = threading.Event()
    receipts = []

    def compact():
        while not stop.is_set() and len(receipts) < 16:
            row = {"start_unix_us": time.time_ns() // 1000}
            try:
                row["receipt"] = node.call("/api/db/compact", {
                    "dry_run": False, "grace_ms": 0, "report_usage": True})
            except Exception as error:
                row["error"] = str(error)
            row["end_unix_us"] = time.time_ns() // 1000
            receipts.append(row)
            if "error" in row or stop.wait(.1):
                return

    thread = threading.Thread(target=compact, daemon=True)
    thread.start()
    try:
        expected = schemas.measured_calls(node, args)
    finally:
        stop.set()
        thread.join(timeout=60)
        save(args.output / "concurrent-compaction.json", {
            "calls": receipts, "thread_stopped": not thread.is_alive()})
    if thread.is_alive() or not receipts or any("error" in row for row in receipts):
        raise RuntimeError("concurrent compaction failed or did not stop")
    status = node.call("/api/db/native-status")
    save(args.output / "after-compaction.json", status)
    before = json.loads((args.output / "writes.json").read_text())["before"]["native_disk"]
    after = status["native_disk"]
    observed = after["space_inventory"]["release_overlaps"] - before["space_inventory"]["release_overlaps"]
    save(args.output / "inventory-overlap.json", {
        "before": before["space_inventory"], "after": after["space_inventory"],
        "new_release_overlaps": observed, "actual_race_window_observed": observed > 0})
    return expected


def disk_bytes(root, depth=6):
    if depth == 0:
        raise RuntimeError("native files exceed the six-level layout")
    total = 0
    for path in root.iterdir():
        if path.is_symlink():
            raise RuntimeError("the generated native root contains a symlink")
        total += disk_bytes(path, depth-1) if path.is_dir() else path.stat().st_size
    return total


def capacity_refusal(node, args, expected):
    """Restart the same owned generated home with its cap below write headroom."""
    usage = disk_bytes(node.home / "data/disk-pack")
    tight_cap = usage + 4096
    os.environ["LASTDB_DISK_CAP_BYTES"] = str(tight_cap)
    node.start()
    name = schemas.schema_name(0)
    body = {"mutations": [{"type": "mutation", "schema": name, "mutation_type": "create",
        "key_value": {"hash": name, "range": "cap-refused"},
        "fields_and_values": {"bucket": name, "id": "cap-refused", "value": "must-not-ack"}}]}
    connection = schemas.measure.UnixHTTP(node.socket)
    try:
        status, response, elapsed = schemas.measure.request(connection, "/api/mutations/batch", body)
    finally:
        connection.close()
    absent = node.call("/api/query", {"schema_name": name, "fields": ["value"],
        "filter": {"HashRangeKeys": [[name, "cap-refused"]]}, "limit": 1})
    values = schemas.check_values(node, expected)
    native = node.call("/api/db/native-status")
    result = {"native_bytes_before_start": usage, "disk_cap_bytes": tight_cap,
        "required_write_headroom_bytes": 64*1024*1024,
        "status": status, "receipt": response, "latency_ms": elapsed,
        "refused_record_absent": not absent["results"], "known_values": values,
        "native_status": native, "shutdown_exit": node.stop()}
    save(args.output / "capacity-refusal.json", result)
    if (status == 200 or "space" not in json.dumps(response).lower()
            or not result["refused_record_absent"] or not values["all_values_match"]):
        raise RuntimeError("the constrained daemon did not refuse the write without data loss")
    os.environ["LASTDB_DISK_CAP_BYTES"] = str(args.disk_cap_bytes)
    node.start()
    receipt = node.call("/api/mutations/batch", body)
    save(args.output / "capacity-restored.json", {
        "disk_cap_bytes": args.disk_cap_bytes, "receipt": receipt,
        "native_status": node.call("/api/db/native-status")})
    if receipt.get("durability") != "durable":
        raise RuntimeError("the write did not recover after capacity was restored")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=256)
    parser.add_argument("--disk-cap-bytes", type=int, default=32*1024*1024*1024)
    args = parser.parse_args()
    args.schemas, args.clients, args.batch_size, args.uds_workers = 64, 2, 8, 8
    args.high_water = 512
    if (args.home.exists() or args.count < args.schemas or args.count % args.clients
            or args.disk_cap_bytes <= 0):
        raise RuntimeError("use a fresh generated home, an even request count, and a positive cap")
    if sys.platform == "darwin" and len(str(args.home.resolve()/"data/folddb-full.sock").encode())+4 > 103:
        raise RuntimeError("use a shorter explicit home for the macOS control socket")
    args.output.mkdir(parents=True, exist_ok=False)
    args.home.mkdir()
    for flag in ("LASTDB_SERIAL_COMPACTION_DIAGNOSTIC", "LASTDB_SERIAL_PREFIX_INSTALL_DIAGNOSTIC",
                 "LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC"):
        os.environ.pop(flag, None)
    os.environ["LASTDB_DISK_CAP_BYTES"] = str(args.disk_cap_bytes)
    node = schemas.previous.Node(args.binary, args.home, args.home.parent/(args.home.name+".log"),
                                 args.high_water, True, workers=args.uds_workers)
    save(args.output / "inputs.json", {
        "binary": str(args.binary.resolve()),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "home": str(args.home.resolve()), "generated_data": True, "count": args.count,
        "schemas": args.schemas, "clients": args.clients, "records_per_batch": args.batch_size,
        "shared_schema_between_clients": False, "uds_workers": args.uds_workers,
        "dependency_high_water": args.high_water, "disk_cap_bytes": args.disk_cap_bytes,
        "durability_parameter": "default", "numeric_latency_trace": True,
        "method": "Real daemon calls with concurrent compaction; no primary data, fixtures, or scheduling injection."})
    try:
        node.start()
        schemas.seed(node, args.schemas, args.output)
        expected = compact_during_calls(node, args)
        values = schemas.check_values(node, expected)
        save(args.output / "memory-read.json", values)
        if not values["all_values_match"]:
            raise RuntimeError("the known-key reads differ from the acknowledged values")
        schemas.recovery(node, args, expected)
        expected[schemas.schema_name(0)]["sentinel"] = hashlib.sha256(b"after-restart").hexdigest()
        save(args.output / "recovery-shutdown.json", {"exit_code": node.stop()})
        capacity_refusal(node, args, expected)
        save(args.output / "shutdown.json", {"exit_code": node.stop()})
        save(args.output / "native-shutdown.json", json.loads((node.home/".shutdown_flush_ready").read_text()))
    except Exception as error:
        save(args.output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()


if __name__ == "__main__":
    main()
