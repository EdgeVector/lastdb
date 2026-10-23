#!/usr/bin/env python3
"""Measure real durable batch calls across independent generated schemas."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "native-log-tail-20261021"))
import capture as previous
import measure

def save(path, value):
    if path.name != "writes.json":
        return previous.save(path, value)
    metadata = {key: item for key, item in value.items() if key != "samples"}
    head = json.dumps(metadata, indent=2)[:-2]
    rows = ",\n".join("    "+json.dumps(row, separators=(",", ":")) for row in value["samples"])
    path.write_text(head+',\n  "samples": [\n'+rows+'\n  ]\n}\n')


def schema_name(number):
    return f"IndependentBatch{number:03}"


def payload(schema, number, batch_size, operation):
    items, expected = [], {}
    for position in range(batch_size):
        key = f"record-{position:03}"
        prefix = f"{schema}-{number:06}-{position:03}-"
        value = prefix + hashlib.sha256(prefix.encode()).hexdigest()[:64-len(prefix)]
        items.append({"type": "mutation", "schema": schema, "mutation_type": operation,
                      "key_value": {"hash": schema, "range": key},
                      "fields_and_values": {"bucket": schema, "id": key, "value": value}})
        expected[key] = hashlib.sha256(value.encode()).hexdigest()
    return {"mutations": items}, expected


def seed(node, count, output):
    def one(number):
        name = schema_name(number)
        return node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": ["bucket", "id", "value"],
            "field_descriptions": {field: f"generated {name} {field}" for field in ("bucket", "id", "value")}}})
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(one, range(count)))
    save(output / "seed.json", {"schemas": count, "receipts": receipts,
                               "distinct_field_descriptions": True})
    save(output / "setup-flush.json", node.call("/api/db/flush", {}))
    with ThreadPoolExecutor(max_workers=8) as pool:
        metadata = list(pool.map(lambda n: node.call("/api/schema/"+schema_name(n)), range(count)))
    save(output / "schema-metadata.json", dict(zip((schema_name(n) for n in range(count)), metadata)))


def save_worker_status(node, output):
    status = node.call("/api/status")
    status = status.get("status", status)
    selected = {key: value for key, value in status.items() if "uds" in key or key == "request_ops"}
    save(output, selected)


def check_values(node, expected):
    def one(item):
        schema, values = item
        response = node.call("/api/query", {"schema_name": schema, "fields": ["value"],
            "filter": {"HashRangeKeys": [[schema, key] for key in values]}, "limit": len(values)})
        actual = {record["key"]["range"]: hashlib.sha256(record["fields"]["value"].encode()).hexdigest()
                  for record in response["results"]}
        return {"schema": schema, "expected": len(values), "actual": len(actual),
                "all_values_match": actual == values,
                "mismatches": [key for key in values if values[key] != actual.get(key)]}
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(one, sorted(expected.items())))
    return {"expected": sum(r["expected"] for r in receipts),
            "actual": sum(r["actual"] for r in receipts),
            "all_values_match": all(r["all_values_match"] for r in receipts), "schemas": receipts}


def summarize(rows, elapsed, batch_size):
    values = sorted(row["latency_ms"] for row in rows)
    return {"count": len(rows), "errors": sum(not row["ok"] for row in rows),
            "average_ms": statistics.mean(values), "median_ms": statistics.median(values),
            "p95_ms": values[math.ceil(len(values)*.95)-1],
            "p99_ms": values[math.ceil(len(values)*.99)-1], "max_ms": values[-1],
            "wall_seconds": elapsed, "batches_per_second": len(rows)/elapsed,
            "records_per_second": len(rows)*batch_size/elapsed,
            "latency_unit": "whole batch request; no division by record count"}


def measured_calls(node, args):
    start = threading.Barrier(args.clients)
    assignments = {str(client): [schema_name(s) for s in range(client, args.schemas, args.clients)]
                   for client in range(args.clients)}

    def worker(client):
        rows, expected = [], {}
        names = assignments[str(client)]
        connection = measure.UnixHTTP(node.socket)
        start.wait()
        try:
            for turn in range(args.count // args.clients):
                number = turn * args.clients + client
                schema = names[turn % len(names)]
                operation = "create" if turn < len(names) else "update"
                body, values = payload(schema, number, args.batch_size, operation)
                status, response, elapsed = measure.request(connection, "/api/mutations/batch", body)
                ok = (status == 200 and response.get("ok", response.get("success", False))
                      and response.get("durability") == "durable"
                      and len(response.get("mutation_ids", [])) == args.batch_size)
                rows.append({"number": number, "client": client, "schema": schema,
                             "mutation_type": operation, "records": args.batch_size,
                             "start_unix_us": connection.last_window[0],
                             "end_unix_us": connection.last_window[1], "latency_ms": elapsed,
                             "status": status, "ok": ok, "receipt": response})
                if not ok:
                    break
                expected[schema] = values
                if (turn + 1) % 100 == 0:
                    print(json.dumps({"client": client, "completed": turn+1}), flush=True)
        finally:
            connection.close()
        return rows, expected

    before = node.call("/api/db/native-status")
    save_worker_status(node, args.output/"worker-status-before.json")
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.clients) as pool:
        results = list(pool.map(worker, range(args.clients)))
    elapsed = time.perf_counter() - started
    rows = sorted([row for result in results for row in result[0]], key=lambda row: row["start_unix_us"])
    expected = {schema: values for result in results for schema, values in result[1].items()}
    after = node.call("/api/db/native-status")
    save_worker_status(node, args.output/"worker-status-after.json")
    measured = {"summary": summarize(rows, elapsed, args.batch_size), "assignments": assignments,
                "before": before, "after": after, "expected_sha256": expected, "samples": rows}
    save(args.output / "writes.json", measured)
    print(json.dumps(measured["summary"]), flush=True)
    if len(rows) != args.count or measured["summary"]["errors"]:
        raise RuntimeError("the measured workload contains refused or missing requests")
    return expected


def recovery(node, args, expected):
    name = schema_name(0)
    sentinel = "durable-independent-schema-before-forced-stop"
    receipt = node.call("/api/mutation", {"type": "mutation", "schema": name,
        "mutation_type": "create", "key_value": {"hash": name, "range": "sentinel"},
        "fields_and_values": {"bucket": name, "id": "sentinel", "value": sentinel}})
    if receipt.get("durability") != "durable":
        raise RuntimeError("the final sentinel ack is not durable")
    code = node.stop(signal.SIGKILL)
    save(args.output / "forced-stop.json", {"receipt": receipt, "exit_code": code,
         "signal": "SIGKILL", "pid": node.process.pid})
    save(args.output / "stopped-boundaries.json", node.stopped_boundaries())
    expected[name]["sentinel"] = hashlib.sha256(sentinel.encode()).hexdigest()
    node.start()
    result = check_values(node, expected)
    clock = node.call("/api/mutation", {"type": "mutation", "schema": name,
        "mutation_type": "update", "key_value": {"hash": name, "range": "sentinel"},
        "fields_and_values": {"value": "after-restart"}})
    result.update(next_write=clock, clock_advanced=clock.get("revision", 0) > receipt.get("revision", 0),
                  native_status=node.call("/api/db/native-status"))
    save(args.output / "recovery.json", result)
    if not result["all_values_match"] or not result["clock_advanced"] or clock.get("durability") != "durable":
        raise RuntimeError("the recovered values, durable ack, or author clock differ")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--schemas", type=int, default=64)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--high-water", type=int, default=4096)
    parser.add_argument("--uds-workers", type=int, default=8)
    args = parser.parse_args()
    if sys.platform == "darwin" and len(str(args.home.resolve()/"data/folddb-full.sock").encode())+4 > 103:
        raise RuntimeError("use a shorter explicit home for the macOS Unix control socket")
    if (args.home.exists() or args.count <= 0 or args.clients <= 0 or args.schemas <= 1
            or args.schemas % args.clients or args.count % args.clients or args.count < args.schemas
            or args.batch_size <= 0 or args.batch_size > 128 or args.uds_workers < args.clients):
        raise RuntimeError("use a fresh home, positive counts, and equal independent schema sets")
    args.output.mkdir(parents=True, exist_ok=False)
    args.home.mkdir()
    for key in ("LASTDB_SERIAL_COMPACTION_DIAGNOSTIC", "LASTDB_SERIAL_PREFIX_INSTALL_DIAGNOSTIC",
                "LASTDB_SERIAL_LOG_PUBLICATION_DIAGNOSTIC", "LASTDB_DISK_CAP_BYTES"):
        os.environ.pop(key, None)
    node = previous.Node(args.binary, args.home, args.home.parent/(args.home.name+".log"),
                         args.high_water, True, workers=args.uds_workers)
    save(args.output / "inputs.json", {"binary": str(args.binary.resolve()),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "home": str(args.home.resolve()), "generated_data": True,
        "count": args.count, "clients": args.clients, "schemas": args.schemas, "uds_workers": args.uds_workers,
        "records_per_batch": args.batch_size, "value_bytes": 64,
        "dependency_high_water": args.high_water, "durability_parameter": "default",
        "shared_schema_between_clients": False,
        "method": "Closed-loop real batch API calls; each client rotates through its own schema set. All latency samples retained."})
    try:
        node.start()
        seed(node, args.schemas, args.output)
        expected = measured_calls(node, args)
        values = check_values(node, expected)
        save(args.output / "memory-read.json", values)
        if not values["all_values_match"]:
            raise RuntimeError("the known-key batch reads differ from the acknowledged values")
        recovery(node, args, expected)
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
