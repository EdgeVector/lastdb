#!/usr/bin/env python3
"""Measure real, sequential LastDB HTTP writes through an owned Unix socket.

Run against a generated isolated home only. No database files are copied.
The caller starts/stops the real daemon and seeds DurableWriteDemo separately.
"""

import argparse
import hashlib
import http.client
import json
import math
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import socket
import statistics
import time


class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def request(client, route, payload=None):
    started = time.perf_counter_ns()
    encoded = None if payload is None else json.dumps(payload).encode()
    client.request("GET" if payload is None else "POST", route, encoded,
                   {"Content-Type": "application/json", "X-LastDB-Client": "durable-latency-proof"})
    response = client.getresponse()
    data = json.loads(response.read())
    elapsed = (time.perf_counter_ns() - started) / 1_000_000
    return response.status, data, elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--hash", default="proof")
    parser.add_argument("--phase", choices=("create", "update", "mixed"), required=True)
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--value-bytes", type=int, default=4096)
    parser.add_argument("--keys", type=int, default=128)
    parser.add_argument("--output", type=Path, default=default_output('write-under-10ms-20261021'))
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    client = UnixHTTP(args.socket)
    before = request(client, "/api/db/native-status")[1]
    rows = []
    expected = {}
    for number in range(args.count):
        operation = args.phase if args.phase != "mixed" else ("create" if number < args.count // 2 else "update")
        position = number if args.phase != "mixed" or operation == "create" else number - args.count // 2
        key = f"record-{position if operation == 'create' else position % args.keys:04}"
        prefix = f"{operation}-{number:06}-"
        value = prefix + hashlib.shake_256(prefix.encode()).hexdigest(args.value_bytes)[
            :max(0, args.value_bytes - len(prefix))]
        payload = {
            "type": "mutation", "schema": "DurableWriteDemo",
            "key_value": {"hash": args.hash, "range": key},
            "fields_and_values": {"bucket": args.hash, "id": key, "value": value},
            "mutation_type": operation, "durability": "durable",
        }
        status, result, elapsed = request(client, "/api/mutation", payload)
        ok = status == 200 and result.get("success", result.get("ok", False)) and result.get("durability") == "durable"
        rows.append({"number": number, "key": key, "mutation_type": operation, "latency_ms": elapsed,
                     "status": status, "ok": ok, "receipt": result})
        if not ok:
            print(json.dumps({"failed_at": number, "status": status, "response": result}))
            break
        expected[key] = hashlib.sha256(value.encode()).hexdigest()
        if (number + 1) % 100 == 0:
            print(json.dumps({"completed": number + 1,
                              "average_ms": statistics.mean(row["latency_ms"] for row in rows)}), flush=True)
    after = request(client, "/api/db/native-status")[1]
    samples = sorted(row["latency_ms"] for row in rows)
    summary = {"phase": args.phase, "hash": args.hash, "value_bytes": args.value_bytes, "count": len(rows), "requested": args.count,
               "errors": sum(not row["ok"] for row in rows),
               "average_ms": statistics.mean(samples), "median_ms": statistics.median(samples),
               "p95_ms": samples[math.ceil(len(samples) * .95) - 1],
               "p99_ms": samples[math.ceil(len(samples) * .99) - 1],
               "min_ms": min(samples), "max_ms": max(samples),
               "under_10_ms": sum(value < 10 for value in samples),
               "method": "sequential client JSON encode through response decode, one Unix socket connection per request, explicit durable receipt"}
    summary["by_operation"] = {
        operation: {"count": len(values), "average_ms": statistics.mean(values)}
        for operation in ("create", "update")
        if (values := [row["latency_ms"] for row in rows if row["mutation_type"] == operation])
    }
    metadata = {"summary": summary, "before": before, "after": after, "expected_sha256": expected}
    head = json.dumps(metadata, indent=2)[:-2]
    samples = ",\n".join("    " + json.dumps(row, separators=(",", ":")) for row in rows)
    args.output.write_text(head + ',\n  "samples": [\n' + samples + '\n  ]\n}\n')
    print(json.dumps(summary), flush=True)
    client.close()


if __name__ == "__main__":
    main()
