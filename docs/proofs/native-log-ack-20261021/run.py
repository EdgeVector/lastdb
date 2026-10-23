#!/usr/bin/env python3
"""Record real daemon write latency and forced-stop recovery on generated data.

The explicit home must not exist. No primary data or copied database is used.
Owned generated files remain until the caller preserves receipts and cleans them.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.dont_write_bytecode = True
import measure


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Node:
    def __init__(self, binary, home, log, high_water):
        self.binary, self.home = binary.resolve(), home.resolve()
        self.high_water = high_water
        self.socket = str(self.home / "data/folddb.sock")
        self.log = log.open("ab")
        self.process = None

    def start(self):
        env = os.environ.copy()
        for key in ("OBS_SENTRY_DSN", "SENTRY_DSN", "LASTDB_FAST_DURABLE_EXPERIMENT",
                    "LASTDB_MUTATION_SYNC_FLUSH", "LASTDB_HOME", "FOLDDB_HOME",
                    "LASTDB_SEARCH_INBOX"):
            env.pop(key, None)
        env.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                   LASTDB_NATIVE_LOG_ACK_EXPERIMENT="1",
                   FOLDDB_DISABLE_KEYCHAIN="1", LASTDB_DEPENDENCY_HIGH_WATER=str(self.high_water),
                   LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="0", LASTDB_UDS_WORKERS="2",
                   RUST_MIN_STACK="67108864", RUST_LOG="warn")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home)],
                                        env=env, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"owned daemon exited {self.process.returncode}")
            try:
                self.call("/api/db/native-status")
                return
            except (OSError, TimeoutError):
                time.sleep(.1)
        raise RuntimeError("owned daemon did not become ready")

    def call(self, route, payload=None):
        client = measure.UnixHTTP(self.socket)
        try:
            status, data, _ = measure.request(client, route, payload)
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

    def check_values(self, expected):
        keys = list(expected)
        actual = {}
        for first in range(0, len(keys), 128):
            batch = keys[first:first + 128]
            response = self.call("/api/query", {
                "schema_name": "DurableWriteDemo", "fields": ["value"],
                "filter": {"HashRangeKeys": [["direct", key] for key in batch]},
                "limit": len(batch),
            })
            for record in response["results"]:
                value = record["fields"]["value"]
                actual[record["key"]["range"]] = hashlib.sha256(value.encode()).hexdigest()
        return {"expected": len(expected), "actual": len(actual), "all_values_match": actual == expected,
                "mismatches": [key for key in keys if actual.get(key) != expected[key]]}

    def stopped_boundaries(self):
        root = self.home / "data/disk-pack"
        current = (root / "CURRENT").read_bytes()
        tail = (root / "JOURNAL_TAIL").read_bytes()
        def checksum(data, offset):
            return hashlib.sha256(data[:offset] + bytes(32) + data[offset + 32:]).digest()
        if (len(current) != 4096 or len(tail) != 4096 or current[:8] != b"LDCUR1\0\0"
                or tail[:8] != b"LDJTL1\0\0" or checksum(current, 128) != current[128:160]
                or checksum(tail, 96) != tail[96:128]):
            raise RuntimeError("stopped native boundary bytes fail their format checksum")
        published = int.from_bytes(current[32:40], "little")
        logged = int.from_bytes(tail[40:48], "little")
        return {"published_batch": published, "logged_batch": logged,
                "pending_batches": logged - published,
                "same_stream": current[80:96] == tail[16:32],
                "current_sha256": hashlib.sha256(current).hexdigest(),
                "tail_sha256": hashlib.sha256(tail).hexdigest(), "checksums_valid": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--durability", choices=("default", "durable"), default="default")
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--high-water", type=int, default=128)
    parser.add_argument("--value-bytes", type=int, default=64)
    args = parser.parse_args()
    if args.home.exists():
        raise RuntimeError("the owned generated home must not exist")
    args.output.mkdir(parents=True, exist_ok=False)
    args.home.mkdir()
    node = Node(args.binary, args.home, args.home.parent / (args.home.name + ".log"), args.high_water)
    save(args.output / "inputs.json", {"binary": str(args.binary.resolve()),
         "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
         "home": str(args.home.resolve()), "generated_data": True,
         "count": args.count, "durability_parameter": args.durability,
         "dependency_high_water": args.high_water, "value_bytes": args.value_bytes,
         "native_log_ack_experiment": True})
    try:
        node.start()
        if not node.call("/api/db/native-status")["native_disk"]["journal_ack_experiment"]:
            raise RuntimeError("the owned daemon did not enable the native journal experiment")
        seed = node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": "DurableWriteDemo",
            "descriptive_name": "Direct Disk Write", "schema_type": "HashRange",
            "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": ["bucket", "id", "value"],
            "field_descriptions": {key: "generated " + key for key in ("bucket", "id", "value")},
        }})
        save(args.output / "seed.json", seed)
        subprocess.run([sys.executable, str(Path(__file__).with_name("measure.py")),
                        "--socket", node.socket, "--hash", "direct", "--phase", "mixed",
                        "--count", str(args.count), "--value-bytes", str(args.value_bytes), "--keys", "128",
                        "--durability", args.durability,
                        "--output", str(args.output / "writes.json")], check=True)
        measured = json.loads((args.output / "writes.json").read_text())
        if measured["summary"]["errors"] or measured["summary"]["count"] != args.count:
            raise RuntimeError("the real write measurement contains refused or missing requests")
        expected = measured["expected_sha256"]
        save(args.output / "memory-read.json", node.check_values(expected))
        if args.recovery:
            sentinel = "direct-ack-before-SIGKILL"
            receipt = node.call("/api/mutation", {
                "type": "mutation", "schema": "DurableWriteDemo", "mutation_type": "create",
                "key_value": {"hash": "direct", "range": "sentinel"},
                "fields_and_values": {"bucket": "direct", "id": "sentinel", "value": sentinel},
            })
            code = node.stop(signal.SIGKILL)
            save(args.output / "forced-stop.json", {"receipt": receipt, "exit_code": code,
                                                    "pid": node.process.pid, "signal": "SIGKILL"})
            save(args.output / "stopped-boundaries.json", node.stopped_boundaries())
            expected["sentinel"] = hashlib.sha256(sentinel.encode()).hexdigest()
            node.start()
            recovered = node.check_values(expected)
            recovered["native_status"] = node.call("/api/db/native-status")
            clock = node.call("/api/mutation", {
                "type": "mutation", "schema": "DurableWriteDemo", "mutation_type": "update",
                "key_value": {"hash": "direct", "range": "sentinel"},
                "fields_and_values": {"value": "after-restart"},
            })
            recovered["next_write"] = clock
            recovered["clock_advanced"] = clock.get("revision", 0) > receipt.get("revision", 0)
            save(args.output / "recovery.json", recovered)
        save(args.output / "shutdown.json", {"exit_code": node.stop()})
        save(args.output / "native-shutdown.json",
             json.loads((node.home / ".shutdown_flush_ready").read_text()))
    finally:
        node.stop()
        node.log.close()


if __name__ == "__main__":
    main()
