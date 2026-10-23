#!/usr/bin/env python3
"""Capture real snapshot write latency and process-stop recovery on generated data."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
SOURCE = Path(__file__).resolve().parent.parent / "native-log-schema-concurrency-20261021/capture.py"
SPEC = importlib.util.spec_from_file_location("independent_schemas", SOURCE)
schemas = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(schemas)
save = schemas.save


class Node(schemas.previous.Node):
    def start(self):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("LASTDB_", "FOLDDB_"))
               and key not in ("OBS_SENTRY_DSN", "SENTRY_DSN")}
        env.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                   FOLDDB_DISABLE_KEYCHAIN="1", LASTDB_UDS_WORKERS=str(self.workers),
                   LASTDB_DEPENDENCY_HIGH_WATER=str(self.high_water),
                   LASTDB_AUTO_MAINTENANCE_START_DELAY_SECS="0", RUST_MIN_STACK="67108864",
                   RUST_LOG="warn,fold_db::native_compaction=debug" if self.trace else "warn")
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

    def stopped_boundaries(self):
        root = self.home / "data/disk-pack"
        current = (root / "CURRENT").read_bytes()
        if len(current) != 4096 or (root / "journal").exists() or (root / "JOURNAL_TAIL").exists():
            raise RuntimeError("the owned home does not use the journal-free snapshot format")
        return {"current_sha256": hashlib.sha256(current).hexdigest(),
                "journal_directory_exists": False, "journal_tail_exists": False}


def compact(node):
    began = time.time_ns() // 1000
    client = schemas.measure.UnixHTTP(node.socket)
    try:
        payload = json.dumps({"dry_run": False, "grace_ms": 0, "report_usage": True}).encode()
        client.request("POST", "/api/db/compact", payload,
                       {"Content-Type": "application/json", "X-LastDB-Client": "snapshot-rebase-proof"})
        response = client.getresponse()
        raw = response.read().decode()
        row = {"start_unix_us": began, "end_unix_us": time.time_ns() // 1000,
               "status": response.status}
        if response.status != 200:
            row["error"] = raw
        else:
            row["receipt"] = json.loads(raw)
        return row
    except Exception as error:
        return {"start_unix_us": began, "end_unix_us": time.time_ns() // 1000, "error": str(error)}
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--schemas", type=int, default=64)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--high-water", type=int, default=4096)
    parser.add_argument("--uds-workers", type=int, default=8)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    if args.home.exists() or args.output.exists() or args.count < args.schemas:
        raise RuntimeError("use fresh explicit paths and at least one write for each schema")
    if args.clients <= 0 or args.schemas % args.clients or args.count % args.clients:
        raise RuntimeError("each client needs a separate equal schema set")
    args.home.mkdir()
    args.output.mkdir(parents=True)
    save(args.output / "inputs.json", {"binary": str(args.binary.resolve()),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "home": str(args.home.resolve()), "generated_data": True, "count": args.count,
        "schemas": args.schemas, "clients": args.clients, "records_per_batch": args.batch_size,
        "durability_parameter": "omitted; default must be durable", "journal": False,
        "explicit_compaction": args.compact, "high_water": args.high_water})
    node = Node(args.binary, args.home, args.home.parent / (args.home.name + ".log"),
                args.high_water, args.compact, workers=args.uds_workers)
    try:
        node.start()
        schemas.seed(node, args.schemas, args.output)
        if args.compact:
            with ThreadPoolExecutor(max_workers=1) as pool:
                job = pool.submit(compact, node)
                expected = schemas.measured_calls(node, args)
                receipt = job.result(timeout=90)
            save(args.output / "compaction.json", receipt)
            if "error" in receipt:
                if not any(reason in receipt["error"] for reason in
                           ("eight catch-ups", "bounded change capacity")):
                    raise RuntimeError("explicit concurrent compaction failed; preserve its receipt")
                retry = compact(node)
                save(args.output / "compaction-after-writes.json", retry)
                if "error" in retry:
                    raise RuntimeError("compaction did not complete after foreground writes ended")
        else:
            expected = schemas.measured_calls(node, args)
        values = schemas.check_values(node, expected)
        save(args.output / "memory-read.json", values)
        if not values["all_values_match"]:
            raise RuntimeError("known-key reads differ from acknowledged values")
        schemas.recovery(node, args, expected)
        save(args.output / "shutdown.json", {"exit_code": node.stop()})
        save(args.output / "native-shutdown.json", json.loads((node.home / ".shutdown_flush_ready").read_text()))
    except Exception as error:
        save(args.output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()


if __name__ == "__main__":
    main()
