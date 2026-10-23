#!/usr/bin/env python3
"""Capture real daemon latency and recovery on owned, generated homes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]
spec = importlib.util.spec_from_file_location(
    "fastpath", PROOF.parent / "durable-write-fastpath-20261022/benchmark.py")
fast = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fast)
capture = fast.capture
capture.PROOF = PROOF
save = capture.save
schemas = capture.base.schemas


def preserve_log(scratch, name, output):
    raw = (scratch / (name + ".log")).read_bytes()
    packed = gzip.compress(raw, mtime=0)
    (output / "daemon.txt.gz").write_bytes(packed)
    save(output / "log-receipt.json", {
        "original_sha256": hashlib.sha256(raw).hexdigest(),
        "gzip_sha256": hashlib.sha256(packed).hexdigest()})


def concurrent(binary, scratch, name, number):
    output = PROOF / name
    output.mkdir()
    home = scratch / ("h" + str(number))
    home.mkdir()
    args = argparse.Namespace(output=output, count=256, schemas=64, clients=4, batch_size=8)
    node = capture.Node(binary, home, scratch / (name + ".log"), 4096, False, workers=8)
    node.profile, node.maintenance = True, True
    save(output / "inputs.json", vars(args) | {"output": str(output), "home": str(home),
         "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
         "generated_data_only": True, "auto_maintenance": True, "explicit_compaction": True})
    try:
        node.start()
        schemas.seed(node, 64, output)
        with ThreadPoolExecutor(max_workers=1) as pool:
            job = pool.submit(capture.base.compact, node)
            expected = schemas.measured_calls(node, args)
            result = job.result(timeout=90)
        save(output / "compaction.json", result)
        if "error" in result:
            if not any(reason in result["error"] for reason in
                       ("eight catch-ups", "bounded change capacity", "compaction_behind")):
                raise RuntimeError("unexpected concurrent compaction failure")
            retry = capture.base.compact(node)
            save(output / "compaction-after-writes.json", retry)
            if "error" in retry:
                raise RuntimeError("compaction did not complete after writes")
        values = schemas.check_values(node, expected)
        save(output / "memory-read.json", values)
        if not values["all_values_match"]:
            raise RuntimeError("acknowledged known-key values differ")
        schemas.recovery(node, args, expected)
        save(output / "shutdown.json", {"exit_code": node.stop()})
        save(output / "native-shutdown.json", json.loads((home / ".shutdown_flush_ready").read_text()))
    except Exception as error:
        save(output / "failure.json", {"error": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        preserve_log(scratch, name, output)


def notices(node):
    cursor, events = None, []
    for _ in range(16):
        page = node.call("/api/app/changes", {"since_cursor": cursor, "limit": 500})
        events.extend(page["changes"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            return events
    raise RuntimeError("notification replay exceeded generated workload")


def independent_queries(node, queries):
    with ThreadPoolExecutor(max_workers=2) as pool:
        return list(pool.map(lambda query: node.call("/api/query", query), queries))


def boundaries(binary, scratch, name="boundaries-distinct", home_name="hd"):
    output = PROOF / name
    output.mkdir()
    home = scratch / home_name
    home.mkdir()
    node = capture.Node(binary, home, scratch / (name + ".log"), 4096, False, workers=8)
    node.profile, node.maintenance = True, True
    calls = []
    try:
        node.start()
        schemas.seed(node, 2, output)
        schema = "LargeNativeBatch"
        save(output / "large-schema.json", node.call("/api/schemas/seed-system", {
            "isolated_copy": True, "schema": {"source": "system_seed", "name": schema,
                "descriptive_name": schema, "schema_type": "Hash",
                "key": {"hash_field": "value"}, "fields": ["value"],
                "field_descriptions": {"value": "generated large native notice value"}}}))
        keys = [f"large-{number:04}" for number in range(1025)]
        body = {"mutations": [{"type": "mutation", "schema": schema, "mutation_type": "create",
                "key_value": {"hash": key}, "fields_and_values": {"value": key}} for key in keys]}
        try:
            response = node.call("/api/mutations/batch", body)
        except RuntimeError as error:
            if "read_resource_limit" not in str(error):
                raise
            save(output / "large-refusal.json", {"error": str(error), "accepted": False})
            response = {"mutation_ids": []}
        calls.append({"operation": "large-create", "response": response})
        save(output / "calls.json", calls)
        accepted = bool(response["mutation_ids"])
        if accepted and (response.get("durability") != "durable" or len(response["mutation_ids"]) != 1025):
            raise RuntimeError("large request did not return 1025 durable identities")
        save(output / "large-forced-stop.json", {"exit_code": node.stop(signal.SIGKILL)})
        save(output / "stopped-boundaries.json", node.stopped_boundaries())
        node.start()
        events = notices(node)
        returned = set(response["mutation_ids"])
        if len(events) != len(returned) or {event["mutation_id"] for event in events} != returned:
            raise RuntimeError("large native notices differ after forced stop")
        save(output / "large-notices.json", events)
        value = independent_queries(node, [{"schema_name": schema, "fields": ["value"],
            "filter": {"HashKey": key}, "limit": 1} for key in (keys[0], keys[-1])])
        save(output / "large-recovery.json", value)
        if any((len(result["results"]) != 1 or result["results"][0]["fields"]["value"] != key)
               if accepted else bool(result["results"])
               for result, key in zip(value, (keys[0], keys[-1]))):
            raise RuntimeError("large acknowledged value differs")
        for operation in ("delete", "create", "purge"):
            items = [schemas.payload(schemas.schema_name(n), 1, 1, operation)[0]["mutations"][0]
                     for n in range(2)]
            if operation in ("delete", "purge"):
                for entry in items:
                    entry.pop("fields_and_values")
            # Both erase schemas need existing keys independently of the large case.
            if operation == "delete":
                seed = {"mutations": [schemas.payload(schemas.schema_name(n), 0, 1, "create")[0]["mutations"][0]
                                      for n in range(2)]}
                calls.append({"operation": "erase-keys", "response": node.call("/api/mutations/batch", seed)})
            receipt = node.call("/api/mutations/batch", {"mutations": items})
            calls.append({"operation": operation, "response": receipt})
            save(output / "calls.json", calls)
            if receipt.get("durability") != "durable":
                raise RuntimeError("erase path did not return a durable receipt")
        save(output / "erase-forced-stop.json", {"exit_code": node.stop(signal.SIGKILL)})
        node.start()
        events = notices(node)
        identities = [identity for call in calls for identity in call["response"]["mutation_ids"]]
        if len(events) != len(identities) or {e["mutation_id"] for e in events} != set(identities):
            raise RuntimeError("erase notices differ after restart")
        save(output / "erase-notices.json", events)
        checks = independent_queries(node, [{"schema_name": schemas.schema_name(n), "fields": ["value"],
            "filter": {"HashRangeKeys": [[schemas.schema_name(n), "record-000"]]}, "limit": 1} for n in range(2)])
        save(output / "erase-recovery.json", checks)
        if any(check["results"] for check in checks):
            raise RuntimeError("purged key reappeared after restart")
        save(output / "shutdown.json", {"exit_code": node.stop()})
        save(output / "native-shutdown.json", json.loads((home / ".shutdown_flush_ready").read_text()))
    except Exception as error:
        save(output / "failure.json", {"error": str(error)})
        raise
    finally:
        node.stop()
        node.log.close()
        preserve_log(scratch, name, output)


def refused_request(binary, scratch):
    output = PROOF / "boundaries"
    node = capture.Node(binary, scratch / "hb", scratch / "boundaries.log", 4096, False, workers=8)
    node.profile, node.maintenance = True, True
    try:
        node.start()
        schema = schemas.schema_name(0)
        result = node.call("/api/query", {"schema_name": schema, "fields": ["value"],
            "filter": {"HashRangeKeys": [[schema, "record-000"]]}, "limit": 1})
        events = notices(node)
        save(output / "refusal-recovery.json", {"query": result, "notices": events})
        if result["results"] or events:
            raise RuntimeError("refused large work published data or notices")
        save(output / "refusal-stop.json", {"exit_code": node.stop()})
    finally:
        node.stop()
        node.log.close()
        preserve_log(scratch, "boundaries", output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--mode", choices=("benchmarks", "boundaries", "final"), required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("hold the compiler exclusion during measurements")
    scratch = args.scratch.resolve(strict=True)
    if scratch.stat().st_uid != os.getuid():
        raise RuntimeError("scratch owner differs")
    if not (scratch / "candidate").exists():
        shutil.copyfile(ROOT / "target/dev-release/lastdbd", scratch / "candidate")
        (scratch / "candidate").chmod(0o755)
        save(PROOF / "identity.json", {
            "source": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "main": subprocess.check_output(["git", "rev-parse", "origin/main"], text=True).strip(),
            "product_diff": subprocess.check_output(["git", "diff", "HEAD", "--", "fold_db", "vendor", "lastdb_node"], text=True),
            "binaries": {name: hashlib.sha256((scratch / name).read_bytes()).hexdigest()
                         for name in ("before", "candidate")},
            "scratch": str(scratch), "uid": os.getuid(),
            "compiler_exclusion": True, "other_host_work_controlled": False,
            "baseline_source": "abab443a97f6c4aedbe1eb469c21dbc16f519427"})
    schemas.recovery = fast.recovery_with_notifications
    if args.mode == "final":
        binary = scratch / "final"
        shutil.copyfile(ROOT / "target/dev-release/lastdbd", binary)
        binary.chmod(0o755)
        save(PROOF / "final-identity.json", {
            "source": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "main": subprocess.check_output(["git", "rev-parse", "origin/main"], text=True).strip(),
            "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "compiler_exclusion": True, "other_host_work_controlled": False})
        capture.capture(binary, scratch, "final", False, True, 5)
        capture.capture(binary, scratch, "final-profile", True, True, 6)
        concurrent(binary, scratch, "final-concurrent", 7)
        boundaries(binary, scratch, "final-boundaries", "hf")
        return
    if args.mode == "boundaries":
        if (scratch / "hb").exists():
            refused_request(scratch / "candidate", scratch)
        boundaries(scratch / "candidate", scratch)
        return
    for name, binary, profile, number in (
            ("reviewed", "before", False, 0), ("integrated", "candidate", False, 1),
            ("integrated-profile", "candidate", True, 2)):
        print(name, flush=True)
        capture.capture(scratch / binary, scratch, name, profile, True, number)
    for number, (name, binary) in enumerate((("reviewed-concurrent", "before"),
                                            ("integrated-concurrent", "candidate")), 3):
        print(name, flush=True)
        concurrent(scratch / binary, scratch, name, number)


if __name__ == "__main__":
    main()
