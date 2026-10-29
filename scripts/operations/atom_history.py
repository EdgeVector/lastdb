#!/usr/bin/env python3
"""Exercise atom history through the actual CLI and a fresh CI daemon.

This uses the existing owned-node launcher. It runs no test runner or benchmark.
The home contains generated data only and never reads or copies a primary.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node  # noqa: E402


def require(value, message):
    if not value:
        raise RuntimeError(message)


def history(node, action, schema="HistoryById", key=None):
    return node.call("/api/atom-history", {
        "location": {"schema": schema, "field": "value", "key": key or {"hash": "one"}},
        "action": action,
    })[0]


def write(node, value=None, operation="update"):
    values = {} if value is None else {"value": value}
    if operation == "create":
        values.update(id="one", bucket="bucket-one")
    return node.call("/api/mutation", {
        "type": "mutation", "schema": "HistoryById", "key_value": {"hash": "one"},
        "fields_and_values": values, "mutation_type": operation,
        "durability": "durable", "convergence": "sync",
    })[0]


def cli(node, binary, *args, success=True):
    env = dict(os.environ, LASTDB_HOME=str(node.home), LASTDB_SOCKET_PATH=str(node.socket))
    run = subprocess.run([str(binary), "history", "HistoryById", "value",
                          "--key-hash", "one", *args], env=env, text=True,
                         capture_output=True, timeout=90)
    require((run.returncode == 0) == success, run.stderr[-1200:])
    return json.loads(run.stdout) if success else run.stderr


def install(node):
    for name, cross in [("HistoryById", False), ("HistoryByBucket", True)]:
        definition = {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange" if cross else "Hash",
            "key": {"hash_field": "bucket", "range_field": "id"} if cross else {"hash_field": "id"},
            "fields": ["id", "bucket", "value"],
            "field_types": {f: "String" for f in ["id", "bucket", "value"]},
            "field_descriptions": {f: "History example " + f for f in ["id", "bucket", "value"]},
            "field_versions": {f: 1 for f in ["id", "bucket", "value"]},
        }
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


def verify(node):
    cursor = None
    for _ in range(100):
        request = {"max_pages": 128, "max_ms": 10000, "pause_ms": 0}
        if cursor is not None:
            request["cursor"] = cursor
        result = node.call("/api/db/verify-refs", request)[0]["verify_refs"]
        require(result["mismatches_total"] == 0, "reference verifier reported a mismatch: " + json.dumps(result))
        if result["done"]:
            return
        cursor = result["next_cursor"]
    raise RuntimeError("reference verification did not complete")


def exercise(node, binary):
    install(node)
    write(node, "first", "create")
    write(node, "second")
    require(history(node, {"op": "list"})["entries"] == [], "off policy retained a value")
    cli(node, binary, "policy", "keep")
    write(node, "third")
    page = cli(node, binary, "list")
    require(len(page["entries"]) == 1, "keep policy did not retain one transition")
    entry = page["entries"][0]["id"]
    require(cli(node, binary, "get", entry)["value"] == "second", "old atom value differs")
    sibling = history(node, {"op": "list"}, "HistoryByBucket", {"hash": "bucket-one", "range": "one"})
    require(len(sibling["entries"]) == 1, "canonical field policy missed the related molecule")
    expected = page["current"]["mutation"]
    cli(node, binary, "restore", entry, "--expected-mutation", expected)
    cli(node, binary, "restore", entry, "--expected-mutation", expected, success=False)
    verify(node)
    print("keep, canonical field policy, CLI fetch, restore, and stale-write refusal passed", flush=True)
    node.stop()
    node.start()
    require(history(node, {"op": "get", "id": entry})["value"] == "second", "restart lost history")
    write(node, operation="delete")
    require(len(history(node, {"op": "list"})["entries"]) >= 3, "delete removed ordinary history")
    cli(node, binary, "restore", entry, "--absent")
    verify(node)
    print("restart, delete retention, and restore of an absent key passed", flush=True)
    cli(node, binary, "policy", "ttl", "--seconds", "2")
    write(node, "short-lived-old")
    write(node, "short-lived-new")
    before = history(node, {"op": "list"})
    expiring = [e["id"] for e in before["entries"] if e["expires_at"] is not None]
    require(expiring, "TTL policy created no expiring entry")
    time.sleep(3)
    after = history(node, {"op": "list"})
    require(not any(e["id"] in expiring for e in after["entries"]), "expired entries remain visible")
    require(any(e["id"] == entry for e in after["entries"]), "policy change changed old keep entries")
    verify(node)
    print("per-field TTL and fixed expiry of existing entries passed", flush=True)
    cli(node, binary, "policy", "off")
    count = len(history(node, {"op": "list"})["entries"])
    write(node, "off-again")
    require(len(history(node, {"op": "list"})["entries"]) == count, "off policy continued capture")
    cursor = None
    for _ in range(100):
        action = {"op": "purge", "cursor": cursor}
        result = history(node, action)
        if result["complete"]:
            break
        cursor = result["cursor"]
    else:
        raise RuntimeError("bounded history purge did not complete")
    require(history(node, {"op": "list"})["entries"] == [], "purge retained selected history")
    verify(node)
    print("off policy, bounded history purge, and final reference verification passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    node = Node(args.daemon.resolve(), args.revision, output, temporary_directory=root,
                environment={"LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
                             "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
                             "EXEMEM_API_URL": "http://127.0.0.1:9",
                             "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"})
    try:
        node.start()
        exercise(node, args.cli.resolve())
    finally:
        node.close()


if __name__ == "__main__":
    main()
