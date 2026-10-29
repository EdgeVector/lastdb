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


def history(node, action, schema="HistoryById", key=None, field="value"):
    return node.call("/api/atom-history", {
        "location": {"schema": schema, "field": field, "key": key or {"hash": "one"}},
        "action": action,
    })[0]


def mutate(node, schema, key, values, operation):
    return node.call("/api/mutation", {
        "type": "mutation", "schema": schema, "key_value": key,
        "fields_and_values": values, "mutation_type": operation,
        "durability": "durable", "convergence": "sync",
    })[0]


def write(node, value=None, operation="update"):
    values = {} if value is None else {"value": value}
    if operation == "create":
        values.update(id="one", bucket="bucket-one")
    return mutate(node, "HistoryById", {"hash": "one"}, values, operation)


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


def numeric_policy(node):
    fields = ["number_bucket", "number", "number_value"]
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": "HistoryByNumber", "descriptive_name": "HistoryByNumber",
        "schema_type": "HashRange", "key": {"hash_field": "number_bucket", "range_field": "number"},
        "fields": fields, "field_types": {f: "Integer" if f == "number" else "String" for f in fields},
        "field_descriptions": {f: "Numeric history example " + f for f in fields},
        "field_versions": {f: 1 for f in fields},
    }})
    result = node.call("/api/atom-history", {
        "location": {"schema": "HistoryByNumber", "field": "number_value"},
        "action": {"op": "policy", "policy": {"mode": "keep"}},
    })[0]
    require(result["policy"] == {"mode": "keep"}, "numeric range policy required key coordinates")
    print("field policy without a key on a numeric range passed", flush=True)


def absent(node, action, schema, key, field="value"):
    try:
        history(node, action, schema, key, field)
    except RuntimeError as error:
        require("HTTP 404" in str(error), "history get failed for another reason: " + str(error))
        return
    raise RuntimeError("history get returned a body")


def inventory_counts(node):
    payload = node.call("/api/db/inventory")[0]
    inventory = payload.get("inventory")
    require(isinstance(inventory, dict) and "per_schema_atoms" in inventory,
            "inventory response has no per_schema_atoms")
    return {row["schema_name"]: row["atom_count"] for row in inventory["per_schema_atoms"]}


def capture_rules(node):
    key = {"hash": "capture-one"}
    mutate(node, "HistoryById", key,
           {"id": "capture-one", "bucket": "bucket-capture", "value": "same"}, "create")
    require(history(node, {"op": "list"}, key=key)["entries"] == [],
            "first write added a sidecar record")
    mutate(node, "HistoryById", key, {"value": "same"}, "update")
    require(history(node, {"op": "list"}, key=key)["entries"] == [],
            "same atom added a sidecar record")
    mutate(node, "HistoryById", key, {"value": "other"}, "update")
    page = history(node, {"op": "list"}, key=key)
    require(len(page["entries"]) == 1, "a new atom did not add one sidecar record")
    require(page["entries"][0]["reason"] == "replace", "replace reason was not replace")
    require(history(node, {"op": "get", "id": page["entries"][0]["id"]}, key=key)["value"] == "same",
            "archived first value differs")
    print("first write, same atom, and one replace passed", flush=True)


def protein_move(node):
    key = {"hash": "move-one"}
    old = {"hash": "from", "range": "move-one"}
    new = {"hash": "to", "range": "move-one"}
    mutate(node, "HistoryById", key,
           {"id": "move-one", "bucket": "from", "value": "stable"}, "create")
    require(history(node, {"op": "list"}, key=key)["entries"] == [],
            "source value history existed before the key move")
    require(history(node, {"op": "list"}, "HistoryByBucket", old)["entries"] == [],
            "old protein key had history before the move")
    mutate(node, "HistoryById", key, {"bucket": "to"}, "update")
    require(history(node, {"op": "list"}, key=key)["entries"] == [],
            "unchanged value atom entered source history")
    moved = history(node, {"op": "list"}, "HistoryByBucket", old)
    require(len(moved["entries"]) == 1,
            "old protein key did not archive one value: " + json.dumps(moved))
    require(moved["entries"][0]["reason"] == "delete", "old protein key reason was not delete")
    require(history(node, {"op": "get", "id": moved["entries"][0]["id"]}, "HistoryByBucket", old)["value"]
            == "stable", "departing value differs")
    require(history(node, {"op": "list"}, "HistoryByBucket", new)["entries"] == [],
            "new protein key had history")
    print("protein key move archived the old key as delete", flush=True)


def expiry_body(node):
    fields = ["id", "note"]
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": "HistoryExpiry", "descriptive_name": "HistoryExpiry",
        "schema_type": "Hash", "key": {"hash_field": "id"}, "fields": fields,
        "field_types": {item: "String" for item in fields},
        "field_descriptions": {item: "Expiry isolated " + item for item in fields},
        "field_versions": {item: 1 for item in fields},
    }})
    result = node.call("/api/atom-history", {
        "location": {"schema": "HistoryExpiry", "field": "note"},
        "action": {"op": "policy", "policy": {"mode": "ttl", "seconds": 2}},
    })[0]
    require(result["policy"] == {"mode": "ttl", "seconds": 2}, "expiry policy was not ttl")
    key = {"hash": "expire-one"}
    inventory_counts(node)
    before = inventory_counts(node)
    mutate(node, "HistoryExpiry", key, {"id": "expire-one", "note": "held"}, "create")
    mutate(node, "HistoryExpiry", key, {"note": "next"}, "update")
    page = history(node, {"op": "list"}, "HistoryExpiry", key, "note")
    expiring = [entry for entry in page["entries"] if entry["expires_at"] is not None]
    require(len(expiring) == 1, "expiry write did not list one body")
    expired_id = expiring[0]["id"]
    after = inventory_counts(node)
    grown = sorted(name for name in set(before) | set(after)
                   if after.get(name, 0) > before.get(name, 0))
    require(len(grown) == 1, "expiry atoms did not land in one schema: "
            + json.dumps({"before": before, "after": after}))
    schema_name = grown[0]
    held = after[schema_name]
    deadline = time.monotonic() + 20
    dropped = None
    last_count = None
    while time.monotonic() < deadline:
        page = history(node, {"op": "list"}, "HistoryExpiry", key, "note")
        visible = any(entry["id"] == expired_id for entry in page["entries"])
        last_count = inventory_counts(node).get(schema_name)
        if not visible and last_count == held - 1:
            dropped = last_count
            break
        time.sleep(1)
    require(dropped == held - 1,
            "expiry worker did not drop the atom body from " + str(held)
            + " to " + str(held - 1) + "; last count " + str(last_count))
    absent(node, {"op": "get", "id": expired_id}, "HistoryExpiry", key, "note")
    verify(node)
    print("ttl worker dropped the expired atom body", flush=True)


def exercise(node, binary):
    install(node)
    numeric_policy(node)
    write(node, "first", "create")
    write(node, "second")
    require(history(node, {"op": "list"})["entries"] == [], "off policy retained a value")
    cli(node, binary, "policy", "keep")
    capture_rules(node)
    protein_move(node)
    expiry_body(node)
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
    # The format marker bytes have no owner read. This restart shows the kept entries stay.
    node.stop()
    node.start()
    require(history(node, {"op": "get", "id": entry})["value"] == "second",
            "off restart hid a kept entry")
    require(len(history(node, {"op": "list"})["entries"]) == count,
            "off restart changed history")
    print("off policy left prior entries readable after restart", flush=True)
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
    cli(node, binary, "policy", "keep")
    write(node, "before-delete")
    write(node, operation="delete")
    require(history(node, {"op": "list"})["entries"], "delete left no retained history")
    write(node, operation="purge")
    require(history(node, {"op": "list"})["entries"] == [], "compliance purge retained history-only key")
    verify(node)
    print("compliance purge of a key with only history and final reference verification passed", flush=True)


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
