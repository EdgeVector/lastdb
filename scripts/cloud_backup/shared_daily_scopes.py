"""Create a proof scope and its ownership record atomically; fence before cleanup."""

import base64
import os
import subprocess
import uuid

import shared_daily_io as io
from support import ACCOUNT, decode_json, require


def directory():
    rows = io.query(io.LEDGER, 16)
    scopes = []
    for row in rows:
        name = row["record_id"]["S"]
        if name == "schema":
            continue
        io.database_id(name)
        value = io.body(row)
        require(value["schema"] == "lastdb.shared-daily-owned.v1"
                and value["scope"]["database_id"] == name
                and value["scope"]["account_id"] == ACCOUNT
                and value["phase"] in ("active", "fenced"), "shared_daily_ownership_record")
        scopes.append(row)
    require(len(scopes) <= io.MAX_SCOPES, "shared_daily_scope_budget_exceeded")
    return scopes


def create(identity, case, targets):
    require(case in io.CASES and len(directory()) < io.MAX_SCOPES, "shared_daily_scope_budget")
    database = io.PREFIX + str(uuid.uuid4()).split("-", 1)[1]
    seeds, writers = {}, []
    for _ in range(7):
        device, stream, seed = str(uuid.uuid4()), str(uuid.uuid4()), os.urandom(32)
        result = subprocess.run(["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
                                input=bytes.fromhex("302e020100300506032b657004220420") + seed,
                                capture_output=True, timeout=10, check=False)
        require(result.returncode == 0 and len(result.stdout) == 44
                and result.stdout[:12] == bytes.fromhex("302a300506032b6570032100"),
                "shared_daily_device_key")
        seeds[device] = base64.b64encode(seed).decode()
        writers.append({"device_id": device, "source_stream": stream,
                        "public_key": result.stdout[12:].hex()})
    writers.sort(key=lambda writer: writer["source_stream"])
    writers = [writers[-1], *writers[:-1]]
    scope = {"account_id": ACCOUNT, "database_id": database,
             "publisher_device": writers[0]["device_id"], "writers": writers}
    pin = io.get(io.LEDGER, "schema")
    if pin:
        pinned = io.body(pin)
        scope.update(schema_name=pinned["runtime_name"], schema_id=pinned["portable_id"])
    ledger = {"schema": "lastdb.shared-daily-owned.v1", "scope": scope,
              "owner": {**identity, "case": case}, "created_at_ms": io.now_ms(),
              "phase": "active", "fenced_at_ms": None, "targets": targets}
    partition = "shared#" + io.database_id(database)
    rows = [io.item(partition, "database", {
        "publisher_device": scope["publisher_device"], "writers": sorted(w["source_stream"] for w in writers),
        "snapshot": None, "writes_open": True, "object_limit_bytes": str(128 * 1024 * 1024),
        "reserved_object_bytes": "0"})]
    for writer in writers:
        rows += [io.item(partition, "device#" + writer["device_id"], {
            "account_id": ACCOUNT, "public_key": list(bytes.fromhex(writer["public_key"])), "can_write": True}),
            io.item(partition, "writer#" + writer["source_stream"], {
                "stream": {"database_id": database, "source_stream": writer["source_stream"]},
                "owner_device": writer["device_id"], "head": None, "reclaimed_through": "0"})]
    rows.append(io.item(io.LEDGER, database, ledger))
    request = {"ClientRequestToken": database, "TransactItems": [{"Put": {
        "TableName": io.TABLE, "Item": row, "ConditionExpression": "attribute_not_exists(record_id)"}}
        for row in rows]}
    try:
        io.aws("dynamodb", "transact-write-items", request)
    except Exception:
        # A lost response can follow a committed transaction. Read the exact genesis.
        pass
    readback = io.aws("dynamodb", "transact-get-items", {"TransactItems": [{"Get": {
        "TableName": io.TABLE, "Key": {name: row[name] for name in ("account_id", "record_id")}}} for row in rows]})
    require([r.get("Item") for r in readback["Responses"]] == rows, "shared_daily_exact_genesis")
    return scope, seeds


def remember_schema(summary):
    binding = summary.get("schema_binding")
    if not binding:
        return
    binding = {name: binding[name] for name in ("runtime_name", "portable_id")}
    existing = io.get(io.LEDGER, "schema")
    if existing:
        require(io.body(existing) == binding, "shared_daily_schema_pin_changed")
        return
    io.aws("dynamodb", "put-item", {"TableName": io.TABLE, "Item": io.item(io.LEDGER, "schema", binding),
                                     "ConditionExpression": "attribute_not_exists(record_id)"})


def fence(database):
    database = io.database_id(database)
    saved = io.get(io.LEDGER, database)
    require(saved is not None, "shared_daily_missing_ownership")
    value = io.body(saved)
    if value["phase"] == "fenced":
        require(io.get("shared#" + database, "database") is None, "shared_daily_fence_changed")
        return value
    root = io.get("shared#" + database, "database")
    require(root is not None, "shared_daily_database_missing_before_fence")
    state = io.body(root)
    require(state["publisher_device"] == value["scope"]["publisher_device"]
            and state["writers"] == sorted(w["source_stream"] for w in value["scope"]["writers"]),
            "shared_daily_scope_authority_changed")
    revision = int(saved["revision"]["N"])
    value.update(phase="fenced", fenced_at_ms=io.now_ms())
    replacement = io.item(io.LEDGER, database, value, revision + 1)
    def conditional(row):
        return {"ConditionExpression": "revision = :r", "ExpressionAttributeValues": {":r": row["revision"]}}
    try:
        io.aws("dynamodb", "transact-write-items", {"TransactItems": [
            {"Delete": {"TableName": io.TABLE, "Key": io.key("shared#" + database, "database"),
                        **conditional(root)}},
            {"Put": {"TableName": io.TABLE, "Item": replacement, **conditional(saved)}}]})
    except Exception:
        pass
    require(io.get(io.LEDGER, database) == replacement
            and io.get("shared#" + database, "database") is None, "shared_daily_fence_readback")
    return value
