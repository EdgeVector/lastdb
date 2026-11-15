"""AWS DEV guard, DynamoDB metadata verification, and cleanup for snapshot gates."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import summary_line  # noqa: E402
from snapshot_cleanup import cleanup_objects, current_attempt  # noqa: E402
from support import OperationError, require  # noqa: E402

ADMISSION_HEAD = "head"
SNAPSHOT_THROUGH = 12
MAX_PROOF_ENTRIES = 256
CUTOFF_DIGEST = re.compile(r"[0-9a-f]{64}")
WRITER_LABEL = re.compile(r"[A-Za-z0-9_-]{1,64}")


def summary(line):
    summary_line(line, echo=True)


def note(numbers, key, value):
    numbers[key] = value
    summary(f"{key}={value}")


def gate_id():
    return current_attempt().database


def require_native_dispatch():
    require(
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("GITHUB_REF") == "refs/heads/main"
        and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
        "native_cutoff_manual_main_only",
    )


def load_guard():
    load_dir = str(Path(__file__).resolve().parents[1] / "mutation_log_load")
    if load_dir not in sys.path:
        sys.path.insert(0, load_dir)
    import guard

    return guard


def aws(guard, operation, payload):
    try:
        return guard.aws_dynamodb(operation, payload)
    except guard.GuardError as error:
        raise OperationError("cleanup_aws") from error


def partition_keys(guard, partition):
    try:
        return list(
            guard.iter_partition_keys(guard.DEV_TABLE, partition, call=lambda op, req: aws(guard, op, req))
        )
    except guard.GuardError as error:
        raise OperationError("snapshot_metadata_page" if str(error) == "page" else str(error)) from error


def delete_keys(guard, partition, pending):
    try:
        return guard.batch_delete_keys(
            guard.DEV_TABLE, partition, pending, call=lambda op, req: aws(guard, op, req)
        )
    except guard.GuardError as error:
        raise OperationError("cleanup_aws") from error


def delete_without_head(guard, database, partition, keys):
    table = guard.DEV_TABLE
    head = guard.delete_batch(table, database, [ADMISSION_HEAD])["RequestItems"][table][0]
    absent = {
        "Delete": {
            "TableName": table,
            "Key": head["DeleteRequest"]["Key"],
            "ConditionExpression": "attribute_not_exists(record_id)",
        }
    }
    for offset in range(0, len(keys), 8):
        batch = guard.delete_batch(table, partition, keys[offset : offset + 8])
        deletes = [{"Delete": {"TableName": table, "Key": item["DeleteRequest"]["Key"]}} for item in batch["RequestItems"][table]]
        aws(guard, "transact-write-items", {"TransactItems": [absent, *deletes]})
    return len(keys)


def verify_cloud_target(guard):
    guard.allow_region_environment(os.environ)
    described = aws(guard, "describe-table", {"TableName": guard.DEV_TABLE})
    guard.allow_table_arn(described.get("Table", {}).get("TableArn", ""), guard.DEV_TABLE)


def cleanup(guard, attempt, owner, *, expect_charge, resume=False, proof_skipped=False):
    verify_cloud_target(guard)
    database = attempt.database
    partitions = guard.load_partitions(database)
    head_present = ADMISSION_HEAD in partition_keys(guard, database)
    if not head_present and not resume:
        require(
            not expect_charge and all(not partition_keys(guard, part) for part in partitions),
            "snapshot_cleanup_missing_head",
        )
        summary("provider_objects=not_registered")
        return
    receipt = cleanup_objects(guard, attempt, owner, summary, expect_charge=expect_charge, receipt_only=not head_present)
    if not receipt:
        require(
            proof_skipped and all(not partition_keys(guard, part) for part in partitions),
            "snapshot_resume_no_verified_receipt",
        )
        summary("provider_objects=not_registered original_proof_skipped")
        return
    deleted = 0
    for partition in partitions:
        keep = [ADMISSION_HEAD] if partition == database and head_present else []
        keys = partition_keys(guard, partition)
        if head_present:
            deleted += delete_keys(guard, partition, [key for key in keys if key not in keep])
        else:
            require(partition != database or ADMISSION_HEAD not in keys, "snapshot_resume_head_present")
            deleted += delete_without_head(guard, database, partition, keys)
        require(partition_keys(guard, partition) == keep, "snapshot_metadata_cleanup_incomplete")
    if head_present:
        deleted += delete_keys(guard, database, [ADMISSION_HEAD])
    summary(f"cleanup_rows={deleted}")
    for partition in partitions:
        require(not partition_keys(guard, partition), "snapshot_metadata_cleanup_incomplete")
    summary("Run-owned signed log, accepted-write facts, submissions, and snapshot metadata are empty.")


def cutoff_rows(guard, partition, expression, values):
    request = guard.query_request(guard.DEV_TABLE, partition)
    request["KeyConditionExpression"] += " AND " + expression
    request["ExpressionAttributeValues"].update({name: {"S": value} for name, value in values.items()})
    result, start = {}, None
    while True:
        if start is not None:
            if start.get("database_id", {}).get("S") != partition:
                raise OperationError("native cutoff database")
            request["ExclusiveStartKey"] = start
        reply = aws(guard, "query", request)
        for item in reply.get("Items", []):
            key = item["record_id"]["S"]
            if item["database_id"]["S"] != partition or key in result:
                raise OperationError("native cutoff database")
            result[key] = json.loads(item["json"]["S"])
            if len(result) > MAX_PROOF_ENTRIES:
                raise OperationError("native cutoff proof size")
        next_start = reply.get("LastEvaluatedKey")
        if not guard.continue_page(next_start, start):
            return result
        if len(result) >= MAX_PROOF_ENTRIES:
            raise OperationError("native cutoff proof size")
        start = next_start


def entry_key(delivery):
    return f"e#{delivery:020d}"


def entry_digest(database, entry):
    """Match the service's immutable accepted-entry fingerprint."""
    digest = hashlib.sha256(b"exemem:mutation-log:accepted-entry:v1\n")

    def number(value):
        digest.update(value.to_bytes(8, "big"))

    def sized(value):
        if isinstance(value, str):
            value = value.encode("utf-8")
        number(len(value))
        digest.update(value)

    if entry.get("incorporated_from") is not None:
        raise OperationError("native cutoff personal entry")
    sized(database)
    sized(entry["writer_key"])
    sized(entry["writer_device"])
    number(entry["writer_seq"])
    number(entry["authored_at_ms"])
    sized(entry["namespace"])
    digest.update(b"\0")
    payload = entry["payload"]
    sized(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    return digest.hexdigest()


def verify_facts(database, through, entries, facts):
    positions = {}
    for key, fact in facts.items():
        _, node, device, last_text = key.split("#")
        first = fact["first_writer_seq"]
        delivery = fact["first_delivery_seq"]
        digests = fact["digests"]
        last = first + len(digests) - 1
        if (
            not WRITER_LABEL.fullmatch(node)
            or not WRITER_LABEL.fullmatch(device)
            or last_text != f"{last:020d}"
            or first < 1
            or delivery < 1
            or not 1 <= len(digests) <= 90
        ):
            raise OperationError("native cutoff write fact")
        for index, digest in enumerate(digests):
            position = delivery + index
            if position > through:
                break
            if position in positions or not CUTOFF_DIGEST.fullmatch(digest):
                raise OperationError("native cutoff write fact")
            positions[position] = (node, device, first + index, digest)
    if set(positions) != set(range(1, through + 1)):
        raise OperationError("native cutoff incomplete write facts")
    for key, entry in entries.items():
        actual = (entry["writer_key"], entry["writer_device"], entry["writer_seq"], entry_digest(database, entry))
        if positions[int(key.removeprefix("e#"))] != actual:
            raise OperationError("native cutoff write fact differs")


def delete_cutoff_entries(guard, database, pending):
    for start in range(0, len(pending), guard.BATCH_LIMIT):
        chunk = pending[start : start + guard.BATCH_LIMIT]
        for _ in range(5):
            request = guard.delete_batch(guard.DEV_TABLE, database, chunk)
            reply = aws(guard, "batch-write-item", request)
            left = guard.unprocessed_ids(reply, guard.DEV_TABLE, database)
            require(len(left) == len(set(left)) and set(left) <= set(chunk), "native_cutoff_pending_keys")
            if not left:
                break
            chunk = left
            time.sleep(0.25)
        else:
            raise OperationError("native_cutoff_delete_incomplete")


def reclaimed_storage(database, *, through=SNAPSHOT_THROUGH):
    guard = load_guard()
    require_native_dispatch()
    require(database == gate_id(), "native_cutoff_scope")
    entries = cutoff_rows(
        guard, database, "record_id BETWEEN :start AND :end", {":start": entry_key(1), ":end": entry_key(through)}
    )
    facts = cutoff_rows(
        guard, guard.writes_partition(database), "begins_with(record_id, :prefix)", {":prefix": "write#"}
    )
    return entries, facts


def prune_verified_prefix(database, snapshot_id, through):
    """Prune only entries with exact facts under the current verified snapshot."""
    guard = load_guard()
    require_native_dispatch()
    require(
        database == gate_id() and CUTOFF_DIGEST.fullmatch(snapshot_id) and through == SNAPSHOT_THROUGH,
        "native_cutoff_scope",
    )
    verify_cloud_target(guard)

    heads = cutoff_rows(guard, database, "record_id = :key", {":key": "head"})
    head = heads.get("head", {})
    if (
        head.get("database_id") != database
        or head.get("kind") != "personal"
        or head.get("write_facts_from") != 1
        or head.get("trimmed_through") != 0
        or head.get("head", {}).get("delivery_seq") != through
    ):
        raise OperationError("native cutoff database head")

    manifest_key = "native-files#" + snapshot_id
    records = cutoff_rows(
        guard,
        guard.snap_partition(database),
        "record_id BETWEEN :start AND :end",
        {":start": "native-cut", ":end": manifest_key},
    )
    expected_cut = {"snapshot_id": snapshot_id, "through": str(through)}
    saved = records.get(manifest_key, {})
    manifest = saved.get("snapshot", {})
    if (
        records.get("native-cut") != expected_cut
        or records.get("native-files") != saved
        or saved.get("id") != snapshot_id
        or manifest.get("through") != str(through)
        or not manifest.get("files")
        or len(saved.get("versions", [])) != len(manifest["files"])
    ):
        raise OperationError("native cutoff manifest")

    entry_bounds = {":start": entry_key(1), ":end": entry_key(through)}
    entry_range = "record_id BETWEEN :start AND :end"
    entries = cutoff_rows(guard, database, entry_range, entry_bounds)
    if set(entries) != {entry_key(position) for position in range(1, through + 1)}:
        raise OperationError("native cutoff delivery key")
    facts = cutoff_rows(
        guard, guard.writes_partition(database), "begins_with(record_id, :prefix)", {":prefix": "write#"}
    )
    verify_facts(database, through, entries, facts)
    delete_cutoff_entries(guard, database, sorted(entries))
    if (
        cutoff_rows(guard, database, entry_range, entry_bounds)
        or cutoff_rows(
            guard, guard.writes_partition(database), "begins_with(record_id, :prefix)", {":prefix": "write#"}
        )
        != facts
    ):
        raise OperationError("native cutoff prefix was not pruned with facts intact")
