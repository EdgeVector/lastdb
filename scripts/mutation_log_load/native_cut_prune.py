"""Remove a verified, immutable-fact-covered prefix of one run-owned DEV log."""

import base64
import hashlib
import json
import os
import random
import re
import time

from guard import (
    BATCH_LIMIT, GuardError, continue_page, database_id_for_run, delete_batch,
    query_request, snap_partition, unprocessed_ids, writes_partition,
)

MAX_PROOF_ENTRIES = 256
DIGEST = re.compile(r"[0-9a-f]{64}")
LABEL = re.compile(r"[A-Za-z0-9_-]{1,64}")


def rows(table, partition, expression, values, ledger, aws):
    request = query_request(table, partition)
    request["KeyConditionExpression"] += " AND " + expression
    request["ExpressionAttributeValues"].update(
        {name: {"S": value} for name, value in values.items()})
    result = {}
    start = None
    while True:
        if start is not None:
            if start.get("database_id", {}).get("S") != partition:
                raise GuardError("native cutoff database")
            request["ExclusiveStartKey"] = start
        reply = aws("dynamodb", "query", request)
        ledger.record("read")
        for item in reply.get("Items", []):
            key = item["record_id"]["S"]
            if item["database_id"]["S"] != partition or key in result:
                raise GuardError("native cutoff database")
            result[key] = json.loads(item["json"]["S"])
            if len(result) > MAX_PROOF_ENTRIES:
                raise GuardError("native cutoff proof size")
        next_start = reply.get("LastEvaluatedKey")
        if not continue_page(next_start, start):
            return result
        if len(result) >= MAX_PROOF_ENTRIES:
            raise GuardError("native cutoff proof size")
        start = next_start


def entry_key(delivery):
    return "e#{:020d}".format(delivery)


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
        raise GuardError("native cutoff personal entry")
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
        if (not LABEL.fullmatch(node) or not LABEL.fullmatch(device)
                or last_text != "{:020d}".format(last) or first < 1 or delivery < 1
                or not 1 <= len(digests) <= 90):
            raise GuardError("native cutoff write fact")
        for index, digest in enumerate(digests):
            position = delivery + index
            if position > through:
                break
            if position in positions or not DIGEST.fullmatch(digest):
                raise GuardError("native cutoff write fact")
            positions[position] = (node, device, first + index, digest)
    if set(positions) != set(range(1, through + 1)):
        raise GuardError("native cutoff incomplete write facts")
    for key, entry in entries.items():
        actual = (entry["writer_key"], entry["writer_device"],
                  entry["writer_seq"], entry_digest(database, entry))
        if positions[int(key.removeprefix("e#"))] != actual:
            raise GuardError("native cutoff write fact differs")


def delete_entries(table, database, pending, ledger, aws, batch_retry):
    misses = 0
    while pending:
        chunk = pending[:BATCH_LIMIT]
        reply = aws("dynamodb", "batch-write-item", delete_batch(table, database, chunk))
        ledger.record("write", 64 * len(chunk))
        left = unprocessed_ids(reply, table, database)
        if len(left) != len(set(left)) or not set(left) <= set(chunk):
            raise GuardError("native cutoff pending keys")
        if set(left) == set(chunk):
            misses = batch_retry(misses, time.sleep, random.randrange)
        else:
            misses = 0
        pending = left + pending[len(chunk):]


def prune_verified_prefix(table, database, snapshot_id, through, ledger):
    """Prune only entries with exact facts under the current verified snapshot."""
    # Import locally: the existing runner supplies AWS limits and calls this from
    # its authorized DEV operation. Direct invocation goes through the same guard.
    from run import aws, batch_retry, target

    if (database != database_id_for_run(os.environ["GITHUB_RUN_ID"])
            or not DIGEST.fullmatch(snapshot_id)
            or not isinstance(through, int) or isinstance(through, bool)
            or not 1 <= through <= MAX_PROOF_ENTRIES):
        raise GuardError("native cutoff proof scope")
    _, target_table, target_database = target()
    if (table, database) != (target_table, target_database):
        raise GuardError("native cutoff proof scope")

    heads = rows(table, database, "record_id = :key", {":key": "head"}, ledger, aws)
    head = heads.get("head", {})
    if (head.get("database_id") != database or head.get("kind") != "personal"
            or head.get("write_facts_from") != 1 or head.get("trimmed_through") != 0
            or head["head"]["delivery_seq"] != through):
        raise GuardError("native cutoff database head")

    manifest_key = "native-files#" + snapshot_id
    records = rows(table, snap_partition(database), "record_id BETWEEN :start AND :end",
                   {":start": "native-cut", ":end": manifest_key}, ledger, aws)
    expected_cut = {"snapshot_id": snapshot_id, "through": str(through)}
    saved = records.get(manifest_key, {})
    manifest = saved.get("snapshot", {})
    if (records.get("native-cut") != expected_cut
            or records.get("native-files") != saved or saved.get("id") != snapshot_id
            or manifest.get("through") != str(through) or not manifest.get("files")
            or len(saved.get("versions", [])) != len(manifest["files"])):
        raise GuardError("native cutoff manifest")

    entry_bounds = {":start": entry_key(1), ":end": entry_key(through)}
    entry_range = "record_id BETWEEN :start AND :end"
    entries = rows(table, database, entry_range, entry_bounds, ledger, aws)
    if any(key != entry_key(int(key.removeprefix("e#"))) for key in entries):
        raise GuardError("native cutoff delivery key")
    facts = rows(table, writes_partition(database), "begins_with(record_id, :prefix)",
                 {":prefix": "write#"}, ledger, aws)
    verify_facts(database, through, entries, facts)
    delete_entries(table, database, sorted(entries), ledger, aws, batch_retry)
    if (rows(table, database, entry_range, entry_bounds, ledger, aws)
            or rows(table, writes_partition(database), "begins_with(record_id, :prefix)",
                    {":prefix": "write#"}, ledger, aws) != facts):
        raise GuardError("native cutoff prefix was not pruned with facts intact")
