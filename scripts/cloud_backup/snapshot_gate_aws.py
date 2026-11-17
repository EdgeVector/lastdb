"""AWS DEV guard, disposable provider cleanup, and DynamoDB metadata checks."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error

_SCRIPTS = Path(__file__).resolve().parents[1]
for _p in (str(_SCRIPTS / "mutation_log_load"), str(_SCRIPTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from harness import summary_line  # noqa: E402
from support import DECIMAL, OperationError, dev_endpoint, load_dev_owner, require  # noqa: E402

ROOT = "/api/cloud/backups/v2/mutation-log"
SNAPSHOT_DEV_ACCOUNT = "f0fa35067ce1bb7cda7810ca96a100df"
GRANT_TTL_MS = 900_000
CLEANUP_BUDGET = 1_200
LEGACY_DATABASE_VERSION = 1
DISPOSABLE_DATABASE_VERSION = 2
DATABASE_PREFIXES = {
    LEGACY_DATABASE_VERSION: "c1da11d0-0000-4000-8000-",
    DISPOSABLE_DATABASE_VERSION: "c1da11d1-0000-4000-8000-",
}
ADMISSION_HEAD = "head"
SNAPSHOT_THROUGH = 12
CUTOFF_DIGEST = re.compile(r"[0-9a-f]{64}")
WRITER_LABEL = re.compile(r"[A-Za-z0-9_-]{1,64}")


@dataclass(frozen=True, repr=False)
class SnapshotOwner:
    key: object
    generation: str
    phrase: bytes
    api_key: str

    @property
    def account(self):
        from run import account_id
        return account_id(self.key.public)


@dataclass(frozen=True)
class RunAttempt:
    run_id: str
    attempt: str
    database_version: int = LEGACY_DATABASE_VERSION

    def __post_init__(self):
        require(re.fullmatch(r"[1-9][0-9]{0,18}", self.run_id)
                and re.fullmatch(r"[1-9][0-9]{0,4}", self.attempt), "snapshot_run_attempt")

    @property
    def database(self):
        return self.database_for(self.database_version)

    def database_for(self, version):
        material = f"{self.run_id}:{self.attempt}".encode("ascii")
        return DATABASE_PREFIXES[version] + hashlib.sha256(material).hexdigest()[:12]

    def identity(self):
        return {"run_id": self.run_id, "run_attempt": self.attempt}

    def fields(self):
        return {"database_id": self.database, **self.identity()}


def current_attempt():
    return RunAttempt(os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
                      DISPOSABLE_DATABASE_VERSION)


def load_owner():
    key, api_key, generation, phrase = load_dev_owner(
        SNAPSHOT_DEV_ACCOUNT, pop_env=True, exact_baseline_keys=True
    )
    return SnapshotOwner(key, generation, phrase, api_key)


def start_tracking(call, attempt):
    result = call("snapshot/bytes", {**attempt.fields(), "action": "track_dev"})
    require(result == {"database_id": attempt.database, "tracking": True}, "snapshot_tracking")


def register_and_track(attempt, owner):
    from run import post

    database = attempt.database
    endpoint = dev_endpoint()

    def call(operation, fields):
        body = json.dumps({**fields, "database_id": database}, separators=(",", ":")).encode("utf-8")
        try:
            status, result, _ = post(endpoint, owner.key, database, "POST", f"{ROOT}/{operation}",
                                     body, owner.api_key, owner.generation)
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException):
            raise OperationError("snapshot_tracking_transport") from None
        require(200 <= status < 300 and isinstance(result, dict), "snapshot_tracking_service")
        return result

    registration = call("create-database", {"kind": "personal"})
    require(registration.get("database_id") == database
            and registration.get("state") == "native", "snapshot_tracking_registration")
    start_tracking(call, attempt)


def released_bytes(value):
    require(isinstance(value, str) and DECIMAL.fullmatch(value)
            and int(value) <= 2**64 - 1, "snapshot_cleanup_released_bytes")
    return int(value)


def cleanup_objects(guard, attempt, owner, summary_fn, *, expect_charge, receipt_only=False):
    from run import post

    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    database = guard.allow_stream(attempt.database)
    endpoint = dev_endpoint()
    action = "cleanup_receipt_dev" if receipt_only else "cleanup_dev"
    body = json.dumps({**attempt.fields(), "action": action}, separators=(",", ":")).encode("utf-8")
    deleted = 0
    deadline = time.monotonic() + CLEANUP_BUDGET
    for _ in range(128):
        if time.monotonic() >= deadline:
            break
        try:
            status, result, _ = post(endpoint, owner.key, database, "POST", f"{ROOT}/snapshot/bytes",
                                     body, owner.api_key, owner.generation)
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException):
            time.sleep(2)
            continue
        if status in (429, 502, 503, 504):
            time.sleep(2)
            continue
        if receipt_only and status == 404 and result == {"code": "not_found", "field": "snapshot.cleanup.receipt"}:
            return False
        require(status == 200 and isinstance(result, dict) and set(result) ==
                {"database_id", "deleted", "complete", "retry_after_ms", "released_bytes"}
                and result.get("database_id") == database, "snapshot_cleanup_service")
        count, complete = result.get("deleted"), result.get("complete")
        wait_ms = result.get("retry_after_ms")
        released = released_bytes(result.get("released_bytes"))
        require(type(count) is int and 0 <= count <= 64 and type(complete) is bool
                and type(wait_ms) is int and 0 <= wait_ms <= GRANT_TTL_MS
                and (not complete or count == wait_ms == 0)
                and (complete or released == 0)
                and (wait_ms == 0 or count == 0)
                and (not receipt_only or complete), "snapshot_cleanup_reply")
        deleted += count
        if complete:
            require(not expect_charge or released > 0, "snapshot_cleanup_missing_charge")
            if receipt_only:
                summary_fn("provider_cleanup_receipt=verified")
            summary_fn(f"provider_objects=empty confirmed_deleted={deleted} released_bytes={released}")
            return True
        if wait_ms:
            time.sleep(min(wait_ms / 1000, max(0, deadline - time.monotonic())))
    raise OperationError("snapshot_cleanup_incomplete_metadata_retained")


def summary(line):
    summary_line(line, echo=True)


def note(numbers, key, value):
    numbers[key] = value
    summary(f"{key}={value}")


def gate_id():
    return current_attempt().database


def require_native_dispatch():
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "native_cutoff_manual_main_only")


def aws(guard, operation, payload):
    try:
        return guard.aws_dynamodb(operation, payload)
    except guard.GuardError as error:
        raise OperationError("cleanup_aws") from error


def partition_keys(guard, partition):
    try:
        return list(guard.iter_partition_keys(guard.DEV_TABLE, partition, call=lambda op, req: aws(guard, op, req)))
    except guard.GuardError as error:
        raise OperationError("snapshot_metadata_page" if str(error) == "page" else str(error)) from error


def delete_keys(guard, partition, pending):
    try:
        return guard.batch_delete_keys(guard.DEV_TABLE, partition, pending, call=lambda op, req: aws(guard, op, req))
    except guard.GuardError as error:
        raise OperationError("cleanup_aws") from error


def delete_without_head(guard, database, partition, keys):
    table = guard.DEV_TABLE
    head = guard.delete_batch(table, database, [ADMISSION_HEAD])["RequestItems"][table][0]
    absent = {"Delete": {"TableName": table, "Key": head["DeleteRequest"]["Key"],
                         "ConditionExpression": "attribute_not_exists(record_id)"}}
    for offset in range(0, len(keys), 8):
        batch = guard.delete_batch(table, partition, keys[offset:offset + 8])
        deletes = [{"Delete": {"TableName": table, "Key": item["DeleteRequest"]["Key"]}}
                   for item in batch["RequestItems"][table]]
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
        require(not expect_charge and all(not partition_keys(guard, part) for part in partitions),
                "snapshot_cleanup_missing_head")
        summary("provider_objects=not_registered")
        return
    receipt = cleanup_objects(guard, attempt, owner, summary, expect_charge=expect_charge, receipt_only=not head_present)
    if not receipt:
        require(proof_skipped and all(not partition_keys(guard, part) for part in partitions),
                "snapshot_resume_no_verified_receipt")
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
        first, delivery, digests = fact["first_writer_seq"], fact["first_delivery_seq"], fact["digests"]
        last = first + len(digests) - 1
        if (not WRITER_LABEL.fullmatch(node) or not WRITER_LABEL.fullmatch(device)
                or last_text != f"{last:020d}" or first < 1 or delivery < 1 or not 1 <= len(digests) <= 90):
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
