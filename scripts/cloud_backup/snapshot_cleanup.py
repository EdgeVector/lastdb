"""Remove only the stopped snapshot gate run's disposable DEV provider objects.

No cache is shared across these unique database IDs. Keep metadata and owner
admission until the service confirms both provider prefixes are empty.
"""

from dataclasses import dataclass
import http.client
import json
import os
from pathlib import Path
import re
import time
import urllib.error

import baseline_data
from support import OperationError, dev_endpoint, read_json, require, write_private


ROOT = "/api/cloud/backups/v2/mutation-log"
# Public hash for the dedicated DEV snapshot signer.
SNAPSHOT_DEV_ACCOUNT = "f0fa35067ce1bb7cda7810ca96a100df"
GRANT_TTL_MS = 900_000
CLEANUP_BUDGET = 1_200
DECIMAL = re.compile(r"0|[1-9][0-9]{0,19}")


@dataclass(frozen=True, repr=False)
class SnapshotOwner:
    key: object
    generation: str
    phrase: bytes
    api_key: str


def run_fields(database):
    return {"database_id": database, "run_id": os.environ["GITHUB_RUN_ID"],
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1")}


def load_owner():
    from node_auth import NodeKey, account_id

    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
    raw = os.environ.pop("LASTDB_CLOUD_DEV_BASELINE", "")
    require(0 < len(api_key.encode()) <= 4096, "dev_api_key")
    require(0 < len(raw) <= 65_536, "snapshot_baseline")
    try:
        baseline = json.loads(raw)
    except json.JSONDecodeError:
        raise OperationError("snapshot_baseline") from None
    require(isinstance(baseline, dict) and set(baseline) == {"account", "account_generation"},
            "snapshot_baseline")
    generation = baseline.get("account_generation")
    try:
        key = NodeKey.from_seed(baseline_data.seed(phrase))
    except baseline_data.BaselineDataError:
        raise OperationError("snapshot_phrase") from None
    require(baseline.get("account") == account_id(key.public) == SNAPSHOT_DEV_ACCOUNT
            and isinstance(generation, str) and DECIMAL.fullmatch(generation)
            and int(generation) <= 2**64 - 1, "snapshot_baseline_scope")
    return SnapshotOwner(key, generation, phrase, api_key)


def start_tracking(call, database):
    result = call("snapshot/bytes", {**run_fields(database), "action": "track_dev"})
    require(result == {"database_id": database, "tracking": True}, "snapshot_tracking")


def register_and_track(guard, database, owner):
    from run import post

    guard.allow_stream(database)
    endpoint = dev_endpoint()

    def call(operation, fields):
        body = json.dumps({**fields, "database_id": database},
                          separators=(",", ":")).encode("utf-8")
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
    start_tracking(call, database)


def released_bytes(value):
    require(isinstance(value, str) and DECIMAL.fullmatch(value)
            and int(value) <= 2**64 - 1, "snapshot_cleanup_released_bytes")
    return int(value)


def cleanup_objects(guard, database, owner, aws, summary):
    # Imports use snapshot_gate.load_guard's mutation_log_load path.
    from run import post

    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    guard.allow_stream(database)
    endpoint = dev_endpoint()
    expect_charge = os.environ.get("REQUIRE_RELEASED_SNAPSHOT_BYTES") == "true"
    receipt = Path(os.environ["RUNNER_TEMP"]) / f"snapshot-provider-cleanup-{database}.json"
    expected = {**run_fields(database), "complete": True, "account": SNAPSHOT_DEV_ACCOUNT,
                "account_generation": owner.generation}
    # A metadata purge can fail after deleting admission rows. A private,
    # exact-run absence receipt permits retry without reusing deleted admission.
    if os.path.lexists(receipt):
        saved = read_json(receipt, 1024)
        require(isinstance(saved, dict) and {**saved, "released_bytes": None}
                == {**expected, "released_bytes": None}, "snapshot_cleanup_receipt")
        released = released_bytes(saved.get("released_bytes"))
        require(not expect_charge or released > 0, "snapshot_cleanup_missing_charge")
        summary(f"provider_objects=empty released_bytes={released} prior_run_local_confirmation")
        return
    # Before registration there can be no upload grants. Once registered, all
    # admission metadata survives provider cleanup, even on partial failure.
    found = aws(guard, "query", guard.query_request(guard.DEV_TABLE, database))
    if not found.get("Items") and not found.get("LastEvaluatedKey"):
        require(not expect_charge, "snapshot_cleanup_missing_metadata")
        summary("provider_objects=not_registered_or_already_cleaned")
        return
    body = json.dumps({**run_fields(database), "action": "cleanup_dev"},
                      separators=(",", ":")).encode("utf-8")
    deleted = 0
    deadline = time.monotonic() + CLEANUP_BUDGET
    # Bounded retries include unknown outcomes. Keep the database's admission
    # metadata until its retirement fence has drained and both providers agree.
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
                and (wait_ms == 0 or count == 0), "snapshot_cleanup_reply")
        deleted += count
        if complete:
            require(not expect_charge or released > 0, "snapshot_cleanup_missing_charge")
            write_private(receipt, json.dumps({**expected,
                                               "released_bytes": str(released)}).encode("ascii"))
            summary(f"provider_objects=empty confirmed_deleted={deleted} released_bytes={released}")
            return
        if wait_ms:
            time.sleep(min(wait_ms / 1000, max(0, deadline - time.monotonic())))
    raise OperationError("snapshot_cleanup_incomplete_metadata_retained")
