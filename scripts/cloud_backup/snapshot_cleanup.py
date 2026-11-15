"""Remove only the stopped snapshot gate run's disposable DEV provider objects.

No cache is shared across these unique database IDs. Keep metadata and owner
admission until the service confirms both provider prefixes are empty.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import http.client
import json
import os
import re
import time
import urllib.error

from support import BaselineDataError, OperationError, dev_endpoint, require, seed


ROOT = "/api/cloud/backups/v2/mutation-log"
# Public hash for the dedicated DEV snapshot signer.
SNAPSHOT_DEV_ACCOUNT = "f0fa35067ce1bb7cda7810ca96a100df"
GRANT_TTL_MS = 900_000
CLEANUP_BUDGET = 1_200
DECIMAL = re.compile(r"0|[1-9][0-9]{0,19}")
DATABASE_PREFIX = "c1da11d0-0000-4000-8000-"


@dataclass(frozen=True, repr=False)
class SnapshotOwner:
    key: object
    generation: str
    phrase: bytes
    api_key: str

    @property
    def account(self):
        from node_auth import account_id

        return account_id(self.key.public)


@dataclass(frozen=True)
class RunAttempt:
    run_id: str
    attempt: str

    def __post_init__(self):
        require(re.fullmatch(r"[1-9][0-9]{0,18}", self.run_id)
                and re.fullmatch(r"[1-9][0-9]{0,4}", self.attempt), "snapshot_run_attempt")

    @property
    def database(self):
        material = f"{self.run_id}:{self.attempt}".encode("ascii")
        return DATABASE_PREFIX + hashlib.sha256(material).hexdigest()[:12]

    def fields(self):
        return {"database_id": self.database, "run_id": self.run_id, "run_attempt": self.attempt}


def current_attempt():
    return RunAttempt(os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_RUN_ATTEMPT", "1"))


def load_owner():
    from node_auth import NodeKey, account_id

    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
    raw = os.environ.pop("LASTDB_CLOUD_DEV_BASELINE", "")
    require(len(api_key.encode(errors="surrogatepass")) <= 4096, "dev_api_key")
    api_key = api_key.strip()
    # Use the same normalized value for CLI stdin and the signed HTTP header.
    require(api_key and api_key.isascii() and api_key.isprintable(), "dev_api_key")
    require(0 < len(raw) <= 65_536, "snapshot_baseline")
    try:
        baseline = json.loads(raw)
    except json.JSONDecodeError:
        raise OperationError("snapshot_baseline") from None
    require(isinstance(baseline, dict) and set(baseline) == {"account", "account_generation"},
            "snapshot_baseline")
    generation = baseline.get("account_generation")
    try:
        key = NodeKey.from_seed(seed(phrase))
    except BaselineDataError:
        raise OperationError("snapshot_phrase") from None
    require(baseline.get("account") == account_id(key.public) == SNAPSHOT_DEV_ACCOUNT
            and isinstance(generation, str) and DECIMAL.fullmatch(generation)
            and int(generation) <= 2**64 - 1, "snapshot_baseline_scope")
    return SnapshotOwner(key, generation, phrase, api_key)


def start_tracking(call, attempt):
    result = call("snapshot/bytes", {**attempt.fields(), "action": "track_dev"})
    require(result == {"database_id": attempt.database, "tracking": True}, "snapshot_tracking")


def register_and_track(guard, attempt, owner):
    from run import post

    database = guard.allow_stream(attempt.database)
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
    start_tracking(call, attempt)


def released_bytes(value):
    require(isinstance(value, str) and DECIMAL.fullmatch(value)
            and int(value) <= 2**64 - 1, "snapshot_cleanup_released_bytes")
    return int(value)


def cleanup_objects(guard, attempt, owner, summary, *, expect_charge, receipt_only=False):
    # Imports use snapshot_gate.load_guard's mutation_log_load path.
    from run import post

    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    database = guard.allow_stream(attempt.database)
    endpoint = dev_endpoint()
    action = "cleanup_receipt_dev" if receipt_only else "cleanup_dev"
    body = json.dumps({**attempt.fields(), "action": action},
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
        if receipt_only and status == 404 and result == {"code": "not_found",
                                                        "field": "snapshot.cleanup.receipt"}:
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
                summary("provider_cleanup_receipt=verified")
            summary(f"provider_objects=empty confirmed_deleted={deleted} released_bytes={released}")
            return True
        if wait_ms:
            time.sleep(min(wait_ms / 1000, max(0, deadline - time.monotonic())))
    raise OperationError("snapshot_cleanup_incomplete_metadata_retained")
