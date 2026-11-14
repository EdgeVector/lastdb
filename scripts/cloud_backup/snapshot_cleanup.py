"""Remove only the stopped snapshot gate run's disposable DEV provider objects.

No cache is shared across these unique database IDs. Keep metadata and owner
admission until the service confirms both provider prefixes are empty.
"""

import json
import os
from pathlib import Path
import time
import urllib.error
import http.client

from support import OperationError, dev_endpoint, read_json, require, write_private


ROOT = "/api/cloud/backups/v2/mutation-log/snapshot/bytes"


def cleanup_objects(guard, database, aws, summary):
    # Imports use snapshot_gate.load_guard's mutation_log_load path.
    from run import load_ci_key, post

    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    guard.allow_stream(database)
    endpoint = dev_endpoint()
    receipt = Path(os.environ["RUNNER_TEMP"]) / f"snapshot-provider-cleanup-{database}.json"
    expected = {"database_id": database, "run_id": os.environ["GITHUB_RUN_ID"],
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1"), "complete": True}
    # A metadata purge can fail after deleting admission rows. A private,
    # exact-run absence receipt permits retry without reusing deleted admission.
    if os.path.lexists(receipt):
        require(read_json(receipt, 1024) == expected, "snapshot_cleanup_receipt")
        summary("provider_objects=empty prior_run_local_confirmation")
        return
    # Before registration there can be no upload grants. Once registered, all
    # admission metadata survives provider cleanup, even on partial failure.
    found = aws(guard, "query", guard.query_request(guard.DEV_TABLE, database))
    if not found.get("Items") and not found.get("LastEvaluatedKey"):
        summary("provider_objects=not_created_or_already_cleaned")
        return
    owner, api_key, generation = load_ci_key()
    body = json.dumps({"action": "cleanup_dev", "database_id": database,
                       "run_id": os.environ["GITHUB_RUN_ID"],
                       "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "1")},
                      separators=(",", ":")).encode("utf-8")
    deleted = 0
    # Bounded retries include unknown outcomes; every request re-lists only
    # this run. Do not remove metadata on an older service or provider error.
    for _ in range(128):
        try:
            status, result, _ = post(endpoint, owner, database, "POST", ROOT,
                                     body, api_key, generation)
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException):
            time.sleep(2)
            continue
        if status in (429, 502, 503, 504):
            time.sleep(2)
            continue
        require(status == 200 and isinstance(result, dict)
                and result.get("database_id") == database, "snapshot_cleanup_service")
        count, complete = result.get("deleted"), result.get("complete")
        require(type(count) is int and 0 <= count <= 64 and type(complete) is bool
                and (not complete or count == 0), "snapshot_cleanup_reply")
        deleted += count
        if complete:
            write_private(receipt, json.dumps(expected).encode("ascii"))
            summary(f"provider_objects=empty confirmed_deleted={deleted}")
            return
    raise OperationError("snapshot_cleanup_incomplete_metadata_retained")
