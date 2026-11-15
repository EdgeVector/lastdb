"""Seed, snapshot upload, signed tail, and source-free restore roundtrip gate."""

import hashlib
import json
import os
import re
import shutil
import time

from node import OwnedNode
from programs import execute
from snapshot_cleanup import current_attempt, register_and_track
from snapshot_gate_aws import gate_id, note, summary
from support import CREDENTIALS_FILE, OperationError, private_file, read_json, require, stable, write_private

RECORDS = 300_000
BODY_CHARS = 1024
BATCH = 100
QUERY_BATCH = 64
MAX_LOG_ENTRIES = 8
MAX_REPLAYED = 8
BUDGET_SECONDS = 60
SCHEMA = "SnapshotGate"
LABEL = b"snapshot-gate-v1"
TAIL_KEY = "after-snapshot"
TAIL_BODY = "signed tail"

SNAPSHOT_PROTOCOL_ERRORS = frozenset(
    b"""
    InvalidInput UnsupportedCapability Unauthorized NotFound GenerationConflict
    StreamAuthorityConflict JobClosed IdentityConflict PredecessorConflict ResourceLimit
    QuotaExceeded MissingDependency IntegrityMismatch RecoveryKeyMismatch SourceUnavailable
    ProviderPending OutcomeUnknown RetryLater
    """.split()
)


def opt_into_first_snapshot(home, database_id):
    """Ask the daemon to hold log pages until this database's first snapshot."""
    parent = home / "log-sync"
    parent.mkdir(mode=0o700)
    directory = parent / database_id
    directory.mkdir(mode=0o700)
    write_private(directory / "snapshot-first", b"snapshot-v1\n")


def cli(programs, home, arguments, timeout, input_bytes=None):
    command = [str(programs.directory / "lastdb"), "--data-dir", str(home), *arguments]
    started = time.monotonic()
    output = execute(command, programs.environment, input_bytes, timeout)
    return time.monotonic() - started, output


def prefixed(line, prefix):
    require(line.startswith(prefix), "snapshot_wait_output")
    return int(line[len(prefix) :])


def record_body(index):
    """1024 hex characters from SHA-256(label || index || block). Incompressible."""
    prefix = LABEL + index.to_bytes(8, "big")
    text = "".join(hashlib.sha256(prefix + bytes([block])).hexdigest() for block in range(16))
    require(len(text) == BODY_CHARS, "record_body_size")
    return text


def content_digest(count):
    hashed = hashlib.sha256()
    for index in range(count):
        hashed.update(record_body(index).encode("ascii"))
    return hashed.hexdigest()


def install_registry(home, database_id):
    folder = home / "databases"
    folder.mkdir(mode=0o700)
    owner = {"id": database_id, "name": "personal", "kind": "personal", "spec": None}
    write_private(folder / "registry.json", json.dumps({"format": 1, "owner": owner, "owned": []}).encode("ascii"))


def record_create(key, body):
    return {
        "type": "mutation", "schema": SCHEMA, "record": {"body": body},
        "key_value": {"hash": key, "range": None}, "alternates": {}, "mutation_type": "create",
        "source_file_name": None, "metadata": None, "file": None,
        "expected": {"type": "absent"}, "durability": "durable", "convergence": "sync",
    }


def install_schema(node, database_id):
    schema = {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}}, "fields": ["body"],
        "field_types": {"body": "String"}, "field_descriptions": {"body": "snapshot gate payload"},
        "field_versions": {"body": 1},
    }
    node.call(f"/api/databases/{database_id}/schemas/seed-system", {"isolated_copy": True, "schema": schema})


def write_records(node, database_id):
    route = f"/api/databases/{database_id}/mutations/batch"
    started = time.monotonic()
    request_seconds = 0.0
    written = hashlib.sha256()
    for start in range(0, RECORDS, BATCH):
        indexes = range(start, min(start + BATCH, RECORDS))
        bodies = [record_body(index) for index in indexes]
        for body in bodies:
            written.update(body.encode("ascii"))
        mutations = [record_create(f"{index:06d}", body) for index, body in zip(indexes, bodies, strict=True)]
        request_start = time.monotonic()
        reply = node.call(route, {"mutations": mutations})
        request_seconds += time.monotonic() - request_start
        require(
            isinstance(reply, dict) and reply.get("count") == len(bodies) and reply.get("durability") == "durable",
            "record_write",
        )
        written_count = start + len(bodies)
        if written_count % 50_000 == 0:
            print(
                f"seeded_records={written_count} elapsed_seconds={time.monotonic() - started:.3f} "
                f"write_request_seconds={request_seconds:.3f}",
                flush=True,
            )
    return written.hexdigest(), time.monotonic() - started, request_seconds


def run_file_count(home):
    return sum(1 for root in ("disk-run", "disk-cas") for _path in (home / "data" / root / "runs").glob("*.ldrun"))


def parse_wait(output):
    lines = output.splitlines()
    require(len(lines) == 3, "snapshot_wait_output")
    through = prefixed(lines[0], b"snapshot_through=")
    head = prefixed(lines[1], b"log_head=")
    entries = prefixed(lines[2], b"log_entries=")
    require(through == head, "snapshot_through_head")
    require(entries <= MAX_LOG_ENTRIES, "snapshot_log_entries")
    return through, entries


def parse_restore(output):
    match = re.search(rb"^snapshot_through=([0-9]+) replayed=([0-9]+)$", output, re.M)
    require(match is not None, "restore_line")
    replayed = int(match.group(2))
    require(replayed <= MAX_REPLAYED, "restore_replayed")
    return int(match.group(1)), replayed


def rows_from_batch(reply):
    require(isinstance(reply, dict) and isinstance(reply.get("results"), list), "record_reply")
    found = {}
    for item in reply["results"]:
        require(isinstance(item, dict) and item.get("status") == 200, "record_status")
        response = item.get("response")
        require(isinstance(response, dict), "record_reply")
        page = response.get("data", response)
        require(isinstance(page, dict) and isinstance(page.get("results"), list), "record_reply")
        for row in page["results"]:
            key = row.get("key", {}).get("hash")
            body = row.get("fields", {}).get("body")
            require(isinstance(key, str) and isinstance(body, str), "record_reply")
            found[key] = body
    return found


def verify_records(node, database_id, digest):
    route = f"/api/databases/{database_id}/queries/batch"
    checked = hashlib.sha256()
    count = 0
    for start in range(0, RECORDS, QUERY_BATCH):
        indexes = range(start, min(start + QUERY_BATCH, RECORDS))
        queries = [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": f"{index:06d}"}} for index in indexes]
        found = rows_from_batch(node.call(route, {"queries": queries}))
        for index in indexes:
            body = found.get(f"{index:06d}")
            require(body == record_body(index), "record_mismatch")
            checked.update(body.encode("ascii"))
            count += 1
    require(count == RECORDS and checked.hexdigest() == digest, "record_digest")
    return count


def write_record(node, database_id, key, body):
    reply = node.call(f"/api/databases/{database_id}/mutations/batch", {"mutations": [record_create(key, body)]})
    require(reply.get("count") == 1 and reply.get("durability") == "durable", "record_write")


def require_record(node, database_id, key, body):
    queries = [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": key}}]
    found = rows_from_batch(node.call(f"/api/databases/{database_id}/queries/batch", {"queries": queries}))
    require(found == {key: body}, "record_readback")


def wait_for_tail(home, database_id, through):
    path = home / "log-sync" / database_id / "prefix.json"
    deadline = time.monotonic() + BUDGET_SECONDS
    while time.monotonic() < deadline:
        if path.exists():
            prefix = read_json(path, 4096)
            require(prefix.get("database_id") == database_id, "signed_tail_database")
            if prefix.get("through") == through + 1:
                return
        time.sleep(0.25)
    raise OperationError("signed_tail_deadline")


def seed(programs, node, database_id, numbers):
    started = time.monotonic()
    listed = programs.command(node.home, ["db", "list", "--json"], timeout=30)
    rows = listed.get("databases") if isinstance(listed, dict) else None
    require(isinstance(rows, list), "database_list")
    matched = [row for row in rows if row.get("name") == "personal" and row.get("id") == database_id]
    require(len(matched) == 1 and matched[0].get("kind") == "personal", "database_registered")
    install_schema(node, database_id)
    digest, write_seconds, request_seconds = write_records(node, database_id)
    note(numbers, "seed_write_seconds", round(write_seconds, 3))
    note(numbers, "seed_write_request_seconds", round(request_seconds, 3))
    note(numbers, "seed_records_per_second", round(RECORDS / write_seconds, 1))
    require(digest == content_digest(RECORDS), "seed_digest")
    flush_started = time.monotonic()
    try:
        require(node.call("/api/db/flush", {}).get("durable") is True, "seed_flush")
    finally:
        note(numbers, "file_ready_seconds", round(time.monotonic() - flush_started, 3))
    note(numbers, "records", RECORDS)
    note(numbers, "digest", digest)
    note(numbers, "run_files_on_disk", run_file_count(node.home))
    note(numbers, "seed_seconds", round(time.monotonic() - started, 3))
    return digest


def snapshot_http_failure(home, database_id):
    """Return only an allowlisted diagnostic from the generated daemon's failure file."""
    if database_id != gate_id():
        return {}
    try:
        stream, before = private_file(home / "log-sync" / database_id / "snapshot-error")
        with stream:
            if before.st_size > 512:
                return {}
            line = stream.read(513).strip()
            if not stable(before, os.fstat(stream.fileno())):
                return {}
    except (OSError, OperationError):
        return {}
    direct = re.fullmatch(rb"snapshot file upload returned HTTP ([345][0-9]{2})(?: [A-Za-z -]{1,64})?", line)
    if direct:
        return {"http_status": int(direct[1]), "snapshot_source": "provider_upload"}
    signed = re.fullmatch(
        rb"signed snapshot request returned HTTP ([345][0-9]{2})"
        rb"(?: (unknown failure|[A-Za-z]{1,40})(?: \([A-Za-z0-9_.-]{1,96}\))?)?",
        line,
    )
    if not signed:
        return {}
    detail = {"http_status": int(signed[1]), "snapshot_source": "signed_snapshot"}
    if signed[2] in SNAPSHOT_PROTOCOL_ERRORS:
        detail["protocol_error"] = signed[2].decode("ascii")
    return detail


def upload(programs, node, database_id, numbers):
    try:
        elapsed, output = cli(
            programs, node.home, ["backup", "snapshot", "--database", database_id, "--wait"], BUDGET_SECONDS
        )
    except OperationError as error:
        if str(error) == "cli_deadline":
            raise OperationError("upload_budget") from None
        if str(error) == "product_cli_refused":
            diagnostic = snapshot_http_failure(node.home, database_id)
            if diagnostic:
                refusal = {**(error.cli_refusal or {}), **diagnostic}
                raise OperationError("product_cli_refused", cli_refusal=refusal) from None
        raise
    require(elapsed < BUDGET_SECONDS, "upload_budget")
    through, entries = parse_wait(output)
    note(numbers, "upload_seconds", round(elapsed, 3))
    note(numbers, "snapshot_through", through)
    note(numbers, "log_entries", entries)


def restore(programs, source, destination, database_id, phrase, numbers):
    try:
        elapsed, output = cli(
            programs,
            source,
            ["backup", "restore", "--database", database_id, "--recovery-phrase-stdin", "--into", str(destination)],
            BUDGET_SECONDS,
            phrase,
        )
    except OperationError as error:
        if str(error) == "cli_deadline":
            raise OperationError("restore_budget") from None
        raise
    require(elapsed < BUDGET_SECONDS, "restore_budget")
    through, replayed = parse_restore(output)
    require(through == numbers["snapshot_through"], "restore_through")
    require(replayed == 1, "restore_signed_tail")
    note(numbers, "restore_seconds", round(elapsed, 3))
    note(numbers, "replayed", replayed)


def check_restored(programs, destination, database_id, digest, numbers, account):
    started = time.monotonic()
    restored = OwnedNode(destination, programs, account=account)
    try:
        restored.start(sign_in=False)
        count = verify_records(restored, database_id, digest)
        require_record(restored, database_id, TAIL_KEY, TAIL_BODY)
        write_record(restored, database_id, "live", "live")
        require_record(restored, database_id, "live", "live")
        elapsed = time.monotonic() - started
        restored.stop()
    finally:
        restored.cleanup_stop()
    note(numbers, "verified_records", count)
    note(numbers, "verify_seconds", round(elapsed, 3))
    note(numbers, "live_write", "ok")


def run_gate(programs, root, guard, owner):
    numbers = {}
    attempt = current_attempt()
    database_id = attempt.database
    note(numbers, "database_id", database_id)
    home = root / "source"
    home.mkdir(mode=0o700)
    node = OwnedNode(home, programs, account=owner.account, isolated_copy=True, keep_saved_credentials=True)
    write_private(home / "identity.key", owner.key.private.private_bytes_raw())
    install_registry(home, database_id)
    opt_into_first_snapshot(home, database_id)
    destination = root / "restored"
    try:
        node.start(sign_in=False)
        digest = seed(programs, node, database_id, numbers)
        node.stop()
        connected = programs.command(
            home,
            ["backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
             "--expected-user-hash", owner.account, "--json"],
            input_bytes=owner.api_key.encode(),
            timeout=60,
        )
        require(
            connected.get("ok") is True
            and connected.get("credentials_saved") is True
            and connected.get("account_id") == owner.account,
            "dev_connection",
        )
        register_and_track(guard, attempt, owner)
        node.start(sign_in=False)
        upload(programs, node, database_id, numbers)
        tail_started = time.monotonic()
        write_record(node, database_id, TAIL_KEY, TAIL_BODY)
        wait_for_tail(home, database_id, numbers["snapshot_through"])
        note(numbers, "signed_tail_seconds", round(time.monotonic() - tail_started, 3))
        node.stop()
        connection = root / "restore-connection"
        connection.mkdir(mode=0o700)
        (home / CREDENTIALS_FILE).rename(connection / CREDENTIALS_FILE)
        shutil.rmtree(home)
        restore(programs, connection, destination, database_id, owner.phrase, numbers)
        check_restored(programs, destination, database_id, digest, numbers, owner.account)
    finally:
        node.cleanup_stop()
    summary("snapshot gate passed")
    return numbers
