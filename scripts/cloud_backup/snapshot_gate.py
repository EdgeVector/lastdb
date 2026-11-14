#!/usr/bin/env python3
"""DEV round trip for one owner signed-database snapshot.

Trusted GitHub Actions on main runs this. Pull requests do not. The records
are generated from a public label, and the script never prints an API key,
recovery phrase, or invite code.
"""

import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

import baseline_data
from node import OwnedNode
from native_records import create, key as record_key, replace
from programs import Programs, execute
from signed_files import retry, running
from support import (CREDENTIALS_FILE, OperationError, child_environment, dev_endpoint, failure,
                     read_json, require, write_private)


RECORDS = 300_000
BODY_CHARS = 1024
BATCH = 100
QUERY_BATCH = 64
MAX_LOG_ENTRIES = 8
MAX_REPLAYED = 8
BUDGET_SECONDS = 60
SCHEMA = "SnapshotGate"
LABEL = b"snapshot-gate-v1"
LOAD_PREFIX = "c1da11d0-0000-4000-8000-"
TAIL_KEY = "after-snapshot"
TAIL_BODY = "signed tail"


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


def summary(line):
    print(line, flush=True)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def note(numbers, key, value):
    numbers[key] = value
    summary(f"{key}={value}")


def gate_id():
    """Use a run-owned ID in the DEV prefix the cleanup role can delete."""
    material = os.environ["GITHUB_RUN_ID"] + ":" + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    require(re.fullmatch(r"[1-9][0-9]{0,18}:[1-9][0-9]{0,4}", material), "run_id")
    return LOAD_PREFIX + hashlib.sha256(material.encode("ascii")).hexdigest()[:12]


def install_registry(home, database_id):
    folder = home / "databases"
    folder.mkdir(mode=0o700)
    write_private(folder / "registry.json", json.dumps({
        "format": 1,
        "owner": {"id": database_id, "name": "personal", "kind": "personal", "spec": None},
        "owned": [],
    }).encode("ascii"))


def mutation(index, body):
    return {
        "type": "mutation", "schema": SCHEMA, "record": {"body": body},
        "key_value": {"hash": f"{index:06d}", "range": None}, "alternates": {},
        "mutation_type": "create", "source_file_name": None, "metadata": None, "file": None,
        "expected": {"type": "absent"}, "durability": "durable", "convergence": "sync",
    }


def install_schema(node, database_id):
    node.call(f"/api/databases/{database_id}/schemas/seed-system", {
        "isolated_copy": True,
        "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "Hash",
            "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": ["body"], "field_types": {"body": "String"},
            "field_descriptions": {"body": "snapshot gate payload"},
            "field_versions": {"body": 1},
        },
    })


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
        mutations = [mutation(index, body) for index, body in zip(indexes, bodies, strict=True)]
        request_start = time.monotonic()
        reply = node.call(route, {"mutations": mutations})
        request_seconds += time.monotonic() - request_start
        require(isinstance(reply, dict) and reply.get("count") == len(bodies)
                and reply.get("durability") == "durable", "record_write")
        written_count = start + len(bodies)
        if written_count % 50_000 == 0:
            print(f"seeded_records={written_count} elapsed_seconds={time.monotonic() - started:.3f} "
                  f"write_request_seconds={request_seconds:.3f}", flush=True)
    return written.hexdigest(), time.monotonic() - started, request_seconds


def run_file_count(home):
    """Count files on disk for diagnostics, including runs awaiting deletion."""
    return sum(1 for root in ("disk-run", "disk-cas")
               for _path in (home / "data" / root / "runs").glob("*.ldrun"))


def cli(programs, home, arguments, timeout, input_bytes=None):
    command = [str(programs.directory / "lastdb"), "--data-dir", str(home), *arguments]
    started = time.monotonic()
    output = execute(command, programs.environment, input_bytes, timeout)
    return time.monotonic() - started, output


def parse_wait(output):
    lines = output.splitlines()
    require(len(lines) == 3, "snapshot_wait_output")
    through = prefixed(lines[0], b"snapshot_through=")
    head = prefixed(lines[1], b"log_head=")
    entries = prefixed(lines[2], b"log_entries=")
    require(through == head, "snapshot_through_head")
    require(entries <= MAX_LOG_ENTRIES, "snapshot_log_entries")
    return through, entries


def prefixed(line, prefix):
    require(line.startswith(prefix), "snapshot_wait_output")
    return int(line[len(prefix):])


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
        queries = [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": f"{index:06d}"}}
                   for index in indexes]
        found = rows_from_batch(node.call(route, {"queries": queries}))
        for index in indexes:
            body = found.get(f"{index:06d}")
            require(body == record_body(index), "record_mismatch")
            checked.update(body.encode("ascii"))
            count += 1
    require(count == RECORDS and checked.hexdigest() == digest, "record_digest")
    return count


def write_record(node, database_id, key, body):
    route = f"/api/databases/{database_id}/mutations/batch"
    reply = node.call(route, {"mutations": [{
        "type": "mutation", "schema": SCHEMA, "record": {"body": body},
        "key_value": {"hash": key, "range": None}, "alternates": {},
        "mutation_type": "create", "source_file_name": None, "metadata": None, "file": None,
        "expected": {"type": "absent"}, "durability": "durable", "convergence": "sync",
    }]})
    require(reply.get("count") == 1 and reply.get("durability") == "durable", "record_write")


def require_record(node, database_id, key, body):
    found = rows_from_batch(node.call(
        f"/api/databases/{database_id}/queries/batch",
        {"queries": [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": key}}]},
    ))
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


def upload(programs, node, database_id, numbers):
    try:
        elapsed, output = cli(programs, node.home, [
            "backup", "snapshot", "--database", database_id, "--wait",
        ], BUDGET_SECONDS)
    except OperationError as error:
        if str(error) == "cli_deadline":
            raise OperationError("upload_budget") from None
        raise
    require(elapsed < BUDGET_SECONDS, "upload_budget")
    through, entries = parse_wait(output)
    note(numbers, "upload_seconds", round(elapsed, 3))
    note(numbers, "snapshot_through", through)
    note(numbers, "log_entries", entries)


def restore(programs, source, destination, database_id, phrase, numbers):
    try:
        elapsed, output = cli(programs, source, [
            "backup", "restore", "--database", database_id, "--recovery-phrase-stdin",
            "--into", str(destination),
        ], BUDGET_SECONDS, phrase)
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


def check_restored(programs, destination, database_id, digest, numbers):
    started = time.monotonic()
    restored = OwnedNode(destination, programs, account=baseline_data.ACCOUNT)
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


def run_gate(programs, root, phrase, api_key):
    numbers = {}
    database_id = gate_id()
    note(numbers, "database_id", database_id)
    home = root / "source"
    home.mkdir(mode=0o700)
    node = OwnedNode(home, programs, account=baseline_data.ACCOUNT, isolated_copy=True,
                     keep_saved_credentials=True)
    write_private(home / "identity.key", baseline_data.seed(phrase))
    install_registry(home, database_id)
    opt_into_first_snapshot(home, database_id)
    destination = root / "restored"
    try:
        # Seed an existing local database before its first cloud connection.
        # Booting offline avoids building a local queue for the seed records.
        node.start(sign_in=False)
        digest = seed(programs, node, database_id, numbers)
        node.stop()
        connected = programs.command(home, [
            "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
            "--expected-user-hash", baseline_data.ACCOUNT, "--json",
        ], input_bytes=api_key, timeout=60)
        require(connected.get("ok") is True and connected.get("credentials_saved") is True
                and connected.get("account_id") == baseline_data.ACCOUNT, "dev_connection")
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
        restore(programs, connection, destination, database_id, phrase, numbers)
        check_restored(programs, destination, database_id, digest, numbers)
    finally:
        node.cleanup_stop()
    summary("snapshot gate passed")
    return numbers


def load_guard():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "mutation_log_load"))
    import guard
    return guard


def aws(guard, operation, payload):
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true",
                       AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="",
                       AWS_REGION=guard.DEV_REGION, AWS_DEFAULT_REGION=guard.DEV_REGION)
    command = ["aws", "dynamodb", operation, "--region", guard.DEV_REGION, "--output", "json",
               "--no-cli-pager", "--cli-input-json",
               json.dumps(payload, separators=(",", ":"))]
    result = subprocess.run(command, capture_output=True, env=environment, timeout=40, check=False)
    if result.returncode:
        raise OperationError("cleanup_aws")
    if not result.stdout.strip():
        return {}
    return json.loads(result.stdout)


def purge_partition(guard, partition):
    pending, start, deleted = [], None, 0
    while True:
        request = guard.query_request(guard.DEV_TABLE, partition)
        if start is not None:
            request["ExclusiveStartKey"] = start
        found = aws(guard, "query", request)
        pending.extend(guard.record_ids_from_query(found, partition))
        nxt = found.get("LastEvaluatedKey")
        if not guard.continue_page(nxt, start):
            break
        start = nxt
    while pending:
        chunk = pending[:25]
        response = aws(guard, "batch-write-item", guard.delete_batch(guard.DEV_TABLE, partition, chunk))
        left = guard.unprocessed_ids(response, guard.DEV_TABLE, partition)
        if left == chunk:
            raise OperationError("cleanup_aws")
        pending = left + pending[len(chunk):]
        deleted += len(chunk) - len(left)
    return deleted


def verify_cloud_target(guard):
    guard.allow_region_environment(os.environ)
    described = aws(guard, "describe-table", {"TableName": guard.DEV_TABLE})
    guard.allow_table_arn(described.get("Table", {}).get("TableArn", ""), guard.DEV_TABLE)


def cleanup(*, native=False):
    guard = load_guard()
    verify_cloud_target(guard)
    deleted = 0
    partitions = guard.load_partitions(gate_id())
    for partition in partitions:
        deleted += purge_partition(guard, partition)
    summary(f"cleanup_rows={deleted}")
    if native:
        for partition in partitions:
            reply = aws(guard, "query", guard.query_request(guard.DEV_TABLE, partition))
            require(not reply.get("Items") and not reply.get("LastEvaluatedKey"),
                    "native_cutoff_cleanup_incomplete")
        summary("Run-owned signed log, accepted-write facts, submissions, and snapshot metadata are empty.")
    # snapshot/bytes accepts read, prepare, download, and publish. It has no
    # owner delete, and the DEV load role cannot reach the R2 or B2 objects.
    summary("provider_objects=left_behind missing=snapshot/bytes delete")


def opt_into_first_snapshot(home, database_id):
    """Ask the daemon to hold log pages until this database's first snapshot."""
    parent = home / "log-sync"
    parent.mkdir(mode=0o700)
    directory = parent / database_id
    directory.mkdir(mode=0o700)
    write_private(directory / "snapshot-first", b"snapshot-v1\n")


SNAPSHOT_THROUGH = 12
FINAL_HEAD = 16
SNAPSHOT_ONLY = {"id": "native-cutoff-base", "note": "only present on the source at snapshot"}
OFFLINE_RECORDS = [
    {"id": "native-cutoff-queue-a", "note": "first disjoint offline write"},
    {"id": "native-cutoff-queue-b", "note": "second disjoint offline write"},
]


def publish_native_snapshot(private, programs, call, source, database):
    private.advance_while_peer_offline(call, database, source)
    private.write(source, database, SNAPSHOT_ONLY)
    private.wait_prefix(source, database, SNAPSHOT_THROUGH, "private_native_source_snapshot_prefix")
    private.check_progress(source, database, 8, SNAPSHOT_THROUGH)
    private.expect_records(source, database, [private.LAGGING_REMOTE, SNAPSHOT_ONLY],
                           "private_native_source_snapshot_records")
    programs.verify()
    try:
        _, output = cli(programs, source.home, [
            "backup", "snapshot", "--database", database, "--wait",
        ], 85)
    except OperationError as error:
        raise OperationError("private_native_snapshot_publish", cli_refusal=error.cli_refusal) from None
    lines = output.splitlines()
    require(len(lines) == 3 and prefixed(lines[0], b"snapshot_through=") == SNAPSHOT_THROUGH
            and prefixed(lines[1], b"log_head=") == SNAPSHOT_THROUGH
            and prefixed(lines[2], b"log_entries=") <= SNAPSHOT_THROUGH,
            "private_native_snapshot_receipt")
    response = call("snapshot/bytes", {"action": "read"})
    snapshot_id = response.get("snapshot_id")
    snapshot = response.get("snapshot")
    require(response.get("database_id") == database and isinstance(snapshot_id, str)
            and CUTOFF_DIGEST.fullmatch(snapshot_id)
            and isinstance(snapshot, dict) and private.decimal(snapshot["through"]) == SNAPSHOT_THROUGH
            and snapshot.get("name") is None and snapshot.get("files"),
            "private_native_snapshot_manifest")
    expect_cut(private, call, snapshot_id)
    return snapshot_id


def expect_cut(private, call, snapshot_id):
    reset = call("list", {"after": "10", "limit": "2", "accept_native_reset": True},
                 expected_error=409)
    require(reset.get("code") == "predecessor_conflict"
            and reset.get("field") == "mutation_log.cursor"
            and private.decimal(reset["snapshot_through"]) == SNAPSHOT_THROUGH
            and reset.get("native_snapshot_id") == snapshot_id
            and reset.get("snapshot_ref") is None, "private_native_signed_cutoff")


def write_offline_tail(private, call, peer, database):
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_offline")
    with running(peer):
        first_offline = {**private.FIELDS, "note": "first edit on the lagging device"}
        private.check_progress(peer, database, 4, 10)
        require(private.read_fields(peer, database, [private.PEER_LATER, SNAPSHOT_ONLY])
                == [[private.PEER_LATER], []],
                "private_native_peer_old_data")
        private.replace_record(peer, database, private.PEER_LATER, first_offline)
        rows = private.read_rows(peer, database, [first_offline])[0]
        require(len(rows) == 1 and rows[0]["fields"] == first_offline, "private_native_peer_first_edit")
        mutations = [replace(private.SCHEMA, rows[0], private.LAGGING_LOCAL)]
        mutations.extend(create(private.SCHEMA, record_key(fields["id"]), fields) for fields in OFFLINE_RECORDS)
        reply = peer.call(f"/api/databases/{database}/mutations/batch", {"mutations": mutations})
        require(reply["ok"] is True and reply["durability"] == "durable"
                and reply["operations"] == {"created": 2, "updated": 1, "deleted": 0, "total": 3},
                "private_native_peer_tail_durable")
        private.expect_records(peer, database, [private.LAGGING_LOCAL, *OFFLINE_RECORDS],
                               "private_native_peer_pending_data")
        private.check_progress(peer, database, 4, 10)
    require(private.cloud_head(call) == SNAPSHOT_THROUGH, "private_native_peer_still_unpublished")


def owned_directory_inode(path):
    value = path.lstat()
    require(stat.S_ISDIR(value.st_mode) and value.st_uid == os.getuid(), "private_native_data_directory")
    return value.st_dev, value.st_ino


def wait_reopen(private, peer, database, snapshot_id, writer, old_inode, old_instance):
    path = peer.home / "log-sync" / database / "native-recovery.json"
    expected = {"version": 1, "database_id": database, "device_id": writer,
                "snapshot_id": snapshot_id, "snapshot_through": SNAPSHOT_THROUGH,
                "through": FINAL_HEAD}

    def reopened():
        require(peer.process is not None and peer.process.poll() is None, "private_native_peer_process")
        if not path.is_file():
            return None
        receipt = read_json(path, 4096)
        require(receipt == expected, "private_native_recovery_receipt")
        try:
            health = peer.call("/health")
            boot = peer.call("/api/system/boot-identity")
        except (OSError, http.client.HTTPException):
            return None
        if health.get("instance_id") == old_instance:
            return None
        require(boot.get("pid") == peer.process.pid and boot.get("build") == peer.programs.version
                and private.device_id(peer) == writer
                and owned_directory_inode(peer.home / "data") != old_inode, "private_native_reopened_data")
        return health["instance_id"], boot["process_start_ts"]

    peer.instance_id, peer.start_ts = retry("private_native_peer_reopen", reopened, bool)
    require(peer.health(), "private_native_reopened_health")


def check_tail(private, call, source, peer):
    page = call("list", {"after": str(SNAPSHOT_THROUGH), "limit": "8", "accept_native_reset": True})
    writer = private.device_id(peer)
    require(private.decimal(page["next"]) == FINAL_HEAD
            and [(private.decimal(item["delivery_seq"]), item["entry"]["device_id"],
                  private.decimal(item["entry"]["writer_seq"])) for item in page["entries"]]
            == [(13, writer, 5), (14, writer, 6), (15, writer, 7), (16, writer, 8)],
            "private_native_delivered_tail")
    for node in (source, peer):
        require(call("head", {"device_id": private.device_id(node)}) == {"last_writer_seq": "8"},
                "private_native_writer_head")


def retention(private, call, snapshot_id, *, allow_older_service=False):
    state = call("snapshot/bytes", {"action": "retention"},
                 allow_older_service=allow_older_service)
    if state is None:
        return None
    require(type(state.get("reclaim_enabled")) is bool and state.get("eligible") is True
            and state.get("snapshot_id") == snapshot_id
            and private.decimal(state["snapshot_through"]) == SNAPSHOT_THROUGH,
            "native_reclaim_retention")
    return state["reclaim_enabled"], private.decimal(state["reclaimed_through"])


def reclaim_step(private, call, request, before, after):
    snapshot_id = request["snapshot_id"]
    for _ in range(4):
        result = call("snapshot/bytes", {**request, "expected_floor": str(before)},
                      allow_unknown_outcome=True)
        if result is not None:
            require(result == {"snapshot_id": snapshot_id, "snapshot_through": str(SNAPSHOT_THROUGH),
                               "reclaimed_through": str(after)}, "native_reclaim_receipt")
        enabled, floor = retention(private, call, snapshot_id)
        require(enabled and floor in (before, after), "native_reclaim_floor")
        if floor == after:
            return
        require(result is None, "native_reclaim_unconfirmed_receipt")
        time.sleep(0.25)
    raise OperationError("native_reclaim_deadline")


def reclaim_signed_prefix(private, call, snapshot_id):
    # A legacy reader can still get the two bodies before the physical floor moves.
    legacy_list = {"after": "10", "limit": "2"}
    before = call("list", legacy_list)
    require(private.decimal(before["next"]) == SNAPSHOT_THROUGH
            and [(private.decimal(item["delivery_seq"]), private.decimal(item["entry"]["writer_seq"]))
                 for item in before["entries"]] == [(11, 7), (12, 8)],
            "native_reclaim_legacy_before")
    # The duplicate receipt advances from this writer's latest sequence.
    old_entry = before["entries"][-1]["entry"]
    require(call("head", {"device_id": old_entry["device_id"]}) == {"last_writer_seq": "8"},
            "native_reclaim_last_writer")
    request = {"action": "reclaim", "snapshot_id": snapshot_id, "require_native_recovery": True}
    refused = call("snapshot/bytes", {**request, "expected_floor": "0", "require_native_recovery": False},
                   expected_error=400)
    require(refused == {"code": "invalid_input", "field": "snapshot.retention.opt_in"}
            and retention(private, call, snapshot_id) == (True, 0), "native_reclaim_requires_opt_in")

    reclaim_step(private, call, request, 0, 8)
    stale = call("snapshot/bytes", {**request, "expected_floor": "0"}, expected_error=409)
    require(stale == {"code": "predecessor_conflict", "field": "snapshot.retention.floor"}
            and retention(private, call, snapshot_id) == (True, 8), "native_reclaim_stale_retry")
    reclaim_step(private, call, request, 8, SNAPSHOT_THROUGH)

    # Repeating the current floor at the cutoff has no further effect.
    current = str(SNAPSHOT_THROUGH)
    complete = call("snapshot/bytes", {**request, "expected_floor": current})
    require(complete == {"snapshot_id": snapshot_id, "snapshot_through": current,
                         "reclaimed_through": current}
            and retention(private, call, snapshot_id) == (True, SNAPSHOT_THROUGH),
            "native_reclaim_complete_retry")
    legacy = call("list", legacy_list, expected_error=400)
    require(legacy == {"code": "unsupported_capability", "field": "snapshot.native_reset_required"},
            "native_reclaim_legacy_refused")
    private.check_entry_retry(call, old_entry, SNAPSHOT_THROUGH)
    summary(f"signed_reclaim=verified floor={SNAPSHOT_THROUGH}")


def catch_up(private, programs, call, database, api_key, source, peer, records):
    require(peer.process is None and peer.clean_stops > 0, "private_native_peer_stopped")
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_no_credentials")
    receipt = peer.home / "log-sync" / database / "native-recovery.json"
    require(not os.path.lexists(receipt), "private_native_peer_recovery_is_new")
    private.check_progress(peer, database, 4, 10)
    writer = private.device_id(peer)
    expected_records = [private.LAGGING_LOCAL, *records[1:], SNAPSHOT_ONLY, *OFFLINE_RECORDS]
    private.connect(programs, source, api_key)
    with running(source, remove_saved=True):
        snapshot_id = publish_native_snapshot(private, programs, call, source, database)
        state = retention(private, call, snapshot_id, allow_older_service=True)
        require(state is None or state[1] == 0, "native_reclaim_initial_floor")
        if state == (True, 0):
            reclaim_signed_prefix(private, call, snapshot_id)
        else:
            prune_verified_prefix(database, snapshot_id, SNAPSHOT_THROUGH)
            reason = "unsupported" if state is None else "disabled"
            summary(f"signed_reclaim={reason} used=run_owned_DEV_prefix_removal")
        expect_cut(private, call, snapshot_id)
        write_offline_tail(private, call, peer, database)
        old_inode = owned_directory_inode(peer.home / "data")
        private.connect(programs, peer, api_key)
        with running(peer, remove_saved=True):
            wait_reopen(private, peer, database, snapshot_id, writer, old_inode, peer.instance_id)
            private.wait_prefix(peer, database, FINAL_HEAD, "private_native_peer_final_position")
            private.wait_prefix(source, database, FINAL_HEAD, "private_native_source_final_position")
            private.check_progress(peer, database, 8, FINAL_HEAD)
            private.check_progress(source, database, 8, FINAL_HEAD)
            private.expect_records(peer, database, expected_records, "private_native_peer_final_records")
            private.expect_records(source, database, expected_records, "private_native_source_final_records")
    check_tail(private, call, source, peer)
    summary("The lagging device uploaded all four offline edits before reopening from the native "
            "snapshot. It kept its device identity and recovered the snapshot-only record, the "
            "latest of its two offline edits to the same key, and both disjoint records.")
    return expected_records


MAX_PROOF_ENTRIES = 256
CUTOFF_DIGEST = re.compile(r"[0-9a-f]{64}")
WRITER_LABEL = re.compile(r"[A-Za-z0-9_-]{1,64}")


def cutoff_rows(guard, partition, expression, values):
    request = guard.query_request(guard.DEV_TABLE, partition)
    request["KeyConditionExpression"] += " AND " + expression
    request["ExpressionAttributeValues"].update(
        {name: {"S": value} for name, value in values.items()})
    result = {}
    start = None
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
        if (not WRITER_LABEL.fullmatch(node) or not WRITER_LABEL.fullmatch(device)
                or last_text != "{:020d}".format(last) or first < 1 or delivery < 1
                or not 1 <= len(digests) <= 90):
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
        actual = (entry["writer_key"], entry["writer_device"],
                  entry["writer_seq"], entry_digest(database, entry))
        if positions[int(key.removeprefix("e#"))] != actual:
            raise OperationError("native cutoff write fact differs")


def delete_cutoff_entries(guard, database, pending):
    # This operation covers one small prefix. Across transient unprocessed
    # DynamoDB responses, retry only the keys still present in that response.
    for start in range(0, len(pending), guard.BATCH_LIMIT):
        chunk = pending[start:start + guard.BATCH_LIMIT]
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


def prune_verified_prefix(database, snapshot_id, through):
    """Prune only entries with exact facts under the current verified snapshot."""
    guard = load_guard()
    require_native_dispatch()
    require(database == gate_id() and CUTOFF_DIGEST.fullmatch(snapshot_id)
            and through == SNAPSHOT_THROUGH, "native_cutoff_scope")
    verify_cloud_target(guard)

    heads = cutoff_rows(guard, database, "record_id = :key", {":key": "head"})
    head = heads.get("head", {})
    if (head.get("database_id") != database or head.get("kind") != "personal"
            or head.get("write_facts_from") != 1 or head.get("trimmed_through") != 0
            or head.get("head", {}).get("delivery_seq") != through):
        raise OperationError("native cutoff database head")

    manifest_key = "native-files#" + snapshot_id
    records = cutoff_rows(guard, guard.snap_partition(database), "record_id BETWEEN :start AND :end",
                          {":start": "native-cut", ":end": manifest_key})
    expected_cut = {"snapshot_id": snapshot_id, "through": str(through)}
    saved = records.get(manifest_key, {})
    manifest = saved.get("snapshot", {})
    if (records.get("native-cut") != expected_cut
            or records.get("native-files") != saved or saved.get("id") != snapshot_id
            or manifest.get("through") != str(through) or not manifest.get("files")
            or len(saved.get("versions", [])) != len(manifest["files"])):
        raise OperationError("native cutoff manifest")

    entry_bounds = {":start": entry_key(1), ":end": entry_key(through)}
    entry_range = "record_id BETWEEN :start AND :end"
    entries = cutoff_rows(guard, database, entry_range, entry_bounds)
    if set(entries) != {entry_key(position) for position in range(1, through + 1)}:
        raise OperationError("native cutoff delivery key")
    facts = cutoff_rows(guard, guard.writes_partition(database), "begins_with(record_id, :prefix)",
                        {":prefix": "write#"})
    verify_facts(database, through, entries, facts)
    delete_cutoff_entries(guard, database, sorted(entries))
    if (cutoff_rows(guard, database, entry_range, entry_bounds)
            or cutoff_rows(guard, guard.writes_partition(database), "begins_with(record_id, :prefix)",
                           {":prefix": "write#"}) != facts):
        raise OperationError("native cutoff prefix was not pruned with facts intact")


def require_native_dispatch():
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch", "native_cutoff_manual_main_only")


def run_native_cutoff(programs, root, phrase, api_key):
    guard = load_guard()
    from node_auth import NodeKey, account_id
    import private_recovery as private
    from run import post

    verify_cloud_target(guard)
    database = guard.allow_stream(gate_id())
    summary(f"database_id={database}")
    raw = os.environ.pop("LASTDB_CLOUD_DEV_BASELINE", "")
    require(0 < len(raw) <= 65536, "native_cutoff_baseline")
    baseline = json.loads(raw)
    require(isinstance(baseline, dict), "native_cutoff_baseline")
    generation = baseline.get("account_generation")
    seed = baseline_data.seed(phrase)
    owner = NodeKey.from_seed(seed)
    require(baseline.get("account") == account_id(owner.public) == baseline_data.ACCOUNT
            and isinstance(generation, str) and re.fullmatch(r"0|[1-9][0-9]{0,19}", generation)
            and int(generation) <= 2**64 - 1, "native_cutoff_baseline_scope")
    endpoint = dev_endpoint()
    api_key = api_key.decode("utf-8")

    def call(operation, fields, *, expected_error=None, allow_unknown_outcome=False,
             allow_older_service=False):
        body = json.dumps({**fields, "database_id": database}, separators=(",", ":")).encode("utf-8")
        try:
            status, result, _ = post(endpoint, owner, database, "POST", f"{private.ROOT}/{operation}",
                                     body, api_key, generation)
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException):
            if allow_unknown_outcome:
                return None
            raise OperationError("private_native_signed_transport") from None
        # The immediately previous native-cut service rejected this unknown action at decode.
        if (allow_older_service and operation == "snapshot/bytes" and fields == {"action": "retention"}
                and status == 400 and result == {"code": "invalid_input", "field": "http.body"}):
            return None
        error_code = result.get("code") if isinstance(result, dict) else None
        if allow_unknown_outcome:
            if status in (502, 504) or (status == 503 and
                                       error_code in (None, "retry_later", "outcome_unknown")):
                return None
            if status == 409 and result == {"code": "predecessor_conflict",
                                            "field": "snapshot.retention.floor"}:
                return None
        require(isinstance(result, dict)
                and (200 <= status < 300 if expected_error is None else status == expected_error),
                "private_native_signed_" + operation.replace("/", "_"))
        return result

    for result in (call("create-database", {"kind": "personal"}), call("access", {})):
        require(result.get("state") == "native" and result.get("database_id") == database,
                "private_native_registration")
    credentials = OwnedNode(root / "connection", programs, account=baseline_data.ACCOUNT)
    credentials.home.mkdir(mode=0o700)
    write_private(credentials.home / "identity.key", seed)
    private.connect(programs, credentials, api_key)
    source, peer = (OwnedNode(root / name, programs, account=baseline_data.ACCOUNT,
                              isolated_copy=True, keep_saved_credentials=True)
                    for name in ("source", "peer"))
    private.prepare_source(source, database, owner)
    private.connected_exchange(programs, call, database, phrase, api_key, credentials, source, peer)
    records = private.reconnect(call, database, source, peer)
    page = call("list", {"after": "0", "limit": "20"})
    require(private.decimal(page["next"]) == 10, "private_native_starting_head")
    private.check_devices(page, private.device_id(source), private.device_id(peer))
    expected = catch_up(private, programs, call, database, api_key, source, peer, records)

    restored = OwnedNode(root / "restored", programs, account=baseline_data.ACCOUNT)
    try:
        receipt = programs.command(credentials.home, [
            "backup", "restore", "--database", database, "--recovery-phrase-stdin",
            "--into", str(restored.home), "--json",
        ], input_bytes=phrase, timeout=100)
    except OperationError as error:
        raise OperationError("private_native_fresh_restore", cli_refusal=error.cli_refusal) from None
    require(receipt["database_id"] == database and receipt["name"] == "personal"
            and receipt["through_delivery_seq"] == FINAL_HEAD
            and receipt["snapshot_through"] == SNAPSHOT_THROUGH
            and receipt["replayed"] == FINAL_HEAD - SNAPSHOT_THROUGH, "private_native_fresh_restore")
    device = private.device_id(restored)
    require(device not in (private.device_id(source), private.device_id(peer))
            and call("head", {"device_id": device}) == {"last_writer_seq": "0"}
            and read_json(restored.home / "log-sync" / database / "session.json", 4096)
            == {"cursor": FINAL_HEAD}, "private_native_fresh_positions")
    with running(restored):
        require(private.read_fields(restored, database, expected) == [[row] for row in expected],
                "private_native_fresh_records")
    summary(f"native_cutoff={SNAPSHOT_THROUGH} delivery_cursor={FINAL_HEAD} offline_edits=4 "
            f"fresh_replayed={FINAL_HEAD - SNAPSHOT_THROUGH}")
    summary("native snapshot cutoff recovery passed")


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    native = command in ("native-cutoff", "cleanup-native-cutoff")
    if native:
        require_native_dispatch()
    if command in ("cleanup", "cleanup-native-cutoff"):
        require(len(sys.argv) == 2, "arguments")
        cleanup(native=native)
        return
    require(command == "run" or native, "command")
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    # Daemon-created shutdown receipts inherit the runner's file-creation mask.
    os.umask(0o077)
    dev_endpoint()
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE").strip().encode()
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY").encode()
    require(0 < len(api_key) <= 4096, "dev_api_key")
    parser_args = sys.argv[2:]
    require(len(parser_args) == 4 and parser_args[0] == "--bin-dir"
            and parser_args[2] == "--version", "arguments")
    with tempfile.TemporaryDirectory(prefix="ldb-snapshot-", dir="/tmp") as temporary:
        root = Path(temporary)
        for name in ("os-home", "tmp"):
            (root / name).mkdir(mode=0o700)
        programs = Programs(Path(parser_args[1]).resolve(strict=True), parser_args[3],
                            child_environment(root))
        try:
            if native:
                run_native_cutoff(programs, root, phrase, api_key)
            else:
                run_gate(programs, root, phrase, api_key)
        except OperationError as error:
            summary("snapshot gate failed: " + json.dumps(failure(error), sort_keys=True))
            raise
        finally:
            del phrase
            del api_key


if __name__ == "__main__":
    main()
