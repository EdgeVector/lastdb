"""Prove a live native reset for one run-owned private DEV database."""

import http.client
import os
import re
import stat

from native_records import create, key as record_key, replace
from private_recovery import (
    FIELDS, LAGGING_LOCAL, LAGGING_REMOTE, PEER_LATER, SCHEMA, advance_while_peer_offline,
    check_progress, cloud_head, connect, device_id, expect_records, read_fields, read_rows,
    replace_record, running, wait_prefix, write,
)
from programs import execute
from routes import decimal
from signed_files import retry, summary
from support import CREDENTIALS_FILE, OperationError, read_json, require


SNAPSHOT_THROUGH = 12
FINAL_HEAD = 16
SNAPSHOT_ONLY = {"id": "native-cutoff-base", "note": "only present on the source at snapshot"}
FIRST_OFFLINE = {**FIELDS, "note": "first edit on the lagging device"}
OFFLINE_RECORDS = [
    {"id": "native-cutoff-queue-a", "note": "first disjoint offline write"},
    {"id": "native-cutoff-queue-b", "note": "second disjoint offline write"},
]


def publish_snapshot(programs, call, source, database):
    advance_while_peer_offline(call, database, source)
    write(source, database, SNAPSHOT_ONLY)
    wait_prefix(source, database, SNAPSHOT_THROUGH, "private_native_source_snapshot_prefix")
    check_progress(source, database, 8, SNAPSHOT_THROUGH)
    expect_records(source, database, [LAGGING_REMOTE, SNAPSHOT_ONLY], "private_native_source_snapshot_records")
    programs.verify()
    try:
        output = execute([str(programs.directory / "lastdb"), "--data-dir", str(source.home),
                          "backup", "snapshot", "--database", database, "--wait"],
                         programs.environment, timeout=85)
    except OperationError as error:
        raise OperationError("private_native_snapshot_publish", cli_refusal=error.cli_refusal) from None
    lines = output.splitlines()
    require(len(lines) == 3 and lines[0] == b"snapshot_through=12"
            and lines[1] == b"log_head=12"
            and re.fullmatch(rb"log_entries=[0-9]+", lines[2]), "private_native_snapshot_receipt")
    response = call("snapshot/bytes", {"action": "read"})
    snapshot_id = response.get("snapshot_id")
    snapshot = response.get("snapshot")
    require(response.get("database_id") == database and isinstance(snapshot_id, str)
            and re.fullmatch(r"[0-9a-f]{64}", snapshot_id)
            and isinstance(snapshot, dict) and decimal(snapshot["through"]) == SNAPSHOT_THROUGH
            and snapshot.get("name") is None and snapshot.get("files"), "private_native_snapshot_manifest")
    expect_cut(call, snapshot_id)
    return snapshot_id


def expect_cut(call, snapshot_id):
    reset = call("list", {"after": "10", "limit": "2", "accept_native_reset": True},
                 expected_error=409)
    require(reset.get("code") == "predecessor_conflict"
            and reset.get("field") == "mutation_log.cursor"
            and decimal(reset["snapshot_through"]) == SNAPSHOT_THROUGH
            and reset.get("native_snapshot_id") == snapshot_id
            and reset.get("snapshot_ref") is None, "private_native_signed_cutoff")


def write_offline_tail(call, peer, database):
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_offline")
    with running(peer):
        check_progress(peer, database, 4, 10)
        require(read_fields(peer, database, [PEER_LATER, SNAPSHOT_ONLY]) == [[PEER_LATER], []],
                "private_native_peer_old_data")
        replace_record(peer, database, PEER_LATER, FIRST_OFFLINE)
        rows = read_rows(peer, database, [FIRST_OFFLINE])[0]
        require(len(rows) == 1 and rows[0]["fields"] == FIRST_OFFLINE, "private_native_peer_first_edit")
        mutations = [replace(SCHEMA, rows[0], LAGGING_LOCAL)]
        mutations.extend(create(SCHEMA, record_key(fields["id"]), fields) for fields in OFFLINE_RECORDS)
        reply = peer.call(f"/api/databases/{database}/mutations/batch", {"mutations": mutations})
        require(reply["ok"] is True and reply["durability"] == "durable"
                and reply["operations"] == {"created": 2, "updated": 1, "deleted": 0, "total": 3},
                "private_native_peer_tail_durable")
        expect_records(peer, database, [LAGGING_LOCAL, *OFFLINE_RECORDS], "private_native_peer_pending_data")
        check_progress(peer, database, 4, 10)
    require(cloud_head(call) == SNAPSHOT_THROUGH, "private_native_peer_still_unpublished")


def owned_directory_inode(path):
    value = path.lstat()
    require(stat.S_ISDIR(value.st_mode) and value.st_uid == os.getuid(), "private_native_data_directory")
    return value.st_dev, value.st_ino


def wait_reopen(peer, database, snapshot_id, writer, old_inode, old_instance):
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
                and device_id(peer) == writer
                and owned_directory_inode(peer.home / "data") != old_inode, "private_native_reopened_data")
        return health["instance_id"], boot["process_start_ts"]

    peer.instance_id, peer.start_ts = retry("private_native_peer_reopen", reopened, bool)
    require(peer.health(), "private_native_reopened_health")


def check_tail(call, source, peer):
    page = call("list", {"after": str(SNAPSHOT_THROUGH), "limit": "8", "accept_native_reset": True})
    writer = device_id(peer)
    require(decimal(page["next"]) == FINAL_HEAD
            and [(decimal(item["delivery_seq"]), item["entry"]["device_id"],
                  decimal(item["entry"]["writer_seq"])) for item in page["entries"]]
            == [(13, writer, 5), (14, writer, 6), (15, writer, 7), (16, writer, 8)],
            "private_native_delivered_tail")
    for node in (source, peer):
        require(call("head", {"device_id": device_id(node)}) == {"last_writer_seq": "8"},
                "private_native_writer_head")


def catch_up(programs, call, database, api_key, source, peer, records, prune):
    """Run only when explicitly provided the exact DEV pruning operation."""
    require(peer.process is None and peer.clean_stops > 0, "private_native_peer_stopped")
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_no_credentials")
    receipt = peer.home / "log-sync" / database / "native-recovery.json"
    require(not os.path.lexists(receipt), "private_native_peer_recovery_is_new")
    check_progress(peer, database, 4, 10)
    writer = device_id(peer)
    expected_records = [LAGGING_LOCAL, *records[1:], SNAPSHOT_ONLY, *OFFLINE_RECORDS]
    connect(programs, source, api_key)
    with running(source, remove_saved=True):
        snapshot_id = publish_snapshot(programs, call, source, database)
        prune(snapshot_id, SNAPSHOT_THROUGH)
        expect_cut(call, snapshot_id)
        write_offline_tail(call, peer, database)
        old_inode = owned_directory_inode(peer.home / "data")
        connect(programs, peer, api_key)
        with running(peer, remove_saved=True):
            wait_reopen(peer, database, snapshot_id, writer, old_inode, peer.instance_id)
            wait_prefix(peer, database, FINAL_HEAD, "private_native_peer_final_position")
            wait_prefix(source, database, FINAL_HEAD, "private_native_source_final_position")
            check_progress(peer, database, 8, FINAL_HEAD)
            check_progress(source, database, 8, FINAL_HEAD)
            expect_records(peer, database, expected_records, "private_native_peer_final_records")
            expect_records(source, database, expected_records, "private_native_source_final_records")
    check_tail(call, source, peer)
    summary("The lagging device uploaded all four offline edits before reopening the native "
            "snapshot. It kept its device identity and recovered the snapshot-only record, the "
            "latest of its two offline edits to the same key, and both disjoint records.")
    return SNAPSHOT_THROUGH, FINAL_HEAD, expected_records
