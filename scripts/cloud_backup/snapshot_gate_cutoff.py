"""Native snapshot cutover, lagging-peer offline tail, reopen, and fresh restore."""

import http.client
import json
import os
from pathlib import Path
import stat
import sys
import urllib.error

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import create, key as record_key, replace_row as replace  # noqa: E402
from node import OwnedNode  # noqa: E402
from snapshot_cleanup import SNAPSHOT_DEV_ACCOUNT, current_attempt, start_tracking  # noqa: E402
from snapshot_gate_aws import (  # noqa: E402
    CUTOFF_DIGEST,
    SNAPSHOT_THROUGH,
    entry_key,
    reclaimed_storage,
    summary,
    verify_cloud_target,
)
from snapshot_gate_reclaim import (  # noqa: E402
    NEWER_THAN_SNAPSHOT,
    auto_reclaim_preimage,
    automatic_floor,
    automatic_supported,
    check_auto_reclaim,
    index_status,
    reclaim_compatibility_prefix,
)
from snapshot_gate_roundtrip import cli, prefixed  # noqa: E402
from support import (  # noqa: E402
    CREDENTIALS_FILE,
    OperationError,
    dev_endpoint,
    read_json,
    require,
    retry,
    running,
    write_private,
)

FINAL_HEAD = 16
SNAPSHOT_ONLY = {"id": "native-cutoff-base", "note": "only present on the source at snapshot"}
OFFLINE_RECORDS = [
    {"id": "native-cutoff-queue-a", "note": "first disjoint offline write"},
    {"id": "native-cutoff-queue-b", "note": "second disjoint offline write"},
]


def prepare_native_snapshot(private, call, source, database):
    private.advance_while_peer_offline(call, database, source)
    private.write(source, database, SNAPSHOT_ONLY)
    private.wait_prefix(source, database, SNAPSHOT_THROUGH, "private_native_source_snapshot_prefix")
    private.check_progress(source, database, 8, SNAPSHOT_THROUGH)
    private.expect_records(
        source, database, [private.LAGGING_REMOTE, SNAPSHOT_ONLY], "private_native_source_snapshot_records"
    )


def expect_cut(private, call, snapshot_id):
    reset = call("list", {"after": "10", "limit": "2", "accept_native_reset": True}, expected_error=409)
    require(
        reset.get("code") == "predecessor_conflict"
        and reset.get("field") == "mutation_log.cursor"
        and private.decimal(reset["snapshot_through"]) == SNAPSHOT_THROUGH
        and reset.get("native_snapshot_id") == snapshot_id
        and reset.get("snapshot_ref") is None,
        "private_native_signed_cutoff",
    )


def publish_native_snapshot(private, programs, call, source, database):
    programs.verify()
    try:
        _, output = cli(programs, source.home, ["backup", "snapshot", "--database", database, "--wait"], 85)
    except OperationError as error:
        raise OperationError("private_native_snapshot_publish", cli_refusal=error.cli_refusal) from None
    lines = output.splitlines()
    require(
        len(lines) == 3
        and prefixed(lines[0], b"snapshot_through=") == SNAPSHOT_THROUGH
        and prefixed(lines[1], b"log_head=") == SNAPSHOT_THROUGH
        and prefixed(lines[2], b"log_entries=") <= SNAPSHOT_THROUGH,
        "private_native_snapshot_receipt",
    )
    response = call("snapshot/bytes", {"action": "read"})
    snapshot_id = response.get("snapshot_id")
    snapshot = response.get("snapshot")
    require(
        response.get("database_id") == database
        and isinstance(snapshot_id, str)
        and CUTOFF_DIGEST.fullmatch(snapshot_id)
        and isinstance(snapshot, dict)
        and private.decimal(snapshot["through"]) == SNAPSHOT_THROUGH
        and snapshot.get("name") is None
        and snapshot.get("files"),
        "private_native_snapshot_manifest",
    )
    expect_cut(private, call, snapshot_id)
    return snapshot_id


def write_offline_tail(private, call, peer, database, current_head):
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_offline")
    with running(peer):
        first_offline = {**private.FIELDS, "note": "first edit on the lagging device"}
        private.check_progress(peer, database, 4, 10)
        require(
            private.read_fields(peer, database, [private.PEER_LATER, SNAPSHOT_ONLY]) == [[private.PEER_LATER], []],
            "private_native_peer_old_data",
        )
        private.replace_record(peer, database, private.PEER_LATER, first_offline)
        rows = private.read_rows(peer, database, [first_offline])[0]
        require(len(rows) == 1 and rows[0]["fields"] == first_offline, "private_native_peer_first_edit")
        mutations = [replace(private.SCHEMA, rows[0], private.LAGGING_LOCAL)]
        mutations.extend(create(private.SCHEMA, record_key(fields["id"]), fields) for fields in OFFLINE_RECORDS)
        reply = peer.call(f"/api/databases/{database}/mutations/batch", {"mutations": mutations})
        require(
            reply["ok"] is True
            and reply["durability"] == "durable"
            and reply["operations"] == {"created": 2, "updated": 1, "deleted": 0, "total": 3},
            "private_native_peer_tail_durable",
        )
        private.expect_records(
            peer, database, [private.LAGGING_LOCAL, *OFFLINE_RECORDS], "private_native_peer_pending_data"
        )
        private.check_progress(peer, database, 4, 10)
    require(private.cloud_head(call) == current_head, "private_native_peer_still_unpublished")


def owned_directory_inode(path):
    value = path.lstat()
    require(stat.S_ISDIR(value.st_mode) and value.st_uid == os.getuid(), "private_native_data_directory")
    return value.st_dev, value.st_ino


def wait_reopen(private, peer, database, snapshot_id, writer, old_inode, old_instance, final_head):
    path = peer.home / "log-sync" / database / "native-recovery.json"
    expected = {
        "version": 1,
        "database_id": database,
        "device_id": writer,
        "snapshot_id": snapshot_id,
        "snapshot_through": SNAPSHOT_THROUGH,
        "through": final_head,
    }

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
        require(
            boot.get("pid") == peer.process.pid
            and boot.get("build") == peer.programs.version
            and private.device_id(peer) == writer
            and owned_directory_inode(peer.home / "data") != old_inode,
            "private_native_reopened_data",
        )
        return health["instance_id"], boot["process_start_ts"]

    peer.instance_id, peer.start_ts = retry("private_native_peer_reopen", reopened, bool)
    require(peer.health(), "private_native_reopened_health")


def check_tail(private, call, source, peer, *, automatic):
    page = call("list", {"after": str(SNAPSHOT_THROUGH), "limit": "8", "accept_native_reset": True})
    source_id, peer_id = private.device_id(source), private.device_id(peer)
    start = SNAPSHOT_THROUGH + 1
    expected = []
    if automatic:
        expected.append((start, source_id, 9))
        start += 1
    expected.extend((start + offset, peer_id, 5 + offset) for offset in range(len(OFFLINE_RECORDS) + 2))
    require(
        private.decimal(page["next"]) == expected[-1][0]
        and [
            (private.decimal(item["delivery_seq"]), item["entry"]["device_id"], private.decimal(item["entry"]["writer_seq"]))
            for item in page["entries"]
        ]
        == expected,
        "private_native_delivered_tail",
    )
    for device, seq in ((source_id, "9" if automatic else "8"), (peer_id, "8")):
        require(call("head", {"device_id": device}) == {"last_writer_seq": seq}, "private_native_writer_head")


def catch_up(private, programs, call, database, api_key, source, peer, records, *, history_supported, automatic):
    require(peer.process is None and peer.clean_stops > 0, "private_native_peer_stopped")
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_native_peer_no_credentials")
    receipt = peer.home / "log-sync" / database / "native-recovery.json"
    require(not os.path.lexists(receipt), "private_native_peer_recovery_is_new")
    private.check_progress(peer, database, 4, 10)
    writer = private.device_id(peer)
    expected_records = [private.LAGGING_LOCAL, *records[1:], SNAPSHOT_ONLY, *OFFLINE_RECORDS]
    private.connect(programs, source, api_key)
    with running(source, remove_saved=True):
        prepare_native_snapshot(private, call, source, database)
        if automatic:
            automatic_supported(call, eligible=True)
            preimage = auto_reclaim_preimage(private, call, database, peer)
        snapshot_id = publish_native_snapshot(private, programs, call, source, database)
        if automatic:
            first_tail = check_auto_reclaim(private, call, source, database, snapshot_id, preimage)
            expected_records.append(NEWER_THAN_SNAPSHOT)
        else:
            reclaim_compatibility_prefix(private, call, database, snapshot_id, history_supported)
        expect_cut(private, call, snapshot_id)
        current_head = SNAPSHOT_THROUGH + int(automatic)
        final_head = FINAL_HEAD + int(automatic)
        write_offline_tail(private, call, peer, database, current_head)
        old_inode = owned_directory_inode(peer.home / "data")
        private.connect(programs, peer, api_key)
        with running(peer, remove_saved=True):
            wait_reopen(private, peer, database, snapshot_id, writer, old_inode, peer.instance_id, final_head)
            private.wait_prefix(peer, database, final_head, "private_native_peer_final_position")
            private.wait_prefix(source, database, final_head, "private_native_source_final_position")
            private.check_progress(peer, database, 8, final_head)
            private.check_progress(source, database, 8 + int(automatic), final_head)
            private.expect_records(peer, database, expected_records, "private_native_peer_final_records")
            private.expect_records(source, database, expected_records, "private_native_source_final_records")
    if automatic:
        private.connect(programs, source, api_key)
        with running(source, remove_saved=True):
            private.expect_records(source, database, expected_records, "native_automatic_reclaim_restarted_records")
            state = call("snapshot/bytes", {"action": "automatic-retention"})
            require(
                automatic_floor(private, state, snapshot_id) == SNAPSHOT_THROUGH,
                "native_automatic_reclaim_final_floor",
            )
            entries, _ = reclaimed_storage(database, through=final_head)
            require(
                set(entries) == {entry_key(seq) for seq in range(current_head, final_head + 1)}
                and entries[entry_key(current_head)] == first_tail,
                "native_automatic_reclaim_final_tail",
            )
    check_tail(private, call, source, peer, automatic=automatic)
    summary(
        "The lagging device uploaded all four offline edits before reopening from the native "
        "snapshot. It kept its device identity and recovered the snapshot-only record, the "
        "latest of its two offline edits to the same key, and both disjoint records."
    )
    return final_head, expected_records


def run_native_cutoff(programs, root, guard, owner, *, require_automatic):
    import private_recovery as private
    from run import post

    verify_cloud_target(guard)
    attempt = current_attempt()
    database = guard.allow_stream(attempt.database)
    summary(f"database_id={database}")
    endpoint = dev_endpoint()

    def call(operation, fields, *, expected_error=None, allow_unknown_outcome=False, allow_older_service=False):
        body = json.dumps({**fields, "database_id": database}, separators=(",", ":")).encode("utf-8")
        try:
            status, result, _ = post(
                endpoint, owner.key, database, "POST", f"{private.ROOT}/{operation}", body, owner.api_key, owner.generation
            )
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException):
            if allow_unknown_outcome:
                return None
            raise OperationError("private_native_signed_transport") from None
        if (
            allow_older_service
            and operation == "snapshot/bytes"
            and fields in ({"action": "retention"}, {"action": "index-status"}, {"action": "automatic-retention"})
            and status == 400
            and result == {"code": "invalid_input", "field": "http.body"}
        ):
            return None
        error_code = result.get("code") if isinstance(result, dict) else None
        if allow_unknown_outcome:
            if status in (502, 504) or (status == 503 and error_code in (None, "retry_later", "outcome_unknown")):
                return None
            if status == 409 and result == {"code": "predecessor_conflict", "field": "snapshot.retention.floor"}:
                return None
        require(
            isinstance(result, dict) and (200 <= status < 300 if expected_error is None else status == expected_error),
            "private_native_signed_" + operation.replace("/", "_"),
        )
        return result

    for result in (call("create-database", {"kind": "personal"}), call("access", {})):
        require(result.get("state") == "native" and result.get("database_id") == database, "private_native_registration")
    start_tracking(call, attempt)
    automatic = automatic_supported(call, eligible=False, allow_older_service=True)
    require(automatic or not require_automatic, "native_automatic_reclaim_required")
    history_supported = index_status(call, database, None, allow_older_service=True)
    if not history_supported:
        summary("history_index=unsupported signed_bulk_reclaim=not_exercised")
    credentials = OwnedNode(root / "connection", programs, account=SNAPSHOT_DEV_ACCOUNT)
    credentials.home.mkdir(mode=0o700)
    write_private(credentials.home / "identity.key", owner.key.private.private_bytes_raw())
    private.connect(programs, credentials, owner.api_key)
    source, peer = (
        OwnedNode(root / name, programs, account=SNAPSHOT_DEV_ACCOUNT, isolated_copy=True, keep_saved_credentials=True)
        for name in ("source", "peer")
    )
    private.prepare_source(source, database, owner.key)
    private.connected_exchange(programs, call, database, owner.phrase, owner.api_key, credentials, source, peer)
    records = private.reconnect(call, database, source, peer)
    page = call("list", {"after": "0", "limit": "20"})
    require(private.decimal(page["next"]) == 10, "private_native_starting_head")
    private.check_devices(page, private.device_id(source), private.device_id(peer))
    final_head, expected = catch_up(
        private,
        programs,
        call,
        database,
        owner.api_key,
        source,
        peer,
        records,
        history_supported=history_supported,
        automatic=automatic,
    )

    restored = OwnedNode(root / "restored", programs, account=SNAPSHOT_DEV_ACCOUNT)
    try:
        receipt = programs.command(
            credentials.home,
            ["backup", "restore", "--database", database, "--recovery-phrase-stdin", "--into", str(restored.home), "--json"],
            input_bytes=owner.phrase,
            timeout=100,
        )
    except OperationError as error:
        raise OperationError("private_native_fresh_restore", cli_refusal=error.cli_refusal) from None
    require(
        receipt["database_id"] == database
        and receipt["name"] == "personal"
        and receipt["through_delivery_seq"] == final_head
        and receipt["snapshot_through"] == SNAPSHOT_THROUGH
        and receipt["replayed"] == final_head - SNAPSHOT_THROUGH,
        "private_native_fresh_restore",
    )
    device = private.device_id(restored)
    require(
        device not in (private.device_id(source), private.device_id(peer))
        and call("head", {"device_id": device}) == {"last_writer_seq": "0"}
        and read_json(restored.home / "log-sync" / database / "session.json", 4096) == {"cursor": final_head},
        "private_native_fresh_positions",
    )
    with running(restored):
        require(private.read_fields(restored, database, expected) == [[row] for row in expected], "private_native_fresh_records")
    if automatic:
        summary(
            f"automatic_reclaim=verified floor={SNAPSHOT_THROUGH} "
            f"retained_delivery={SNAPSHOT_THROUGH + 1} accepted_facts=unchanged"
        )
    summary(
        f"native_cutoff={SNAPSHOT_THROUGH} delivery_cursor={final_head} offline_edits=4 "
        f"fresh_replayed={final_head - SNAPSHOT_THROUGH}"
    )
    summary("native snapshot cutoff recovery passed")
