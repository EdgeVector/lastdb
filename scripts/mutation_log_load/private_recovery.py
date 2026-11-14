"""Sync a private database between real owner devices and recover it in DEV."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cloud_backup"))

from drive import LoadError
from native_records import create, key as record_key, replace
from node import OwnedNode
from node_auth import NodeKey, account_id
from programs import Programs
from routes import ROOT, decimal
from signed_files import retry, running, summary
from support import (CREDENTIALS_FILE, OperationError, canonical_uuid, failure,
                     read_json, require, save_json, write_private)

SCHEMA = "PrivateRecovery"
FIELDS = {"id": "signed-recovery", "note": "owner-only DEV record"}
SOURCE_ONLINE = {"id": "source-online", "note": "first device published while connected"}
PEER_ONLINE = {"id": "peer-online", "note": "second device published while connected"}
SOURCE_OFFLINE = {"id": "source-offline", "note": "first device saved before reconnecting"}
PEER_OFFLINE = {"id": "peer-offline", "note": "second device saved before reconnecting"}
SOURCE_EARLIER = {**FIELDS, "note": "first device's older edit"}
PEER_LATER = {**FIELDS, "note": "second device's newer edit"}
PEER_EARLIER = {**SOURCE_ONLINE, "note": "second device's older edit"}
SOURCE_LATER = {**SOURCE_ONLINE, "note": "first device's newer edit"}
LAGGING_REMOTE = {**FIELDS, "note": "first device's edit while peer was offline"}
LAGGING_LOCAL = {**FIELDS, "note": "second device's local edit while offline"}
# The publisher submits at most one 64-entry page before attempting a pull.
PUBLISH_PAGE_SIZE = 64
# The batch query endpoint caps each call at 64 keys.
QUERY_BATCH_SIZE = 64


def connect(programs, node, api_key):
    receipt = programs.command(node.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", node.account, "--json",
    ], input_bytes=api_key.encode(), timeout=30)
    require(receipt["ok"] is True and receipt["credentials_saved"] is True
            and not receipt["registration_attempted"]
            and receipt["account_id"] == node.account, "private_dev_connection")


def restore(programs, credentials, destination, database, phrase, through, *, stage):
    try:
        receipt = programs.command(credentials, [
            "backup", "restore", "--database", database, "--recovery-phrase-stdin",
            "--into", str(destination), "--json",
        ], input_bytes=phrase, timeout=100)
    except OperationError as error:
        raise OperationError(stage, cli_refusal=error.cli_refusal) from None
    require(receipt["database_id"] == database and receipt["name"] == "personal"
            and receipt["through_delivery_seq"] == through
            and receipt["snapshot_through"] == 0 and receipt["replayed"] == through, stage)


def prepare_source(node, database, owner):
    """Give the generated first home the run-owned ID before the daemon opens it."""
    folder = node.home / "databases"
    folder.mkdir(mode=0o700, parents=True)
    write_private(node.home / "identity.key", owner.private.private_bytes_raw())
    save_json(folder / "registry.json", {"format": 1, "owned": [], "owner": {
        "id": database, "kind": "personal", "name": "personal", "spec": None,
    }})


def seed(source, database):
    route = f"/api/databases/{database}"
    source.call(f"{route}/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": list(FIELDS), "field_types": dict.fromkeys(FIELDS, "String"),
        "field_descriptions": {name: f"{SCHEMA} {name}" for name in FIELDS},
    }})
    write(source, database, FIELDS)


def write(node, database, fields):
    mutation = create(SCHEMA, record_key(fields["id"]), fields)
    node.call(f"/api/databases/{database}/mutation", mutation)


def read_rows(node, database, records):
    queries = [{"schema_name": SCHEMA, "filter": {"HashKey": fields["id"]}, "limit": 2}
               for fields in records]
    batch = node.call(f"/api/databases/{database}/queries/batch", {"queries": queries})["results"]
    require(len(batch) == len(records), "private_keyed_read_count")
    result = []
    for fields, reply in zip(records, batch):
        response = reply.get("response")
        require(reply.get("status") == 200 and isinstance(response, dict)
                and response.get("ok") is True and response.get("has_more") is False,
                "private_keyed_read")
        rows = response["results"]
        require(all(row["key"] == record_key(fields["id"]) for row in rows),
                "private_keyed_read_identity")
        result.append(rows)
    return result


def read_fields(node, database, records):
    fields = []
    for start in range(0, len(records), QUERY_BATCH_SIZE):
        rows = read_rows(node, database, records[start:start + QUERY_BATCH_SIZE])
        fields.extend([row["fields"] for row in matched] for matched in rows)
    return fields


def replace_record(node, database, before, after):
    rows = read_rows(node, database, [before])[0]
    require(len(rows) == 1 and rows[0]["fields"] == before, "private_conflict_preimage")
    request = replace(SCHEMA, rows[0], after)
    node.call(f"/api/databases/{database}/mutation", request)


def expect_records(node, database, records, stage):
    return retry(stage, lambda: read_fields(node, database, records),
                 lambda rows: rows == [[fields] for fields in records])


def cloud_head(call):
    value = call("head", {})["cursor"]
    return 0 if value is None else decimal(value)


def wait_head(call, through, stage):
    retry(stage, lambda: cloud_head(call), lambda head: head >= through)


def wait_prefix(node, database, through, stage):
    path = node.home / "log-sync" / database / "prefix.json"
    retry(stage, lambda: read_json(path, 4096) if path.is_file() else {},
          lambda mark: mark.get("database_id") == database and mark.get("through") == through)


def connected_exchange(programs, call, database, phrase, api_key, credentials, source, peer):
    connect(programs, source, api_key)
    with running(source):
        seed(source, database)
        wait_head(call, 2, "private_cloud_prefix")
        wait_prefix(source, database, 2, "private_source_seed_download")
        restore(programs, credentials.home, peer.home, database, phrase, 2,
                stage="private_peer_seed_restore")
        connect(programs, peer, api_key)
        with running(peer):
            write(source, database, SOURCE_ONLINE)
            expect_records(peer, database, [SOURCE_ONLINE], "private_peer_download")
            wait_prefix(peer, database, 3, "private_peer_download_position")
            write(peer, database, PEER_ONLINE)
            expect_records(source, database, [PEER_ONLINE], "private_peer_upload")
            wait_prefix(source, database, 4, "private_source_download_position")
            wait_prefix(peer, database, 4, "private_peer_sees_its_upload")


@contextmanager
def offline_devices(source, peer):
    """Start with local capture enabled and refuse these child daemons' HTTPS."""
    with socket.socket() as unavailable:
        # Keep this port bound without listening so nothing can take it.
        unavailable.bind(("127.0.0.1", 0))
        proxy = f"http://127.0.0.1:{unavailable.getsockname()[1]}"
        environment = dict(source.programs.environment)
        for name in tuple(environment):
            if name.lower() in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
                environment.pop(name)
        environment.update(HTTPS_PROXY=proxy, ALL_PROXY=proxy)
        previous = source.programs.environment
        source.programs.environment = environment
        try:
            with running(source), running(peer):
                yield
        finally:
            source.programs.environment = previous


def write_offline(call, database, source, peer):
    with offline_devices(source, peer):
        write(source, database, SOURCE_OFFLINE)
        write(peer, database, PEER_OFFLINE)
        replace_record(source, database, FIELDS, SOURCE_EARLIER)
        replace_record(peer, database, SOURCE_ONLINE, PEER_EARLIER)
        # Both daemons share the runner wall clock. Separate the two rounds so
        # the accepted author milliseconds differ without relying on upload order.
        earlier = time.time_ns()
        time.sleep(0.05)
        require(time.time_ns() - earlier >= 25_000_000, "private_conflict_clock_spacing")
        replace_record(source, database, SOURCE_ONLINE, SOURCE_LATER)
        replace_record(peer, database, FIELDS, PEER_LATER)
        require(read_fields(source, database, [SOURCE_OFFLINE, PEER_OFFLINE, FIELDS, SOURCE_ONLINE]) ==
                [[SOURCE_OFFLINE], [], [SOURCE_EARLIER], [SOURCE_LATER]], "private_source_saved_offline")
        require(read_fields(peer, database, [SOURCE_OFFLINE, PEER_OFFLINE, FIELDS, SOURCE_ONLINE]) ==
                [[], [PEER_OFFLINE], [PEER_LATER], [PEER_EARLIER]], "private_peer_saved_offline")
        require(cloud_head(call) == 4, "private_offline_head_unchanged")
    require(cloud_head(call) == 4, "private_offline_shutdown_head_unchanged")


def reconnect(call, database, source, peer):
    records = [PEER_LATER, SOURCE_LATER, PEER_ONLINE, SOURCE_OFFLINE, PEER_OFFLINE]
    write_offline(call, database, source, peer)
    with running(source, remove_saved=True):
        wait_prefix(source, database, 7, "private_source_reconnect")
        with running(peer, remove_saved=True):
            expect_records(peer, database, records, "private_peer_reconnect_records")
            expect_records(source, database, records, "private_source_reconnect_records")
            wait_prefix(source, database, 10, "private_source_reconnect_download")
            wait_prefix(peer, database, 10, "private_peer_reconnect_download")
    for node in (source, peer):
        require(not os.path.lexists(node.home / CREDENTIALS_FILE), "private_restart_no_credentials")
        with running(node):
            require(read_fields(node, database, records) == [[row] for row in records],
                    "private_restart_records")
    summary("Two owner devices published and applied each other's edits, including edits "
            "saved before reconnecting. Both chose the newer authored value for two contested "
            "records even when the older edit uploaded last, and agreed after a clean restart.")
    return records


def device_id(node):
    with (node.home / "log-sync" / "device-id").open("r", encoding="ascii") as value:
        return canonical_uuid(value.read(64).strip())


def check_devices(page, first, second):
    entries = page["entries"]
    writers = [(stored["entry"]["device_id"], decimal(stored["entry"]["writer_seq"]))
               for stored in entries]
    require(first != second
            and [decimal(stored["delivery_seq"]) for stored in entries] == list(range(1, 11))
            and writers == [(first, 1), (first, 2), (first, 3), (second, 1),
                            (first, 4), (first, 5), (first, 6), (second, 2),
                            (second, 3), (second, 4)], "private_exact_two_device_log")
    authored = [decimal(stored["entry"]["authored_at_ms"]) for stored in entries]
    # Compare writes 6 and 10 for FIELDS, then writes 9 and 7 for SOURCE_ONLINE.
    require(authored[5] < authored[9] and authored[8] < authored[6],
            "private_conflict_authored_order")


def check_progress(node, database, upload, download):
    spool = node.home / "data" / "disk-run" / "accepted-v1" / "cloud-upload.json"
    state = read_json(spool, 4096)
    position = node.home / "log-sync" / database / "session.json"
    require(state.get("database_id") == database and state.get("device_id") == device_id(node)
            and state.get("writer_seq") == upload
            and read_json(position, 4096) == {"cursor": download}, "private_local_progress")


def check_entry_retry(call, entry, head):
    saved = {name: entry[name] for name in ("writer_seq", "authored_at_ms", "namespace", "payload_b64")}
    request = {"device_id": entry["device_id"], "entries": [saved]}
    duplicate = call("append", request)
    require(duplicate == {"result": "duplicate", "next_seq": str(decimal(saved["writer_seq"]) + 1),
                          "delivery_seqs": []} and cloud_head(call) == head,
            "private_exact_retry")
    changed = {**saved, "payload_b64": "AQ"}
    refused = call("append", {**request, "entries": [changed]}, expected_error=409)
    require(refused.get("code") == "identity_conflict"
            and refused.get("field") == "mutation_log.conflict" and cloud_head(call) == head,
            "private_different_edit_refused")


def advance_while_peer_offline(call, database, source):
    replace_record(source, database, PEER_LATER, LAGGING_REMOTE)
    wait_prefix(source, database, 11, "private_source_extra_download")
    page = call("list", {"after": "10", "limit": "20"})
    entries = page["entries"]
    require(decimal(page["next"]) == 11 and len(entries) == 1
            and decimal(entries[0]["delivery_seq"]) == 11
            and entries[0]["entry"]["device_id"] == device_id(source)
            and decimal(entries[0]["entry"]["writer_seq"]) == 7, "private_source_extra_entry")
    check_entry_retry(call, entries[0]["entry"], 11)


def write_lagging_tail(call, database, peer, creates):
    with running(peer):
        rows = read_rows(peer, database, [PEER_LATER])[0]
        require(len(rows) == 1 and rows[0]["fields"] == PEER_LATER, "private_lagging_old_value")
        mutations = [create(SCHEMA, record_key(fields["id"]), fields) for fields in creates]
        mutations.append(replace(SCHEMA, rows[0], LAGGING_LOCAL))
        reply = peer.call(f"/api/databases/{database}/mutations/batch", {"mutations": mutations})
        require(reply["ok"] is True and reply["durability"] == "durable"
                and reply["operations"] == {"created": PUBLISH_PAGE_SIZE, "updated": 1,
                                           "deleted": 0, "total": PUBLISH_PAGE_SIZE + 1},
                "private_lagging_tail_count")
        written = read_rows(peer, database, [LAGGING_LOCAL])[0]
        require(len(written) == 1 and written[0]["fields"] == LAGGING_LOCAL,
                "private_lagging_tail_visible_offline")
        revision = written[0]["revision"]
    require(cloud_head(call) == 11, "private_lagging_tail_unpublished")
    return revision


def reconnect_lagging_peer(programs, call, database, api_key, source, peer, records):
    require(peer.process is None and peer.clean_stops > 0, "private_lagging_clean_stop")
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_lagging_offline")
    check_progress(peer, database, 4, 10)
    creates = [{"id": f"offline-tail-{index:02d}", "note": "saved before the peer reconnected"}
               for index in range(PUBLISH_PAGE_SIZE)]
    final_head = 12 + PUBLISH_PAGE_SIZE
    expected = [LAGGING_LOCAL, *records[1:], *creates]
    connect(programs, source, api_key)
    with running(source, remove_saved=True):
        advance_while_peer_offline(call, database, source)
        revision = write_lagging_tail(call, database, peer, creates)
        check_progress(peer, database, 4, 10)
        connect(programs, peer, api_key)
        with running(peer, remove_saved=True):
            wait_prefix(peer, database, final_head, "private_lagging_peer_download")
            wait_prefix(source, database, final_head, "private_lagging_source_download")
            check_progress(source, database, 7, final_head)
            check_progress(peer, database, 5 + PUBLISH_PAGE_SIZE, final_head)
            expect_records(peer, database, expected, "private_lagging_peer_records")
            expect_records(source, database, expected, "private_lagging_source_records")
            latest = read_rows(peer, database, [LAGGING_LOCAL])[0]
            require(len(latest) == 1 and latest[0]["revision"] == revision,
                    "private_lagging_pending_revision_unchanged")
    page = call("list", {"after": "11", "limit": "90"})
    entries = page["entries"]
    writer = device_id(peer)
    require(decimal(page["next"]) == final_head
            and [(decimal(item["delivery_seq"]), item["entry"]["device_id"],
                  decimal(item["entry"]["writer_seq"])) for item in entries]
            == [(seq, writer, seq - 7) for seq in range(12, final_head + 1)],
            "private_lagging_uploaded_tail")
    summary("A device resumed from its local download position and uploaded all 65 queued "
            "edits. Applying the cloud suffix preserved the newer local edit and its revision.")
    return final_head, expected


def check_restore(programs, call, database, owner, api_key):
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE").strip().encode()
    account = account_id(owner.public)
    with tempfile.TemporaryDirectory(prefix="ldb-signed-", dir="/tmp") as temporary:
        root = Path(temporary).resolve()
        credentials = OwnedNode(root / "connection", programs, account=account)
        credentials.home.mkdir(mode=0o700)
        write_private(credentials.home / "identity.key", owner.private.private_bytes_raw())
        connect(programs, credentials, api_key)
        source = OwnedNode(root / "source", programs, account=account, isolated_copy=True,
                           keep_saved_credentials=True)
        peer = OwnedNode(root / "peer", programs, account=account, isolated_copy=True,
                         keep_saved_credentials=True)
        prepare_source(source, database, owner)
        connected_exchange(programs, call, database, phrase, api_key, credentials, source, peer)
        records = reconnect(call, database, source, peer)
        page = call("list", {"after": "0", "limit": "20"})
        through = decimal(page["next"])
        require(through == len(page["entries"]) == 10, "private_log_entries")
        first, second = device_id(source), device_id(peer)
        check_devices(page, first, second)
        check_progress(source, database, 6, 10)
        check_progress(peer, database, 4, 10)
        through, records = reconnect_lagging_peer(
            programs, call, database, api_key, source, peer, records)
        restored = OwnedNode(root / "restored", programs, account=account)
        restore(programs, credentials.home, restored.home, database, phrase, through,
                stage="private_fresh_log_restore")
        require(device_id(restored) not in (first, second), "private_restored_device_identity")
        require(call("head", {"device_id": device_id(restored)}) == {"last_writer_seq": "0"},
                "private_never_writer_contents")
        require(read_json(restored.home / "log-sync" / database / "session.json", 4096)
                == {"cursor": through}, "private_fresh_local_download")
        with running(restored):
            require(read_fields(restored, database, records) == [[row] for row in records],
                    "private_recovered_record")
    summary("Each device kept separate local upload and download positions. The cloud "
            "recognized an exact resend and refused a different edit at the same number. "
            "A fresh home replayed the retained log and read both devices' edits.")


def recover(post, endpoint, database, owner, api_key, generation, ledger):
    os.umask(0o077)

    def call(operation, fields, *, key=owner, scope_generation=generation, allowed=True,
             expected_error=None):
        body = json.dumps({"database_id": database, **fields}, separators=(",", ":")).encode()
        status, response, _ = post(
            endpoint, key, database, "POST", f"{ROOT}/{operation}", body, api_key,
            scope_generation)
        ledger.record("read" if operation in ("head", "list", "access")
                      else "write", len(body))
        if allowed and expected_error is None and 200 <= status < 300 and isinstance(response, dict):
            return response
        if not allowed and status in (401, 403) and isinstance(response, dict):
            return response
        if expected_error == status and isinstance(response, dict):
            return response
        raise LoadError(f"private {operation} returned unexpected HTTP {status}")

    for registration in (call("create-database", {"kind": "personal"}),
                         call("access", {}),
                         call("create-database", {"kind": "personal"})):
        if registration.get("state") != "native" or registration.get("database_id") != database:
            raise LoadError("private database registration did not round trip")

    programs = Programs(Path(os.environ["CLIENT_BIN_DIR"]).resolve(strict=True),
                        os.environ["CLIENT_VERSION"], {
                            name: value for name, value in os.environ.items()
                            if not name.startswith("LASTDB_CLOUD_DEV_")
                        })
    try:
        check_restore(programs, call, database, owner, api_key)
    except OperationError as error:
        raise LoadError(f"LastDB private recovery: {json.dumps(failure(error))}") from None

    other = NodeKey.generate()
    for operation, fields in (
        ("access", {}), ("head", {}), ("append", {"device_id": "other", "entries": [{
            "writer_seq": "1", "authored_at_ms": "1",
            "namespace": SCHEMA, "payload_b64": "AA",
        }]}),
        ("list", {"after": "0", "limit": "2"}),
    ):
        call(operation, fields, key=other, allowed=False)
    call("head", {}, scope_generation=str(int(generation) + 1), allowed=False)
    summary("An unrelated key could not read or append to the private log.")
