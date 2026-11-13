"""Sync a private database between real owner devices and recover it in DEV."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
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
from support import (CREDENTIALS_FILE, OperationError, canonical_uuid, failure, private_directory,
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
RESET_REMOTE = {**FIELDS, "note": "first device's edit included in the snapshot"}
RESET_LOCAL = {**FIELDS, "note": "second device's unpublished edit during snapshot reset"}
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
            and receipt["through_delivery_seq"] == through, stage)


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
        wait_prefix(source, database, 2, "private_source_seed_ack")
        restore(programs, credentials.home, peer.home, database, phrase, 2,
                stage="private_peer_seed_restore")
        connect(programs, peer, api_key)
        with running(peer):
            write(source, database, SOURCE_ONLINE)
            expect_records(peer, database, [SOURCE_ONLINE], "private_peer_download")
            wait_prefix(peer, database, 3, "private_peer_download_ack")
            write(peer, database, PEER_ONLINE)
            expect_records(source, database, [PEER_ONLINE], "private_peer_upload")
            wait_prefix(source, database, 4, "private_source_download_ack")
            wait_prefix(peer, database, 4, "private_peer_upload_ack")


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
            wait_prefix(source, database, 10, "private_source_reconnect_ack")
            wait_prefix(peer, database, 10, "private_peer_reconnect_ack")
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


def check_writer_tips(call, first, second, restored):
    require(restored not in (first, second), "private_restored_device_identity")
    for device, sequence in ((first, 7), (second, 5 + PUBLISH_PAGE_SIZE), (restored, 0)):
        head = call("head", {"device_id": device})
        require(decimal(head["last_writer_seq"]) == sequence, "private_trimmed_writer_tip")


def checkpoint_peer(root, peer, database):
    require(peer.process is None and not peer.socket.exists() and peer.clean_stops > 0,
            "private_checkpoint_clean_stop")
    require(not os.path.lexists(peer.home / CREDENTIALS_FILE), "private_checkpoint_no_credentials")
    private_directory(peer.home)
    lagging = OwnedNode(root / "lagging", peer.programs, account=peer.account, isolated_copy=True,
                        keep_saved_credentials=True)
    shutil.copytree(peer.home, lagging.home, symlinks=True)
    private_directory(lagging.home)
    scope = lagging.home / "log-sync" / database
    require(device_id(lagging) == device_id(peer)
            and read_json(scope / "session.json", 4096) == {"cursor": 10}
            and not (scope / "snapshot-restore.json").exists(), "private_checkpoint_cursor")
    return lagging


def cut_after_checkpoint(call, database, source, peer):
    replace_record(source, database, PEER_LATER, RESET_REMOTE)
    expect_records(peer, database, [RESET_REMOTE], "private_peer_cut_read")
    wait_prefix(source, database, 11, "private_source_cut_ack")
    wait_prefix(peer, database, 11, "private_peer_cut_ack")
    page = call("list", {"device_id": "private-proof", "after": "0", "limit": "20"})
    require(decimal(page["next"]) == len(page["entries"]) == 11
            and page["entries"][-1]["entry"]["device_id"] == device_id(source)
            and decimal(page["entries"][-1]["entry"]["writer_seq"]) == 7, "private_cut_entry")
    ref = verify_snapshot(call, page["entries"], 11)
    trim = call("trim", {})
    require(decimal(trim["through"]) == 11 and trim["snapshot_due"] is False,
            "private_trimmed_prefix")
    return ref


def write_reset_tail(call, database, lagging, creates):
    with running(lagging):
        rows = read_rows(lagging, database, [PEER_LATER])[0]
        require(len(rows) == 1 and rows[0]["fields"] == PEER_LATER, "private_checkpoint_old_value")
        mutations = [create(SCHEMA, record_key(fields["id"]), fields) for fields in creates]
        mutations.append(replace(SCHEMA, rows[0], RESET_LOCAL))
        reply = lagging.call(f"/api/databases/{database}/mutations/batch", {"mutations": mutations})
        require(reply["ok"] is True and reply["durability"] == "durable"
                and reply["operations"] == {"created": PUBLISH_PAGE_SIZE, "updated": 1,
                                           "deleted": 0, "total": PUBLISH_PAGE_SIZE + 1},
                "private_reset_tail_count")
        written = read_rows(lagging, database, [RESET_LOCAL])[0]
        require(len(written) == 1 and written[0]["fields"] == RESET_LOCAL,
                "private_reset_tail_visible_offline")
        revision = written[0]["revision"]
    require(cloud_head(call) == 11, "private_reset_tail_unpublished")
    return revision


def reset_live_peer(programs, call, root, database, api_key, source, peer, records):
    lagging = checkpoint_peer(root, peer, database)
    creates = [{"id": f"reset-tail-{index:02d}", "note": "saved before snapshot reset"}
               for index in range(PUBLISH_PAGE_SIZE)]
    final_head = 12 + PUBLISH_PAGE_SIZE
    expected = [RESET_LOCAL, *records[1:], *creates]
    connect(programs, source, api_key)
    connect(programs, peer, api_key)
    with running(source, remove_saved=True):
        with running(peer, remove_saved=True):
            ref = cut_after_checkpoint(call, database, source, peer)
        revision = write_reset_tail(call, database, lagging, creates)
        connect(programs, lagging, api_key)
        with running(lagging, remove_saved=True):
            journal = lagging.home / "log-sync" / database / "snapshot-restore.json"
            retry("private_live_snapshot_installed", lambda: read_json(journal) if journal.is_file() else {},
                  lambda log: log.get("complete") is True and log.get("tip_ref") == ref
                  and log.get("through_delivery_seq") == 11)
            wait_prefix(lagging, database, final_head, "private_reset_peer_ack")
            wait_prefix(source, database, final_head, "private_reset_source_ack")
            expect_records(lagging, database, expected, "private_reset_peer_records")
            expect_records(source, database, expected, "private_reset_source_records")
            latest = read_rows(lagging, database, [RESET_LOCAL])[0]
            require(len(latest) == 1 and latest[0]["revision"] == revision,
                    "private_reset_pending_revision_unchanged")
    page = call("list", {"device_id": "private-proof", "after": "11", "limit": "90"})
    entries = page["entries"]
    writer = device_id(lagging)
    require(decimal(page["next"]) == final_head
            and [(decimal(item["delivery_seq"]), item["entry"]["device_id"],
                  decimal(item["entry"]["writer_seq"])) for item in entries]
            == [(seq, writer, seq - 7) for seq in range(12, final_head + 1)],
            "private_reset_uploaded_tail")
    summary("A device restored its cloud snapshot after reopening an older local copy. The "
            "snapshot did not rewrite its pending edit, and all 65 local edits reached the cloud.")
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
        page = call("list", {"device_id": "private-proof", "after": "0", "limit": "20"})
        through = decimal(page["next"])
        require(through == len(page["entries"]) == 10, "private_log_entries")
        first, second = device_id(source), device_id(peer)
        check_devices(page, first, second)
        through, records = reset_live_peer(programs, call, root, database, api_key, source, peer, records)
        restored = OwnedNode(root / "restored", programs, account=account)
        restore(programs, credentials.home, restored.home, database, phrase, through,
                stage="private_snapshot_suffix_restore")
        check_writer_tips(call, first, second, device_id(restored))
        with running(restored):
            require(read_fields(restored, database, records) == [[row] for row in records],
                    "private_recovered_record")
    summary("After trimming the acknowledged prefix, the cloud reported each device's own "
            "write count. A fresh LastDB home with no writes restored the signed snapshot and "
            "the later edits, and read both devices' records by key.")


def verify_snapshot(call, entries, through):
    snapshot = call("snapshot", {})
    manifest = call("delivery/snapshot", {})
    ref = snapshot["snapshot_ref"]
    require(manifest["snapshot_ref"] == ref and decimal(manifest["through_delivery_seq"]) == through,
            "private_snapshot_prefix")
    part = call("delivery/snapshot", {"snapshot_ref": ref, "part": "0"})
    require([item["payload"] for item in json.loads(part["part"])]
            == [item["entry"]["payload_b64"] for item in entries], "private_snapshot_entries")
    ack = call("ack", {"device_id": "private-proof", "through": str(through)})
    require(decimal(ack["through"]) == through, "private_snapshot_ack")
    return ref


def recover(post, endpoint, database, owner, api_key, generation, ledger):
    os.umask(0o077)

    def call(operation, fields, *, key=owner, scope_generation=generation, allowed=True):
        body = json.dumps({"database_id": database, **fields}, separators=(",", ":")).encode()
        status, response, _ = post(
            endpoint, key, database, "POST", f"{ROOT}/{operation}", body, api_key,
            scope_generation)
        ledger.record("read" if operation in ("head", "list", "access", "delivery/snapshot")
                      else "write", len(body))
        if allowed and 200 <= status < 300 and isinstance(response, dict):
            return response
        if not allowed and status in (401, 403) and isinstance(response, dict):
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
        ("list", {"device_id": "other", "after": "0", "limit": "2"}),
        ("delivery/snapshot", {}),
    ):
        call(operation, fields, key=other, allowed=False)
    call("head", {}, scope_generation=str(int(generation) + 1), allowed=False)
    summary("An unrelated key could not read, append, or fetch the private snapshot.")
