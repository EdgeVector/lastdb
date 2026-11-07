"""Exercise remote writer tails, a returning writer, and a fresh device."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time

import native_records
import shared_snapshot
from automatic_source import durable_batch
from baseline import private_write
from programs import execute
from node import Connection, OwnedNode
from support import (CREDENTIALS_FILE, OperationError, decode_json, hash_file,
                     private_directory, require, save_json)


WRITER_COUNT = 7
LOCAL_SCHEMA = "CloudProofLocalOnly"
LOCAL_RECORD = {"id": "local-only"}
SNAPSHOT_WRITE = "during-snapshot"
PEER_TAIL = "after-snapshot-peer"
REJOIN_PENDING = "returning-pending"
REJOIN_NEXT = "returning-next"


def empty_home(operation, name):
    home = operation.root / name
    home.mkdir(mode=0o700)
    private_write(home / "identity.key", operation.account_seed)
    publisher = name == "first"
    node = OwnedNode(home, operation.programs, isolated_copy=operation.args.multiwriter and publisher)
    operation.nodes.append(node)
    node.start(shared_schema=operation.runtime_schema())
    node.stop()
    return node


def refuse_empty_join(operation, publisher, writer):
    operation.summary["stage"] = "refuse-empty-writer"
    node = empty_home(operation, "empty-join")
    operation.writer(node, writer, "install", "--schema-bindings", str(operation.bindings))
    for identity, flags, reason in (
        (writer, (), "shared_restore_required"),
        (publisher, ("--initialize-publisher",), "shared_publisher_restore_required"),
    ):
        try:
            operation.bind_writer(node, identity, *flags)
        except OperationError as error:
            require(str(error) == "product_cli_refused" and error.cli_refusal is not None
                    and error.cli_refusal["reason"] == reason, "shared_exact_empty_writer_refusal")
        else:
            raise OperationError("shared_empty_writer_accepted")
    operation.summary["empty_writer_refused"] = True


def refuse_unbound_write(operation, node):
    record = operation.record("premature-write", "before-writer-binding")
    mutation = native_records.create(operation.schema, native_records.key("premature-write"), record)
    body = json.dumps({"mutations": [mutation]}, separators=(",", ":")).encode()
    connection = Connection(node.socket)
    try:
        connection.request("POST", "/api/mutations/batch", body,
                           {"X-LastDB-Client": "cloud-backup-ci", "Content-Type": "application/json"})
        response = connection.getresponse()
        require(response.status == 400 and
                b"shared schema writes require an active writer binding" in response.read(4096),
                "shared_exact_unbound_write_refusal")
    finally:
        connection.close()
    operation.summary["unbound_write_refused"] = True


def seed_local_only(operation, node):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": LOCAL_SCHEMA, "descriptive_name": LOCAL_SCHEMA,
        "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": ["id"], "field_types": {"id": "String"},
        "field_descriptions": {"id": "Device-local record outside the shared disk files."}}})
    key = native_records.key(LOCAL_RECORD["id"])
    durable_batch(node, [native_records.create(LOCAL_SCHEMA, key, LOCAL_RECORD)])
    row, = native_records.read_records(node, LOCAL_SCHEMA, [key])
    require(row["fields"] == LOCAL_RECORD and has_local_only_schema(node),
            "shared_local_only_source_record")
    payload = operation.payload("private-local-file")
    digest = hashlib.sha256(payload).hexdigest()
    path = operation.root / "private-file.bin"
    data = node.home / "data"
    public_cas = data / "disk-cas/CURRENT"
    private = data / "disk-private/CURRENT"
    public_before, private_before = hash_file(public_cas), hash_file(private)
    private_write(path, payload)
    try:
        reply = native_records.put_file(node, LOCAL_SCHEMA, LOCAL_RECORD, path, target=key)
        require(reply["file"]["sha256"] == digest and hash_file(public_cas) == public_before
                and hash_file(private) != private_before, "shared_local_only_file_routed_privately")
    finally:
        path.unlink()
    proof = {"schema": LOCAL_SCHEMA, "records": [LOCAL_RECORD],
             "file_bytes": len(payload), "file_sha256": digest}
    node.verify_file(proof)
    operation.summary["local_only_file_stored_privately"] = True
    return proof


def has_local_only_schema(node):
    reply = node.call("/api/schemas?include_system=true")
    schemas = reply.get("data", reply)["schemas"]
    return any(schema["name"] == LOCAL_SCHEMA for schema in schemas)


def extra_records(operation):
    return {f"peer-{index}": operation.record(f"peer-{index}", "independent-tail")
            for index in range(2, len(operation.writers) - 1)}


def snapshot_record(operation):
    return operation.record(SNAPSHOT_WRITE, "acknowledged-during-publication")


def snapshot_command(operation, node, publisher):
    operation.summary["stage"] = "snapshot-command-live-upload"
    workspace = operation.root / "snapshot-command"
    args = shared_snapshot.arguments(operation, node, publisher, workspace)
    path = node.home / CREDENTIALS_FILE
    target = native_records.key(SNAPSHOT_WRITE)
    expected = snapshot_record(operation)
    node.start()
    save_json(path, operation.credentials)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            operation.programs.verify()
            before_upload = shared_snapshot.source_file_identities(node)
            pending = pool.submit(execute, args, operation.programs.environment, b"", 120)
            shared_snapshot.wait_for_upload(workspace, pending)
            originals, counts = shared_snapshot.observe_originals(node, workspace, before_upload)
            operation.summary["snapshot_original_file_inodes"] = sum(counts.values())
            durable_batch(node, [native_records.create(operation.schema, target, expected)])
            row, = native_records.read_records(node, operation.schema, [target])
            shared_snapshot.check_frozen(originals)
            require(row["fields"] == expected and shared_snapshot.upload_pending(workspace),
                    "shared_write_visible_during_upload")
            operation.summary.update(live_write_during_upload=True, snapshot_frozen_files_unchanged=True)
            receipt = decode_json(pending.result())
        through = operation.summary["publisher_tail_through"] + 1
        shared_snapshot.check_receipt(node, receipt, through)
        require(shared_snapshot.provider_counts(receipt) == counts
                and receipt["snapshot_id"] != operation.snapshot, "shared_snapshot_exact_provider_counts")
        require(decode_json(execute(args, operation.programs.environment, b"", 120)) == receipt,
                "shared_snapshot_command_exact_retry")
    finally:
        operation.summary["snapshot_command_workspace_created"] = workspace.is_dir()
        operation.summary["snapshot_command_upload_started"] = (
            workspace / shared_snapshot.CHECKPOINT / shared_snapshot.INTENT).is_file()
        path.unlink()
    node.stop()
    node.start()
    row, = native_records.read_records(node, operation.schema, [target])
    require(row["fields"] == expected, "shared_snapshot_command_durable_after_restart")
    node.stop()
    operation.assert_owned_root()
    private_directory(workspace)
    shutil.rmtree(workspace)
    operation.summary.update(replacement_snapshot_id=receipt["snapshot_id"],
                             snapshot_command_uploaded_through=through,
                             snapshot_provider_files=counts, snapshot_original_hold_released=True,
                             live_write_after_restart=True, snapshot_command_exact_retry=True)


def peer_tail_record(operation):
    return operation.record(PEER_TAIL, "peer-after-replacement")


def publish_peer_tail(operation, node, writer):
    operation.summary["stage"] = "publish-peer-tail-after-replacement"
    node.start()
    durable_batch(node, [native_records.create(operation.schema, native_records.key(PEER_TAIL),
                                               peer_tail_record(operation))])
    node.stop()
    through = operation.summary["peer_tail_through"] + 1
    operation.summary["peer_after_replacement_through"] = operation.push(node, writer, through)


def rejoin_records(operation):
    return {REJOIN_PENDING: operation.record(REJOIN_PENDING, "authored-before-rejoin"),
            REJOIN_NEXT: operation.record(REJOIN_NEXT, "authored-after-rejoin")}


def rejoin_data_directory(path):
    metadata = path.lstat()
    require(stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid()
            and metadata.st_mode & 0o022 == 0, "shared_returning_data_directory")
    return metadata


def rejoin_writer(operation, node, writer, local_file):
    operation.summary["stage"] = "author-pending-returning-writer"
    records = rejoin_records(operation)
    node.start()
    missed, = node.call("/api/queries/batch", {"queries": [{
        "schema_name": operation.schema, "filter": {"HashKey": PEER_TAIL}, "limit": 2, "offset": 0,
    }]})["results"]
    require(missed["status"] == 200 and missed["response"]["ok"] is True
            and missed["response"]["returned_count"] == 0 and missed["response"]["results"] == [],
            "shared_returning_writer_missed_peer_tail")
    pending = native_records.key(REJOIN_PENDING)
    durable_batch(node, [native_records.create(operation.schema, pending, records[REJOIN_PENDING])])
    digest = operation.attach(node, records[REJOIN_PENDING], REJOIN_PENDING)
    node.stop()

    operation.summary["stage"] = "rejoin-returning-writer"
    data = node.home / "data"
    original = rejoin_data_directory(data)
    through = operation.summary["snapshot_command_uploaded_through"] + 2
    reply = operation.writer(node, writer, "rejoin", "--batches", "2").splitlines()
    prefix = b"Shared writer rejoined. Previous data is at "
    require(len(reply) == 3 and reply[:2] == [f"Accepted shared writer position {through}".encode(),
                                              b"Shared writer caught up"]
            and reply[2].startswith(prefix), "shared_returning_writer_accepted_pending_batch")
    previous = Path(reply[2][len(prefix):].decode())
    require(previous != data and previous.name == "data" and previous.is_relative_to(node.home)
            and previous.resolve(strict=True) == previous, "shared_returning_previous_data_path")
    retained, active = rejoin_data_directory(previous), rejoin_data_directory(data)
    require((retained.st_dev, retained.st_ino) == (original.st_dev, original.st_ino)
            and (active.st_dev, active.st_ino) != (original.st_dev, original.st_ino),
            "shared_returning_previous_data_retained")

    operation.summary["stage"] = "verify-returning-writer-and-append"
    node.start()
    local, = native_records.read_records(node, LOCAL_SCHEMA, [native_records.key(LOCAL_RECORD["id"])])
    require(local["fields"] == LOCAL_RECORD, "shared_returning_private_record")
    node.verify_file(local_file)
    rows = native_records.read_records(node, operation.schema, [pending, native_records.key(PEER_TAIL)])
    require([row["fields"] for row in rows] == [records[REJOIN_PENDING], peer_tail_record(operation)],
            "shared_returning_pending_and_remote_records")
    node.verify_file({"schema": operation.schema, "records": [{"id": REJOIN_PENDING}],
                      "file_bytes": len(operation.payload(REJOIN_PENDING)), "file_sha256": digest})
    durable_batch(node, [native_records.create(operation.schema, native_records.key(REJOIN_NEXT),
                                               records[REJOIN_NEXT])])
    node.stop()
    operation.push(node, writer, through + 1)
    operation.summary.update(returning_writer_stream=writer["source_stream"],
                             returning_writer_pending_through=through,
                             returning_writer_next_through=through + 1,
                             returning_writer_private_file=True, returning_writer_previous_data=True)


def pull(operation, node, writer):
    return operation.writer(node, writer, "pull", "--schema-bindings", str(operation.bindings),
                            "--ranges", "1").splitlines()


def exchange_tails(operation, publisher, publisher_writer, peer_writer):
    operation.summary["stage"] = "publish-independent-writer-tails"
    records = extra_records(operation)
    positions = {peer_writer["source_stream"]: 3}
    for writer, (kind, record) in zip(operation.writers[2:-1], records.items(), strict=True):
        node = operation.join_writer(kind, writer)
        node.start()
        durable_batch(node, [native_records.create(operation.schema, native_records.key(kind), record)])
        node.stop()
        require(node.clean_stops == 2, "shared_independent_writer_clean_stops")
        positions[writer["source_stream"]] = operation.push(node, writer, 1)

    operation.summary["stage"] = "pull-five-remote-writer-tails"
    for stream, sequence in sorted(positions.items()):
        require(pull(operation, publisher, publisher_writer) == [
            f"Applied shared peer {stream} through {sequence}".encode(),
            b"Applied 1 shared peer ranges",
        ], "shared_multiwriter_exact_resumed_pull")
    require(pull(operation, publisher, publisher_writer) == [b"Applied 0 shared peer ranges"],
            "shared_idle_writer_holds_delete_cleanup")
    operation.summary.update(writer_count=WRITER_COUNT, peer_applied_positions=positions,
                             idle_writer_held_cleanup=True)

    publisher.start()
    kinds = ["peer-only", *records]
    rows = native_records.read_records(publisher, operation.schema, [native_records.key(k) for k in kinds])
    records["peer-only"] = operation.record("peer-only", "peer-tail")
    require([row["fields"] for row in rows] == [records[kind] for kind in kinds],
            "shared_multiwriter_exact_live_import")
    publisher.stop()


def join_restored_writer(operation, node, reader):
    operation.summary["stage"] = "bind-restored-idle-writer"
    reply = operation.bind_writer(node, reader)
    require(reply.splitlines() == [b"Shared writer bound"], "shared_restored_idle_writer_bound")
    operation.summary["stage"] = "push-restored-writer-and-cleanup"
    record = operation.record("restored-writer", "first-write")
    node.start()
    durable_batch(node, [native_records.create(operation.schema, native_records.key("restored-writer"), record)])
    node.stop()
    operation.push(node, reader, 1)
    require(pull(operation, node, reader) == [
        b"Reclaimed 1 shared delete notes", b"Applied 0 shared peer ranges",
    ], "shared_delete_reclaimed_after_all_writers_advanced")
    operation.summary.update(restored_writer_accepted_through=1, local_only_excluded=True,
                             delete_notes_reclaimed=1)
