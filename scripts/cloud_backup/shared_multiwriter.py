"""Exercise five remote writer tails and a fresh device joining from restore."""

from concurrent.futures import ThreadPoolExecutor
import shutil
import time

import native_records
from automatic_source import durable_batch
from programs import execute
from support import CREDENTIALS_FILE, canonical_uuid, decode_json, private_directory, read_json, require, save_json


WRITER_COUNT = 7
LOCAL_SCHEMA = "CloudProofLocalOnly"
LOCAL_RECORD = {"id": "local-only"}
SNAPSHOT_WRITE = "during-snapshot"
PEER_TAIL = "after-snapshot-peer"


def seed_local_only(node):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": LOCAL_SCHEMA, "descriptive_name": LOCAL_SCHEMA,
        "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": ["id"], "field_types": {"id": "String"},
        "field_descriptions": {"id": "Private generated checkpoint exclusion record."}}})
    key = native_records.key(LOCAL_RECORD["id"])
    durable_batch(node, [native_records.create(LOCAL_SCHEMA, key, LOCAL_RECORD)])
    row, = native_records.read_records(node, LOCAL_SCHEMA, [key])
    require(row["fields"] == LOCAL_RECORD and has_local_only_schema(node),
            "shared_local_only_source_record")


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
    args = [str(operation.programs.directory / "lastdb"), "--data-dir", str(node.home),
            "snapshot", "--device-id", publisher["device_id"], "--device-key-file",
            str(operation.device_file(publisher["device_id"])), "--workspace", str(workspace), "--json"]
    path = node.home / CREDENTIALS_FILE
    intent = workspace / "checkpoint" / "shared-snapshot-intent.json"
    target = native_records.key(SNAPSHOT_WRITE)
    expected = snapshot_record(operation)
    node.start()
    save_json(path, operation.credentials)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            operation.programs.verify()
            pending = pool.submit(execute, args, operation.programs.environment, b"", 120)
            wait_for_upload(intent, pending)
            durable_batch(node, [native_records.create(operation.schema, target, expected)])
            row, = native_records.read_records(node, operation.schema, [target])
            require(row["fields"] == expected and upload_pending(intent), "shared_write_visible_during_upload")
            operation.summary["live_write_during_upload"] = True
            receipt = decode_json(pending.result())
        through = operation.summary["publisher_tail_through"] + 1
        require(set(receipt) == {"snapshot_id", "writer_uploaded_through"}
                and receipt["writer_uploaded_through"] == through
                and canonical_uuid(receipt["snapshot_id"]) != operation.snapshot,
                "shared_snapshot_command_completed_tail")
        require(decode_json(execute(args, operation.programs.environment, b"", 120)) == receipt,
                "shared_snapshot_command_exact_retry")
    finally:
        operation.summary["snapshot_command_workspace_created"] = workspace.is_dir()
        operation.summary["snapshot_command_upload_started"] = intent.is_file()
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
                             live_write_after_restart=True, snapshot_command_exact_retry=True)


def wait_for_upload(intent, future):
    deadline = time.monotonic() + 30
    while not future.done() and time.monotonic() < deadline:
        if upload_pending(intent):
            return
        time.sleep(0.01)
    if future.done():
        future.result()
    require(False, "shared_snapshot_command_upload_window")


def upload_pending(intent):
    try:
        return read_json(intent, 256 * 1024)["phase"]["phase"] == "prepared"
    except FileNotFoundError:
        return False


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


def pull(operation, node, writer):
    return operation.writer(node, writer, "pull", "--schema-bindings", str(operation.bindings),
                            "--ranges", "1").splitlines()


def exchange_tails(operation, publisher, publisher_writer, peer_writer):
    operation.summary["stage"] = "publish-independent-writer-tails"
    records = extra_records(operation)
    positions = {peer_writer["source_stream"]: 3}
    for writer, (kind, record) in zip(operation.writers[2:-1], records.items(), strict=True):
        node = operation.initialize(kind, writer)
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
    reply = operation.writer(node, reader, "bind", "--database-id", operation.scope["database_id"],
                             "--source-stream", reader["source_stream"], "--schema-bindings",
                             str(operation.bindings))
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
