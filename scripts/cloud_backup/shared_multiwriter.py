"""Exercise five remote writer tails and a fresh device joining from restore."""

import native_records
from automatic_source import durable_batch
from support import require


WRITER_COUNT = 7
LOCAL_SCHEMA = "CloudProofLocalOnly"
LOCAL_RECORD = {"id": "local-only"}


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
    reply = node.call("/api/schemas")
    schemas = reply.get("data", reply)["schemas"]
    return any(schema["name"] == LOCAL_SCHEMA for schema in schemas)


def extra_records(operation):
    return {f"peer-{index}": operation.record(f"peer-{index}", "independent-tail")
            for index in range(2, len(operation.writers) - 1)}


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
