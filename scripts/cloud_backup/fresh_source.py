"""Seed an ordinary Backup V2 source without reading the protected baseline."""

import hashlib
import os

import baseline_data
import native_records
from automatic_source import Source, durable_batch
from node import OwnedNode
from restore_receipts import BACKUP_PROFILE, HOME_FORMAT, NODE_MARKER, node_marker
from support import FILE_BYTES, OperationError, require


def write_private(path, payload):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def seed(operation, phrase):
    stream = operation.ci_stream_id
    home = operation.root / "source"
    home.mkdir(mode=0o700)
    try:
        identity = baseline_data.seed(phrase)
        key = baseline_data.content_key(identity)
        records = baseline_data.records(key, stream)
        payload = baseline_data.file_bytes(key, stream)
    except baseline_data.BaselineDataError as error:
        raise OperationError(str(error)) from None
    write_private(home / "identity.key", identity)
    del identity, key
    schema = baseline_data.SCHEMA
    expected = {"account": baseline_data.ACCOUNT, "schema": schema, "records": records,
                "file_bytes": len(payload), "file_sha256": hashlib.sha256(payload).hexdigest()}
    require(expected["file_bytes"] == FILE_BYTES, "fresh_source_file_size")
    node = OwnedNode(home, operation.programs, backup_source=True, ci_stream_id=stream,
                     terminal_r2=operation.terminal_r2)
    operation.node = node
    node.start()
    require(node_marker(home) == NODE_MARKER, "fresh_source_home_format")
    operation.summary.update(generated_home_format=HOME_FORMAT, native_backup_profile=BACKUP_PROFILE)
    source = Source(node, expected, operation.args.source, operation.summary)
    source.connect(operation.api_key)
    fields = ["id", "note"]
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": schema, "descriptive_name": schema, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": fields, "field_types": {name: "String" for name in fields},
        "field_descriptions": {name: schema + " " + name for name in fields}}})
    durable_batch(node, [native_records.create(schema, native_records.key(record["id"]), record)
                         for record in records])
    path = operation.root / "fresh-file.bin"
    write_private(path, payload)
    del payload
    reply = native_records.put_file(node, schema, records[0], path)
    path.unlink()
    require(reply["file"]["sha256"] == expected["file_sha256"], "fresh_source_file_marker")
    operation.summary["source_reads"] = node.verify_records(expected)
    return source
