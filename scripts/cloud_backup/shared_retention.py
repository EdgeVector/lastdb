"""Measure settled native storage after real DEV shared upload and replay."""

import hashlib
import time

import native_records
from automatic_source import durable_batch
from shared_multiwriter import pull
from support import require


KEY = "retention-overwrites"
WAVES = 2
BATCHES = 3
OVERWRITES_PER_BATCH = 4
OVERWRITES = WAVES * BATCHES * OVERWRITES_PER_BATCH
MAX_SETTLED_GROWTH = 128 * 1024


def label(version):
    return f"{KEY}-{version}"


def record(operation, version):
    # The complete record rides in the file command's bounded HTTP header.
    payload = hashlib.shake_256(label(version).encode()).hexdigest(3072)
    return operation.record(KEY, payload)


def drain(operation, node, writer):
    for _ in range(20):
        if pull(operation, node, writer)[-1:] == [b"Applied 0 shared peer ranges"]:
            return
    require(False, "shared_retention_peer_not_caught_up")


def replace(operation, node, version):
    previous, = native_records.read_records(node, operation.schema, [native_records.key(KEY)])
    value = record(operation, version)
    durable_batch(node, [native_records.replace(operation.schema, previous, value)])
    return value


def settled(operation, node, version):
    node.start()
    value, = native_records.read_records(node, operation.schema, [native_records.key(KEY)])
    require(value["fields"] == record(operation, version), "shared_retention_live_record")
    payload = operation.payload(label(version))
    node.verify_file({"schema": operation.schema, "records": [{"id": KEY}],
                      "file_bytes": len(payload), "file_sha256": hashlib.sha256(payload).hexdigest()})
    cleanup = node.programs.command(node.home, ["db", "cleanup", "--wait", "--timeout", "30", "--json"])
    require(cleanup["complete"] is True, "shared_retention_native_cleanup")
    status = node.call("/api/db/native-status", {"inventory": True})
    homes = status.get("data", status)["storage_diagnostics"]
    main, cas = homes["main"], homes["cas"]
    require(main["inventory"]["complete"] and cas["inventory"]["complete"],
            "shared_retention_complete_inventory")
    metadata = main["inventory"]["collections"]["metadata"]
    files = cas["inventory"]["collections"]["cas_blobs"]
    result = {"metadata_keys": metadata["live_keys"],
              "metadata_bytes": metadata["live_stored_value_bytes"],
              "file_keys": files["live_keys"], "file_bytes": files["live_stored_value_bytes"],
              "native_bytes": sum(home["files"]["total_allocated_bytes"] for home in homes.values())}
    node.stop()
    return result


def within_bound(before, after):
    return (after["metadata_keys"] == before["metadata_keys"]
            and after["metadata_bytes"] <= before["metadata_bytes"] + 2048
            and after["file_keys"] == before["file_keys"]
            and after["file_bytes"] == before["file_bytes"]
            and after["native_bytes"] <= before["native_bytes"] + MAX_SETTLED_GROWTH)


def establish_baseline(operation, first, peer, publisher, receiver):
    drain(operation, first, publisher)
    drain(operation, peer, receiver)
    through = operation.summary["snapshot_command_uploaded_through"]
    first.start()
    value = record(operation, "original")
    durable_batch(first, [native_records.create(operation.schema, native_records.key(KEY), value)])
    operation.attach(first, value, label("original"))
    first.stop()
    operation.push(first, publisher, through + 2)
    drain(operation, peer, receiver)

    # Leave a local mutation pending that still needs the publisher's old file.
    peer.start()
    replace(operation, peer, "pending-on-peer")
    peer.stop()
    time.sleep(0.01)
    first.start()
    value = replace(operation, first, 0)
    operation.attach(first, value, label(0))
    first.stop()
    operation.push(first, publisher, through + 4)
    drain(operation, peer, receiver)
    # Verify the new live file has displaced the version needed by the pending
    # mutation before pushing. Upload must still resolve the old exact bytes.
    peer.start()
    row, = native_records.read_records(peer, operation.schema, [native_records.key(KEY)])
    require(row["fields"] == value, "shared_retention_pending_file_displaced")
    peer.stop()
    operation.push(peer, receiver, operation.summary["peer_after_replacement_through"] + 1)
    drain(operation, first, publisher)
    operation.summary["displaced_pending_file_uploaded"] = True
    return through + 4


def run(operation, first, peer, publisher, receiver):
    operation.summary["stage"] = "shared-retention-baseline"
    through = establish_baseline(operation, first, peer, publisher, receiver)
    version = 0
    points = [{"writer": settled(operation, first, version), "peer": settled(operation, peer, version)}]
    operation.summary["retention_measurements"] = points
    require(all(row["file_keys"] == 3 for row in points[0].values()), "shared_retention_exact_live_files")
    for _ in range(WAVES):
        for _ in range(BATCHES):
            operation.summary["stage"] = "shared-retention-overwrite-files"
            first.start()
            for _ in range(OVERWRITES_PER_BATCH):
                version += 1
                value = replace(operation, first, version)
                operation.attach(first, value, label(version))
            first.stop()
            through += 2 * OVERWRITES_PER_BATCH
            operation.summary["stage"] = "shared-retention-upload-overwrites"
            operation.push(first, publisher, through)
            operation.summary["stage"] = "shared-retention-replay-overwrites"
            drain(operation, peer, receiver)
        operation.summary["stage"] = "shared-retention-measure-overwrites"
        points.append({"writer": settled(operation, first, version),
                       "peer": settled(operation, peer, version)})
        require(all(within_bound(points[0][role], points[-1][role]) for role in ("writer", "peer")),
                "shared_retention_settled_storage_grew")
    operation.summary.update(shared_retention_overwrites=version, shared_retention_uploaded_through=through)

def expect_restore(operation, records, files):
    records[KEY] = record(operation, OVERWRITES)
    files[KEY] = hashlib.sha256(operation.payload(label(OVERWRITES))).hexdigest()
