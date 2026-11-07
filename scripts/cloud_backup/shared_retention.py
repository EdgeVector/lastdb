"""Measure settled native storage after real DEV shared upload and replay."""

import hashlib
import time

import native_records
import shared_daemon
from automatic_source import durable_batch
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


def replace(operation, node, version):
    previous, = native_records.read_records(node, operation.schema, [native_records.key(KEY)])
    value = record(operation, version)
    durable_batch(node, [native_records.replace(operation.schema, previous, value)])
    return value


def settled(operation, node, version, *, dedicated=False):
    node.start(storage_inventory=True)
    value, = native_records.read_records(node, operation.schema, [native_records.key(KEY)])
    require(value["fields"] == record(operation, version), "shared_retention_live_record")
    payload = operation.payload(label(version))
    node.verify_file({"schema": operation.schema, "records": [{"id": KEY}],
                      "file_bytes": len(payload), "file_sha256": hashlib.sha256(payload).hexdigest()})
    cleanup = node.programs.command(node.home, ["db", "cleanup", "--wait", "--timeout", "30", "--json"])
    require(cleanup.get("complete") is True, "shared_retention_native_cleanup")
    status = node.call("/api/db/native-status", {"inventory": True})
    homes = status.get("data", status).get("storage_diagnostics")
    require(isinstance(homes, dict), "shared_retention_storage_diagnostics_missing")
    expected = {"main", "cas", "private"} if dedicated else {"main", "cas"}
    require(homes.keys() == expected and all(home["inventory"]["complete"] for home in homes.values()),
            "shared_retention_complete_inventory")
    metadata = [home["inventory"]["collections"]["metadata"] for home in homes.values()
                if "metadata" in home["inventory"]["collections"]]
    files = homes["cas"]["inventory"]["collections"]["cas_blobs"]
    private_files = (homes["private"]["inventory"]["collections"].get("cas_blobs", {})
                     if dedicated else {})
    result = {"metadata_keys": sum(item["live_keys"] for item in metadata),
              "metadata_bytes": sum(item["live_stored_value_bytes"] for item in metadata),
              "file_keys": files["live_keys"], "file_bytes": files["live_stored_value_bytes"],
              "private_file_keys": private_files.get("live_keys", 0),
              "private_file_bytes": private_files.get("live_stored_value_bytes", 0),
              "native_bytes": sum(home["files"]["total_allocated_bytes"] for home in homes.values())}
    node.stop()
    return result


def measure(operation, first, peer, version):
    measurements = {}
    for role, node in (("writer", first), ("peer", peer)):
        operation.summary["stage"] = "shared-retention-measure-" + role
        measurements[role] = settled(operation, node, version, dedicated=True)
    return measurements


def within_bound(before, after):
    return (after["metadata_keys"] == before["metadata_keys"]
            and after["metadata_bytes"] <= before["metadata_bytes"] + 2048
            and after["file_keys"] == before["file_keys"]
            and after["file_bytes"] == before["file_bytes"]
            and after["private_file_keys"] == before["private_file_keys"]
            and after["private_file_bytes"] == before["private_file_bytes"]
            and after["native_bytes"] <= before["native_bytes"] + MAX_SETTLED_GROWTH)


def establish_baseline(operation, first, peer, publisher, receiver):
    through = operation.summary["snapshot_command_uploaded_through"]
    first.start()
    value = record(operation, "original")
    durable_batch(first, [native_records.create(operation.schema, native_records.key(KEY), value)])
    operation.attach(first, value, label("original"))
    shared_daemon.upload(operation, first, through + 2)
    first.stop()

    peer.start()
    shared_daemon.receive(operation, peer, last=(publisher["source_stream"], through + 2))
    # The first pending change references the publisher's exact old file.
    # The next change displaces that file before the daemon can upload either.
    pending = replace(operation, peer, "pending-on-peer")
    digest = operation.attach(peer, pending, label("displaced-before-upload"))
    peer.verify_file({"schema": operation.schema, "records": [{"id": KEY}],
                      "file_bytes": len(operation.payload(label("displaced-before-upload"))),
                      "file_sha256": digest})
    peer.stop()
    time.sleep(0.01)
    first.start()
    value = replace(operation, first, 0)
    operation.attach(first, value, label(0))
    shared_daemon.upload(operation, first, through + 4)
    first.stop()

    peer_end = operation.summary["peer_after_replacement_through"] + 2
    peer.start()
    shared_daemon.policy(peer, True)
    shared_daemon.wait(operation, peer, "shared_retention_pending_upload_deadline",
                       uploaded=peer_end, last=(publisher["source_stream"], through + 4))
    shared_daemon.policy(peer, False)
    row, = native_records.read_records(peer, operation.schema, [native_records.key(KEY)])
    require(row["fields"] == value, "shared_retention_latest_peer_file")
    peer.stop()
    first.start()
    shared_daemon.receive(operation, first, last=(receiver["source_stream"], peer_end))
    first.stop()
    operation.summary["displaced_pending_file_uploaded"] = True
    return through + 4


def run(operation, first, peer, publisher, receiver):
    operation.summary["stage"] = "shared-retention-baseline"
    through = establish_baseline(operation, first, peer, publisher, receiver)
    version = 0
    points = [measure(operation, first, peer, version)]
    operation.summary["retention_measurements"] = points
    require(all(row["file_keys"] == 3 for row in points[0].values())
            and points[0]["writer"]["private_file_keys"] == 1
            and points[0]["peer"]["private_file_keys"] == 0, "shared_retention_exact_live_files")
    for _ in range(WAVES):
        for _ in range(BATCHES):
            operation.summary["stage"] = "shared-retention-overwrite-files"
            first.start()
            for _ in range(OVERWRITES_PER_BATCH):
                version += 1
                value = replace(operation, first, version)
                operation.attach(first, value, label(version))
            through += 2 * OVERWRITES_PER_BATCH
            operation.summary["stage"] = "shared-retention-upload-overwrites"
            shared_daemon.upload(operation, first, through)
            first.stop()
            operation.summary["stage"] = "shared-retention-replay-overwrites"
            peer.start()
            shared_daemon.receive(operation, peer, last=(publisher["source_stream"], through))
            peer.stop()
        points.append(measure(operation, first, peer, version))
        require(all(within_bound(points[0][role], points[-1][role]) for role in ("writer", "peer")),
                "shared_retention_settled_storage_grew")
    operation.summary.update(shared_retention_overwrites=version, shared_retention_uploaded_through=through)

def expect_restore(operation, records, files):
    records[KEY] = record(operation, OVERWRITES)
    files[KEY] = hashlib.sha256(operation.payload(label(OVERWRITES))).hexdigest()
