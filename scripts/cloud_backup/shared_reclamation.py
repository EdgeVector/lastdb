"""Prove real shared DEV reclamation before a source-free restore."""

import os
import time

import native_records
import shared_daemon
import shared_snapshot
from automatic_source import durable_batch
from node import OwnedNode
from support import OperationError, canonical_uuid, decode_json, read_json, require, save_json


FILE_BYTES = 32 * 1024
TIMEOUT = 85 * 60


def publish(directory, name, value):
    path = directory / name
    pending = path.with_suffix(".pending")
    save_json(pending, value)
    os.replace(pending, path)


def await_report(directory, name, deadline, process=None):
    path = directory / name
    while time.monotonic() < deadline:
        if path.is_file():
            return read_json(path)
        if process is not None and process.poll() is not None:
            raise OperationError("shared_reclamation_source_stopped_early")
        time.sleep(0.5)
    raise OperationError("shared_reclamation_" + name + "_deadline")


def run(operation):
    mailbox = operation.args.output.parent / "provider-observation"
    require(mailbox.is_dir(), "shared_reclamation_observer_required")
    publisher, peer, reader = operation.writers[:3]
    node = operation.initialize_publisher(publisher)
    node.start()
    keep = operation.record("checkpoint", "kept")
    deleted = operation.record("deleted", "old-b2-bytes")
    durable_batch(node, [native_records.create(operation.schema, native_records.key(
        record["shared_proof_record_identifier"]), record) for record in (keep, deleted)])
    operation.attach(node, deleted, "old-b2", FILE_BYTES)

    operation.summary["stage"] = "reclamation_publish_initial"
    first = shared_snapshot.invoke(operation, node, publisher, operation.root / "initial", 4)
    operation.snapshot = canonical_uuid(first["snapshot_id"])
    node.stop()
    # The peer opens a genuine recovery hold on the old snapshot.
    second = operation.join_writer("peer", peer)
    operation.summary["stage"] = "reclamation_observe_initial"
    publish(mailbox, "initial", {"snapshot": operation.snapshot, "publisher": publisher["source_stream"]})
    await_report(mailbox, "observed-initial", time.monotonic() + 90)

    node.start()
    removed, = native_records.read_records(node, operation.schema, [native_records.key("deleted")])
    durable_batch(node, [native_records.delete(operation.schema, removed)])
    kept_digest = operation.attach(node, keep, "retained-b2", FILE_BYTES)
    operation.summary["stage"] = "reclamation_publish_replacement"
    # Exercise the real snapshot path. There is no operator compaction or provider delete.
    cut = shared_snapshot.invoke(operation, node, publisher, operation.root / "replacement", 6)
    replacement = canonical_uuid(cut["snapshot_id"])
    require(replacement != operation.snapshot, "shared_reclamation_replacement_identity")
    operation.summary.update(snapshot_id=operation.snapshot, replacement_snapshot_id=replacement)
    node.stop()

    operation.summary["stage"] = "reclamation_upload_uncovered_tail"
    second.start()
    tail = operation.record("peer-only", "uncovered")
    durable_batch(second, [native_records.create(operation.schema, native_records.key("peer-only"), tail)])
    tail_digest = operation.attach(second, tail, "uncovered-r2", FILE_BYTES)
    through = shared_daemon.upload(operation, second, 3)
    second.stop()
    operation.remove_sources(operation.nodes)
    operation.summary["source_homes_removed"] = True
    operation.summary["stage"] = "reclamation_wait_for_exact_provider_absence"
    publish(mailbox, "replacement", {"snapshot": replacement, "peer": peer["source_stream"],
                                      "peer_through": through})
    observation = await_report(mailbox, "observed-replacement", time.monotonic() + TIMEOUT)
    operation.summary["automatic_reclamation"] = observation

    operation.summary["stage"] = "reclamation_source_free_restore"
    destination = operation.root / "restored"
    device = reader["device_id"]
    receipt = decode_json(operation.restore(destination, device, device))
    require(receipt == {"activated": True, "destination": str(destination)},
            "shared_reclamation_restore_activation")
    restored = OwnedNode(destination, operation.programs)
    operation.nodes.append(restored)
    files = {"checkpoint": kept_digest, "peer-only": tail_digest}
    for _ in range(2):
        restored.start()
        operation.verify(restored, {"checkpoint": keep, "peer-only": tail}, files,
                         absent=("deleted",), file_bytes=FILE_BYTES)
        restored.stop()
    require(restored.clean_stops == 2 and not restored.forced, "shared_reclamation_restore_clean_stops")
    operation.summary.update(result="passed", restored_boots=2, manual_push_pull_commands=0,
                             exact_live_records=2, exact_absent_records=1, file_sha256=files)
