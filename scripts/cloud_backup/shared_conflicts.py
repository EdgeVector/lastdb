"""Run independent same-key writes and opposite peer replay orders in DEV."""

import hashlib
import shutil
import time

import native_records
import shared_daemon
import shared_snapshot
from automatic_source import durable_batch
from node import OwnedNode
from support import decode_json, private_directory, require


CASES = ("winner", "deleted", "revived", "file-race", "file-delete", "delete-file")
KEYS = ("checkpoint", *CASES)
OLDER_DELETES = {"revived", "delete-file"}
NEWER_DELETES = {"deleted", "file-delete"}
OLDER_FILES = ("file-race", "file-delete")
NEWER_FILES = ("file-race", "delete-file")
# Each authoring stream defines the schema once, before its first record.
INITIAL_POSITION = 1 + len(KEYS) + 1
TAIL_LENGTH = len(CASES) + 2
PEER_END = 1 + TAIL_LENGTH


def values(operation, version):
    deleted = OLDER_DELETES if version == "older" else NEWER_DELETES
    records = {key: operation.record(key, version) for key in CASES if key not in deleted}
    records["checkpoint"] = operation.record("checkpoint", "baseline")
    labels = OLDER_FILES if version == "older" else NEWER_FILES
    files = {key: hashlib.sha256(operation.payload(f"{version}-{key}")).hexdigest() for key in labels}
    files["checkpoint"] = hashlib.sha256(operation.payload("baseline-file")).hexdigest()
    return records, files


def read_version(operation, node, version, expected_rows=None):
    records, files = values(operation, version)
    operation.verify(node, records, files, absent=KEYS)
    rows = native_records.read_records(node, operation.schema,
                                      [native_records.key(key) for key in sorted(records)])
    if expected_rows is not None:
        require(rows == expected_rows, "shared_conflict_exact_revisions_and_metadata")
    return rows


def verify(operation, node, version, expected_rows=None):
    node.start()
    rows = read_version(operation, node, version, expected_rows)
    node.stop()
    return rows


def initialize(operation):
    a, b, c, d = operation.writers[:4]
    require(a["source_stream"] > b["source_stream"], "shared_conflict_reverse_stream_order")
    operation.summary.update(operation="offline-conflicts", writer_count=len(operation.writers),
                             conflict_cases=list(CASES), authored_streams=2)
    operation.summary["stage"] = "publish-conflict-baseline"
    first = operation.initialize_publisher(a)
    first.start()
    durable_batch(first, [native_records.create(operation.schema, native_records.key(key),
                                               operation.record(key, "baseline")) for key in KEYS])
    operation.attach(first, operation.record("checkpoint", "baseline"), "baseline-file")
    publication = operation.root / "publication"
    receipt = shared_snapshot.invoke(operation, first, a, publication, INITIAL_POSITION)
    operation.snapshot = receipt["snapshot_id"]
    operation.summary.update(snapshot_id=operation.snapshot, checkpoint_through=INITIAL_POSITION,
                             initial_snapshot_files=shared_snapshot.provider_counts(receipt))
    first.stop()
    operation.summary["stage"] = "restore-common-conflict-baseline"
    peer = operation.join_writer("peer", b)
    forward = operation.join_writer("forward", c)
    reverse = operation.join_writer("reverse", d)
    operation.summary["common_snapshot_before_independent_writes"] = True
    return (first, peer, forward, reverse), publication


def author(operation, node, version):
    operation.summary["stage"] = f"author-offline-{version}"
    node.start()
    rows = native_records.read_records(node, operation.schema,
                                      [native_records.key(key) for key in CASES])
    require(all(row["fields"] == operation.record(key, "baseline")
                for key, row in zip(CASES, rows, strict=True)), "shared_conflict_same_preimage")
    deleted = OLDER_DELETES if version == "older" else NEWER_DELETES
    durable_batch(node, [native_records.delete(operation.schema, row) if key in deleted else
                        native_records.replace(operation.schema, row, operation.record(key, version))
                        for key, row in zip(CASES, rows, strict=True)])
    for key in (OLDER_FILES if version == "older" else NEWER_FILES):
        operation.attach(node, operation.record(key, version), f"{version}-{key}")
    node.stop()
    return verify(operation, node, version)


def exchange(operation, nodes, expected_rows):
    first, peer, forward, reverse = nodes
    a, b = operation.writers[:2]
    a_end, b_end = INITIAL_POSITION + TAIL_LENGTH, PEER_END
    a_range, b_range = (a["source_stream"], a_end), (b["source_stream"], b_end)
    operation.summary["stage"] = "replay-older-first"
    first.start()
    shared_daemon.upload(operation, first, a_end)
    first.stop()
    forward.start()
    shared_daemon.receive(operation, forward, ranges=1, last=a_range)
    read_version(operation, forward, "older")

    operation.summary["stage"] = "replay-newer-first"
    peer.start()
    shared_daemon.upload(operation, peer, b_end)
    read_version(operation, peer, "newer", expected_rows)
    peer.stop()
    reverse.start()
    # Both tails are available. Two applications ending at A prove B ran first.
    shared_daemon.receive(operation, reverse, ranges=2, last=a_range)
    read_version(operation, reverse, "newer", expected_rows)
    reverse.stop()
    shared_daemon.receive(operation, forward, ranges=2, last=b_range)
    read_version(operation, forward, "newer", expected_rows)
    forward.stop()

    operation.summary["stage"] = "converge-original-offline-writers"
    first.start()
    shared_daemon.receive(operation, first, ranges=1, last=b_range)
    read_version(operation, first, "newer", expected_rows)
    first.stop()
    forward_order = [{"source_stream": stream, "through": through} for stream, through in (a_range, b_range)]
    operation.summary.update(forward_replay=forward_order, reverse_replay=list(reversed(forward_order)),
                             opposite_replay_orders_converged=True, original_writers_converged=True,
                             exact_original_revisions_preserved=True)


def repeat_after_restart(operation, nodes, expected_rows):
    operation.summary["stage"] = "verify-conflict-restart-and-idle-retry"
    ends = (INITIAL_POSITION + TAIL_LENGTH, PEER_END, 0, 0)
    for node, uploaded in zip(nodes, ends, strict=True):
        node.start()
        shared_daemon.policy(node, True)
        state = shared_daemon.wait(operation, node, "shared_conflict_restart_sync_deadline",
                                   uploaded=uploaded, ranges=0)
        require(state["last_upload_at"] is None, "shared_conflict_upload_cursor_survived")
        read_version(operation, node, "newer", expected_rows)
        shared_daemon.policy(node, False)
        node.stop()
    operation.summary.update(restart_convergence=True, replay_retry_idle=True, upload_retry_idle=True)


def source_free_restore(operation, publication, expected_rows):
    operation.summary["stage"] = "remove-conflict-sources-before-restore"
    operation.remove_sources(operation.nodes)
    operation.assert_owned_root()
    private_directory(publication)
    shutil.rmtree(publication)
    operation.summary.update(source_homes_removed=True, publication_workspace_removed=True)
    operation.summary["stage"] = "restore-conflicts-without-sources"
    reader = operation.writers[4]
    destination = operation.root / "restored"
    receipt = decode_json(operation.restore(destination, reader["device_id"], reader["device_id"]))
    require(receipt == {"activated": True, "destination": str(destination)},
            "shared_conflict_restore_activation")
    node = OwnedNode(destination, operation.programs)
    operation.nodes.append(node)
    for _ in range(2):
        verify(operation, node, "newer", expected_rows)
    require(node.clean_stops == 2, "shared_conflict_restore_two_clean_boots")
    records, files = values(operation, "newer")
    operation.summary.update(result="passed", restored_boots=2, strict_clean_stops=2,
                             manual_push_pull_commands=0,
                             exact_live_records=len(records), exact_absent_records=len(NEWER_DELETES),
                             file_sha256=files, recovery_device=reader["device_id"])


def run(operation):
    nodes, publication = initialize(operation)
    author(operation, nodes[0], "older")
    # Real wall time separates the two authored waves; no clock or log bytes are injected.
    time.sleep(0.025)
    expected_rows = author(operation, nodes[1], "newer")
    operation.summary["both_authored_before_either_upload"] = True
    exchange(operation, nodes, expected_rows)
    repeat_after_restart(operation, nodes, expected_rows)
    source_free_restore(operation, publication, expected_rows)
