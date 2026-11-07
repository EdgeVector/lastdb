"""Keep six enrolled devices running while their daemon workers exchange data."""

from concurrent.futures import ThreadPoolExecutor
import time
import shutil

import native_records
import shared_snapshot
from automatic_source import durable_batch
from node import OwnedNode
from support import OperationError, decode_json, private_directory, require


def policy(node, enabled, log_interval=1, snapshot_interval=86400):
    request = ({"action": "enable_automatic", "log_interval_seconds": log_interval,
                "snapshot_interval_seconds": snapshot_interval} if enabled else {"action": "disable_automatic"})
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            result = node.call("/api/sync/byte-backup-v2", request)
            require(result["automatic_backup_enabled"] is enabled, "shared_automatic_policy")
            node.shared_sync = enabled
            return
        except OperationError as error:
            if str(error) != "socket_http_status":
                raise
            time.sleep(0.25)
    raise OperationError("shared_automatic_policy_deadline")


def wait_all(operation, nodes, records, files, absent_keys=()):
    def verify(node):
        operation.verify(node, records, files)
        if absent_keys:
            require(absent(node, operation.schema, *absent_keys), "shared_automatic_deleted_keys")

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
                list(pool.map(verify, nodes))
            return
        except OperationError:
            time.sleep(1)
    raise OperationError("shared_automatic_replication_deadline")


def absent(node, schema, *keys):
    queries = [{"schema_name": schema, "filter": {"HashKey": key}, "limit": 1, "offset": 0}
               for key in keys]
    results = node.call("/api/queries/batch", {"queries": queries})["results"]
    return all(result["status"] == 200 and result["response"]["returned_count"] == 0
               for result in results)


def create(operation, node, key, version):
    record = operation.record(key, version)
    durable_batch(node, [native_records.create(operation.schema, native_records.key(key), record)])
    return record


def pause_and_resume(operation, nodes, records, files):
    publisher, peer = nodes[:2]
    operation.summary["stage"] = "automatic-policy-pause"
    policy(peer, False)
    records["pending-local"] = create(operation, peer, "pending-local", "while-disabled")
    records["missed-cloud"] = create(operation, publisher, "missed-cloud", "while-peer-disabled")
    time.sleep(3)
    require(absent(publisher, operation.schema, "pending-local")
            and absent(peer, operation.schema, "missed-cloud"), "shared_automatic_pause_both_directions")
    policy(peer, True)
    wait_all(operation, nodes, records, files)
    operation.summary.update(automatic_policy_paused_both_directions=True,
                             automatic_policy_resume_caught_up=True)


def restart(operation, nodes, records, files):
    operation.summary["stage"] = "automatic-restart-catch-up"
    publisher, peer = nodes[:2]
    peer.stop()
    records["while-offline"] = create(operation, publisher, "while-offline", "missed-while-stopped")
    files["while-offline"] = operation.attach(publisher, records["while-offline"], "while-offline")
    wait_all(operation, [publisher], records, files)
    peer.start()
    wait_all(operation, nodes, records, files)
    records["after-restart"] = create(operation, peer, "after-restart", "automatic-upload-after-restart")
    wait_all(operation, nodes, records, files)
    operation.summary.update(automatic_policy_survived_restart=True,
                             automatic_download_after_restart=True,
                             automatic_upload_after_restart=True)



def snapshot_schedule(operation, publisher):
    operation.summary["stage"] = "automatic-independent-snapshot-schedule"
    policy(publisher, True, log_interval=86400, snapshot_interval=60)
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        state = publisher.call("/api/sync/byte-backup-v2", {"action": "status"})["shared_sync"]
        if state["progress"]["next_sync_at"] > time.time() + 3600:
            previous = state["last_snapshot_id"]
            break
        time.sleep(1)
    else:
        raise OperationError("shared_automatic_sync_schedule_deadline")
    while time.monotonic() < deadline:
        state = publisher.call("/api/sync/byte-backup-v2", {"action": "status"})["shared_sync"]
        if state["last_snapshot_id"] is not None and state["last_snapshot_id"] != previous:
            operation.summary.update(automatic_snapshot_shorter_than_sync=True,
                                     replacement_snapshot_id=state["last_snapshot_id"],
                                     automatic_snapshot=True)
            return
        time.sleep(1)
    raise OperationError("shared_automatic_snapshot_schedule_deadline")

def run(operation):
    operation.summary["stage"] = "automatic-bootstrap"
    publisher = operation.initialize_publisher(operation.writers[0])
    publisher.start()
    records = {key: operation.record(key, "automatic-base") for key in ("checkpoint", "winner")}
    durable_batch(publisher, [native_records.create(operation.schema, native_records.key(key), value)
                              for key, value in records.items()])
    files = {"checkpoint": operation.attach(publisher, records["checkpoint"], "automatic-base")}
    workspace = operation.root / "initial-automatic-snapshot"
    receipt = shared_snapshot.invoke(operation, publisher, operation.writers[0], workspace, 3)
    operation.snapshot = receipt["snapshot_id"]
    operation.summary["snapshot_id"] = operation.snapshot
    publisher.stop()

    # Reserve the last enrolled device for a fresh restore with no previous hold.
    nodes = [publisher]
    for index, writer in enumerate(operation.writers[1:-1], 1):
        nodes.append(operation.join_writer("peer" if index == 1 else f"auto-{index}", writer))
    operation.summary["stage"] = "automatic-running-devices"
    for node in nodes:
        node.start()
    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        list(pool.map(lambda node: policy(node, True), nodes))

    def write(item):
        index, node = item
        key = f"automatic-{index}"
        value = create(operation, node, key, "independent-running-writer")
        digest = operation.attach(node, value, key) if index < 2 else None
        return key, value, digest

    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        written = list(pool.map(write, enumerate(nodes)))
    for key, value, digest in written:
        records[key] = value
        if digest is not None:
            files[key] = digest
    wait_all(operation, nodes, records, files)
    operation.summary.update(automatic_running_writers=len(nodes), automatic_upload=True,
                             automatic_download_apply=True, manual_push_pull_commands=0,
                             automatic_bound_schema_alias=operation.schema.endswith("/CloudSharedProofAlias"))

    operation.summary["stage"] = "automatic-delete-and-file-update"
    removed, = native_records.read_records(publisher, operation.schema, [native_records.key("automatic-0")])
    durable_batch(publisher, [native_records.delete(operation.schema, removed)])
    del records["automatic-0"]
    del files["automatic-0"]
    peer = nodes[1]
    prior, = native_records.read_records(peer, operation.schema, [native_records.key("automatic-1")])
    records["automatic-1"] = operation.record("automatic-1", "new-file-version")
    durable_batch(peer, [native_records.replace(operation.schema, prior, records["automatic-1"])])
    files["automatic-1"] = operation.attach(peer, records["automatic-1"], "replacement-file")
    wait_all(operation, nodes, records, files)
    require(all(absent(node, operation.schema, "automatic-0") for node in nodes),
            "shared_automatic_deleted_key_absent")
    operation.summary.update(automatic_delete_applied=True, automatic_replacement_file_applied=True)
    pause_and_resume(operation, nodes, records, files)
    restart(operation, nodes, records, files)

    snapshot_schedule(operation, publisher)
    # Preserve the current snapshot and every uncovered suffix while all sources leave.
    for node in nodes:
        node.stop()
    operation.remove_sources(nodes)
    private_directory(workspace)
    shutil.rmtree(workspace)
    operation.summary.update(source_homes_removed=True, publication_workspace_removed=True)

    operation.summary["stage"] = "automatic-source-free-restore"
    destination = operation.root / "automatic-restored"
    reader = operation.writers[-1]["device_id"]
    restored = decode_json(operation.restore(destination, reader, reader))
    require(restored == {"activated": True, "destination": str(destination)},
            "shared_automatic_restore_activation")
    node = OwnedNode(destination, operation.programs)
    operation.nodes.append(node)
    for _ in range(2):
        node.start()
        operation.verify(node, records, files)
        require(absent(node, operation.schema, "automatic-0"), "shared_automatic_restored_delete")
        node.stop()
    operation.summary.update(result="passed", restored_boots=2, exact_live_records=len(records),
                             file_sha256=files, provider_deletions=0)
