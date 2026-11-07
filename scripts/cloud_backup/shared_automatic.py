"""Keep six enrolled devices running while their daemon workers exchange data."""

from concurrent.futures import ThreadPoolExecutor
import time
import shutil

import native_records
from shared_daemon import policy
from shared_multiwriter import refused_write
import shared_snapshot
from automatic_source import durable_batch
from node import OwnedNode
from support import OperationError, decode_json, private_directory, require, save_json


LATER_SCHEMA = "CloudSharedLater"
LATER_KEY = "later-schema"
UNADMITTED = b"shared schema writes require an active writer binding that admits the schema"


def wait_all(operation, nodes, records, files, absent_keys=()):
    def verify(node):
        try:
            operation.verify(node, records, files, absent=absent_keys)
            return None
        except OperationError as error:
            return str(error)

    deadline = time.monotonic() + 90
    with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
        while True:
            failures = list(pool.map(verify, nodes))
            if not any(failures):
                return
            if time.monotonic() >= deadline:
                break
            time.sleep(1)
        operation.summary["automatic_replication_observations"] = [
            dict(state, readback_failure=reason)
            for state, reason in zip(pool.map(sync_observation, nodes), failures)]
    raise OperationError("shared_automatic_replication_deadline")


def sync_observation(node):
    try:
        status = node.call("/api/sync/byte-backup-v2", {"action": "status"})["shared_sync"]
    except OperationError as error:
        return {"status_failure": str(error)}
    progress = status["progress"]
    return {**{key: progress[key] for key in ("running", "uploaded_through", "applied_ranges",
                                              "blocked_reason", "last_sync_at")},
            "snapshot_published": status["last_snapshot_id"] is not None,
            "needs_restore": progress["needs_restore"] is not None}


def holds(node, schema, key, record):
    try:
        result, = node.call("/api/queries/batch", {"queries": [
            {"schema_name": schema, "filter": {"HashKey": key}, "limit": 2, "offset": 0}]})["results"]
    except (OperationError, ValueError):
        return False
    rows = (result.get("response") or {}).get("results") or []
    return result.get("status") == 200 and len(rows) == 1 and rows[0].get("fields") == record


def wait_later(operation, nodes, later):
    deadline = time.monotonic() + 90
    while not all(holds(node, *later) for node in nodes):
        if time.monotonic() >= deadline:
            operation.summary["automatic_replication_observations"] = [
                dict(sync_observation(node), later_schema_record=holds(node, *later)) for node in nodes]
            raise OperationError("shared_automatic_later_schema_deadline")
        time.sleep(1)


def admit_later_schema(operation, node, writer):
    """Define a second schema on one stopped writer. Only its writer log carries it."""
    require(operation.programs.environment["FOLD_SCHEMA_SERVICE_URL"] == "http://127.0.0.1:9",
            "shared_automatic_schema_service_unreachable")
    namespace = operation.schema.split("/")[0]
    identity = operation.define_schema(node, writer, namespace, LATER_SCHEMA,
                                       "Shared Storage Later Proof Record")
    schema = f"{namespace}/{LATER_SCHEMA}"
    bindings = operation.root / "later-schemas.json"
    save_json(bindings, {schema: identity["definition"]})
    reply = operation.writer(node, writer, "admit", "--schema-bindings", str(bindings))
    require(reply.splitlines() == [b"Shared schemas admitted"], "shared_automatic_later_schema_admitted")
    operation.summary["later_schema_binding"] = {"runtime_name": schema,
                                                 "portable_id": identity["portable_id"]}
    return schema


def replicate_later_schema(operation, nodes, schema):
    operation.summary["stage"] = "automatic-later-schema"
    publisher, peer = nodes[:2]
    record = operation.record(LATER_KEY, "later-schema")
    durable_batch(peer, [native_records.create(schema, native_records.key(LATER_KEY), record)])
    later = (schema, LATER_KEY, record)
    wait_later(operation, nodes, later)
    # The publisher installed the schema from the peer's log but never admitted it.
    require(refused_write(operation, publisher, schema, "unadmitted-later", UNADMITTED),
            "shared_automatic_unadmitted_write_refusal")
    operation.summary.update(automatic_later_schema_replicated=True,
                             automatic_unadmitted_write_refused=True,
                             schema_service_unreachable=True)
    return later


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
    later = admit_later_schema(operation, peer, operation.writers[1])
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
    return replicate_later_schema(operation, nodes, later)



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


def author_uncovered_tail(operation, nodes, records, files):
    operation.summary["stage"] = "automatic-post-snapshot-tail"
    publisher, peer, reader = nodes[:3]
    records["after-snapshot"] = create(operation, peer, "after-snapshot", "uncovered-peer-tail")
    files["after-snapshot"] = operation.attach(peer, records["after-snapshot"], "after-snapshot")
    wait_all(operation, [peer, reader], records, files)
    require(absent(publisher, operation.schema, "after-snapshot"), "shared_automatic_tail_outside_cut")
    operation.summary["automatic_uncovered_peer_tail"] = True


def run(operation):
    operation.summary["stage"] = "automatic-bootstrap"
    publisher = operation.initialize_publisher(operation.writers[0])
    publisher.start()
    records = {key: operation.record(key, "automatic-base") for key in ("checkpoint", "winner")}
    durable_batch(publisher, [native_records.create(operation.schema, native_records.key(key), value)
                              for key, value in records.items()])
    files = {"checkpoint": operation.attach(publisher, records["checkpoint"], "automatic-base")}
    workspace = operation.root / "initial-automatic-snapshot"
    # One definition, two records, and one file.
    receipt = shared_snapshot.invoke(operation, publisher, operation.writers[0], workspace, 4)
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
    wait_all(operation, nodes, records, files, absent_keys=("automatic-0",))
    operation.summary.update(automatic_delete_applied=True, automatic_replacement_file_applied=True)
    pause_and_resume(operation, nodes, records, files)
    later = restart(operation, nodes, records, files)

    snapshot_schedule(operation, publisher)
    author_uncovered_tail(operation, nodes, records, files)
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
        require(holds(node, *later), "shared_automatic_restored_later_schema")
        node.stop()
    operation.summary.update(result="passed", restored_boots=2, exact_live_records=len(records),
                             file_sha256=files, provider_deletions=0)
