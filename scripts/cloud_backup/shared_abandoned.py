"""Prove shared DEV upload expiry using the real service clock and providers."""

import time
import uuid

import native_records
import shared_daily_cleanup as storage
import shared_daily_io as io
import shared_snapshot
from automatic_source import durable_batch
from node import OwnedNode
from shared_abandoned_client import Client, digest
from shared_reclamation import FILE_BYTES, await_report, publish
from shared_reclamation_observer import control, location_set, provider_objects, refresh_session
from support import OperationError, canonical_uuid, decode_json, require


TIMEOUT = 115 * 60
DRAIN_MS = 35 * 60 * 1000


def run(operation):
    mailbox = operation.args.output.parent / "provider-observation"
    require(mailbox.is_dir(), "shared_abandoned_observer_required")
    publisher, reader = operation.writers[:2]
    operation.summary["stage"] = "abandoned_publish_accepted_baseline"
    node = operation.initialize_publisher(publisher)
    node.start()
    record = operation.record("checkpoint", "retained-during-upload-expiry")
    durable_batch(node, [native_records.create(operation.schema, native_records.key("checkpoint"), record)])
    sha256 = operation.attach(node, record, "accepted-b2", FILE_BYTES)
    receipt = shared_snapshot.invoke(operation, node, publisher, operation.root / "accepted", 3)
    operation.snapshot = canonical_uuid(receipt["snapshot_id"])
    node.stop()
    operation.remove_sources(operation.nodes)
    operation.summary.update(snapshot_id=operation.snapshot, source_homes_removed=True)

    operation.summary["stage"] = "abandoned_upload_and_stage"
    client = Client(operation, publisher)
    old = str(uuid.uuid4())
    incomplete = client.upload(old, "b2", operation.payload("abandoned-b2-part", FILE_BYTES))
    staged = client.upload(old, "b2", operation.payload("abandoned-b2-object", FILE_BYTES))
    manifest = client.upload(old, "r2", operation.payload("abandoned-r2-manifest", FILE_BYTES))
    client.complete(staged)
    client.complete(manifest)
    page = {"database_id": client.database, "snapshot_id": old, "page_index": 0,
            "objects": [staged["object"]]}
    require(client.post("inventory", page) is None, "shared_abandoned_inventory_response")
    publish(mailbox, "abandoned", {"accepted": operation.snapshot, "snapshot": old,
                                  "incomplete": incomplete, "staged": [staged, manifest], "page": page})
    operation.summary["stage"] = "abandoned_wait_for_active_window"
    accepted = await_report(mailbox, "start-active", time.monotonic() + TIMEOUT)

    operation.summary["stage"] = "abandoned_start_active_upload"
    active = client.upload(str(uuid.uuid4()), "b2", operation.payload("active-b2-part", FILE_BYTES))
    publish(mailbox, "active", active)
    operation.summary["stage"] = "abandoned_wait_for_exact_provider_absence"
    evidence = await_report(mailbox, "observed-expiry", time.monotonic() + TIMEOUT)
    operation.summary["abandoned_reclamation"] = evidence

    operation.summary["stage"] = "abandoned_replay_and_complete_active"
    proposed = {**accepted["manifest"], "snapshot_id": old, "replaces": operation.snapshot,
                "encrypted_manifest": manifest["object"], "object_count": 1,
                "inventory_sha256": digest("inventory", page["objects"])}
    client.post("publish", proposed, expired=True)
    client.post("inventory", page, expired=True)
    client.post("prepare", incomplete, expired=True)
    require(client.post("publish", accepted["manifest"]) == accepted["receipt"],
            "shared_abandoned_accepted_receipt_replay")
    client.complete(active)
    fresh = client.upload(active["role"]["snapshot_id"], "r2",
                          operation.payload("fresh-r2-after-expiry", FILE_BYTES))
    client.complete(fresh)
    operation.summary.update(expired_staging_refused=True, accepted_receipt_replayed=True,
                             active_upload_completed=True, fresh_upload_verified=True)

    operation.summary["stage"] = "abandoned_source_free_restore"
    destination = operation.root / "restored"
    device = reader["device_id"]
    receipt = decode_json(operation.restore(destination, device, device))
    require(receipt == {"activated": True, "destination": str(destination)},
            "shared_abandoned_restore_activation")
    restored = OwnedNode(destination, operation.programs)
    operation.nodes.append(restored)
    files = {"checkpoint": sha256}
    for _ in range(2):
        restored.start()
        operation.verify(restored, {"checkpoint": record}, files, file_bytes=FILE_BYTES)
        restored.stop()
    require(restored.clean_stops == 2 and not restored.forced, "shared_abandoned_restore_clean_stops")
    operation.summary.update(result="passed", restored_boots=2, manual_push_pull_commands=0,
                             exact_live_records=1, file_sha256=files)


def upload_id(database, target, secret, key):
    result = io.aws("s3api", "list-multipart-uploads", {
        "Bucket": target["bucket"], "Prefix": key, "MaxUploads": 4,
    }, target=target, credentials=secret)
    uploads = result.get("Uploads", [])
    require(key.startswith(storage.prefix(database, target)) and not result.get("IsTruncated", False)
            and len(uploads) <= 1 and all(row["Key"] == key for row in uploads),
            "shared_abandoned_exact_multipart_listing")
    return uploads[0]["UploadId"] if uploads else None


def unfinished_part(database, target, secret, transfer):
    resource = transfer["resource"]
    key = resource["key"]
    multipart = transfer["multipart"]
    require(multipart["state"] == "ready" and resource["bucket"] == target["bucket"]
            and key == storage.prefix(database, target) + transfer["input"]["object"]["version_id"]
            and upload_id(database, target, secret, key) == multipart["upload_id"],
            "shared_abandoned_exact_multipart_identity")
    result = io.aws("s3api", "list-parts", {
        "Bucket": target["bucket"], "Key": key, "UploadId": multipart["upload_id"], "MaxParts": 4,
    }, target=target, credentials=secret)
    parts = [(part["PartNumber"], part["Size"]) for part in result.get("Parts", [])]
    require(not result.get("IsTruncated", False) and parts == [(1, FILE_BYTES)],
            "shared_abandoned_exact_uploaded_part")
    return key


def version(request):
    return canonical_uuid(request["object"]["version_id"])


def exact_transfer(rows, request):
    saved = rows.get("transfer#" + version(request))
    require(saved and saved["input"] == request and saved["lease_expires_ms"] > io.now_ms(),
            "shared_abandoned_live_transfer")
    return saved


def draining_until(rows, transfers):
    by_version = {version(transfer["input"]): transfer for transfer in transfers}
    intents = {item["id"]: item for key, item in rows.items()
               if key.startswith("abandoned-delete#") and item["id"] in by_version}
    if len(intents) != len(transfers):
        return None
    for vid, item in intents.items():
        require(item["fence_ms"] >= by_version[vid]["lease_expires_ms"]
                and item["not_before_ms"] == item["fence_ms"] + DRAIN_MS,
                "shared_abandoned_exact_drain_window")
    require(io.now_ms() < min(item["not_before_ms"] for item in intents.values()),
            "shared_abandoned_drain_observation_late")
    return max(item["not_before_ms"] for item in intents.values())


def observe(mailbox, scope, targets, process):
    database = io.database_id(scope["database_id"])
    credentials = {target["name"]: storage.credentials(target) for target in targets}
    b2 = next(target for target in targets if target["name"] == "b2")
    secret = credentials["b2"]
    old = await_report(mailbox, "abandoned", time.monotonic() + 260, process)
    accepted_id, old_id = canonical_uuid(old["accepted"]), canonical_uuid(old["snapshot"])
    stage_key, page_key = "staging#" + old_id, f"inventory#{old_id}#00000"
    requests = [old["incomplete"], *old["staged"]]
    versions = [version(request) for request in requests]
    first = control(database)
    accepted = first["snapshot#" + accepted_id]
    stage = first[stage_key]
    require(first["database"]["abandoned_reclamation_enabled"] is True
            and first["database"]["snapshot"] == accepted["receipt"]
            and "snapshot#" + old_id not in first and stage["state"] == "open"
            and first[page_key] == old["page"]["objects"], "shared_abandoned_initial_staging")
    transfers = [exact_transfer(first, request) for request in requests]
    multipart_key = unfinished_part(database, b2, secret, transfers[0])
    old_objects = location_set({"sealed#" + vid: first["sealed#" + vid] for vid in versions[1:]},
                               targets, database)
    baseline = location_set({key: value for key, value in first.items() if key.startswith("sealed#")
                             and value["input"]["role"].get("snapshot_id") == accepted_id}, targets, database)
    require(len(old_objects) == 2 and {name for name, _, _ in old_objects} == {"r2", "b2"}
            and baseline and old_objects | baseline <= provider_objects(database, targets, credentials),
            "shared_abandoned_initial_provider_bytes")

    deadline = time.monotonic() + 110 * 60
    refresh_at = time.monotonic() + 38 * 60
    # Give the active control its own fresh hour, while the old lease is still open.
    active_at = min(stage["expires_ms"], *(item["lease_expires_ms"] for item in transfers)) - 5 * 60 * 1000
    active = drain_until = None
    expected_bytes = sum(int(request["object"]["bytes"]) for request in requests)
    while time.monotonic() < deadline and process.poll() is None:
        if time.monotonic() >= refresh_at:
            refresh_session()
            refresh_at = time.monotonic() + 38 * 60
        if active is None and io.now_ms() >= active_at:
            rows = control(database)
            require((rows.get(stage_key) or {}).get("state") == "open"
                    and rows.get(page_key) == old["page"]["objects"],
                    "shared_abandoned_live_staging_preserved")
            for request in requests:
                exact_transfer(rows, request)
            publish(mailbox, "start-active", accepted)
            active = await_report(mailbox, "active", time.monotonic() + 90, process)
            rows = control(database)
            active_transfer = exact_transfer(rows, active)
            active_key = unfinished_part(database, b2, secret, active_transfer)
            reserved = int(rows["database"]["reserved_object_bytes"])
        if active is not None:
            rows = control(database)
            if drain_until is None:
                drain_until = draining_until(rows, transfers)
                if drain_until is not None:
                    require(old_objects <= provider_objects(database, targets, credentials)
                            and unfinished_part(database, b2, secret, transfers[0]) == multipart_key,
                            "shared_abandoned_provider_bytes_preserved_during_drain")
            if all((rows.get("reclaimed#" + vid) or {}).get("released") is True for vid in versions):
                break
        time.sleep(15)
    else:
        raise OperationError("shared_abandoned_worker_deadline")

    worker = rows["abandoned#v1"]
    require(drain_until is not None and io.now_ms() >= drain_until
            and rows[stage_key]["state"] == "reclaimable" and page_key not in rows
            and "snapshot#" + old_id not in rows and rows["database"]["snapshot"] == accepted["receipt"]
            and all("transfer#" + vid not in rows and "sealed#" + vid not in rows for vid in versions)
            and worker["released_objects"] == len(versions) and worker["released_bytes"] == expected_bytes
            and worker["pruned_pages"] == 1 and worker["pending_deletions"] == 0
            and all(rows["reclaimed#" + vid]["input"] == request for vid, request in zip(versions, requests))
            and int(rows["database"]["reserved_object_bytes"]) == reserved - expected_bytes,
            "shared_abandoned_staging_and_quota_released")
    surviving = provider_objects(database, targets, credentials)
    require(not old_objects & surviving and baseline <= surviving
            and surviving == location_set(rows, targets, database)
            and upload_id(database, b2, secret, multipart_key) is None
            and unfinished_part(database, b2, secret, exact_transfer(rows, active)) == active_key,
            "shared_abandoned_exact_provider_absence_and_active_part")
    publish(mailbox, "observed-expiry", {"accepted_snapshot": accepted_id, "expired_snapshot": old_id,
            "released_objects": worker["released_objects"], "released_bytes": expected_bytes,
            "pruned_pages": worker["pruned_pages"], "old_b2_versions_absent": 1,
            "old_r2_objects_absent": 1, "old_b2_multipart_absent": True,
            "active_b2_multipart_preserved": True, "provider_bytes_preserved_during_drain": True})
