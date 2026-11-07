"""Read exact owned DEV control and provider state without doing the deletion."""

from concurrent.futures import ThreadPoolExecutor
import time

import shared_daily_cleanup as storage
import shared_daily_io as io
import shared_reclamation as scenario
from support import canonical_uuid, require


def control(database):
    return {row["record_id"]["S"]: io.body(row) for row in io.query("shared#" + database, 256)}


def location_set(rows, targets, database):
    objects = set()
    for key, row in rows.items():
        if not key.startswith("sealed#"):
            continue
        sealed, binding = row["input"], row["resource"]
        require(sealed["database_id"] == database and key == "sealed#" + sealed["object"]["version_id"],
                "shared_reclamation_sealed_scope")
        target = next((item for item in targets if item["bucket"] == binding["bucket"]), None)
        require(target is not None and binding["key"].startswith(storage.prefix(database, target)),
                "shared_reclamation_provider_scope")
        version = row.get("version")
        require((target["name"] == "b2") == bool(version), "shared_reclamation_exact_provider_version")
        objects.add((target["name"], binding["key"], version))
    return objects


def provider_objects(database, targets, credentials):
    def listing(item):
        return {(item["name"], obj["Key"], obj.get("VersionId"))
                for obj in storage.contents(database, item, credentials[item["name"]])}

    with ThreadPoolExecutor(max_workers=len(targets)) as executor:
        return set().union(*executor.map(listing, targets))


def snapshot_b2_bytes(rows, snapshot):
    inputs = [value["input"] for key, value in rows.items() if key.startswith("sealed#")]
    return sum(int(item["object"]["bytes"]) for item in inputs if item["role"] == {
        "kind": "native_snapshot", "snapshot_id": snapshot, "destination": "b2"})


def active_hold(rows, snapshot):
    return any(value["view"]["snapshot"]["snapshot_id"] == snapshot
               and int(value["view"]["expires_at_ms"]) > io.now_ms()
               for key, value in rows.items() if key.startswith("hold#"))


def observe(mailbox, scope, targets, process):
    database = io.database_id(scope["database_id"])
    with ThreadPoolExecutor(max_workers=len(targets)) as executor:
        credentials = dict(zip((target["name"] for target in targets),
                               executor.map(storage.credentials, targets)))
    initial = scenario.await_report(mailbox, "initial", time.monotonic() + 180, process)
    old_id = canonical_uuid(initial["snapshot"])
    old_rows = control(database)
    old_objects = provider_objects(database, targets, credentials)
    initial_roles = {v["input"]["role"]["kind"] for k, v in old_rows.items() if k.startswith("sealed#")}
    require(old_rows["database"]["reclamation_enabled"] is True
            and old_rows["database"]["snapshot"]["snapshot_id"] == old_id
            and old_objects == location_set(old_rows, targets, database)
            and {"native_snapshot", "segment", "file"} <= initial_roles
            and snapshot_b2_bytes(old_rows, old_id) >= scenario.FILE_BYTES
            and active_hold(old_rows, old_id), "shared_reclamation_initial_cloud_cut")
    scenario.publish(mailbox, "observed-initial", {})

    replacement = scenario.await_report(mailbox, "replacement", time.monotonic() + 180, process)
    new_id = canonical_uuid(replacement["snapshot"])
    peer = canonical_uuid(replacement["peer"])
    publisher = canonical_uuid(initial["publisher"])
    peer_through = replacement["peer_through"]
    before = control(database)
    require(new_id != old_id and before["database"]["snapshot"]["snapshot_id"] == new_id
            and any(w["source_stream"] == peer for w in scope["writers"])
            and any(w["source_stream"] == publisher and w["device_id"] == scope["publisher_device"]
                    for w in scope["writers"])
            and active_hold(before, old_id)
            and old_objects <= provider_objects(database, targets, credentials),
            "shared_reclamation_old_objects_protected_during_hold")
    covers = {value["source_stream"]: int(value["through"]["sequence"])
              for value in before["snapshot#" + new_id]["manifest"]["covers"]}
    require(covers.get(publisher) == 6 and covers.get(peer, 0) < peer_through,
            "shared_reclamation_expected_snapshot_covers")

    deadline = time.monotonic() + scenario.TIMEOUT - 30
    while time.monotonic() < deadline and process.poll() is None:
        rows = control(database)
        status = rows.get("reclamation#v1") or {}
        if status.get("phase") == "complete" and (status.get("target") or {}).get("snapshot_id") == new_id:
            break
        time.sleep(15)
    else:
        require(False, "shared_reclamation_worker_completion_deadline")

    surviving = provider_objects(database, targets, credentials)
    require(not old_objects & surviving and surviving == location_set(rows, targets, database),
            "shared_reclamation_exact_old_provider_versions_absent")
    require([key for key in rows if key.startswith("snapshot#")] == ["snapshot#" + new_id]
            and int(rows["writer#" + publisher]["reclaimed_through"]) == covers[publisher]
            and all(key.startswith("batch#" + peer + "#") for key in rows if key.startswith("batch#"))
            and f"batch#{peer}#{peer_through:020}" in rows,
            "shared_reclamation_one_snapshot_and_uncovered_tail")
    b2_bytes = snapshot_b2_bytes(rows, new_id)
    # Two independent high-entropy file bodies cannot fit in this stored byte bound.
    require(scenario.FILE_BYTES <= b2_bytes < 2 * scenario.FILE_BYTES,
            "shared_reclamation_deleted_payload_omitted")
    scenario.publish(mailbox, "observed-replacement", {
        "initial_snapshot": old_id, "current_snapshot": new_id,
        "old_b2_versions_absent": sum(target == "b2" for target, _, _ in old_objects),
        "old_r2_objects_absent": sum(target == "r2" for target, _, _ in old_objects),
        "old_objects_preserved_during_hold": True,
        "retained_snapshot_b2_bytes": b2_bytes, "covered_publisher_through": covers[publisher],
        "uncovered_peer_through": peer_through,
    })
