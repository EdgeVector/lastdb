"""History-index and signed/automatic/compatibility prefix reclaim verification."""

import time

from snapshot_gate_aws import (
    SNAPSHOT_THROUGH,
    entry_digest,
    entry_key,
    summary,
    verify_facts,
)
from support import OperationError, require, retry

NEWER_THAN_SNAPSHOT = {"id": "native-cutoff-tail", "note": "written after publishing the snapshot"}


def retention(private, call, snapshot_id, *, allow_older_service=False):
    state = call("snapshot/bytes", {"action": "retention"}, allow_older_service=allow_older_service)
    if state is None:
        return None
    require(
        type(state.get("reclaim_enabled")) is bool
        and state.get("eligible") is True
        and state.get("snapshot_id") == snapshot_id
        and private.decimal(state["snapshot_through"]) == SNAPSHOT_THROUGH,
        "native_reclaim_retention",
    )
    return state["reclaim_enabled"], private.decimal(state["reclaimed_through"])


def automatic_supported(call, *, eligible, allow_older_service=False):
    state = call("snapshot/bytes", {"action": "automatic-retention"}, allow_older_service=allow_older_service)
    if state is None:
        return False
    require(
        state
        == {
            "reclaim_enabled": False,
            "eligible": eligible,
            "snapshot_id": None,
            "snapshot_through": "0",
            "reclaimed_through": "0",
        },
        "native_automatic_reclaim_initial",
    )
    return True


def automatic_floor(private, state, snapshot_id):
    require(
        state.get("reclaim_enabled") is True
        and state.get("eligible") is True
        and state.get("snapshot_id") == snapshot_id
        and private.decimal(state["snapshot_through"]) == SNAPSHOT_THROUGH,
        "native_automatic_reclaim_snapshot",
    )
    floor = private.decimal(state["reclaimed_through"])
    require(0 <= floor <= SNAPSHOT_THROUGH, "native_automatic_reclaim_outside_snapshot")
    return floor


def index_status(call, database, first, *, allow_older_service=False):
    state = call("snapshot/bytes", {"action": "index-status"}, allow_older_service=allow_older_service)
    if state is None:
        return False
    require(
        state
        == {
            "database_id": database,
            "index_enabled": True,
            "eligible": first is not None,
            "first_indexed_delivery": first,
        },
        "native_history_index_status",
    )
    return True


def check_indexed_history(call, database, read_storage, entries, facts):
    require(index_status(call, database, "1"), "native_history_index_available")
    request = {"action": "index-history", "expected_first_indexed_delivery": "1"}
    result = call("snapshot/bytes", request)
    require(
        result == {"database_id": database, "first_indexed_delivery": "1", "indexed_entries": "0"},
        "native_history_index_no_op",
    )
    stale = call("snapshot/bytes", {**request, "expected_first_indexed_delivery": "2"}, expected_error=409)
    require(
        stale == {"code": "predecessor_conflict", "field": "snapshot.index.first_delivery"},
        "native_history_index_stale_retry",
    )
    require(
        index_status(call, database, "1") and read_storage() == (entries, facts),
        "native_history_index_preserved_storage",
    )
    summary("history_index=verified_already_indexed_no_op legacy_backfill=not_exercised")


def reclaim_receipt(snapshot_id, floor):
    return {
        "snapshot_id": snapshot_id,
        "snapshot_through": str(SNAPSHOT_THROUGH),
        "reclaimed_through": str(floor),
    }


def reclaim_step(private, call, request, before, after):
    snapshot_id = request["snapshot_id"]
    for _ in range(4):
        result = call("snapshot/bytes", {**request, "expected_floor": str(before)}, allow_unknown_outcome=True)
        if result is not None:
            require(result == reclaim_receipt(snapshot_id, after), "native_reclaim_receipt")
        enabled, floor = retention(private, call, snapshot_id)
        require(enabled and floor in (before, after), "native_reclaim_floor")
        if floor == after:
            return
        require(result is None, "native_reclaim_unconfirmed_receipt")
        time.sleep(0.25)
    raise OperationError("native_reclaim_deadline")


def auto_reclaim_preimage(private, call, database, read_storage, peer):
    entries, facts = read_storage()
    require(
        set(entries) == {entry_key(seq) for seq in range(1, SNAPSHOT_THROUGH + 1)},
        "native_automatic_reclaim_initial_bodies",
    )
    verify_facts(database, SNAPSHOT_THROUGH, entries, facts)
    page = call("list", {"after": "9", "limit": "1"})
    require(
        private.decimal(page["next"]) == 10 and len(page["entries"]) == 1,
        "native_automatic_reclaim_old_delivery",
    )
    entry = page["entries"][0]
    require(
        private.decimal(entry["delivery_seq"]) == 10
        and entry["entry"]["device_id"] == private.device_id(peer)
        and private.decimal(entry["entry"]["writer_seq"]) == 4,
        "native_automatic_reclaim_old_writer",
    )
    return facts, entry["entry"]


def check_auto_reclaim(private, call, source, database, read_storage, snapshot_id, preimage):
    initial_facts, old_entry = preimage
    tail = SNAPSHOT_THROUGH + 1
    private.write(source, database, NEWER_THAN_SNAPSHOT)
    private.wait_prefix(source, database, tail, "native_automatic_reclaim_tail")
    previous, facts_with_tail = read_storage(through=tail)
    tail_key = entry_key(tail)
    require(
        tail_key in previous and all(facts_with_tail.get(key) == value for key, value in initial_facts.items()),
        "native_automatic_reclaim_initial_tail",
    )
    new_facts = {key: value for key, value in facts_with_tail.items() if key not in initial_facts}
    require(
        len(new_facts) == 1
        and next(iter(new_facts.values()))
        == {
            "first_writer_seq": 9,
            "first_delivery_seq": tail,
            "digests": [entry_digest(database, previous[tail_key])],
        },
        "native_automatic_reclaim_tail_fact",
    )
    retry(
        "native_automatic_reclaim_wait",
        lambda: call("snapshot/bytes", {"action": "automatic-retention"}),
        lambda state: automatic_floor(private, state, snapshot_id) == SNAPSHOT_THROUGH,
    )
    require(
        read_storage(through=tail) == ({tail_key: previous[tail_key]}, facts_with_tail),
        "native_automatic_reclaim_storage",
    )
    legacy = call("list", {"after": "10", "limit": "2"}, expected_error=400)
    require(
        legacy == {"code": "unsupported_capability", "field": "snapshot.native_reset_required"},
        "native_automatic_reclaim_legacy_refused",
    )
    private.check_entry_retry(call, old_entry, tail)
    return previous[tail_key]


def check_bulk_reclaim(private, call, read_storage, request, entries, facts):
    target = SNAPSHOT_THROUGH - 1
    window = {**request, "target_through": str(target), "max_steps": 2}
    receipt = reclaim_receipt(request["snapshot_id"], target)
    result = call("snapshot/bytes", {**window, "expected_floor": "0"})
    require(result == receipt, "native_bulk_reclaim_two_steps")
    require(retention(private, call, request["snapshot_id"]) == (True, target), "native_bulk_reclaim_floor")
    stale = call("snapshot/bytes", {**window, "expected_floor": "0"}, expected_error=409)
    require(
        stale == {"code": "predecessor_conflict", "field": "snapshot.retention.floor"},
        "native_bulk_reclaim_stale_retry",
    )
    repeated = call("snapshot/bytes", {**window, "expected_floor": str(target)})
    require(
        repeated == receipt and retention(private, call, request["snapshot_id"]) == (True, target),
        "native_bulk_reclaim_fixed_target",
    )
    last = entry_key(SNAPSHOT_THROUGH)
    require(
        read_storage() == ({last: entries[last]}, facts),
        "native_bulk_reclaim_preserved_suffix",
    )
    summary(f"signed_bulk_reclaim=verified max_steps=2 floor={target} retained_delivery={SNAPSHOT_THROUGH}")
    reclaim_step(private, call, {**window, "target_through": str(SNAPSHOT_THROUGH)}, target, SNAPSHOT_THROUGH)


def reclaim_signed_prefix(private, call, database, read_storage, snapshot_id, *, history_supported):
    entries, facts = read_storage()
    require(
        set(entries) == {entry_key(position) for position in range(1, SNAPSHOT_THROUGH + 1)},
        "native_reclaim_initial_bodies",
    )
    verify_facts(database, SNAPSHOT_THROUGH, entries, facts)
    if history_supported:
        check_indexed_history(call, database, read_storage, entries, facts)
    legacy_list = {"after": "10", "limit": "2"}
    before = call("list", legacy_list)
    require(
        private.decimal(before["next"]) == SNAPSHOT_THROUGH
        and [(private.decimal(item["delivery_seq"]), private.decimal(item["entry"]["writer_seq"])) for item in before["entries"]]
        == [(11, 7), (12, 8)],
        "native_reclaim_legacy_before",
    )
    old_entry = before["entries"][-1]["entry"]
    require(call("head", {"device_id": old_entry["device_id"]}) == {"last_writer_seq": "8"}, "native_reclaim_last_writer")
    request = {"action": "reclaim", "snapshot_id": snapshot_id, "require_native_recovery": True}
    refused = call(
        "snapshot/bytes", {**request, "expected_floor": "0", "require_native_recovery": False}, expected_error=400
    )
    require(
        refused == {"code": "invalid_input", "field": "snapshot.retention.opt_in"}
        and retention(private, call, snapshot_id) == (True, 0),
        "native_reclaim_requires_opt_in",
    )

    if history_supported:
        check_bulk_reclaim(private, call, read_storage, request, entries, facts)
    else:
        reclaim_step(private, call, request, 0, 8)
        stale = call("snapshot/bytes", {**request, "expected_floor": "0"}, expected_error=409)
        require(
            stale == {"code": "predecessor_conflict", "field": "snapshot.retention.floor"}
            and retention(private, call, snapshot_id) == (True, 8),
            "native_reclaim_stale_retry",
        )
        reclaim_step(private, call, request, 8, SNAPSHOT_THROUGH)

    current = str(SNAPSHOT_THROUGH)
    complete = call("snapshot/bytes", {**request, "expected_floor": current})
    require(
        complete == reclaim_receipt(snapshot_id, SNAPSHOT_THROUGH)
        and retention(private, call, snapshot_id) == (True, SNAPSHOT_THROUGH),
        "native_reclaim_complete_retry",
    )
    legacy = call("list", legacy_list, expected_error=400)
    require(
        legacy == {"code": "unsupported_capability", "field": "snapshot.native_reset_required"},
        "native_reclaim_legacy_refused",
    )
    require(read_storage() == ({}, facts), "native_reclaim_physical_prefix")
    private.check_entry_retry(call, old_entry, SNAPSHOT_THROUGH)
    summary(f"signed_reclaim=verified floor={SNAPSHOT_THROUGH} physical_prefix=absent accepted_facts=unchanged")
