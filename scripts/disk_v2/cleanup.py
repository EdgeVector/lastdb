"""Explicit cleanup through the real owner socket in the existing CI operation."""

import time
import uuid


def validate(receipt):
    if (receipt.get("phase") != "complete" or receipt.get("complete") is not True
            or receipt.get("scope") != "already_deleted_records_in_active_main_and_cas_stores"
            or receipt.get("backup_copies_excluded") is not True
            or receipt.get("live_cas_orphan_repair_included") is not False
            or receipt.get("last_error") is not None):
        raise RuntimeError(f"cleanup lacks a scoped completion receipt: {receipt}")
    for name in ("main", "cas"):
        state = receipt[name]
        if state["tiered_runs"] or state["merging_runs"] or state["held_retired_bytes"]:
            raise RuntimeError(f"cleanup completed with unfinished {name} work")
        for measure in ("frontier", "generation"):
            key = name + "_" + measure
            if receipt["completed_through"][key] < receipt["target"][key]:
                raise RuntimeError(f"cleanup did not cover its {name} target")
    if not receipt.get("request_id") or receipt.get("completed_at") is None:
        raise RuntimeError("cleanup lacks a durable request identity or completion time")


def request_and_wait(node):
    request_id = str(uuid.uuid4())
    first, _, _ = node.call("/api/db/cleanup", {"request_id": request_id})
    target = first["cleanup"]["target"]
    until = time.monotonic() + 300
    while True:
        value, _, _ = node.call("/api/db/cleanup")
        receipt = value["cleanup"]
        if receipt["request_id"] != request_id or receipt["target"] != target:
            raise RuntimeError("cleanup request identity or target changed")
        if receipt["complete"]:
            validate(receipt)
            repeated, _, _ = node.call("/api/db/cleanup", {"request_id": request_id})
            if repeated["cleanup"] != receipt:
                raise RuntimeError("same-id cleanup repeat changed its completed receipt")
            return receipt
        if receipt["phase"] in ("retrying", "recovery_required") or time.monotonic() >= until:
            raise RuntimeError(f"cleanup did not complete: {receipt}")
        time.sleep(0.25)


def verify_saved(node, receipt):
    value, _, _ = node.call("/api/db/cleanup")
    validate(value["cleanup"])
    if value["cleanup"] != receipt:
        raise RuntimeError("node reopen changed the saved cleanup receipt")
