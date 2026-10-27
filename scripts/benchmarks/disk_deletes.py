"""Measure durable deletes and absence by the exact generated keys."""

import time

from workload import HASH, SCHEMA, Workload, key, read_request


def prepare_delete(workload, logical_storage):
    target = workload.recipe["delete_target_bytes"]
    deadline = time.monotonic() + 300
    for _ in range(512):
        report = logical_storage(workload.node)
        if report["total_logical_bytes"] >= target:
            workload.delete_start_records = workload.live_records
            workload.delete_keep_records = (workload.live_records + 3) // 4
            return
        if time.monotonic() >= deadline:
            break
        workload.phase = "prepare-delete-corpus"
        workload.write(workload.live_records, 128, "delete-seed", True)
        workload.live_records += 128
    raise ValueError("The bounded delete seed did not reach its logical-size target")


def delete_records(workload, count):
    first = workload.live_records - count
    if first < workload.delete_keep_records:
        raise ValueError("The delete workload must retain one quarter of its starting records")
    for start in range(first, workload.live_records, 128):
        numbers = list(range(start, min(start + 128, workload.live_records)))
        request = {"mutations": [{"type": "mutation", "schema": SCHEMA, "mutation_type": "delete",
                                 "durability": "durable", "key_value": {"hash": HASH, "range": key(n)},
                                 "fields_and_values": {}} for n in numbers]}
        result, _, _ = workload.node.call("/api/mutations/batch", request)
        Workload.validate_write(result, len(numbers))
        for number in numbers:
            del workload.expected[number]
    workload.live_records = first
    workload.deleted_records = getattr(workload, "deleted_records", 0) + count


def verify_deleted(workload):
    first = workload.live_records
    until = first + getattr(workload, "deleted_records", 0)
    for start in range(first, until, 64):
        numbers = list(range(start, min(start + 64, until)))
        result, _, _ = workload.node.call("/api/query", read_request(numbers))
        if result.get("returned_count") != 0 or result.get("results") != []:
            raise ValueError("A deleted key still returns data")
