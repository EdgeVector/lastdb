"""Measure durable deletes and absence by the exact generated keys."""

from workload import HASH, SCHEMA, Workload, key, read_request


def delete_records(workload, count):
    first = workload.live_records - count
    if first < workload.recipe["records"]:
        raise ValueError("The delete workload must retain its original live record count")
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
