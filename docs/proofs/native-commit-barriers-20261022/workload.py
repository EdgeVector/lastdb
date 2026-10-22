"""Generated independent schemas and exact-key checks for a real daemon run."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import statistics

def save(path, value):
    if path.name != "writes.json":
        return path.write_text(json.dumps(value, indent=2) + "\n")
    metadata = {key: item for key, item in value.items() if key != "samples"}
    head = json.dumps(metadata, indent=2)[:-2]
    rows = ",\n".join("    "+json.dumps(row, separators=(",", ":")) for row in value["samples"])
    path.write_text(head+',\n  "samples": [\n'+rows+'\n  ]\n}\n')


def schema_name(number):
    return f"IndependentBatch{number:03}"


def payload(schema, number, batch_size, operation):
    items, expected = [], {}
    for position in range(batch_size):
        key = f"record-{position:03}"
        prefix = f"{schema}-{number:06}-{position:03}-"
        value = prefix + hashlib.sha256(prefix.encode()).hexdigest()[:64-len(prefix)]
        items.append({"type": "mutation", "schema": schema, "mutation_type": operation,
                      "key_value": {"hash": schema, "range": key},
                      "fields_and_values": {"bucket": schema, "id": key, "value": value}})
        expected[key] = hashlib.sha256(value.encode()).hexdigest()
    return {"mutations": items}, expected


def seed(node, count, output):
    def one(number):
        name = schema_name(number)
        return node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": ["bucket", "id", "value"],
            "field_descriptions": {field: f"generated {name} {field}" for field in ("bucket", "id", "value")}}})
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(one, range(count)))
    save(output / "seed.json", {"schemas": count, "receipts": receipts,
                               "distinct_field_descriptions": True})
    save(output / "setup-flush.json", node.call("/api/db/flush", {}))
    with ThreadPoolExecutor(max_workers=8) as pool:
        metadata = list(pool.map(lambda n: node.call("/api/schema/"+schema_name(n)), range(count)))
    save(output / "schema-metadata.json", dict(zip((schema_name(n) for n in range(count)), metadata)))


def save_worker_status(node, output):
    status = node.call("/api/status")
    status = status.get("status", status)
    selected = {key: value for key, value in status.items() if "uds" in key or key == "request_ops"}
    save(output, selected)


def check_values(node, expected):
    def one(item):
        schema, values = item
        response = node.call("/api/query", {"schema_name": schema, "fields": ["value"],
            "filter": {"HashRangeKeys": [[schema, key] for key in values]}, "limit": len(values)})
        actual = {record["key"]["range"]: hashlib.sha256(record["fields"]["value"].encode()).hexdigest()
                  for record in response["results"]}
        return {"schema": schema, "expected": len(values), "actual": len(actual),
                "all_values_match": actual == values,
                "mismatches": [key for key in values if values[key] != actual.get(key)]}
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(one, sorted(expected.items())))
    return {"expected": sum(r["expected"] for r in receipts),
            "actual": sum(r["actual"] for r in receipts),
            "all_values_match": all(r["all_values_match"] for r in receipts), "schemas": receipts}


def summarize(rows, elapsed, batch_size):
    values = sorted(row["latency_ms"] for row in rows)
    return {"count": len(rows), "errors": sum(not row["ok"] for row in rows),
            "average_ms": statistics.mean(values), "median_ms": statistics.median(values),
            "p95_ms": values[math.ceil(len(values)*.95)-1],
            "p99_ms": values[math.ceil(len(values)*.99)-1], "max_ms": values[-1],
            "wall_seconds": elapsed, "batches_per_second": len(rows)/elapsed,
            "records_per_second": len(rows)*batch_size/elapsed,
            "latency_unit": "whole batch request; no division by record count"}

