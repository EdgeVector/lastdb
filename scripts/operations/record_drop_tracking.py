"""Exercise schema-owned expiry cleanup through the existing CI daemon.

record_home.py supplies a fresh generated home. These public operations cover
more tracking rows than one native request admits, retry, reinstall, restart
and preservation of a second schema using the same hash.
"""
import time

from record_runs import definition, mutation, seed, write, FIELDS, RANGED, TTL_SECONDS

TRACKED_HASHES = 40_050


def require(value, message):
    if not value:
        raise RuntimeError(message)


def state(node, schema):
    current = node.call("/api/db/schema-retention", {"action": "get", "schema": schema})[0]
    hashes, cursor = [], None
    for _ in range(1_000):
        request = {"action": "partitions", "schema": schema, "limit": 256}
        if cursor:
            request["cursor"] = cursor
        page = node.call("/api/db/schema-retention", request)[0]
        hashes.extend(page["observed_hash_partitions"])
        following = page["next_cursor"]
        if following is None:
            current["observed_hash_partitions"] = sorted(hashes)
            return current
        require(following != cursor, "the partition page cursor did not advance")
        cursor = following
    raise RuntimeError("partition registry did not end in 1,000 pages")


def check(node):
    for schema, expected in [("RecordAged", []), ("RecordDropKeep", ["aged"])]:
        current = state(node, schema)
        require(current["observed_hash_partitions"] == expected,
                f"{schema} partition tracking differs: {current}")
        require((current["retention_policy"] is None) == (schema == "RecordAged"),
                f"{schema} policy changed across the drop: {current}")
    rows = node.call("/api/query", {"schema_name": "RecordDropKeep", "fields": FIELDS,
                                    "filter": {"HashKey": "aged"}})[0]["results"]
    require(len(rows) == 1 and rows[0]["fields"]["value"] == "kept", "the drop erased another schema")


def exercise(node):
    key = f"{int(time.time()):020d}-kept"
    kept = definition("RecordDropKeep", "HashRange", RANGED, FIELDS)
    seed(node, kept)
    node.call("/api/db/schema-retention",
              {"action": "set", "schema": kept["name"], "ttl_seconds": TTL_SECONDS})
    write(node, [mutation(kept["name"], {"hash": "aged", "range": key},
                          {"id": key, "bucket": "aged", "value": "kept"}, "create")])
    hashes = [f"drop-{number:05d}" for number in range(TRACKED_HASHES)]
    write(node, [mutation("RecordAged", {"hash": hash_key, "range": key},
                          {"id": key, "bucket": hash_key, "value": "drop"}, "create")
                 for hash_key in hashes])
    before = state(node, "RecordAged")
    require(before["observed_hash_partitions"] == ["aged"] + hashes,
            "the drop target does not hold the complete generated partition registry")
    node.call("/api/schemas/drop", {"schema": "RecordAged", "must_exist": True})
    again = node.call("/api/schemas/drop", {"schema": "RecordAged"})[0]
    require(not again["existed"], "the repeated schema drop was not an idempotent miss")
    seed(node, definition("RecordAged", "HashRange", RANGED, FIELDS))
    check(node)
    print(f"Schema drop: {TRACKED_HASHES + 1} tracked hashes removed in bounded batches; "
          "repeated drop, reinstall and other schema passed", flush=True)
