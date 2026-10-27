"""Actual conditional-write latency on generated CI records and known keys."""

import hashlib
import struct

from aggregate_requests import durable, encoded, rpc

SCHEMA, HASH = "ConditionalWrites", "generated"


def mutation(key, fields, verb="update", **options):
    return {"type": "mutation", "schema": SCHEMA, "mutation_type": verb,
            "durability": "durable", "key_value": {"hash": HASH, "range": key},
            "fields_and_values": fields, **options}


def expect(value):
    return {"type": "value", "field": "value", "value": value}


def atom_hash(value):
    # The product's public content identity uses schema and JSON length prefixes.
    schema, content = SCHEMA.encode(), encoded(value)
    return hashlib.sha256(struct.pack(">I", len(schema)) + schema
                          + struct.pack(">I", len(content)) + content).hexdigest()


def read_records(node, expected):
    # A field-set bucket is one batch read, so partial records need no serial reads.
    buckets = {}
    for key, value in expected.items():
        buckets.setdefault(tuple(sorted(value)), []).append(key)
    actual = {}
    for fields, keys in buckets.items():
        result = rpc(node, "/api/query", {"schema_name": SCHEMA, "fields": fields,
                     "filter": {"HashRangeKeys": [[HASH, key] for key in keys]}, "limit": len(keys)})[0]
        rows = result.get("results", [])
        if result.get("returned_count") != len(keys) or len(rows) != len(keys):
            raise RuntimeError("Conditional known-key read is incomplete")
        seen = set()
        for row in rows:
            key = row["key"]
            if key.get("hash") != HASH or key.get("range") not in keys or key["range"] in seen:
                raise RuntimeError("Conditional read returned an unexpected or duplicate key")
            seen.add(key["range"])
            if row["fields"] != expected[key["range"]]:
                raise RuntimeError("Conditional values differ from accepted writes")
            actual[key["range"]] = row["fields"]
    return hashlib.sha256(encoded(actual)).hexdigest()


def require_absent(node, keys):
    result = rpc(node, "/api/query", {"schema_name": SCHEMA, "fields": ["extra"],
                 "filter": {"HashRangeKeys": [[HASH, key] for key in keys]}, "limit": len(keys)})[0]
    if result.get("returned_count") != 0 or result.get("results") != []:
        raise RuntimeError("A refused missing-key write created a field")


def reject(result, phrase):
    if phrase not in result.get("error", ""):
        raise RuntimeError("A conditional refusal has a different cause: " + str(result))


def commit(node, bodies):
    result = rpc(node, "/api/mutations/batch", {"mutations": bodies})[0]
    durable(result)
    if result.get("count") != len(bodies) or len(result.get("mutation_ids", [])) != len(bodies):
        raise RuntimeError("A conditional batch lacks all input identities")
    return result


def exercise(node, samples, timed):
    fields = {"bucket": "String", "id": "String", "value": "Integer", "extra": "Integer"}
    rpc(node, "/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
        "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
        "fields": list(fields), "field_types": fields,
        "field_descriptions": {field: SCHEMA + " generated " + field for field in fields}}})
    expected = {key: {"bucket": HASH, "id": key, "value": 0}
                for key in ["single", "repeat", "same-key", *[f"guarded-{i}" for i in range(32)],
                            *[f"heal-{i}" for i in range(samples)]]}
    commit(node, [mutation(key, value, "create") for key, value in expected.items()])
    value = 0
    repeat_operations = []
    for i in range(samples):
        result = timed("cas-value/1", "/api/mutation", mutation("single", {"value": value + 1}, expected=expect(value)))
        durable(result)
        value += 1
        result = timed("cas-value-refuse/1", "/api/mutation", mutation("single", {"value": -1}, expected=expect(-2)), (409,))
        reject(result, "cas_conflict")
        read_records(node, {"single": {**expected["single"], "value": value}})
        result = timed("cas-hash/1", "/api/mutation", mutation("single", {"value": value + 1},
                       expected={"type": "content_hash", "field": "value", "hash": atom_hash(value)}))
        durable(result)
        value += 1
        result = timed("cas-absent-refuse/1", "/api/mutation", mutation("single", {"value": -1},
                       expected={"type": "absent", "field": "value"}), (409,))
        reject(result, "cas_conflict")
        read_records(node, {"single": {**expected["single"], "value": value}})
        new_key = f"new-{i}"
        new_value = {"bucket": HASH, "id": new_key, "value": i}
        durable(timed("cas-absent-create/1", "/api/mutation", mutation(new_key, new_value, "create",
                      expected={"type": "absent", "field": "value"})))
        expected[new_key] = new_value
        value += 1
        durable(timed("must-exist/1", "/api/mutation", mutation("single", {"value": value}, must_exist=True)))
        expected["single"]["value"] = value
        heal_key = f"heal-{i}"
        durable(timed("must-exist-fallback/1", "/api/mutation", mutation(heal_key, {"extra": i}, must_exist=True)))
        expected[heal_key]["extra"] = i
        result = timed("must-exist-refuse/1", "/api/mutation", mutation(f"missing-{i}", {"extra": i}, must_exist=True), (400,))
        reject(result, "update target not found")
        body = mutation("repeat", {"value": i + 1})
        durable(rpc(node, "/api/mutation", body)[0])
        result = timed("repeat-request/1", "/api/mutation", body)
        durable(result)
        repeat_operations.append(result["operations"])
        expected["repeat"]["value"] = i + 1
        bodies = [mutation("same-key", {"value": n}) for n in (i * 3 + 1, i * 3 + 2, i * 3 + 1)]
        result = timed("same-key-batch/3", "/api/mutations/batch", {"mutations": bodies})
        durable(result)
        if result.get("count") != 3 or len(result.get("mutation_ids", [])) != 3:
            raise RuntimeError("Same-key batch lacks three receipt identities")
        expected["same-key"]["value"] = i * 3 + 1
        bodies = [mutation(f"guarded-{n}", {"value": i + 1}, expected=expect(i), must_exist=True) for n in range(32)]
        result = timed("guarded-batch/32", "/api/mutations/batch", {"mutations": bodies})
        durable(result)
        if result.get("count") != 32 or len(result.get("mutation_ids", [])) != 32:
            raise RuntimeError("Guarded batch lacks all32 receipt identities")
        for n in range(32):
            expected[f"guarded-{n}"]["value"] = i + 1
        read_records(node, expected)
        require_absent(node, [f"missing-{i}"])
    operational(node, expected)
    return {"expected": expected, "values_sha256": read_records(node, expected),
            "repeat_request_operations": repeat_operations, "public_repeat_is_signed_replay": False}


def operational(node, expected):
    # An earlier must-exist failure must not hide a later CAS failure: CAS runs first.
    body = {"mutations": [mutation("absent-order", {"extra": 1}, must_exist=True),
                          mutation("single", {"value": -5}, expected=expect(-6))]}
    reject(rpc(node, "/api/mutations/batch", body, (409,))[0], "cas_conflict")
    # A mismatched hash and an invalid expected field retain their typed refusals.
    reject(rpc(node, "/api/mutation", mutation("single", {"extra": 1},
               expected={"type": "content_hash", "field": "value", "hash": "0" * 64}), (409,))[0], "cas_conflict")
    reject(rpc(node, "/api/mutation", mutation("single", {"extra": 1},
               expected={"type": "absent", "field": "unknown"}), (400,))[0], "absent from schema")
    read_records(node, expected)
    require_absent(node, ["absent-order"])
    durable(rpc(node, "/api/mutation", mutation("single", {}, "delete"))[0])
    durable(rpc(node, "/api/mutation", mutation("single", expected["single"], "create",
                expected={"type": "absent", "field": "value"}))[0])
    # A public retry carries a new clock; a repeat with a now-stale expectation refuses.
    reject(rpc(node, "/api/mutation", mutation("single", {"value": -7}, expected=expect(-8)), (409,))[0], "cas_conflict")
