"""Actual writes through key materialization and field validation in CI."""

import hashlib
from concurrent.futures import ThreadPoolExecutor

from aggregate_requests import durable, encoded, rpc

TEXT, NUMBER, BOOLEAN, HASH = "TextPreparation", "NumberPreparation", "BooleanPreparation", "generated"
FIELDS = {TEXT: ["bucket", "position", "value", "body"],
          NUMBER: ["bucket", "position", "value", "body"], BOOLEAN: ["active", "value", "body"]}


def key(schema, coordinate):
    return {"hash": coordinate} if schema == BOOLEAN else {"hash": HASH, "range": coordinate}


def mutation(schema, coordinate, values, verb="update"):
    return {"type": "mutation", "schema": schema, "mutation_type": verb, "durability": "durable",
            "key_value": key(schema, coordinate), "fields_and_values": values}


def full(schema, coordinate, values):
    keys = ({"active": coordinate == "true"} if schema == BOOLEAN else
            {"bucket": HASH, "position": int(coordinate) if schema == NUMBER else coordinate})
    return {**keys, **values}


def verify_records(node, expected):
    requests = []
    for schema, records in expected.items():
        keys = list(records)
        for start in range(0, len(keys), 128):
            chosen = keys[start:start + 128]
            # Hash-only schemas use independent exact-key requests in parallel.
            if schema == BOOLEAN:
                requests.extend((schema, [coordinate], {"HashKey": coordinate}) for coordinate in chosen)
            else:
                requests.append((schema, chosen, {"HashRangeKeys": [[HASH, coordinate] for coordinate in chosen]}))

    def read(spec):
        schema, chosen, selector = spec
        result = rpc(node, "/api/query", {"schema_name": schema, "fields": FIELDS[schema],
                     "filter": selector, "limit": len(chosen)})[0]
        rows = result.get("results", [])
        if result.get("returned_count") != len(chosen) or len(rows) != len(chosen):
            raise RuntimeError("A known-key materialization read is incomplete")
        seen, output = set(), []
        for row in rows:
            coordinate = row["key"].get("hash" if schema == BOOLEAN else "range")
            if coordinate not in chosen or coordinate in seen or row["fields"] != expected[schema][coordinate]:
                raise RuntimeError("A materialized key or payload differs from the accepted write")
            if schema != BOOLEAN and row["key"].get("hash") != HASH:
                raise RuntimeError("A materialization read returned an unexpected hash")
            seen.add(coordinate)
            output.append((schema, coordinate, row["fields"]))
        return output

    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = [row for batch in pool.map(read, requests) for row in batch]
    return hashlib.sha256(encoded(sorted(rows))).hexdigest()


def reject(result, phrase):
    if phrase not in result.get("error", ""):
        raise RuntimeError("A preparation refusal has an unexpected cause: " + str(result))


def batch_result(result, count):
    durable(result)
    if result.get("count") != count or len(result.get("mutation_ids", [])) != count:
        raise RuntimeError("A preparation batch lacks its input identities")


def setup(node):
    for schema in (TEXT, NUMBER, BOOLEAN):
        types = ({"active": "Boolean"} if schema == BOOLEAN else
                 {"bucket": "String", "position": "Integer" if schema == NUMBER else "String"})
        types.update(value="Integer", body="String")
        rpc(node, "/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": schema, "descriptive_name": schema,
            "schema_type": "Hash" if schema == BOOLEAN else "HashRange",
            "key": {"hash_field": "active"} if schema == BOOLEAN else {"hash_field": "bucket", "range_field": "position"},
            "fields": FIELDS[schema], "field_types": types,
            "field_descriptions": {field: schema + " generated " + field for field in types}}})
    expected = {TEXT: {coordinate: full(TEXT, coordinate, {"value": 0, "body": "seed"})
                      for coordinate in ["known", "atomic", *[f"update-{n}" for n in range(32)]]},
                NUMBER: {}, BOOLEAN: {coordinate: full(BOOLEAN, coordinate, {"value": 0, "body": "seed"})
                                      for coordinate in ("true", "false")}}
    writes = [mutation(schema, coordinate, values, "create") for schema, records in expected.items()
              for coordinate, values in records.items()]
    batch_result(rpc(node, "/api/mutations/batch", {"mutations": writes})[0], len(writes))
    return expected


def exercise(node, samples, timed):
    expected = setup(node)

    def single(case, schema, coordinate, values, verb="update", explicit=False):
        body = mutation(schema, coordinate, full(schema, coordinate, values) if explicit else values, verb)
        durable(timed(case, "/api/mutation", body))
        expected[schema][coordinate] = full(schema, coordinate, values)
        verify_records(node, {schema: {coordinate: expected[schema][coordinate]}})

    for i in range(samples):
        values = {"value": i + 1, "body": f"payload-{i}-" + "x" * 1000}
        single("create-omitted/1", TEXT, f"omitted-{i}", values, "create")
        single("update-omitted/1", TEXT, "known", values)
        single("create-explicit/1", TEXT, f"explicit-{i}", values, "create", True)
        for case, coordinates, verb in (
                ("batch-create/32", [f"batch-{i}-{n}" for n in range(32)], "create"),
                ("batch-update/32", [f"update-{n}" for n in range(32)], "update")):
            bodies = [mutation(TEXT, coordinate, values, verb) for coordinate in coordinates]
            batch_result(timed(case, "/api/mutations/batch", {"mutations": bodies}), 32)
            changed = {coordinate: full(TEXT, coordinate, values) for coordinate in coordinates}
            expected[TEXT].update(changed)
            verify_records(node, {TEXT: changed})
        single("integer-key/1", NUMBER, str(i + 1), values, "create")
        single("boolean-key/1", BOOLEAN, "true" if i % 2 == 0 else "false", values)
        refusals = [
            ("field-type-refuse/1", mutation(TEXT, "known", {"value": "invalid"}), "Type error in field"),
            ("key-conflict-refuse/1", mutation(TEXT, "known", {"bucket": "wrong", "value": -1}), "conflicts with key_value"),
            ("nonscalar-key-refuse/1", mutation(TEXT, "known", {"bucket": ["wrong"], "value": -1}), "must be a scalar")]
        for case, body, phrase in refusals:
            reject(timed(case, "/api/mutation", body, (400,)), phrase)
            verify_records(node, {TEXT: {"known": expected[TEXT]["known"]}})
        body = {"mutations": [mutation(TEXT, "known", {"value": 100000 + i}),
                               mutation(TEXT, "atomic", {"value": "invalid"})]}
        reject(timed("batch-type-refuse/2", "/api/mutations/batch", body, (400,)), "Type error in field")
        verify_records(node, {TEXT: {coordinate: expected[TEXT][coordinate] for coordinate in ("known", "atomic")}})
    operational(node, expected)
    return {"expected": expected, "values_sha256": verify_records(node, expected),
            "operational_checks": ["payload-derived keys", "Hash ignores range", "atom size refusal", "restart"]}


def operational(node, expected):
    values = full(TEXT, "from-fields", {"value": 7, "body": "derived coordinates"})
    body = mutation(TEXT, "from-fields", values, "create")
    body["key_value"] = {}
    durable(rpc(node, "/api/mutation", body)[0])
    expected[TEXT]["from-fields"] = values
    values = {"value": 999, "body": "ignored range"}
    body = mutation(BOOLEAN, "true", values)
    body["key_value"]["range"] = "ignored"
    durable(rpc(node, "/api/mutation", body)[0])
    expected[BOOLEAN]["true"] = full(BOOLEAN, "true", values)
    reject(rpc(node, "/api/mutation", mutation(TEXT, "known", {"body": "z" * 300000}), (413,))[0],
           "atom_content_too_large")
    verify_records(node, expected)
