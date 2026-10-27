"""Real aggregate requests and known-key reads on an owned generated CI home."""

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from node import UnixConnection

SOURCE, MEMBER, TARGET = "AggregateSource", "AggregateMembers", "AggregateTotals"
PREFIX = "__lastdb_aggregate_"
RESERVED = [PREFIX + field for field in (
    "source_schema", "source_key", "target_schema", "target_key", "winner_counter",
    "winner_written_at", "winner_writer", "winner_mutation", "metric_fingerprint", "values")]
VALID, TOKEN = PREFIX + "valid", PREFIX + "guard_token"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def rpc(node, path, body, expected=(200,), request_id=None):
    body = encoded(body)
    headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-aggregate-benchmark"}
    if request_id:
        headers["X-LastDB-Request-Id"] = request_id
    connection = UnixConnection(node.socket)
    started = time.perf_counter_ns()
    try:
        connection.request("POST", path, body, headers)
        response = connection.getresponse()
        raw = response.read()
        elapsed = (time.perf_counter_ns() - started) / 1e6
        if response.status not in expected:
            raise RuntimeError(f"{path}: HTTP {response.status}: {raw[:800]!r}")
        result = json.loads(raw) if response.status == 200 else {"error": raw.decode()}
        if response.status == 200 and result.get("ok") is not True:
            raise RuntimeError(f"{path}: missing successful envelope: {raw[:800]!r}")
        return result.get("data", result), elapsed, response.status, hashlib.sha256(body).hexdigest()
    finally:
        connection.close()


def durable(result):
    if result.get("receipt", result).get("durability") != "durable":
        raise RuntimeError("An aggregate write lacks a durable acknowledgement")


def schemas(node):
    declarations = [
        (SOURCE, "HashRange", {"hash_field": "bucket", "range_field": "id"},
         {"bucket": "String", "id": "String", "value": "Integer"}),
        (MEMBER, "HashRange", {"hash_field": "bucket", "range_field": "id"},
         {**{field: "String" for field in ["bucket", "id", *RESERVED]}, PREFIX + "values": "Any"}),
        (TARGET, "Hash", {"hash_field": "id"},
         {"id": "String", "count": "Integer", "amount": "Integer", VALID: "Integer", TOKEN: "String"})]
    for name, kind, key, fields in declarations:
        rpc(node, "/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": kind, "key": key, "fields": list(fields), "field_types": fields,
            "field_descriptions": {field: name + " generated " + field for field in fields}}})


def source_write(group, number, value, create=False):
    key = {"hash": group, "range": f"member-{number:04d}"}
    return {"type": "mutation", "schema": SOURCE,
            "mutation_type": "create" if create else "update", "durability": "durable",
            "key_value": key, "fields_and_values": {"bucket": group, "id": key["range"], "value": value},
            "aggregate_set": {"target_schema_name": TARGET, "target_key_value": {"hash": group},
                              "member_schema_name": MEMBER, "member_key_value": key,
                              "contribution": {"count": 1, "amount": value}}}


def request(group, token):
    return {"source_schema_name": SOURCE, "source_partition_key_value": {"hash": group},
            "target_schema_name": TARGET, "target_key_value": {"hash": group},
            "member_schema_name": MEMBER, "expected_guard_token": token}


def summary(node, group, values, valid=None, token=None):
    result = rpc(node, "/api/query", {"schema_name": TARGET, "fields": ["count", "amount", VALID, TOKEN],
                                     "filter": {"HashKey": group}, "limit": 1})[0]
    rows = result.get("results", [])
    if len(rows) != 1 or result.get("returned_count") != 1:
        raise RuntimeError("The aggregate summary is absent or duplicated")
    fields = rows[0]["fields"]
    if (fields["count"] != len(values) or fields["amount"] != sum(values.values())
            or (valid is not None and fields[VALID] != valid)
            or (token is not None and fields[TOKEN] != token)):
        raise RuntimeError("The aggregate summary differs from the real source mutations")
    return fields


def records(node, group, values):
    expected = {f"member-{number:04d}": value for number, value in values.items()}
    query = {"limit": len(values), "filter": {"HashRangeKeys": [[group, key] for key in expected]}}
    for schema, fields in [(SOURCE, ["bucket", "id", "value"]), (MEMBER, RESERVED)]:
        result = rpc(node, "/api/query", {**query, "schema_name": schema, "fields": fields})[0]
        rows = result.get("results", [])
        if len(rows) != len(values) or result.get("returned_count") != len(values):
            raise RuntimeError("A known-key source/member read is incomplete")
        seen = set()
        for row in rows:
            key, actual = row["key"], row["fields"]
            ranged = key.get("range")
            if key.get("hash") != group or ranged not in expected or ranged in seen:
                raise RuntimeError("A source/member read returned an unexpected key")
            seen.add(ranged)
            if schema == SOURCE:
                if actual != {"bucket": group, "id": ranged, "value": expected[ranged]}:
                    raise RuntimeError("A source value differs after aggregate repair")
            elif (json.loads(actual[PREFIX + "source_key"]) != key
                  or actual[PREFIX + "source_schema"] != SOURCE
                  or actual[PREFIX + "target_schema"] != TARGET
                  or json.loads(actual[PREFIX + "target_key"]).get("hash") != group
                  or actual[PREFIX + "values"] != {"count": 1, "amount": expected[ranged]}):
                raise RuntimeError("A member identity or contribution differs")
    return {"source_records": len(values), "member_records": len(values),
            "values_sha256": hashlib.sha256(encoded(sorted(expected.items()))).hexdigest()}


def repeat(node, group, token):
    result = rpc(node, "/api/aggregate/finalize", request(group, token))[0]
    durable(result)
    if result.get("mutation_ids") != [] or result.get("revision") is not None:
        raise RuntimeError("Repeated finalize does not return a durable empty receipt")


def update_source(node, group, size, values, first, timed):
    values[0] += 1
    body = source_write(group, 0, values[0])
    durable(timed(f"source-update/{size}", "/api/mutation", body))
    valid = 0 if first else 1
    token = summary(node, group, values, valid)[TOKEN]
    refused = timed(f"unchanged-source/{size}", "/api/mutation", body, (400,))
    if "byte-identical source fields" not in refused["error"]:
        raise RuntimeError("The unchanged source failed for an unrelated reason")
    summary(node, group, values, valid, token)
    return token


def exercise_group(node, size, samples, timed):
    group, values = f"members-{size}", {number: number + 1 for number in range(size)}
    writes = [source_write(group, number, value, True) for number, value in values.items()]
    # AggregateSet currently uses the single-mutation API. Submit the collected seed concurrently.
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(lambda body: rpc(node, "/api/mutation", body)[0], writes):
            durable(result)
    token = summary(node, group, values, 0)[TOKEN]
    for cycle in range(samples):
        token = update_source(node, group, size, values, cycle == 0, timed)
        repaired = timed(f"repair/{size}", "/api/aggregate/repair", request(group, token))
        durable(repaired)
        token = repaired["repair_token"]
        if not token.startswith("repair:"):
            raise RuntimeError("Repair did not return its generated token")
        summary(node, group, values, 0, token)
        refused = timed(f"stale-finalize/{size}", "/api/aggregate/finalize", request(group, "old-token"), (400,))
        if "guard token" not in refused["error"]:
            raise RuntimeError("The stale token failed for an unrelated reason")
        summary(node, group, values, 0, token)
        durable(timed(f"finalize/{size}", "/api/aggregate/finalize", request(group, token)))
        summary(node, group, values, 1, token)
        repeated = timed(f"repeat-finalize/{size}", "/api/aggregate/finalize", request(group, token))
        durable(repeated)
        if repeated.get("mutation_ids") != [] or repeated.get("revision") is not None:
            raise RuntimeError("Repeated finalize wrote another mutation")
    rpc(node, "/api/aggregate/repair", request(group, "old-token"), (400,))
    wrong = request(group, token)
    wrong["target_key_value"] = {"hash": "other-summary"}
    rpc(node, "/api/aggregate/repair", wrong, (400,))
    summary(node, group, values, 1, token)
    token = rpc(node, "/api/aggregate/repair", request(group, token))[0]["repair_token"]
    summary(node, group, values, 0, token)
    barrier = threading.Barrier(2)

    def concurrent(path, body, expected):
        barrier.wait(timeout=30)
        return rpc(node, path, body, expected)

    values[0] += 1000
    with ThreadPoolExecutor(max_workers=2) as pool:
        write = pool.submit(concurrent, "/api/mutation", source_write(group, 0, values[0]), (200,))
        finalize = pool.submit(concurrent, "/api/aggregate/finalize", request(group, token), (200, 400))
        durable(write.result()[0])
        final = finalize.result()
        if final[2] == 200:
            durable(final[0])
    current = summary(node, group, values)[TOKEN]
    rpc(node, "/api/aggregate/finalize", request(group, token), (400,))
    result = rpc(node, "/api/aggregate/repair", request(group, current))[0]
    durable(result)
    token = result["repair_token"]
    summary(node, group, values, 0, token)
    durable(rpc(node, "/api/aggregate/finalize", request(group, token))[0])
    summary(node, group, values, 1, token)
    return group, values, token, records(node, group, values), final[2]
