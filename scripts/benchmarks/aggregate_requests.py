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
         {"bucket": "String", "id": "String", "value": "Integer", "state": "Any"}),
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
            "key_value": key, "fields_and_values": {"bucket": group, "id": key["range"], "value": value, "state": {"active": True}},
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


def records(node, group, values, member_values=None):
    expected = {f"member-{number:04d}": value for number, value in values.items()}
    expected_member = {f"member-{number:04d}": value for number, value in (member_values or values).items()}
    query = {"limit": len(values), "filter": {"HashRangeKeys": [[group, key] for key in expected]}}
    for schema, fields in [(SOURCE, ["bucket", "id", "value", "state"]), (MEMBER, RESERVED)]:
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
                if actual != {"bucket": group, "id": ranged, "value": expected[ranged], "state": {"active": True}}:
                    raise RuntimeError("A source value differs after aggregate repair")
            elif (json.loads(actual[PREFIX + "source_key"]) != key
                  or actual[PREFIX + "source_schema"] != SOURCE
                  or actual[PREFIX + "target_schema"] != TARGET
                  or json.loads(actual[PREFIX + "target_key"]).get("hash") != group
                  or actual[PREFIX + "values"] != {"count": 1, "amount": expected_member[ranged]}):
                raise RuntimeError("A member identity or contribution differs")
    return {"source_records": len(values), "member_records": len(values),
            "values_sha256": hashlib.sha256(encoded(sorted(expected.items()))).hexdigest()}


def repeat(node, group, token):
    result = rpc(node, "/api/aggregate/finalize", request(group, token))[0]
    durable(result)
    if result.get("mutation_ids") != [] or result.get("revision") is not None:
        raise RuntimeError("Repeated finalize does not return a durable empty receipt")

