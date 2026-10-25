"""Generated coordinates and complete public query-page verification."""

import hashlib
import json
import math
import re
import statistics

from condition_values import extend

SCHEMA, HASH, COUNT = "RangeContinuation", "generated", 16384
FIELDS = ["bucket", "id", "title", "body"]
EDGE_HASH = "edge-\u00e9-\u732b"
EDGE_KEYS = ["", "a", "a\0", "a\0b", "aa", "b", "\u00e9", "\u732b", "\U00010000", "\U0010ffff"]


def key(number):
    return f"key-{number:06d}"


def fields(hashed, ranged, revision="seed"):
    title = json.dumps([hashed, ranged, revision], ensure_ascii=True)
    return extend({"bucket": hashed, "id": ranged, "title": title,
                   "body": hashlib.shake_256(title.encode()).hexdigest(512)})


def query(hashed, start, end, limit=256):
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": limit,
            "filter": {"HashRangeRange": {"hash": hashed, "start": start, "end": end}}}


def prefix(hashed, value, limit=256):
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": limit,
            "filter": {"HashRangePrefix": {"hash": hashed, "prefix": value}}}


def selected(expected, request):
    selector = request["filter"]
    if "HashKey" in selector:
        return [(hashed, ranged) for hashed, ranged in sorted(expected) if hashed == selector["HashKey"]]
    if "HashRangeRange" in selector:
        spec = selector["HashRangeRange"]
        return [(spec["hash"], ranged) for hashed, ranged in sorted(expected)
                if hashed == spec["hash"] and spec["start"] <= ranged < spec["end"]]
    spec = selector["HashRangePrefix"]
    return [(spec["hash"], ranged) for hashed, ranged in sorted(expected)
            if hashed == spec["hash"] and ranged.startswith(spec["prefix"])]


def validate(result, expected, coordinates, projected_fields=None):
    records = result.get("results", [])
    actual = [(row["key"].get("hash"), row["key"].get("range")) for row in records]
    if actual != coordinates or result.get("returned_count") != len(coordinates):
        raise RuntimeError("The query page membership, order, or returned count differs")
    for coordinate, row in zip(coordinates, records):
        wanted = expected[coordinate]
        if projected_fields is not None:
            # Current co-key responses also retain the declared primary hash
            # field. Check that exact established shape on both binaries.
            wanted = {name: wanted[name] for name in ["bucket", *projected_fields]}
        if row["fields"] != wanted:
            raise RuntimeError("A generated query page returns stale or incorrect fields")
    if result.get("unresolved_rows") != 0:
        raise RuntimeError("A generated query page reports an unresolved record")


def validate_cursor(cursor, previous, coordinate):
    if not isinstance(cursor, dict) or set(cursor) - {"hash", "range", "comparison"}:
        raise RuntimeError("A range cursor has an unsupported wire shape")
    if {name: cursor.get(name) for name in ("hash", "range")} != {
            "hash": coordinate[0], "range": coordinate[1]}:
        raise RuntimeError("The range cursor differs from the last returned API key")
    tag = cursor.get("comparison")
    if "comparison" in cursor and (not isinstance(tag, str) or not re.fullmatch(r"qc1:[0-9a-f]{64}", tag)):
        raise RuntimeError("The range cursor has an invalid comparison identifier")
    if previous and previous.get("comparison") is not None and previous["comparison"] != tag:
        raise RuntimeError("The range comparison identifier changed during continuation")


def walk(node, expected, request, label, baseline=False, *, reopened=False, legacy_cursor=False):
    wanted = selected(expected, request)
    offset = request.get("offset", 0)
    cursor = request.get("cursor")
    if cursor is not None:
        wanted = [coordinate for coordinate in wanted
                  if (coordinate[1], coordinate[0]) > (cursor.get("range", ""), cursor.get("hash", ""))]
    wanted = wanted[offset:]
    position, samples = 0, []
    maximum_pages = COUNT // max(1, request["limit"]) + 16
    for _ in range(maximum_pages):
        result, receipt = node.call("/api/query", request, expected=(200, 503) if baseline else (200,))
        if receipt["http_status"] == 503:
            message = result.get("message", "")
            known_limit = "bounded continuation" in message or (reopened and message ==
                "read resource limit exceeded: disk-pack v1: read resource limit exceeded")
            if (samples or result.get("error") != "read_resource_limit"
                    or result.get("retryable") is not False
                    or not known_limit):
                raise RuntimeError("The baseline broad query returns an unexpected failure: " + label)
            return {"label": label, "outcome": "expected_baseline_limitation", "http_status": 503,
                    "request_id": receipt["request_id"], "error": result,
                    "failure_elapsed_ms": receipt["elapsed_ms"], "latency_sample": False}
        count = len(result.get("results", []))
        if count > request["limit"] or position + count > len(wanted):
            raise RuntimeError("The query page exceeds its selected interval")
        validate(result, expected, wanted[position:position + count], request.get("fields"))
        if receipt["storage_read_receipt"] is None:
            raise RuntimeError("A measured query lacks its storage read receipt")
        samples.append({"request_id": receipt["request_id"], "elapsed_ms": receipt["elapsed_ms"],
                        "request_sha256": hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest(),
                        "records": count, "storage_read_receipt": receipt["storage_read_receipt"]})
        if "process_counters" in receipt:
            samples[-1]["process_counters"] = receipt["process_counters"]
        position += count
        if result.get("has_more") is False:
            if position != len(wanted):
                raise RuntimeError("A complete range omits selected generated values")
            break
        if result.get("has_more") is not True:
            raise RuntimeError("A query page lacks its explicit continuation state")
        following = result.get("next_cursor")
        if not count or not following or following == cursor:
            raise RuntimeError("The exclusive range cursor cannot advance")
        validate_cursor(following, cursor, wanted[position - 1])
        cursor = following
        # Matched latency uses the same supported legacy request on both binaries.
        # Broad operations echo the full cursor, including its comparison identity.
        wire_cursor = {name: following[name] for name in ("hash", "range")} if legacy_cursor else following
        request = {**request, "cursor": wire_cursor}
        request.pop("offset", None)
    else:
        raise RuntimeError("The generated cursor walk exceeds its finite page bound")
    return {"label": label, "outcome": "complete", "records": position, "pages": len(samples),
            "coordinates_sha256": hashlib.sha256(json.dumps(wanted).encode()).hexdigest(),
            "samples": samples}


def latency(samples):
    values = sorted(row["elapsed_ms"] for row in samples)
    if not values:
        raise RuntimeError("A range latency distribution is empty")
    return {"requests": len(values), "p50_ms": statistics.median(values),
            "p95_ms": values[math.ceil(.95 * len(values)) - 1],
            "p99_ms": values[math.ceil(.99 * len(values)) - 1], "max_ms": values[-1]}
