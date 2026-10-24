"""Validate ordinary public pages against known generated source coordinates."""

import hashlib
import json
import re

from ranges import latency, validate
from condition_values import extend

SCHEMA, HASH, COUNT, SHIFT = "ReceivedRangeBatches", "generated", 16384, 8192
TOTAL = COUNT + SHIFT
HIDDEN_SCHEMA = SCHEMA + "Alternate"
HIDDEN_INTERVALS = [(5000, 5640), (24000, TOTAL)]
FIELDS = ["bucket", "id", "title", "body"]
EDGE_HASH = "edge-\u00e9-\u732b"
EDGE_KEYS = ["", "a", "a\0", "a\0b", "aa", "b", "\u00e9", "\u732b", "\U00010000", "\U0010ffff"]


def key(number):
    return f"key-{number:06d}"


def hidden(number):
    return any(first <= number < end for first, end in HIDDEN_INTERVALS)


def fields(hashed, ranged, source, revision="seed"):
    title = json.dumps([hashed, ranged, source, revision], ensure_ascii=True)
    return extend({"bucket": hashed, "id": ranged, "title": title,
                   "body": hashlib.shake_256(title.encode()).hexdigest(128)}, source)


def query(start="", end="\U0010ffff", limit=256, hashed=HASH):
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": limit,
            "filter": {"HashRangeRange": {"hash": hashed, "start": start, "end": end}}}


def prefix(value="key-", limit=256, hashed=HASH):
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": limit,
            "filter": {"HashRangePrefix": {"hash": hashed, "prefix": value}}}


def selected(expected, request):
    selector = request["filter"]
    if "HashKey" in selector:
        keys = [coordinate for coordinate in expected if coordinate[0] == selector["HashKey"]]
    elif "HashRangeRange" in selector:
        spec = selector["HashRangeRange"]
        keys = [coordinate for coordinate in expected if coordinate[0] == spec["hash"]
                and spec["start"] <= coordinate[1] < spec["end"]]
    else:
        spec = selector["HashRangePrefix"]
        keys = [coordinate for coordinate in expected if coordinate[0] == spec["hash"]
                and coordinate[1].startswith(spec["prefix"])]
    cursor = request.get("cursor")
    if cursor is not None:
        keys = [coordinate for coordinate in keys
                if (coordinate[1], coordinate[0]) > (cursor.get("range", ""), cursor.get("hash", ""))]
    return sorted(keys, key=lambda coordinate: (coordinate[1], coordinate[0]))[request.get("offset", 0):]


def walk(node, expected, request, label, baseline=False):
    wanted, position, samples = selected(expected, request), 0, []
    cursor = request.get("cursor")
    maximum_pages = len(wanted) // max(1, request["limit"]) + 16
    for _ in range(maximum_pages):
        result, receipt = node.call("/api/query", request, expected=(200, 503) if baseline else (200,))
        if receipt["http_status"] == 503:
            message = result.get("message", "")
            known = ("bounded continuation" in message or message ==
                     "read resource limit exceeded: disk-pack v1: read resource limit exceeded"
                     or re.fullmatch(
                         r"read resource limit exceeded: native required work: "
                         r"examined_keys=10000\+1/10000, charged_bytes=\d+\+\d+/268435456",
                         message) is not None)
            if (samples or result.get("error") != "read_resource_limit"
                    or result.get("retryable") is not False or not known):
                raise RuntimeError("An unexpected baseline failure occurs in " + label)
            return {"label": label, "outcome": "expected_baseline_limitation", "http_status": 503,
                    "error": result, "request_id": receipt["request_id"],
                    "failure_elapsed_ms": receipt["elapsed_ms"], "latency_sample": False}
        count = len(result.get("results", []))
        if count > request["limit"] or position + count > len(wanted):
            raise RuntimeError("A received range page exceeds the requested interval")
        validate(result, expected, wanted[position:position + count], request.get("fields"))
        if receipt["storage_read_receipt"] is None:
            raise RuntimeError("A received range page lacks its native read receipt")
        samples.append({"request_id": receipt["request_id"], "elapsed_ms": receipt["elapsed_ms"],
            "request_sha256": hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest(),
            "records": count, "storage_read_receipt": receipt["storage_read_receipt"]})
        position += count
        if result.get("has_more") is False:
            if position != len(wanted):
                raise RuntimeError("A complete received range omits known generated values")
            break
        following = result.get("next_cursor")
        if (result.get("has_more") is not True or not count or not following
                or following == cursor or following != {
                    "hash": wanted[position - 1][0], "range": wanted[position - 1][1]}):
            raise RuntimeError("A received range cursor does not advance past its last returned key")
        cursor = following
        request = {**request, "cursor": following}
        request.pop("offset", None)
    else:
        raise RuntimeError("The received range cursor walk exceeds its finite page bound")
    return {"label": label, "outcome": "complete", "records": position, "pages": len(samples),
            "coordinates_sha256": hashlib.sha256(json.dumps(wanted).encode()).hexdigest(), "samples": samples}
