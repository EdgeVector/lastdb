"""Strict numeric fields and the count-only private read-reuse event."""

import re

MAX_FIELDS = 512
NAME = re.compile(r"[A-Za-z0-9_-]{1,96}\Z")
U64_MAX = 2 ** 64 - 1


def integer(value):
    if type(value) is not int or not 0 <= value <= U64_MAX:
        raise ValueError("A diagnostic value is not a nonnegative u64 integer")
    return value


def numeric_map(values):
    if not isinstance(values, dict) or len(values) > MAX_FIELDS:
        raise ValueError("A numeric diagnostic map exceeds its bound")
    for key, value in values.items():
        if not isinstance(key, str) or not NAME.fullmatch(key):
            raise ValueError("A numeric diagnostic field name is invalid")
        integer(value)
    return values


def write_read_event(event):
    if set(event) != {"kind", "started_us", "ended_us", "counts"}:
        raise ValueError("A private read-reuse event has an invalid envelope")
    if integer(event["ended_us"]) < integer(event["started_us"]):
        raise ValueError("A diagnostic event has reversed clock bounds")
    counts = numeric_map(event["counts"])
    if set(counts) != {"saved_records", "saved_bytes", "reused_records", "reused_bytes"}:
        raise ValueError("A private read-reuse event has invalid counters")
    return "write_reads"
