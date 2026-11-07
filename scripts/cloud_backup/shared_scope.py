"""Validate the protected DEV scope before touching generated homes."""

import os
import re

from support import ACCOUNT, canonical_uuid, decode_json, require


def protected_json(name):
    raw = os.environ.pop(name, "")
    require(0 < len(raw.encode()) <= 4096, "shared_protected_input_size")
    return decode_json(raw)


def scope(writer_count):
    value = protected_json("LASTDB_CLOUD_DEV_SHARED_SCOPE")
    required = {"account_id", "database_id", "publisher_device", "writers"}
    require(isinstance(value, dict) and set(value) == required and value["account_id"] == ACCOUNT,
            "shared_admitted_scope")
    writers = value["writers"]
    require(isinstance(writers, list) and len(writers) == writer_count
            and all(isinstance(writer, dict) and set(writer) == {
                "device_id", "source_stream", "public_key"} for writer in writers),
            "shared_admitted_writers")
    ids = [value["database_id"]]
    keys = set()
    for writer in writers:
        ids.extend([writer["device_id"], writer["source_stream"]])
        key = writer["public_key"]
        require(is_digest(key), "shared_admitted_public_key")
        keys.add(key)
    require(len(set(map(canonical_uuid, ids))) == len(ids) and len(keys) == writer_count
            and value["publisher_device"] in {writer["device_id"] for writer in writers},
            "shared_distinct_writers")
    return value


def is_digest(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def is_schema_alias(value):
    return isinstance(value, str) and re.fullmatch(
        r"[A-Za-z0-9_-]{1,64}(?:/[A-Za-z0-9_-]{1,64})?", value) is not None
