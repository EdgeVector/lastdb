"""Domain-separated digest used by a LastDBNode token.

The frame matches ``input_digest``: a big-endian domain length, the domain,
a big-endian body length, and RFC 8785 JSON. Only the strings, integers,
objects, and arrays a token uses are accepted.
"""

import hashlib
import json


DOMAIN = "lastdb:share:node-read-authority:v1"


def canonical_bytes(value):
    return _canonical(value).encode("utf-8")


def input_digest(domain, value):
    body = canonical_bytes(value)
    domain_bytes = domain.encode("utf-8")
    frame = (
        len(domain_bytes).to_bytes(4, "big")
        + domain_bytes
        + len(body).to_bytes(8, "big")
        + body
    )
    return hashlib.sha256(frame).hexdigest()


def _canonical(value):
    if value is None or isinstance(value, bool) or isinstance(value, float):
        raise ValueError("canonical")
    if isinstance(value, int):
        if value < 0:
            raise ValueError("canonical")
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, list):
        return "[" + ",".join(_canonical(item) for item in value) + "]"
    if isinstance(value, dict):
        keys = sorted(value)
        parts = [_canonical(key) + ":" + _canonical(value[key]) for key in keys]
        return "{" + ",".join(parts) + "}"
    raise ValueError("canonical")
