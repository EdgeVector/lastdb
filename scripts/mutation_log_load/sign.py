"""Sign one mutation-log request the way the DEV server checks it.

The signed text is the body hash, the method, the path, the database id,
and the timestamp, in that order, separated by newlines. The signature is
HMAC-SHA256 of that text. The key never appears in the signed text.
"""

import hashlib
import hmac


def body_hash(body):
    return hashlib.sha256(body).hexdigest()


def canonical(method, path, database_id, timestamp, body):
    material = "\n".join([
        body_hash(body), method.upper(), path, database_id, str(timestamp),
    ])
    return material.encode("utf-8")


def sign(secret, method, path, database_id, timestamp, body):
    if not isinstance(secret, bytes) or secret == b"":
        raise ValueError("missing signing key")
    digest = hmac.new(secret, canonical(method, path, database_id, timestamp, body), hashlib.sha256)
    return digest.hexdigest()


def headers(secret, method, path, database_id, timestamp, body):
    return {
        "x-lastdb-content-sha256": body_hash(body),
        "x-lastdb-database-id": database_id,
        "x-lastdb-timestamp-ms": str(timestamp),
        "x-lastdb-signature": sign(secret, method, path, database_id, timestamp, body),
    }
