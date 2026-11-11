"""LastDBNode bearer token for one mutation-log request.

``Authorization: LastDBNode <b64url(body)>.<b64url(Ed25519 signature)>``.
The signature covers the lowercase hex of
``input_digest("lastdb:share:node-read-authority:v1", body)``.
``account_id`` is the first 16 bytes of SHA-256 over the raw public key,
as 32 hex characters. ``database_id`` and ``stream_id`` are the same value.
"""

import base64
import hashlib
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey)

from canon import DOMAIN, input_digest


SKEW_MS = 60_000
TOKEN_FIELDS = (
    "account_id", "database_id", "issued_at_ms", "method", "node_public_key",
    "nonce", "path", "stream_id", "version",
)


class TokenError(Exception):
    """The token does not match the server rule."""


class NodeKey:
    def __init__(self, private):
        self.private = private
        self.public = private.public_key().public_bytes_raw()

    @classmethod
    def generate(cls):
        return cls(Ed25519PrivateKey.generate())

    @classmethod
    def from_seed(cls, seed):
        if len(seed) != 32:
            raise TokenError("key")
        return cls(Ed25519PrivateKey.from_private_bytes(seed))

    def sign(self, message):
        return self.private.sign(message)


def account_id(public_key):
    if len(public_key) != 32:
        raise TokenError("key")
    return hashlib.sha256(public_key).digest()[:16].hex()


def within_skew(issued_at_ms, now_ms):
    return abs(int(now_ms) - int(issued_at_ms)) <= SKEW_MS


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def body_for(key, method, path, database_id, issued_at_ms, nonce):
    ident = account_id(key.public)
    body = {
        "account_id": ident,
        "database_id": database_id,
        "issued_at_ms": str(int(issued_at_ms)),
        "method": method.upper(),
        "node_public_key": key.public.hex(),
        "nonce": nonce,
        "path": path,
        "stream_id": database_id,
        "version": 2,
    }
    if set(body) != set(TOKEN_FIELDS) or body["database_id"] != body["stream_id"]:
        raise TokenError("body")
    if not within_skew(issued_at_ms, issued_at_ms):
        raise TokenError("skew")
    return body


def authorization(key, method, path, database_id, issued_at_ms, nonce=None):
    if nonce is None:
        nonce = os.urandom(32).hex()
    if len(nonce) != 64 or any(ch not in "0123456789abcdef" for ch in nonce):
        raise TokenError("nonce")
    body = body_for(key, method, path, database_id, issued_at_ms, nonce)
    digest = input_digest(DOMAIN, body)
    signature = key.sign(digest.encode("ascii"))
    from canon import canonical_bytes
    token = _b64url(canonical_bytes(body)) + "." + _b64url(signature)
    return "LastDBNode " + token, digest


def parse_authorization(header):
    scheme, _, token = header.partition(" ")
    if scheme != "LastDBNode" or header != scheme + " " + token:
        raise TokenError("header")
    encoded_body, dot, encoded_sig = token.partition(".")
    if not dot or "." in encoded_sig or not encoded_body or not encoded_sig:
        raise TokenError("header")
    body = json_body(encoded_body)
    signature = _b64url_decode(encoded_sig)
    if len(signature) != 64 or set(body) != set(TOKEN_FIELDS):
        raise TokenError("body")
    if body["database_id"] != body["stream_id"]:
        raise TokenError("database")
    return body, signature


def verify_authorization(header, now_ms):
    body, signature = parse_authorization(header)
    if not within_skew(body["issued_at_ms"], now_ms):
        raise TokenError("skew")
    public = bytes.fromhex(body["node_public_key"])
    if account_id(public) != body["account_id"]:
        raise TokenError("account")
    digest = input_digest(DOMAIN, body)
    Ed25519PublicKey.from_public_bytes(public).verify(signature, digest.encode("ascii"))
    return body


def json_body(encoded):
    import json
    return json.loads(_b64url_decode(encoded))


def _b64url_decode(value):
    pad = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + pad)
