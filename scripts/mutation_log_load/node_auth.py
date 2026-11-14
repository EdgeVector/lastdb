"""LastDBNode bearer token for one mutation-log request.

``Authorization: LastDBNode <b64url(token JSON)>.<b64url(Ed25519 signature)>``.
The digest is ``NodeReadToken`` domain ``lastdb:share:node-read-authority:v1``.
The signature covers the ASCII hex digest, not the token JSON. The token
includes ``database_id``. A LastDBShare header is refused. The header is at
most 4096 bytes.
"""

import base64
import hashlib
import json
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey)


# Environment::as_str and the key in lastdb_node/environments.json.
DEV_ENVIRONMENT = "dev"
# Revision 0 is an empty member list. The caller supplies the account generation.
MEMBER_LIST_REVISION = "0"
AUTHORITY_DOMAIN = b"lastdb:share:node-read-authority:v1"
HEADER_LIMIT = 4096
SKEW_MS = 60_000
TOKEN_FIELDS = (
    "database_id", "environment", "issued_at_ms", "member_list_revision",
    "method", "node_public_key", "nonce", "path", "stream", "version",
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


def _canonical(value):
    """RFC 8785 for the strings, integers, arrays, and objects this digest uses."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, list):
            return b"[" + b",".join(_canonical(item) for item in value) + b"]"
        if isinstance(value, dict):
            parts = []
            for key in sorted(value):
                parts.append(_canonical(key) + b":" + _canonical(value[key]))
            return b"{" + b",".join(parts) + b"}"
        raise TokenError("canonical")
    return str(value).encode("ascii")


def _authority_digest(covered):
    canonical = _canonical(covered)
    framed = (
        len(AUTHORITY_DOMAIN).to_bytes(4, "big")
        + AUTHORITY_DOMAIN
        + len(canonical).to_bytes(8, "big")
        + canonical
    )
    return hashlib.sha256(framed).hexdigest()


def _covered(public_hex, account, account_generation, path, database_id, issued, nonce, body):
    stream = {
        "scope": {
            "account_generation": account_generation,
            "account_id": account,
        },
        "stream_id": database_id,
    }
    covered = [
        2, database_id, stream, "POST", path, hashlib.sha256(body).hexdigest(),
        issued, nonce, MEMBER_LIST_REVISION, public_hex, DEV_ENVIRONMENT,
    ]
    return stream, covered


def authorization(key, path, database_id, issued_at_ms, body, account_generation, nonce=None):
    """Sign the live DEV token. ``account_generation`` comes from the CI baseline."""
    if nonce is None:
        nonce = os.urandom(32).hex()
    if len(nonce) != 64 or any(ch not in "0123456789abcdef" for ch in nonce):
        raise TokenError("nonce")
    if not isinstance(body, bytes) or not isinstance(account_generation, str):
        raise TokenError("body")
    if not account_generation.isdigit() or (account_generation.startswith("0") and account_generation != "0"):
        raise TokenError("account")
    issued = str(int(issued_at_ms))
    public = key.public.hex()
    stream, covered = _covered(
        public, account_id(key.public), account_generation, path, database_id, issued, nonce, body)
    digest = _authority_digest(covered)
    token_json = json.dumps({
        "version": 2,
        "environment": DEV_ENVIRONMENT,
        "database_id": database_id,
        "stream": stream,
        "method": "POST",
        "path": path,
        "issued_at_ms": issued,
        "nonce": nonce,
        "member_list_revision": MEMBER_LIST_REVISION,
        "node_public_key": public,
    }, separators=(",", ":")).encode("utf-8")
    signature = key.sign(digest.encode("ascii"))
    token = _b64url(token_json) + "." + _b64url(signature)
    header = "LastDBNode " + token
    if header.startswith("LastDBShare") or len(header.encode("ascii")) > HEADER_LIMIT:
        raise TokenError("header")
    return header, {"digest": digest}


def parse_authorization(header):
    if not isinstance(header, str) or header.startswith("LastDBShare"):
        raise TokenError("header")
    if len(header.encode("ascii")) > HEADER_LIMIT:
        raise TokenError("header")
    scheme, _, token = header.partition(" ")
    if scheme != "LastDBNode" or header != scheme + " " + token:
        raise TokenError("header")
    encoded_body, dot, encoded_sig = token.partition(".")
    if not dot or "." in encoded_sig or not encoded_body or not encoded_sig:
        raise TokenError("header")
    body = json.loads(_b64url_decode(encoded_body))
    signature = _b64url_decode(encoded_sig)
    if len(signature) != 64 or set(body) != set(TOKEN_FIELDS):
        raise TokenError("body")
    _check_token(body)
    return body, signature


def verify_authorization(header, now_ms, raw_body):
    body, signature = parse_authorization(header)
    if not within_skew(body["issued_at_ms"], now_ms):
        raise TokenError("skew")
    public = bytes.fromhex(body["node_public_key"])
    if account_id(public) != body["stream"]["scope"]["account_id"]:
        raise TokenError("account")
    if body["database_id"] != body["stream"]["stream_id"]:
        raise TokenError("body")
    _stream, covered = _covered(
        body["node_public_key"], body["stream"]["scope"]["account_id"],
        body["stream"]["scope"]["account_generation"], body["path"],
        body["database_id"], body["issued_at_ms"], body["nonce"], raw_body)
    digest = _authority_digest(covered)
    Ed25519PublicKey.from_public_bytes(public).verify(signature, digest.encode("ascii"))
    return body


def _check_token(body):
    stream = body.get("stream")
    scope = stream.get("scope") if isinstance(stream, dict) else None
    if not isinstance(stream, dict) or set(stream) != {"scope", "stream_id"}:
        raise TokenError("stream")
    if not isinstance(scope, dict) or set(scope) != {"account_id", "account_generation"}:
        raise TokenError("stream")
    if body.get("database_id") != stream.get("stream_id"):
        raise TokenError("body")
    if body["environment"] != DEV_ENVIRONMENT or body["method"] != "POST":
        raise TokenError("body")
    if body["version"] != 2 or not isinstance(body["issued_at_ms"], str):
        raise TokenError("body")
    if not isinstance(body["member_list_revision"], str):
        raise TokenError("body")
    if len(body["nonce"]) != 64 or len(body["node_public_key"]) != 64:
        raise TokenError("body")
    if not isinstance(body["path"], str) or not body["path"]:
        raise TokenError("body")
    if not isinstance(stream["stream_id"], str) or not stream["stream_id"]:
        raise TokenError("stream")


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value):
    pad = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + pad)
