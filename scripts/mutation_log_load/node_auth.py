"""LastDBNode bearer token for one mutation-log request.

``Authorization: LastDBNode <b64url(token JSON)>.<b64url(Ed25519 signature)>``.
The token JSON and the digest input come from ``node_read_token``. The
signature covers the ASCII hex digest, not the token JSON. A LastDBShare
header is refused. The header is at most 4096 bytes.
"""

import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey, Ed25519PublicKey)


# Environment::as_str and the key in lastdb_node/environments.json.
DEV_ENVIRONMENT = "dev"
# Personal create uses the first account generation. Revision 0 is an empty member list.
ACCOUNT_GENERATION = "1"
MEMBER_LIST_REVISION = "0"
HEADER_LIMIT = 4096
SKEW_MS = 60_000
TOKEN_FIELDS = (
    "environment", "issued_at_ms", "member_list_revision", "method",
    "node_public_key", "nonce", "path", "stream", "version",
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


def helper_path():
    override = os.environ.get("NODE_READ_TOKEN_BIN")
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[2] / "target" / "debug" / "node_read_token"


def encode_request(payload):
    """Ask the protocol helper for the token JSON and the digest input."""
    binary = helper_path()
    if not binary.is_file():
        raise TokenError("helper")
    result = subprocess.run(
        [str(binary)], input=json.dumps(payload).encode("utf-8"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
    if result.returncode != 0 or not result.stdout:
        detail = result.stderr.decode("utf-8", "replace").strip().splitlines()
        message = detail[-1] if detail else "helper"
        raise TokenError(message[:200])
    encoded = json.loads(result.stdout)
    for key in ("digest", "token_json", "canonical"):
        if not isinstance(encoded.get(key), str) or not encoded[key]:
            raise TokenError("helper")
    return encoded


def authorization(key, path, database_id, issued_at_ms, body, nonce=None):
    if nonce is None:
        nonce = os.urandom(32).hex()
    if len(nonce) != 64 or any(ch not in "0123456789abcdef" for ch in nonce):
        raise TokenError("nonce")
    if not isinstance(body, bytes):
        raise TokenError("body")
    payload = {
        "environment": DEV_ENVIRONMENT,
        "account_id": account_id(key.public),
        "account_generation": ACCOUNT_GENERATION,
        "stream_id": database_id,
        "path": path,
        "issued_at_ms": str(int(issued_at_ms)),
        "nonce": nonce,
        "member_list_revision": MEMBER_LIST_REVISION,
        "node_public_key": key.public.hex(),
        "body": body.decode("utf-8"),
    }
    encoded = encode_request(payload)
    signature = key.sign(encoded["digest"].encode("ascii"))
    token = _b64url(encoded["token_json"].encode("utf-8")) + "." + _b64url(signature)
    header = "LastDBNode " + token
    if header.startswith("LastDBShare") or len(header.encode("ascii")) > HEADER_LIMIT:
        raise TokenError("header")
    return header, encoded


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
    encoded = encode_request(_payload_from_token(body, raw_body))
    if encoded["digest"].encode("ascii") == b"":
        raise TokenError("digest")
    Ed25519PublicKey.from_public_bytes(public).verify(
        signature, encoded["digest"].encode("ascii"))
    return body


def _payload_from_token(body, raw_body):
    return {
        "environment": body["environment"],
        "account_id": body["stream"]["scope"]["account_id"],
        "account_generation": body["stream"]["scope"]["account_generation"],
        "stream_id": body["stream"]["stream_id"],
        "path": body["path"],
        "issued_at_ms": body["issued_at_ms"],
        "nonce": body["nonce"],
        "member_list_revision": body["member_list_revision"],
        "node_public_key": body["node_public_key"],
        "body": raw_body.decode("utf-8"),
    }


def _check_token(body):
    stream = body.get("stream")
    scope = stream.get("scope") if isinstance(stream, dict) else None
    if not isinstance(stream, dict) or set(stream) != {"scope", "stream_id"}:
        raise TokenError("stream")
    if not isinstance(scope, dict) or set(scope) != {"account_id", "account_generation"}:
        raise TokenError("stream")
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
