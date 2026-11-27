"""Locate, fetch, and verify the signed node release lists. Used by node_list.py."""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from node_list_rules import ListError, canonical, check_list

PROFILE = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"
MAX_OBJECT = 256 * 1024
ENVELOPE_KEYS = {"alg", "key_id", "payload_sha256", "sig"}


def profile(env):
    return json.loads(PROFILE.read_text())["environments"][env]


def node_index(env):
    data = profile(env)
    if data.get("node_index"):
        return data["node_index"].rstrip("/")
    base = data["registry_index"].rstrip("/")
    if not base.endswith("/v1/lists"):
        raise ListError("registry_index does not end in /v1/lists, so node_index cannot be derived")
    return base[: -len("/v1/lists")] + "/v1/node"


def pinned_keys(env, extra=()):
    texts = list(extra) or profile(env).get("node_release_keys") or []
    if not 1 <= len(texts) <= 2:
        raise ListError(f"environments.json has no node_release_keys for {env} (need one or two)")
    keys = []
    for text in texts:
        try:
            raw = base64.b64decode(text, validate=True)
        except ValueError as error:
            raise ListError("A pinned key is not base64") from error
        if len(raw) != 32:
            raise ListError("A pinned key is not 32 bytes")
        keys.append(raw)
    return keys


def object_url(index, channel, target, suffix):
    return f"{index}/{channel}/{target}.json{suffix}"


def s3_location(index):
    parts = urllib.parse.urlsplit(index)
    match = re.fullmatch(r"([a-z0-9][a-z0-9.-]+[a-z0-9])\.s3\.([a-z0-9-]+)\.amazonaws\.com", parts.netloc)
    if parts.scheme != "https" or match is None:
        raise ListError("node_index is not an S3 virtual-host HTTPS URL")
    return match[1], match[2], parts.path.strip("/")


# ---------------------------------------------------------------- reading

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(url):
    """HTTPS only, no redirect, at most 256 KiB. A missing object (403 or 404) gives None."""
    if not url.startswith("https://"):
        raise ListError("A list is read over HTTPS only")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(urllib.request.Request(url, headers={"User-Agent": "lastdb-node-list"}), timeout=30) as reply:
            raw = reply.read(MAX_OBJECT + 1)
    except urllib.error.HTTPError as error:
        if error.code in (403, 404):
            return None
        raise ListError(f"Reading {url} answered HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise ListError(f"Reading {url} failed: {error.reason}") from error
    if len(raw) > MAX_OBJECT:
        raise ListError("A list object exceeds 256 KiB")
    return raw


def verify_pair(raw, sig_raw, keys, channel, target):
    """Verify the exact bytes before they are parsed, then apply every rule. Returns the list."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        envelope = json.loads(sig_raw)
    except ValueError as error:
        raise ListError("The signature object is not JSON") from error
    if type(envelope) is not dict or set(envelope) != ENVELOPE_KEYS or envelope["alg"] != "ed25519":
        raise ListError("The signature envelope differs from the contract")
    digest = hashlib.sha256(raw).hexdigest()
    if envelope["payload_sha256"] != digest:
        raise ListError("The signature covers other bytes than the list")
    matching = [key for key in keys if hashlib.sha256(key).hexdigest() == envelope["key_id"]]
    if not matching:
        raise ListError("The list is signed by a key that is not pinned")
    try:
        signature = base64.b64decode(envelope["sig"], validate=True)
        if len(signature) != 64:
            raise ValueError
        Ed25519PublicKey.from_public_bytes(matching[0]).verify(signature, digest.encode("ascii"))
    except (ValueError, InvalidSignature, TypeError) as error:
        raise ListError("The list signature does not verify") from error
    try:
        doc = json.loads(raw)
    except ValueError as error:
        raise ListError("The list is not JSON") from error
    check_list(doc, channel, target)
    if canonical(doc) != raw:
        raise ListError("The list bytes are not the canonical form")
    return doc


def read_pair(index, channel, target):
    raw = fetch(object_url(index, channel, target, ""))
    sig = fetch(object_url(index, channel, target, ".sig"))
    return raw, sig


def read_verified(index, channel, target, keys):
    """The verified list, or None when both objects are missing. One retry after a mismatch."""
    for attempt in (1, 2):
        raw, sig = read_pair(index, channel, target)
        if raw is None and sig is None:
            return None
        if raw is None or sig is None:
            if attempt == 1:
                time.sleep(2)
                continue
            raise ListError(f"{channel}/{target}: only one of .json and .sig exists")
        try:
            return verify_pair(raw, sig, keys, channel, target), raw
        except ListError:
            if attempt == 2:
                raise
            time.sleep(2)
    raise ListError("unreachable")
