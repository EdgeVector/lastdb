"""Require signed, published app declarations for the exact proved API."""

import base64
import hashlib
import json
from pathlib import Path
import urllib.request

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
import rfc8785

PROFILE = Path(__file__).resolve().parents[2] / "folddb_profile" / "environments.json"
KEY = "3xySKyIR6nD2GjBXq15Jp6UtkasK2+OAhCUEvBd60sw="
SIGNED = ("app_id", "channel", "app_version", "api_version", "lastdb_build",
          "outbox_format_version", "source", "sha", "audience")


def download(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        raw = response.read(2 * 1024 ** 2 + 1)
    if len(raw) > 2 * 1024 ** 2:
        raise ValueError("Registry response exceeds the signed-list bound")
    return raw


def catalog(api):
    base = json.loads(PROFILE.read_text())["environments"]["prod"]["registry_index"]
    raw = download(f"{base}/stable/api/{api}.json")
    envelope = json.loads(download(f"{base}/stable/api/{api}.json.sig"))
    digest = hashlib.sha256(raw).hexdigest()
    key = base64.b64decode(KEY, validate=True)
    if (envelope.get("alg") != "ed25519" or envelope.get("payload_sha256") != digest
            or envelope.get("key_id") != hashlib.sha256(key).hexdigest()):
        raise ValueError("Invalid registry list signature envelope")
    Ed25519PublicKey.from_public_bytes(key).verify(
        base64.b64decode(envelope["sig"], validate=True), digest.encode())
    catalog = json.loads(raw)
    if catalog.get("api_version") != api or catalog.get("channel") != "stable" or catalog.get("list_version") != 1:
        raise ValueError("Registry list belongs to another interface")
    return catalog


def require_apps(qualification):
    api = qualification["api_version"]
    listing = catalog(api)
    for app, proof in qualification["apps"].items():
        declared_outbox = proof["declaration"].get("outbox_format_version")
        matches = [row for row in listing["apps"] if row.get("app_id") == app
                   and row.get("sha") == proof["sha"] and row.get("api_version") == api
                   and row.get("channel") == "stable" and row.get("status") == "listed"
                   and row.get("app_version") == proof["declaration"]["app_version"]
                   and (declared_outbox is None or row.get("outbox_format_version") == declared_outbox)
                   and row.get("outbox_format_version") in (None, proof["outbox_format_version"])
                   and row.get("source") == f"https://github.com/EdgeVector/{app}.git"]
        if not matches:
            raise RuntimeError(f"Missing published {app} compatibility for API {api}; publication stays blocked")
        for row in matches:
            signed = {name: row[name] for name in SIGNED if name in row}
            Ed25519PublicKey.from_public_bytes(base64.b64decode(row["publisher_key"], validate=True)).verify(
                base64.b64decode(row["publisher_sig"], validate=True), rfc8785.dumps(signed))
    print(f'Authenticated API {api} stable list sequence {listing["seq"]}', flush=True)
    return {"api_version": api, "apps": {app: proof["sha"] for app, proof in qualification["apps"].items()}}
