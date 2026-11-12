"""Exercise owner-only recovery using the signed DEV mutation-log endpoints."""

import base64
import json
import os
import time

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from drive import LoadError
from node_auth import NodeKey
from routes import ROOT, decimal


def recover(post, endpoint, database, owner, api_key, generation, ledger):
    def call(operation, fields, *, key=owner, scope_generation=generation, allowed=True):
        body = json.dumps({"database_id": database, **fields}, separators=(",", ":")).encode()
        status, response, _ = post(
            endpoint, key, database, "POST", f"{ROOT}/{operation}", body, api_key,
            scope_generation)
        ledger.record("read" if operation in ("head", "list", "access", "delivery/snapshot")
                      else "write", len(body))
        if allowed and 200 <= status < 300 and isinstance(response, dict):
            return response
        if not allowed and status in (401, 403) and isinstance(response, dict):
            return response
        raise LoadError(f"private {operation} returned unexpected HTTP {status}")

    for registration in (call("create-database", {"kind": "personal"}),
                         call("access", {}),
                         call("create-database", {"kind": "personal"})):
        if registration.get("state") != "native" or registration.get("database_id") != database:
            raise LoadError("private database registration did not round trip")

    aead = AESGCM(AESGCM.generate_key(bit_length=256))
    nonce = os.urandom(12)
    payload = os.urandom(512)
    sealed = nonce + aead.encrypt(nonce, payload, database.encode())
    encoded = base64.urlsafe_b64encode(sealed).rstrip(b"=").decode()
    entry = {
        "writer_seq": "1", "authored_at_ms": str(time.time_ns() // 1_000_000),
        "namespace": "ci-private", "payload_b64": encoded,
    }
    append = {"device_id": "private-writer", "entries": [entry]}
    call("append", append)
    page = call("list", {"device_id": "private-restore", "after": "0", "limit": "2"})
    if decimal(page["next"]) != 1 or len(page["entries"]) != 1:
        raise LoadError("private recovery did not list the accepted write")
    cut = call("snapshot", {})
    manifest = call("delivery/snapshot", {})
    ref = cut["snapshot_ref"]
    if (manifest["snapshot_ref"] != ref or decimal(manifest["through_delivery_seq"]) != 1
            or decimal(manifest["parts"]) != 1):
        raise LoadError("private snapshot did not cover the accepted write")
    request = {"snapshot_ref": ref, "part": "0"}
    part = call("delivery/snapshot", request)
    records = json.loads(part["part"])
    if len(records) != 1 or records[0]["payload"] != encoded:
        raise LoadError("private snapshot did not return the exact encrypted entry")
    downloaded = records[0]["payload"]
    restored = base64.urlsafe_b64decode(downloaded + "=" * (-len(downloaded) % 4))
    if aead.decrypt(restored[:12], restored[12:], database.encode()) != payload:
        raise LoadError("private snapshot content differs")
    ack = call("ack", {"device_id": "private-restore", "through": "1"})
    if decimal(ack["through"]) != decimal(manifest["through_delivery_seq"]):
        raise LoadError("private recovery acknowledgement did not reach the snapshot")

    other = NodeKey.generate()
    for operation, fields in (
        ("access", {}), ("head", {}), ("append", append),
        ("list", {"device_id": "other", "after": "0", "limit": "2"}),
        ("delivery/snapshot", {}), ("delivery/snapshot", request),
    ):
        call(operation, fields, key=other, allowed=False)
    call("head", {}, scope_generation=str(int(generation) + 1), allowed=False)
    print("Owner created the private database, restored its encrypted snapshot, and acknowledged it.")
    print("An unrelated key could not read, append, or fetch the snapshot.")
