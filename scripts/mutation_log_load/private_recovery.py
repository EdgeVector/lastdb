"""Back up a real private LastDB write and restore it from signed DEV routes."""

import json
import os
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cloud_backup"))

from drive import LoadError
from native_records import create, key as record_key
from node import OwnedNode
from node_auth import NodeKey, account_id
from programs import Programs
from routes import ROOT, decimal
from signed_files import retry, running, summary
from support import OperationError, failure, require, write_private

SCHEMA = "PrivateRecovery"
FIELDS = {"id": "signed-recovery", "note": "owner-only DEV record"}


def connect(programs, node, api_key):
    receipt = programs.command(node.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", node.account, "--json",
    ], input_bytes=api_key.encode(), timeout=30)
    require(receipt["ok"] is True and receipt["credentials_saved"] is True
            and not receipt["registration_attempted"]
            and receipt["account_id"] == node.account, "private_dev_connection")


def restore(programs, credentials, destination, database, phrase, through):
    receipt = programs.command(credentials, [
        "backup", "restore", "--stream", database, "--recovery-phrase-stdin",
        "--into", str(destination), "--json",
    ], input_bytes=phrase, timeout=100)
    require(receipt["database_id"] == database and receipt["through_delivery_seq"] == through,
            "private_recovery_prefix")


def seed(source, database):
    route = f"/api/databases/{database}"
    source.call(f"{route}/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": list(FIELDS), "field_types": dict.fromkeys(FIELDS, "String"),
        "field_descriptions": {name: f"{SCHEMA} {name}" for name in FIELDS},
    }})
    mutation = create(SCHEMA, record_key(FIELDS["id"]), FIELDS)
    source.call(f"{route}/mutation", mutation)


def check_restore(programs, call, database, owner, api_key):
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE").strip().encode()
    account = account_id(owner.public)
    with tempfile.TemporaryDirectory(prefix="ldb-signed-", dir="/tmp") as temporary:
        root = Path(temporary).resolve()
        credentials = OwnedNode(root / "connection", programs, account=account)
        credentials.home.mkdir(mode=0o700)
        write_private(credentials.home / "identity.key", owner.private.private_bytes_raw())
        with running(credentials, remove_saved=True):
            connect(programs, credentials, api_key)
            source = OwnedNode(root / "source", programs, account=account, isolated_copy=True)
            restore(programs, credentials.home, source.home, database, phrase, 0)
            with running(source, remove_saved=True):
                connect(programs, source, api_key)
                seed(source, database)
                retry("private_cloud_prefix", lambda: call("head", {}),
                      lambda head: head.get("cursor") is not None and decimal(head["cursor"]) >= 2)
            page = call("list", {"device_id": "private-proof", "after": "0", "limit": "20"})
            through = decimal(page["next"])
            require(2 <= through == len(page["entries"]), "private_log_entries")
            verify_snapshot(call, page["entries"], through)
            restored = OwnedNode(root / "restored", programs, account=account)
            restore(programs, credentials.home, restored.home, database, phrase, through)
            with running(restored):
                read = programs.command(restored.home, [
                    "--db", "recovered", "get", SCHEMA, "--key-hash", FIELDS["id"], "--json",
                ], timeout=30)
                require([row["fields"] for row in read["results"]] == [FIELDS],
                        "private_recovered_record")
    summary("A fresh LastDB home restored the signed database and read the original record by key.")


def verify_snapshot(call, entries, through):
    snapshot = call("snapshot", {})
    manifest = call("delivery/snapshot", {})
    ref = snapshot["snapshot_ref"]
    require(manifest["snapshot_ref"] == ref and decimal(manifest["through_delivery_seq"]) == through,
            "private_snapshot_prefix")
    part = call("delivery/snapshot", {"snapshot_ref": ref, "part": "0"})
    require([item["payload"] for item in json.loads(part["part"])]
            == [item["entry"]["payload_b64"] for item in entries], "private_snapshot_entries")
    ack = call("ack", {"device_id": "private-proof", "through": str(through)})
    require(decimal(ack["through"]) == through, "private_snapshot_ack")


def recover(post, endpoint, database, owner, api_key, generation, ledger):
    os.umask(0o077)

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

    programs = Programs(Path(os.environ["CLIENT_BIN_DIR"]).resolve(strict=True),
                        os.environ["CLIENT_VERSION"], {
                            name: value for name, value in os.environ.items()
                            if not name.startswith("LASTDB_CLOUD_DEV_")
                        })
    try:
        check_restore(programs, call, database, owner, api_key)
    except OperationError as error:
        raise LoadError(f"LastDB private recovery: {json.dumps(failure(error))}") from None

    other = NodeKey.generate()
    for operation, fields in (
        ("access", {}), ("head", {}), ("append", {"device_id": "other", "entries": [{
            "writer_seq": "1", "authored_at_ms": "1",
            "namespace": SCHEMA, "payload_b64": "AA",
        }]}),
        ("list", {"device_id": "other", "after": "0", "limit": "2"}),
        ("delivery/snapshot", {}),
    ):
        call(operation, fields, key=other, allowed=False)
    call("head", {}, scope_generation=str(int(generation) + 1), allowed=False)
    summary("An unrelated key could not read, append, or fetch the private snapshot.")
