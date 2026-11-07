"""Create one owned DEV account, write a real source, and publish its stopped cut."""

import hashlib
import http.client
import json
import os
import re
import time
import uuid
from urllib.parse import urlsplit

import native_records
from automatic_source import durable_batch
from fresh_source import write_private
from restore_receipts import NODE_MARKER, node_marker
from support import (ACCOUNT, CREDENTIALS_FILE, FILE_BYTES, OperationError, canonical_uuid,
                     decode_json, dev_endpoint, private_file, read_json, require, save_json, stable)
from programs import execute


SCHEMA = "CloudFreshAccount"
PHRASE_FILE = "recovery-phrase.txt"
MAX_INVENTORY_BYTES = 64 * 1024 * 1024


def private_bytes(path, minimum, maximum):
    stream, before = private_file(path)
    with stream:
        require(minimum <= before.st_size <= maximum, "fresh_private_file_size")
        data = stream.read(maximum + 1)
        require(len(data) == before.st_size and stable(before, os.fstat(stream.fileno())),
                "fresh_private_file_changed")
    return data


def initialize(programs, home):
    phrase_path = home / PHRASE_FILE
    programs.verify()
    args = programs.cli_arguments(["cloud", "backup-v2", "init", "--into", str(home),
                                   "--recovery-phrase-out", str(phrase_path), "--json"])
    environment = dict(programs.environment, LASTDB_BYTE_BACKUP_V2_ENABLED="true")
    receipt = decode_json(execute([str(programs.directory / "lastdb"), *args], environment, timeout=30))
    account = receipt.get("account_id")
    require(isinstance(account, str) and re.fullmatch(r"[0-9a-f]{32}", account)
            and account not in (ACCOUNT, "0" * 32), "fresh_account_identity")
    require(receipt == {"schema": "lastdb.native-backup-init.v1", "ok": True, "environment": "dev",
                        "account_id": account, "home": str(home), "recovery_phrase_file": str(phrase_path),
                        "cloud_sync_off": True, "local_store_opened": False, "network_request": False,
                        "identity_seed_output": False, "recovery_phrase_output": False}, "fresh_init_receipt")
    return account


def public_key(programs, home, account):
    seed = private_bytes(home / "identity.key", 32, 32)
    pkcs8 = bytes.fromhex("302e020100300506032b657004220420")
    spki = bytes.fromhex("302a300506032b6570032100")
    try:
        raw = execute(["/usr/bin/openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
                      programs.environment, pkcs8 + seed, timeout=10)
    except (OSError, OperationError):
        raise OperationError("fresh_public_key_derivation") from None
    require(len(raw) == len(spki) + 32 and raw.startswith(spki), "fresh_public_key_encoding")
    key = raw[len(spki):]
    require(hashlib.sha256(key).digest()[:16].hex() == account, "fresh_public_key_account")
    return key.hex()


def register(programs, home, account, invite):
    receipt = programs.command(home, ["cloud", "backup-v2", "connect", "--dev", "--invite-code-stdin",
                                     "--json"], input_bytes=invite, timeout=90)
    require(receipt == {"schema": "lastdb.native-backup-connect.v1", "ok": True, "environment": "dev",
                        "account_id": account, "home": str(home), "credential_file": CREDENTIALS_FILE,
                        "cloud_sync_off": True, "local_store_opened": False, "local_store_read_only": False,
                        "database_writer_opened": False, "registration_attempted": True,
                        "credentials_saved": True, "api_key_output": False, "restored_home": False,
                        "automatic_backup_enabled": False}, "fresh_registration_receipt")
    credentials = read_json(home / CREDENTIALS_FILE, max_bytes=16384)
    key = credentials.get("api_key")
    require(set(credentials) == {"api_key", "api_url"} and credentials["api_url"] == dev_endpoint()
            and isinstance(key, str) and re.fullmatch(r"[\x21-\x7e]{1,4096}", key),
            "fresh_dev_credentials")
    return key.encode()


def bind(node, key):
    receipt = node.programs.command(node.home, ["cloud", "backup-v2", "connect", "--existing", "--env", "dev",
            "--api-key-stdin", "--expected-user-hash", node.account, "--json"], input_bytes=key)
    source_home = canonical_uuid(receipt.get("source_home"))
    connection = {"account_id": node.account, "source_home": source_home, "environment": "dev",
                  "manual_backup_enabled": True, "automatic_backup_enabled": False,
                  "registration_attempted": False}
    require(receipt == {**connection, "ok": True, "user_hash": node.account}, "fresh_manual_connection")
    status = node.programs.command(node.home, ["cloud", "backup-v2", "status", "--json"])
    require(status["ok"] is True and status["user_hash"] == node.account
            and status["manual_connection"] == connection and status["automatic_backup_enabled"] is False
            and status["automatic_backup"] is None and status["running"] is False
            and status["request_id"] is None and status["phase"] is None
            and status["last_accepted_backup"] is None and status["blocked_reason"] is None
            and status["retained_source_bytes"] in (None, 0), "fresh_source_has_no_backup")


def seed(node, root):
    require(node_marker(node.home) == NODE_MARKER, "fresh_source_home_format")
    fields = ["id", "note"]
    schema = node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}}, "fields": fields,
        "field_types": {name: "String" for name in fields},
        "field_descriptions": {name: "Fresh account proof " + name for name in fields}}})
    require(schema.get("ok") is True and schema.get("system_seed_schema") == SCHEMA and schema.get("source") == "system_seed"
            and schema.get("user_hash") == node.account, "fresh_schema_receipt")
    records = [{"id": f"record-{index:02d}", "note": f"DEV fresh account {node.account} record {index:02d}"}
               for index in range(16)]
    durable_batch(node, [native_records.create(SCHEMA, native_records.key(item["id"]), item) for item in records])
    payload = hashlib.shake_256(b"lastdb-fresh-dev-file-v1\0" + bytes.fromhex(node.account)).digest(FILE_BYTES)
    digest = hashlib.sha256(payload).hexdigest()
    path = root / "file.bin"
    write_private(path, payload)
    del payload
    reply = native_records.put_file(node, SCHEMA, records[0], path)
    path.unlink()
    require(reply["file"]["sha256"] == digest, "fresh_file_marker")
    return {"account": node.account, "schema": SCHEMA, "records": records,
            "file_bytes": FILE_BYTES, "file_sha256": digest}


def positive(value, maximum):
    return type(value) is int and 0 < value <= maximum


def accepted_point(accepted, account, request):
    begin = accepted["begin"]
    job = begin["job"]
    stream = job["stream"]
    require(accepted["version"] == 2 and accepted["ordinal"] == "1"
            and begin["version"] == 2 and begin["descriptor"] is None and begin["predecessor"] is None
            and begin["recovery_dependencies"] == [] and begin["required_destinations"] == ["r2", "b2"]
            and begin.get("r2_multipart") is None and job["commit_id"] == request
            and stream["scope"] == {"account_id": account, "account_generation": "0"},
            "fresh_manual_first_snapshot")
    digest = accepted["commit_sha256"]
    inventory = accepted["inventory_root"]
    require(all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value)
                for value in (digest, accepted["root_receipt_id"], inventory["sha256"]))
            and all(isinstance(inventory[name], str) and re.fullmatch(r"[1-9][0-9]{0,9}", inventory[name])
                    for name in ("stored_bytes", "object_count", "page_count"))
            and int(inventory["stored_bytes"]) <= MAX_INVENTORY_BYTES, "fresh_snapshot_inventory")
    return {"account_id": account, "account_generation": "0", "stream_id": canonical_uuid(stream["stream_id"]),
            "commit_id": request, "commit_sha256": digest}


def publish(node):
    require(node.process is not None and node.start_ts is not None, "fresh_source_identity")
    request = str(uuid.uuid4())
    require(not request.startswith("a17dbac2-"), "fresh_manual_request")
    value = node.programs.command(node.home, ["cloud", "backup-v2", "start", "--request-id", request, "--json"])
    deadline = time.monotonic() + 420
    while time.monotonic() < deadline:
        require(value["ok"] is True and value["user_hash"] == node.account
                and value["request_id"] == request and value["backup_kind"] == "snapshot"
                and value["automatic_backup_enabled"] is False, "fresh_snapshot_binding")
        if value["phase"] == "published" and value["running"] is False:
            require(value["retained_source_bytes"] == 0 and value["blocked_reason"] is None
                    and positive(value["objects_verified"], 4096)
                    and positive(value["native_files"], 4096)
                    and positive(value["native_bytes"], MAX_INVENTORY_BYTES), "fresh_snapshot_receipt")
            accepted = value["last_accepted_backup"]
            return accepted_point(accepted, node.account, request), value
        # The saved phase and runtime activity are read separately. A final
        # commit can land between those reads, so a clean sample can lag.
        require(value["phase"] in {"preparing", "capturing", "uploading", "inventory",
                                   "committing", "published"}
                and value["blocked_reason"] is None, "fresh_snapshot_stopped")
        time.sleep(1)
        value = node.programs.command(node.home, ["cloud", "backup-v2", "status", "--json"], timeout=20)
    raise OperationError("fresh_snapshot_deadline")


def await_restore_admission(account, key):
    """Observe the ordinary point-retention fence before requesting restore grants."""
    endpoint = urlsplit(dev_endpoint())
    body = json.dumps({"version": 2, "account_id": account, "required_destinations": ["r2", "b2"]})
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        connection = http.client.HTTPSConnection(endpoint.hostname, timeout=10)
        try:
            connection.request("POST", "/api/cloud/backups/v2/context/read", body,
                               {"Content-Type": "application/json", "X-API-Key": key.decode("ascii")})
            response = connection.getresponse()
            raw = response.read(65537)
            require(response.status == 200 and len(raw) <= 65536, "fresh_restore_context_http")
            result = decode_json(raw)
        finally:
            connection.close()
        require(result.get("version") == 2
                and result.get("scope") == {"account_id": account, "account_generation": "0"}
                and type(result.get("new_work_allowed")) is bool, "fresh_restore_context_binding")
        if result["new_work_allowed"]:
            return
        time.sleep(2)
    raise OperationError("fresh_restore_scope_still_fenced")


def restore_inputs(home, credentials, key):
    phrase = private_bytes(home / PHRASE_FILE, 1, 4096)
    require(re.fullmatch(rb"[a-z]+(?: [a-z]+){23}", phrase), "fresh_phrase_encoding")
    credentials.mkdir(mode=0o700)
    save_json(credentials / CREDENTIALS_FILE, {"api_url": dev_endpoint(), "api_key": key.decode("ascii")})
    return phrase
