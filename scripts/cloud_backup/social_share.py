"""Exercise the sharing configuration API with two isolated nodes in DEV."""

import json
import os
from pathlib import Path
import time
import uuid

from node import Connection
from signed_files import retry, run_operation, summary
from support import OperationError, decode_json, require


SPEC = {"name": "social", "version": 1}
SCHEMA = "social/Album"
ALBUMS = ("Before sharing", "While shared", "While revoked")


def request(node, method, path, body=None, status=200):
    with Connection(node.socket) as connection:
        content = None if body is None else json.dumps(body).encode()
        connection.request(method, path, content, {"Content-Type": "application/json"})
        reply = connection.getresponse()
        require(reply.status == status, f"social_{method.lower()}_http_{reply.status}_wanted_{status}")
        if status in (403, 404):
            return None
        return decode_json(reply.read(65537))


def album(node, database, index):
    return node.call(f"/api/databases/{database}/query", {
        "schema_name": SCHEMA,
        "filter": {"HashKey": f"album-{index}"},
    })["results"]


def write_album(node, database, definition, index):
    node.call(f"/api/databases/{database}/mutation", {
        "type": "mutation",
        "schema": SCHEMA,
        "definition_id": definition,
        "key_value": {"hash": f"album-{index}", "range": None},
        "alternates": {},
        "mutation_type": "create",
        "expected": {"type": "absent"},
        "record": {"title": ALBUMS[index], "description": "Generated DEV record",
                   "cover_photo_id": "none", "created_at": "2026-11-13T00:00:00Z"},
    })


def wait_for_album(node, database, index):
    retry(f"social_album_{index}", lambda: album(node, database, index),
          lambda rows: len(rows) == 1 and rows[0]["fields"]["title"] == ALBUMS[index])


def replace_members(publisher, path, revision, members):
    body = {"expectedRevision": str(revision), "requestId": str(uuid.uuid4()), "members": members}
    ack = retry("social_config_update", lambda: request(publisher, "PUT", path, body),
                lambda result: result.get("committed") is True)
    require(ack["requestId"] == body["requestId"] and int(ack["revision"]) == revision + 1,
            "social_revision_ack")
    require(request(publisher, "PUT", path, body)["revision"] == ack["revision"],
            "social_exact_request_retry")
    deployed = request(publisher, "GET", path)
    applied = [{key: value for key, value in member.items() if key != "regrant"} for member in members]
    require(deployed["revision"] == ack["revision"] and deployed["members"] == applied,
            "social_deployed_configuration")
    return revision + 1


def transfer(programs, root, publisher, recipient, api_key):
    name = f"ci-social-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    database = publisher.call("/api/databases", {"name": name, "kind": "shareable", "spec": SPEC})
    database = str(uuid.UUID(database["database"]["id"], version=4))
    summary(f"Owned DEV mutation log `{database}` for `{name}`.")
    print(f"Social DEV database {database}", flush=True)
    schema = Path(__file__).resolve().parents[2] / "spec/social/Album.json"
    definition = publisher.call(f"/api/databases/{database}/schemas/declare",
                                json.loads(schema.read_text()))["definition_id"]
    write_album(publisher, database, definition, 0)
    connected = programs.command(publisher.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", publisher.account, "--json",
    ], input_bytes=api_key, timeout=20)
    require(connected["environment"] == "dev" and connected["account_id"] == publisher.account
            and connected["credentials_saved"] is True, "social_dev_connection")
    reader_contact = root / "reader.contact"
    reader_key = programs.command(recipient.home, [
        "share", "contact", "--out", str(reader_contact),
    ])["recipient_public_key"]
    publisher_key = programs.command(publisher.home, [
        "share", "contact", "--out", str(root / "publisher.contact"),
    ])["recipient_public_key"]
    require(reader_key != publisher_key, "social_separate_identity")
    path = f"/api/databases/{database}/share/config"
    initial = retry("social_initial_config", lambda: request(publisher, "GET", path),
                    lambda config: config.get("databaseId") == database)
    require(initial["revision"] == "0" and initial["members"] == [], "social_initial_members")
    reader = {"nodeKey": reader_key, "role": "read", "state": "active"}
    revoked = {**reader, "state": "revoked"}
    try:
        revision = replace_members(publisher, path, 0, [reader])
        received = open_share(publisher, recipient, database, reader_contact, reader_key, publisher_key)
        require(received.get("id") == database and received.get("spec") == SPEC
                and received.get("access") == "reader", "social_received_metadata")
        request(recipient, "GET", path, status=404)
        wait_for_album(recipient, database, 0)
        recipient.stop()
        recipient.start()
        listed = next(row for row in recipient.call("/api/databases")["databases"]
                      if row["id"] == database)
        require(listed.get("spec") == SPEC, "social_spec_after_restart")
        write_album(publisher, database, definition, 1)
        wait_for_album(recipient, database, 1)
        revision = replace_members(publisher, path, revision, [revoked])
        write_album(publisher, database, definition, 2)
        stale = {"expectedRevision": "0", "requestId": str(uuid.uuid4()), "members": [reader]}
        conflict = request(publisher, "PUT", path, stale, status=409)
        require(conflict.get("revision") == str(revision), "social_stale_revision_conflict")
        for _ in range(12):
            require(album(recipient, database, 2) == [], "social_revoked_read_new_album")
            time.sleep(1)
        wait_for_album(recipient, database, 0)
        replace_members(publisher, path, revision, [{**reader, "regrant": True}])
        wait_for_album(recipient, database, 2)
    finally:
        deployed = request(publisher, "GET", path)
        if deployed["members"] == [reader]:
            revision = replace_members(publisher, path, int(deployed["revision"]), [revoked])
            summary(f"Final ExeMem revoke acknowledged at revision {revision}.")
    message = "ExeMem confirmed grant, revoke and regrant. Recipient preserved the spec across restart, read later albums, stopped receiving updates after revoke, and caught up after regrant."
    print(message, flush=True)
    summary(message)


def open_share(publisher, recipient, database, contact, reader_key, publisher_key):
    invite = publisher.call(f"/api/databases/{database}/invitations", {
        "contactCode": contact.read_text().strip(),
        "expectedNodeKey": reader_key,
    })
    return recipient.call("/api/databases/accept", {
        "invitation": invite["invitation"],
        "publisher": publisher_key,
        "database_id": database,
        "name": "family",
    })["database"]


if __name__ == "__main__":
    run_operation(transfer)
