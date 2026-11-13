"""Exercise Social sharing with two isolated LastDB nodes in DEV."""

import fcntl
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from signed_files import retry, run_operation, summary
from social_app import (call as app_call, command as social_command, install, observe_access,
                        open_content_apps, read_album, report_content_flow, sdk, window)
from support import OperationError, decode_json, require


ROOT = Path(__file__).resolve().parents[2]
SPEC = {"name": "social", "version": 1}
SCHEMA = "social/Album"
ALBUMS = ("Before sharing", "While shared", "While revoked")


def social(node, *arguments, receipt=None, refusal=None):
    command = [*social_command(node), *arguments]
    try:
        result = subprocess.run(command, input=json.dumps(receipt).encode() if receipt else None,
                                capture_output=True, env=node.programs.environment, timeout=180)
    except subprocess.TimeoutExpired as error:
        raise OperationError(f"social_{arguments[0]}_deadline") from error
    if refusal is not None:
        require(result.returncode == 1 and not result.stdout and refusal in result.stderr,
                f"social_{arguments[0]}_expected_refusal")
        return None
    if result.returncode:
        # An invitation is held in memory and must never enter workflow output.
        detail = result.stderr.decode(errors="replace").partition(" returned HTTP ")[2][:3]
        suffix = f"_http_{detail}" if detail.isascii() and detail.isdigit() else ""
        raise OperationError(f"social_{arguments[0]}_failed{suffix}")
    return decode_json(result.stdout)


def album(node, database, index):
    return node.call(f"/api/databases/{database}/query", {
        "schema_name": SCHEMA,
        "filter": {"HashKey": f"album-{index}"},
    })["results"]


def write_album(node, database, definition, index):
    node.call(f"/api/databases/{database}/mutation", {
        "type": "mutation", "schema": SCHEMA, "definition_id": definition,
        "key_value": {"hash": f"album-{index}", "range": None}, "alternates": {},
        "mutation_type": "create", "expected": {"type": "absent"},
        "record": {"title": ALBUMS[index], "description": "Generated DEV record",
                   "cover_photo_id": "none", "created_at": "2026-11-13T00:00:00Z"},
    })


def wait_for_album(node, database, index):
    retry(f"social_album_{index}", lambda: album(node, database, index),
          lambda rows: len(rows) == 1 and rows[0]["fields"]["title"] == ALBUMS[index])


def roster(node, database):
    configurations = social(node, "grants", "--database", database)
    require(len(configurations) == 1 and configurations[0]["databaseId"] == database,
            "social_roster_database")
    return configurations[0]


def expect_roster(node, database, revision, member=None):
    config = roster(node, database)
    require(config.get("revision") == str(revision) and config.get("members") == ([member] if member else []),
            "social_applied_configuration")


def share(publisher, database, contact, node_key, revision):
    arguments = ["add", "--database", database, "--user", "Ana", "--node-key", node_key]
    if contact is not None:
        arguments.extend(["--contact", str(contact)])
    receipt = social(publisher, *arguments)
    require(receipt["databaseId"] == database and receipt["revision"] == str(revision)
            and isinstance(receipt.get("invitation"), str) and bool(receipt["invitation"]),
            "social_invitation_handoff")
    expect_roster(publisher, database, revision, {"nodeKey": node_key, "role": "read", "state": "active"})
    return receipt


def revoke(publisher, database, reader_key, revision):
    result = social(publisher, "remove", "--user", "Ana", "--database", database)
    require(result == {"databaseId": database, "revision": str(revision),
                       "nodeKey": reader_key, "state": "revoked"}, "social_revoke_ack")
    expect_roster(publisher, database, revision, {"nodeKey": reader_key, "role": "read", "state": "revoked"})


def transfer(programs, root, publisher, recipient, api_key):
    if revision := os.environ.get("SOCIAL_INSTALL_REVISION"):
        install(publisher, root, revision)
    name = f"ci-social-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    created = sdk(publisher, "create", name=name)
    database = str(uuid.UUID(created["id"]))
    require(database == created["id"] and created["spec"] == SPEC, "social_created_database")
    summary(f"Owned DEV mutation log `{database}` for `{name}`.")
    print(f"Social DEV database {database}", flush=True)
    schema = json.loads((ROOT / "spec/social/Album.json").read_text())
    definition = publisher.call(f"/api/databases/{database}/schemas/declare", schema)["definition_id"]
    write_album(publisher, database, definition, 0)
    connected = programs.command(publisher.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", publisher.account, "--json",
    ], input_bytes=api_key, timeout=20)
    require(connected["environment"] == "dev" and connected["account_id"] == publisher.account
            and connected["credentials_saved"] is True, "social_dev_connection")
    contact = root / "reader.contact"
    reader_key = programs.command(recipient.home, [
        "share", "contact", "--out", str(contact),
    ])["recipient_public_key"]
    publisher_key = programs.command(publisher.home, [
        "share", "contact", "--out", str(root / "publisher.contact"),
    ])["recipient_public_key"]
    require(reader_key != publisher_key, "social_separate_identity")
    retry("social_initial_configuration", lambda: roster(publisher, database),
          lambda config: config.get("revision") == "0" and config.get("members") == [])
    with window(publisher, database) as owner_app:
        app_call(owner_app, "/api/home", authorized=False, status=403)
        person = app_call(owner_app, "/api/remember", {
            "name": "Ana", "nodeKey": reader_key, "contactCode": contact.read_text().strip(),
        })
        require(person == {"name": "Ana", "nodeKey": reader_key}
                and person in app_call(owner_app, "/api/home")["people"], "social_app_saved_contact")
        with window(recipient) as recipient_app:
            share_albums(publisher, recipient, name, database, definition, reader_key, publisher_key, recipient_app)
    check_role_and_overview(programs, root, publisher, database, name, contact, reader_key)


def share_albums(publisher, recipient, name, database, definition, reader_key, publisher_key, recipient_app):
    try:
        receipt = sdk(publisher, "invite", database=name, recipient={"name": "Ana", "nodeKey": reader_key})
        require(receipt["databaseId"] == database and receipt["publisher"] == publisher_key and receipt["revision"] == "1",
                "social_publisher_identity")
        received = sdk(recipient, "accept", publisher=publisher_key, name="family", receipt=receipt)
        require(received.get("id") == database and received.get("spec") == SPEC
                and received.get("access") == "reader", "social_received_metadata")
        wait_for_album(recipient, database, 0)
        recipient.stop()
        recipient.start()
        listed = next(row for row in recipient.call("/api/databases")["databases"]
                      if row["id"] == database)
        require(listed.get("spec") == SPEC, "social_spec_after_restart")
        observe_access(recipient_app, database, "active", 1)
        selections = open_content_apps(recipient, recipient_app, database)
        require(read_album(recipient, selections, database, "album-0", ALBUMS[0]), "social_apps_initial_album")
        grants = social(publisher, "list", "--database", database)
        require(len(grants) == 1 and grants[0]["user"] == "Ana", "social_remembers_contact")
        share(publisher, database, None, reader_key, 1)
        write_album(publisher, database, definition, 1)
        wait_for_album(recipient, database, 1)
        require(read_album(recipient, selections, database, "album-1", ALBUMS[1]), "social_apps_second_album")
        require(sdk(publisher, "revoke", database=name, nodeKey=reader_key) == "2", "social_sdk_revoke")
        expect_roster(publisher, database, 2, {"nodeKey": reader_key, "role": "read", "state": "revoked"})
        write_album(publisher, database, definition, 2)
        for _ in range(12):
            require(album(recipient, database, 2) == [], "social_revoked_read_new_album")
            time.sleep(1)
        observe_access(recipient_app, database, "revoked")
        require(read_album(recipient, selections, database, "album-2")
                and read_album(recipient, selections, database, "album-0", ALBUMS[0]), "social_apps_revoked")
        wait_for_album(recipient, database, 0)
        share(publisher, database, None, reader_key, 3)
        wait_for_album(recipient, database, 2)
        observe_access(recipient_app, database, "active", 3)
        require(read_album(recipient, selections, database, "album-2", ALBUMS[2]), "social_apps_caught_up")
    finally:
        applied = roster(publisher, database)
        if any(member["state"] == "active" for member in applied["members"]):
            final_revision = int(applied["revision"]) + 1
            revoke(publisher, database, reader_key, final_revision)
            print(f"Final ExeMem revoke acknowledged at revision {final_revision}.", flush=True)
            summary(f"Final ExeMem revoke acknowledged at revision {final_revision}.")
    message = ("The Social SDK created a named database and returned a sealed invitation when called by name. "
               "The recipient opened the same database ID through the SDK. "
               "ExeMem confirmed grant, revoke and regrant. The recipient kept the Social v1 spec across "
               "restart, read later albums, stopped receiving them after revoke, and caught up after regrant.")
    print(message, flush=True)
    summary(message)
    report_content_flow()


def check_role_and_overview(programs, root, publisher, other_database, name, contact, reader_key):
    # Use a second generated database so the original transfer keeps its four revisions.
    created = social(publisher, "create-db", f"{name}-roles")
    database = str(uuid.UUID(created["id"]))
    summary(f"Owned DEV role-check mutation log `{database}`.")
    print(f"Social DEV role-check database {database}", flush=True)
    retry("social_role_configuration", lambda: roster(publisher, database),
          lambda config: config.get("revision") == "0")
    grant_file = root / "contributor.grant"
    arguments = ("add", "--database", database, "--user", "Ana", "--node-key", reader_key)
    try:
        programs.command(publisher.home, [
            "--db", database, "share", "contributors", "grant", "--recipient", str(contact),
            "--recipient-key", reader_key, "--out", str(grant_file),
        ], timeout=30)
        contributor = {"nodeKey": reader_key, "role": "submit", "state": "active"}
        expect_roster(publisher, database, 1, contributor)
        social(publisher, *arguments, refusal=b"already has contributor access")
        expect_roster(publisher, database, 1, contributor)
        social(publisher, "remove", "--database", database, "--node-key", reader_key)
        expect_roster(publisher, database, 2, {**contributor, "state": "revoked"})
        share(publisher, database, None, reader_key, 3)
        check_overview(publisher, other_database, database, reader_key)
    finally:
        applied = roster(publisher, database)
        if any(member["state"] == "active" for member in applied.get("members", [])):
            social(publisher, "remove", "--database", database, "--node-key", reader_key)
    expect_roster(publisher, database, 4, {"nodeKey": reader_key, "role": "read", "state": "revoked"})
    message = ("Social refused an active contributor, restored a revoked contributor as a reader, "
               "and kept a confirmed roster visible while another database's roster was unavailable.")
    print(message, flush=True)
    summary(message)


def check_overview(publisher, busy_database, confirmed_database, reader_key):
    # Hold the existing update lock on this generated home to make one real
    # LastDB roster temporarily unavailable. No data changes under this lock.
    lock = publisher.home / "share-members" / f"{busy_database}.lock"
    with lock.open("rb") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        configurations = {row["databaseId"]: row for row in social(publisher, "grants")}
        require(configurations[busy_database].get("status") == "unavailable"
                and configurations[busy_database].get("httpStatus") == 503
                and configurations[confirmed_database].get("revision") == "3", "social_partial_rosters")
        rows = {row["databaseId"]: row for row in social(publisher, "list")}
        require(rows[busy_database].get("status") == "unavailable"
                and rows[confirmed_database].get("nodeKey") == reader_key
                and rows[confirmed_database].get("role") == "read", "social_partial_overview")


if __name__ == "__main__":
    run_operation(transfer)
