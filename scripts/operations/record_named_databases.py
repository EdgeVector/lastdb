"""Owned named databases on a fresh generated home.

Creates private and shareable siblings under ordinary names, including names
that used to be reserved, and checks that the same key stays isolated. After
the daemon restarts, the same database ids still return those values through
the socket and the lastdb CLI.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import mutation, send_unix_request  # noqa: E402
import v1_records as v1  # noqa: E402
from record_runs import (  # noqa: E402
    FIELDS, MISSING_DATABASE, RANGED, definition, refused, require, system_seed,
)


def prepare(node, cli, output):
    require(read_value(node, None) == "v3", "personal key one changed before named databases")
    refuse(node, {"name": "personal"}, "invalid database name")
    refuse(node, {"name": "notes", "kind": "personal",
                  "spec": {"name": "notes", "version": 1}}, "invalid spec binding")
    notes = create(node, "notes", None, kind="personal")
    refused(lambda: node.call("/api/databases/spec", {
        "id": notes["id"], "spec": {"name": "notes", "version": 1},
    }), "invalid spec binding")
    refuse(node, {"name": "notes", "spec": {"name": "notes", "version": 0}}, "invalid spec binding")
    family = create(node, "social-family", {"name": "social", "version": 1})
    alpha = create(node, "alpha", None)
    work = create(node, "work", {"name": "work", "version": 1})
    graph = create(node, "graph", {"name": "social", "version": 1})
    refuse(node, {"name": "social-family", "spec": {"name": "social", "version": 1}},
           "already registered")
    personal_id = owner_id(node)
    refused(lambda: node.call("/api/databases/spec", {
        "id": personal_id, "spec": {"name": "social", "version": 1},
    }), "HTTP 404")
    alpha = bind(node, alpha["id"], {"name": "notes", "version": 1})
    install(node, notes["id"])
    install(node, family["id"])
    install(node, alpha["id"])
    install(node, work["id"])
    install(node, graph["id"])
    write_value(node, notes["id"], "owner-only")
    write_value(node, family["id"], "family")
    write_value(node, alpha["id"], "alpha")
    write_value(node, work["id"], "work")
    write_value(node, graph["id"], "graph")
    require(read_value(node, None) == "v3", "a sibling write changed personal")
    require(read_value(node, "notes") == "owner-only", "notes did not keep its value")
    require(read_value(node, family["id"]) == "family", "social-family did not keep its value")
    require(read_value(node, "social-family") == "family", "the owned name did not select its store")
    require(read_value(node, alpha["id"]) == "alpha", "alpha did not keep its value")
    require(read_value(node, work["id"]) == "work", "work did not keep its value")
    require(read_value(node, graph["id"]) == "graph", "graph did not keep its value")
    refused(lambda: read_value(node, MISSING_DATABASE), "database is not registered on this node")
    saved = {
        "personal_id": personal_id,
        "notes_id": notes["id"],
        "family_id": family["id"],
        "alpha_id": alpha["id"],
        "work_id": work["id"],
        "graph_id": graph["id"],
    }
    (output / "named-databases.json").write_text(json.dumps(saved))
    check_list(node, saved, alpha_spec={"name": "notes", "version": 1})
    cli_read(node, cli, family["id"], "family")
    cli_read(node, cli, "alpha", "alpha")
    check_sessions(node, notes["id"], family["id"])
    print("Named databases: create under ordinary names and keyed isolation passed", flush=True)


def check(node, cli, output):
    saved = json.loads((output / "named-databases.json").read_text())
    require(read_value(node, None) == "v3", "personal changed across restart")
    require(read_value(node, saved["notes_id"]) == "owner-only", "notes changed across restart")
    require(read_value(node, saved["family_id"]) == "family", "social-family changed across restart")
    require(read_value(node, saved["alpha_id"]) == "alpha", "alpha changed across restart")
    require(read_value(node, saved["work_id"]) == "work", "work changed across restart")
    require(read_value(node, saved["graph_id"]) == "graph", "graph changed across restart")
    require(owner_id(node) == saved["personal_id"], "the personal database id changed across restart")
    check_list(node, saved, alpha_spec={"name": "notes", "version": 1})
    cli_read(node, cli, saved["family_id"], "family")
    cli_read(node, cli, "social-family", "family")
    cli_read(node, cli, "personal", "v3")
    cli_read(node, cli, "notes", "owner-only")
    wrong = run_cli(node, cli, MISSING_DATABASE)
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector read personal data: " + wrong.stderr)
    check_cli_operations(node, cli, output, saved["notes_id"])
    print("Named databases survived restart with the same ids and isolated values", flush=True)


def create(node, name, spec, kind="shareable"):
    body = {"name": name, "kind": kind}
    if spec is not None:
        body["spec"] = spec
    database = node.call("/api/databases", body)[0]["database"]
    require(database["name"] == name and database["kind"] == kind, "create returned the wrong row")
    require(database["access"] == "owner" and database["state"] == "ready", "create was not ready")
    require(database["spec"] == spec, "create did not store the spec binding")
    require(uuid.UUID(database["id"]).version == 4, "create did not return a stable id")
    return database


def bind(node, database_id, spec):
    database = node.call("/api/databases/spec", {"id": database_id, "spec": spec})[0]["database"]
    require(database["id"] == database_id and database["spec"] == spec, "spec binding did not stick")
    return database


def install(node, database_id):
    schema = definition("RecordById", "Hash", FIELDS, {"by_bucket": RANGED},
                        description_prefix="Named database example ")
    system_seed(node, schema, database_id)


def write_value(node, database_id, value):
    v1.write_items(node, [v1.create_item(
        "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": value},
        alternates={"by_bucket": {"hash": "b1", "range": "one"}}, database=database_id,
    )])


def read_value(node, database):
    options = {} if database is None else {"database": database}
    rows = v1.read_items(node, [v1.point("RecordById", {"hash": "one"}, **options)])[0]["records"]
    require(len(rows) == 1, f"{database or 'personal'} returned {len(rows)} rows for key one")
    return rows[0]["record"]["value"]


def owner_id(node):
    rows = node.call("/api/databases")[0]["databases"]
    owner = rows[0]
    require(owner["name"] == "personal" and owner["kind"] == "personal" and owner["spec"] is None,
            "the owner row is no longer personal")
    return owner["id"]


def check_list(node, saved, alpha_spec):
    rows = node.call("/api/databases")[0]["databases"]
    by_name = {row["name"]: row for row in rows}
    require(by_name["personal"]["id"] == saved["personal_id"], "personal id changed")
    notes = by_name["notes"]
    require(notes["id"] == saved["notes_id"] and notes["kind"] == "personal"
            and notes["spec"] is None, "notes lost its owner-only registration")
    require(by_name["social-family"]["id"] == saved["family_id"], "social-family id changed")
    require(by_name["social-family"]["spec"] == {"name": "social", "version": 1},
            "social-family lost its spec")
    require(by_name["alpha"]["id"] == saved["alpha_id"], "alpha id changed")
    require(by_name["alpha"]["spec"] == alpha_spec, "alpha lost its spec binding")
    require(by_name["work"]["id"] == saved["work_id"], "work id changed")
    require(by_name["work"]["spec"] == {"name": "work", "version": 1}, "work lost its spec")
    require(by_name["graph"]["id"] == saved["graph_id"], "graph id changed")
    require(by_name["graph"]["spec"] == {"name": "social", "version": 1}, "graph lost its spec")
    require(len(rows) == 6, "the database list has an unexpected row")
    require([row["name"] for row in rows] ==
            ["personal", "notes", "social-family", "alpha", "work", "graph"],
            "owned databases were not listed after personal")


def refuse(node, body, words):
    refused(lambda: node.call("/api/databases", body), words)


def cli_read(node, cli, selector, value):
    result = run_cli(node, cli, selector)
    require(result.returncode == 0, "lastdb get failed: " + result.stderr)
    got = json.loads(result.stdout)["results"][0]["fields"]["value"]
    require(got == value, f"lastdb --db {selector} read {got}")


def run_cmd(node, binary, *arguments):
    environment = {**os.environ, "LASTDB_HOME": str(node.home), "LASTDB_SOCKET_PATH": str(node.socket)}
    return subprocess.run([str(binary), *arguments], capture_output=True, text=True, timeout=60, env=environment)


def run_cli(node, cli, selector):
    return run_cmd(node, cli, "--db", selector, "get", "RecordById", "--key-hash", "one", "--json")


def check_cli_operations(node, cli, output, database_id):
    daemon_help = run_cmd(node, node.binary, "--help")
    require(daemon_help.returncode == 0 and "--share-owner-home" not in daemon_help.stdout,
            "the standalone publication flag is still available")
    share_help = run_cmd(node, cli, "share", "--help")
    subcommands = {line.split()[0] for line in share_help.stdout.splitlines() if line.strip()}
    require(share_help.returncode == 0 and {"accept", "contributors"} <= subcommands and "files" not in subcommands,
            "share --help subcommands drifted: " + share_help.stdout)
    listed = run_cmd(node, cli, "db", "list", "--json")
    require(listed.returncode == 0 and any(db["id"] == database_id for db in json.loads(listed.stdout)["databases"]),
            "db list missed the named database: " + listed.stderr)
    keys = run_cmd(node, cli, "--db", database_id, "get-keys", "RecordById", "--json")
    require(keys.returncode == 0 and [k["hash"] for k in json.loads(keys.stdout)["keys"]] == ["one"],
            "selected database get-keys failed: " + keys.stderr)
    request = output / "cli-mutate.json"
    request.write_text(json.dumps(mutation(
        "RecordById", {"hash": "cli-op"}, {"id": "cli-op", "bucket": "b1", "value": "via-cli"}, "create",
        alternates={"by_bucket": {"hash": "b1", "range": "cli-op"}}, expected={"type": "absent"},
    )))
    written = run_cmd(node, cli, "--db", database_id, "mutate", "--request-file", str(request))
    require(written.returncode == 0 and "record_revision" in json.loads(written.stdout),
            "selected database mutate failed: " + written.stderr)
    got = run_cmd(node, cli, "--db", database_id, "get", "RecordById", "--key-hash", "cli-op", "--json")
    require(got.returncode == 0 and json.loads(got.stdout)["results"][0]["fields"]["value"] == "via-cli",
            "selected database read missed CLI mutation: " + got.stderr)
    wrong = run_cmd(node, cli, "--db", MISSING_DATABASE, "mutate", "--request-file", str(request))
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must refuse the mutation before dispatch: " + wrong.stderr)
    clear_file(node, cli, output, database_id)


SESSION_OTHER = "a database session addresses only its own database"


def issue_session(node, database_id, intent):
    body = node.call("/api/databases/session", {"databaseId": database_id, "intent": intent})[0]
    token = body.get("session")
    require(isinstance(token, str) and token, "a database session did not return a token")
    return token


def session_call(node, token, path, body):
    headers = {"Content-Type": "application/json", "X-LastDB-Database-Session": token}
    status, raw, _headers, _elapsed, _sent = send_unix_request(node.socket, "POST", path, body, headers)
    return status, raw


def record_at(node, database, key):
    options = {} if database is None else {"database": database}
    rows = v1.read_items(node, [v1.point("RecordById", {"hash": key}, **options)])[0]["records"]
    if not rows:
        return None
    require(len(rows) == 1, f"{database or 'personal'} returned {len(rows)} rows for {key}")
    return rows[0]["record"]["value"]


def session_record(schema_key, value):
    return v1.create_item(
        "RecordById", {"hash": schema_key},
        {"id": schema_key, "bucket": "b1", "value": value},
        alternates={"by_bucket": {"hash": "b1", "range": schema_key}},
    )


def check_sessions(node, notes_id, family_id):
    """A session reads and writes only its own database through /v1."""
    token = issue_session(node, notes_id, "read")
    status, raw = session_call(node, token, "/v1/read", {"reads": [
        {"schema": "RecordById", "key": {"hash": "one"}},
        {"schema": "RecordById", "key": {"hash": "one"}, "database": notes_id},
        {"schema": "RecordById", "key": {"hash": "one"}, "database": family_id},
    ]})
    body = json.loads(raw)
    results = body["results"]
    require(status == 200 and results[0]["records"][0]["record"]["value"] == "owner-only",
            "an omitted database did not read the session database: " + raw.decode())
    require(results[1]["records"][0]["record"]["value"] == "owner-only",
            "the session database id did not read that database: " + raw.decode())
    error = results[2]["error"]
    require(error["code"] == "permission_denied" and error["message"] == SESSION_OTHER and error["index"] == 2,
            "a read session item naming another database was not refused: " + repr(error))
    status, raw = session_call(node, token, "/v1/write", {"mutations": [session_record("session-read", "no")]})
    require(status == 403 and raw == b"database session refused",
            "a read session write was not refused: " + repr(raw))
    token = issue_session(node, notes_id, "write")
    status, raw = session_call(node, token, "/v1/write", {"mutations": [session_record("session-write", "from-session")]})
    written = json.loads(raw)
    require(status == 200 and written["written"] == 1, "a write session did not write: " + raw.decode())
    require(record_at(node, notes_id, "session-write") == "from-session",
            "the write session did not write its own database")
    require(record_at(node, None, "session-write") is None,
            "the write session wrote the personal database")
    v1.write_items(node, [v1.delete_item(
        "RecordById", {"hash": "session-write"}, written["revisions"][0], database=notes_id,
    )])
    require(record_at(node, notes_id, "session-write") is None,
            "the session proof record stayed behind")
    other = session_record("session-other", "crossed")
    other["database"] = family_id
    status, raw = session_call(node, token, "/v1/write", {"mutations": [other]})
    refused = json.loads(raw)["error"]
    require(status == 403 and refused["code"] == "permission_denied"
            and refused["message"] == SESSION_OTHER and refused["index"] == 0,
            "a write session item naming another database was not refused: " + raw.decode())
    require(record_at(node, family_id, "session-other") is None,
            "a refused session write landed in the other database")


def clear_file(node, cli, output, database_id):
    """`lastdb mutate` links a file by path, then clears it with `"file": null`."""
    folder = output / "cli-file"
    folder.mkdir()
    (folder / "note.txt").write_text("keep this file where it is\n")
    created = mutation(
        "RecordById", {"hash": "cli-file"}, {"id": "cli-file", "bucket": "b1", "value": "with-file"},
        "create", alternates={"by_bucket": {"hash": "b1", "range": "cli-file"}}, expected={"type": "absent"},
    )
    request = folder / "create.json"
    request.write_text(json.dumps({"mutation": created, "file": {"source": "note.txt", "name": "note.txt"}}))
    written = run_cmd(node, cli, "--db", database_id, "mutate", "--request-file", str(request))
    require(written.returncode == 0, "mutate did not link a file: " + written.stderr)
    revision = json.loads(written.stdout)["record_revision"]
    got = run_cmd(node, cli, "--db", database_id, "get", "RecordById", "--key-hash", "cli-file", "--json")
    linked = json.loads(got.stdout)["results"][0].get("metadata", {}).get("file")
    require(got.returncode == 0 and isinstance(linked, dict) and linked.get("name") == "note.txt",
            "mutate did not link the file by path: " + got.stdout)
    replacement = mutation(
        "RecordById", {"hash": "cli-file"}, {"id": "cli-file", "bucket": "b1", "value": "with-file"},
        "replace", alternates={"by_bucket": {"hash": "b1", "range": "cli-file"}},
        expected={"type": "revision", "revision": revision},
    )
    clear = folder / "clear.json"
    clear.write_text(json.dumps({"mutation": replacement, "file": None}))
    cleared = run_cmd(node, cli, "--db", database_id, "mutate", "--request-file", str(clear))
    require(cleared.returncode == 0, "mutate did not clear a file: " + cleared.stderr)
    again = run_cmd(node, cli, "--db", database_id, "get", "RecordById", "--key-hash", "cli-file", "--json")
    require(again.returncode == 0 and "metadata" not in json.loads(again.stdout)["results"][0],
            "the cleared record still names a file: " + again.stdout)
