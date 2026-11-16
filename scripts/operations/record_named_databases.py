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
from harness import conditioned, mutation  # noqa: E402
from record_runs import FIELDS, RANGED, definition, refused, require, system_seed  # noqa: E402


def prepare(node, cli, output):
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
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
    missing = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    refused(lambda: read_value(node, missing), "database is not registered")
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
    wrong = run_cli(node, cli, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
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
    request = mutation(
        "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": value}, "create",
        alternates={"by_bucket": {"hash": "b1", "range": "one"}}, expected={"type": "absent"},
    )
    node.call(f"/api/databases/{database_id}/mutation", conditioned(node, [request])[0])


def read_value(node, database):
    path = "/api/query" if database is None else f"/api/databases/{database}/query"
    rows = node.call(path, {"schema_name": "RecordById", "filter": {"HashKey": "one"}})[0]["results"]
    require(len(rows) == 1, f"{database or 'personal'} returned {len(rows)} rows for key one")
    return rows[0]["fields"]["value"]


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
    wrong = run_cmd(node, cli, "--db", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "mutate", "--request-file", str(request))
    require(wrong.returncode == 1 and "database is not registered" in wrong.stderr,
            "an unknown database selector must refuse the mutation before dispatch: " + wrong.stderr)
