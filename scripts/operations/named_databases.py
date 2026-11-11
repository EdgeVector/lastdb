"""Create two owned databases and read them back through the CLI and SDK.

The daemon, home, and socket come from the fresh record-home operation.
Personal stays the unscoped default. Received shares stay on `db list`.
This script does not copy a primary and does not start cloud backup.
"""

import json
import os
from pathlib import Path
import subprocess


def require(value, message):
    if not value:
        raise RuntimeError(message)


def exercise(cli, node):
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    personal = record(node, "RecordById", "one")
    require(personal["fields"]["value"] == "v3", "personal record changed before named databases")
    command = cli_runner(node)
    listed = databases(command, cli)
    original = owner_row(listed)
    refuse(command, cli, "db", "create", "personal", "database name is reserved and not provisioned")
    refuse(command, cli, "db", "create", "graph", "database name is reserved and not provisioned")
    refuse(command, cli, "db", "create", "social", "reserved database name requires its spec")
    refuse(command, cli, "db", "create", "bad name", "invalid database name")
    notes = created(command, cli, "notes")
    family = created(command, cli, "social-family", "--spec-name", "social", "--spec-version", "1")
    require(family["spec"] == {"name": "social", "version": 1}, "social-family spec binding differs")
    refuse(command, cli, "db", "create", "notes", "database name is already registered")
    rows = databases(command, cli)
    require(owner_row(rows)["id"] == original["id"], "personal changed id when an owned database was created")
    require({row["id"] for row in rows} >= {original["id"], notes["id"], family["id"]},
            "list dropped personal or an owned database")
    require(select(command, cli, "notes")["id"] == notes["id"], "select by name missed notes")
    require(select(command, cli, notes["id"])["id"] == notes["id"], "select by id missed notes")
    seed(node, notes["id"])
    seed(node, family["id"])
    write(command, cli, node, "notes", "n1", "from-notes")
    write(command, cli, node, family["id"], "s1", "from-social")
    require(read(command, cli, "notes", "n1") == "from-notes", "notes did not keep its record")
    require(read(command, cli, family["id"], "s1") == "from-social", "social-family did not keep its record")
    require(read(command, cli, "notes", "s1") is None, "notes returned the social-family record")
    require(read(command, cli, "social-family", "n1") is None, "social-family returned the notes record")
    require(record(node, "RecordById", "one")["fields"]["value"] == "v3",
            "an owned write changed the personal record")
    absent = command(cli, "--db", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "get", "RecordById",
                     "--key-hash", "one", "--json")
    require(absent.returncode == 1 and "database is not registered" in absent.stderr,
            "an unknown database id fell back to personal: " + absent.stderr)
    sdk = sdk_database(node, "v3")
    require(read(command, cli, sdk["id"], "sdk-1", schema=sdk["schema"]) == "from-sdk",
            "CLI select did not read the SDK database")
    node.stop()
    node.start()
    require(read(command, cli, "notes", "n1") == "from-notes", "notes lost its record across restart")
    require(read(command, cli, "social-family", "s1") == "from-social",
            "social-family lost its record across restart")
    require(read(command, cli, sdk["name"], "sdk-1", schema=sdk["schema"]) == "from-sdk",
            "the SDK database lost its record across restart")
    restarted = command(cli, "get", "RecordById", "--key-hash", "one", "--json")
    require(restarted.returncode == 0 and json.loads(restarted.stdout)["results"][0]["fields"]["value"] == "v3",
            "the unscoped personal default changed across restart: " + restarted.stderr)
    selected = command(cli, "--db", "personal", "get", "RecordById", "--key-hash", "one", "--json")
    require(selected.returncode == 0 and json.loads(selected.stdout)["results"][0]["fields"]["value"] == "v3",
            "explicit personal selection changed across restart: " + selected.stderr)
    print("Named databases: create, list, select, keyed mutation, isolation, and restart passed",
          flush=True)


def cli_runner(node):
    def command(binary, *arguments):
        environment = {
            **os.environ,
            "LASTDB_HOME": str(node.home),
            "LASTDB_SOCKET_PATH": str(node.socket),
        }
        return subprocess.run([str(binary), *arguments], capture_output=True, text=True,
                              timeout=60, env=environment, check=False)
    return command


def databases(command, cli):
    listed = command(cli, "db", "list", "--json")
    require(listed.returncode == 0, "database list failed: " + listed.stderr)
    rows = json.loads(listed.stdout)["databases"]
    require(isinstance(rows, list) and rows, "database list was empty")
    return rows


def owner_row(rows):
    found = [row for row in rows if row.get("name") == "personal" or row.get("id") == "personal"]
    require(len(found) == 1 and found[0]["access"] == "owner" and found[0]["kind"] == "personal",
            "personal default is missing from the database list: " + json.dumps(rows))
    return found[0]


def created(command, cli, name, *arguments):
    result = command(cli, "db", "create", name, *arguments, "--json")
    require(result.returncode == 0, f"database create {name} failed: " + result.stderr)
    database = json.loads(result.stdout)
    require(database["name"] == name and database["kind"] == "shareable"
            and database["access"] == "owner" and database["state"] == "ready",
            f"created database {name} differs: " + result.stdout)
    require(database["id"] != name and database["id"] != "personal",
            f"created database {name} reused a selector as its id")
    return database


def select(command, cli, selector):
    result = command(cli, "db", "select", selector, "--json")
    require(result.returncode == 0, f"database select {selector} failed: " + result.stderr)
    return json.loads(result.stdout)


def refuse(command, cli, *arguments, message):
    result = command(cli, *arguments)
    require(result.returncode != 0 and message in result.stderr,
            f"{' '.join(arguments)} did not refuse with {message}: " + result.stderr)


def seed(node, selector):
    node.call(f"/api/databases/{selector}/schemas/seed-system", {
        "isolated_copy": True,
        "schema": {
            "source": "system_seed",
            "name": "NamedNote",
            "descriptive_name": "Named database note",
            "schema_type": "Hash",
            "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": ["id", "value"],
            "field_types": {"id": "String", "value": "String"},
            "field_descriptions": {
                "id": "Named database note id",
                "value": "Named database note value",
            },
            "field_versions": {"id": 1, "value": 1},
            "field_versions": {"id": 1, "value": 1},
        },
    })


def write(command, cli, node, selector, key, value):
    request = node.root / f"named-{selector}-{key}.json"
    request.write_text(json.dumps({
        "type": "mutation",
        "schema": "NamedNote",
        "key_value": {"hash": key, "range": None},
        "alternates": {},
        "record": {"id": key, "value": value},
        "mutation_type": "create",
        "expected": {"type": "absent"},
    }))
    written = command(cli, "--db", selector, "mutate", "--request-file", request)
    require(written.returncode == 0 and "record_revision" in json.loads(written.stdout),
            f"mutation on {selector} failed: " + written.stderr)


def read(command, cli, selector, key, schema="NamedNote"):
    result = command(cli, "--db", selector, "get", schema, "--key-hash", key, "--json")
    require(result.returncode == 0, f"read {selector} {key} failed: " + result.stderr)
    rows = json.loads(result.stdout)["results"]
    if not rows:
        return None
    require(len(rows) == 1, f"read {selector} {key} returned {len(rows)} rows")
    return rows[0]["fields"]["value"]


def record(node, schema, key):
    page = node.call("/api/query", {"schema_name": schema, "filter": {"HashKey": key}})[0]
    require(len(page["results"]) == 1, f"{schema} {key} was not one personal record")
    return page["results"][0]


def sdk_database(node, personal_value):
    script = Path(__file__).with_name("named_databases_sdk.mjs")
    result = subprocess.run(
        ["node", str(script), str(node.socket), personal_value],
        capture_output=True, text=True, timeout=60, check=False,
    )
    require(result.returncode == 0, "SDK named database operation failed: " + result.stderr)
    created_database = json.loads(result.stdout)
    require(created_database["name"] == "sdk-ledger" and created_database["value"] == "from-sdk",
            "SDK operation returned a different database: " + result.stdout)
    return created_database
