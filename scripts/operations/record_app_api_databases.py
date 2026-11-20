"""The databases extension on the app socket of a fresh generated home.

An app that did not declare databases is refused. An app that did lists and
creates a database, places schemas, reads the schema list, and calls the
signed-history grant actions (grant, revoke, grants, heads, relays,
bootstrap_relay) on a home with no cloud account. A following /v1/write and /v1/read round-trips one record of a schema the
write list names.

declare_schema does not consult the write list. The app places
databasesproof/Other and social/Outside, which the write list does not name,
and those calls succeed. A /v1/write of a record of either schema in that
database is refused. The write list names social/Note. declare_schema with
app_id social places that schema and a record round-trips. Without app_id
the owner handler refuses social/Note because the namespace is not the
calling app.

invite, accept, submit, and submission_status need two nodes that share one
database. No operation in this tree does that, so this proof does not call them.
This fresh home creates no received database. Every listed row omits publish.
Only a DEV member node (People people-direct, two nodes) can show
publish.pending and publish.lastAttempt. On a received row, pending is null
until this process has seen the spool, and null again after a restart. A
number is the count last recorded by a path that already had the store open.
The list does not open a store.
A service authorization refusal on submit is the cloud's decision. This
fresh home has no cloud, so it cannot show the 403. The DEV two-node run
(People people-circles, exercise_reader) is the proof.
share_link is not called: it needs a connected publisher. The grant actions
on this fresh home have no cloud account and answer 409 no_cloud_account at once.
share_config and set_share_config are retired: they are unknown actions.
"""
import json
from pathlib import Path

import record_app_api_calls
import v1_records as v1
from record_app_api_calls import create
from record_runs import definition, require, require_hex32_id

APP = "databasesproof"
# The owner handler requires schema.name to be "{app_id}/{local}".
SCHEMA = "databasesproof/Note"
# Not in the write list. The databases extension still places it.
OTHER = "databasesproof/Other"
# A schema in another namespace. The write list names it. app_id is that namespace.
FOREIGN = "social/Note"
FOREIGN_NAMESPACE = "social"
# Another namespace, and not in the write list. declare_schema still places it.
OUTSIDE = "social/Outside"
NAME = "dbext"
NOTE = "kept"
FOREIGN_NOTE = "kept-social"
CHAIN_ACTIONS = ("grant", "revoke", "grants", "heads", "relays", "bootstrap_relay")
ACTIONS = (
    "list", "create", "accept", "schemas", "declare_schema", *CHAIN_ACTIONS,
    "invite", "share_link", "submit", "submission_status",
)
NO_ACCOUNT = "this node has no cloud account"
GRANTEE = "ab" * 32


def exercise(node):
    sock = Path(node.socket).with_name("apps.sock")
    register(node, [])
    without_extension(sock)
    register(node, ["databases"])
    database = create_database(node, sock)
    place(sock, database, SCHEMA)
    place_ungranted(sock, database)
    place_other_namespace(sock, database)
    refused(sock, {"action": "nope"}, 400, "invalid_request", "unknown action nope")
    same_schemas(node, sock, database)
    prove_chain(sock, database)
    round_trip(sock, database)
    round_trip(sock, database, FOREIGN, FOREIGN_NOTE)
    print("Databases extension: list, create, declare_schema without a write grant, "
          "a schema in another namespace, schemas, the signed-history grant "
          "actions without a cloud account, and a named read and write passed", flush=True)


def register(node, extensions):
    names = [SCHEMA, FOREIGN]
    reply = node.call("/api/apps/register", {"app": APP, "access": {
        "read": names, "write": names, "extensions": extensions,
    }})[0]
    require(reply.get("app") == APP, "databases proof app was not registered: " + repr(reply.get("app")))


def without_extension(sock):
    message = f"the app {APP} did not declare the extension databases"
    for action in ACTIONS:
        refused(sock, {"action": action}, 403, "permission_denied", message)


def create_database(node, sock):
    status, reply = app_post(sock, {"action": "create", "name": NAME, "sharingMode": "whole_database"})
    require(status == 200, f"create failed: {status} {reply.get('error')!r}")
    row = reply["database"]
    require(row["name"] == NAME and row["sharingMode"] == "whole_database"
            and row["kind"] == "shareable" and row["access"] == "owner" and row["state"] == "ready",
            "create returned another row")
    require_hex32_id(row["id"], label="app create")
    listed = same_list(node, sock)
    owned_rows_omit_publish(listed)
    found = [item for item in listed["databases"] if item.get("name") == NAME]
    require(len(found) == 1 and found[0]["id"] == row["id"], "the owner list omitted the created database")
    return row["id"]


def owned_rows_omit_publish(reply):
    rows = reply["databases"]
    require(isinstance(rows, list) and rows, "database list was empty")
    require(all(isinstance(row, dict) and "publish" not in row for row in rows),
            "an owned database list row included publish")
    print("Received publish is only visible on a DEV member node "
          "(People people-direct, two nodes). This fresh home has no received "
          "database, so it cannot show publish.pending or publish.lastAttempt. "
          "On a received row, pending is null until this process has seen the "
          "spool, and null again after a restart. The list does not open a store.",
          flush=True)


def same_list(node, sock):
    status, reply = app_post(sock, {"action": "list"})
    owner_status, raw = owner_exchange(node, "GET", "/api/databases", None)
    require(status == 200 and owner_status == 200 and reply == json.loads(raw),
            f"list differs from GET /api/databases: extension {status} owner {owner_status}")
    return reply


def place(sock, database, name, app_id=None):
    schema = definition(name, "Hash", ["id", "note"], description_prefix="Databases extension ")
    schema.pop("source", None)
    body = {"action": "declare_schema", "database": database, "schema": schema}
    if app_id is not None:
        body["app_id"] = app_id
    status, reply = app_post(sock, body)
    expect = APP if app_id is None else app_id
    require(status == 200 and reply.get("schema_name") == name and reply.get("app_id") == expect
            and reply.get("resolution") == "register",
            f"declare_schema of {name} failed: {status} {reply.get('error')!r}")
    return schema


def place_ungranted(sock, database):
    """The write list does not name this schema. The extension still places it."""
    place(sock, database, OTHER)
    write_refused(sock, database, OTHER)


def place_other_namespace(sock, database):
    schema = place(sock, database, FOREIGN, FOREIGN_NAMESPACE)
    place(sock, database, OUTSIDE, FOREIGN_NAMESPACE)
    write_refused(sock, database, OUTSIDE)
    refused(sock, {"action": "declare_schema", "database": database, "schema": schema},
            400, "invalid_request", "schema.name namespace must match request.app_id")


def write_refused(sock, database, schema):
    item = create(schema, {"hash": "one"}, {"id": "one", "note": "no"}, database=database)
    status, reply = v1_post(sock, "/v1/write", {"mutations": [item]})
    error = reply.get("error") or {}
    require(status == 403 and error.get("code") == "permission_denied" and error.get("index") == 0
            and error.get("message") == f"the app {APP} did not declare that it writes {schema}",
            f"a write of {schema} was {status} {error!r}")


def same_schemas(node, sock, database):
    status, reply = app_post(sock, {
        "action": "schemas", "database": database,
        "include_full": True, "include_definition_ids": True,
    })
    path = f"/api/databases/{database}/schemas?include_full=true&include_definition_ids=true"
    owner_status, raw = owner_exchange(node, "GET", path, None)
    require(status == 200 and owner_status == 200 and reply == json.loads(raw),
            f"schemas differ: extension {status} owner {owner_status}")
    names = {row["name"] for row in reply["schemas"]}
    require({SCHEMA, OTHER, FOREIGN, OUTSIDE} <= names, "schemas omitted a placed schema")


def prove_chain(sock, database):
    """The grant surface answers from the signed chain, never from a side channel.

    With no cloud account every action that reaches the chain answers 409
    no_cloud_account at once, and this node does not wait. A request the node
    can refuse from its own fields is 400 before that check. share_config and
    set_share_config are unknown actions.

    The signed chain is not proven here. A connected publisher and member are
    the DEV proof (People): grant read, submit, write, and any; a revoke whose
    cutoffs name the grantee's device streams; grants showing the folded roles
    and the grant tip; heads; relays; bootstrap_relay saving the first signed
    relay URL; invite for a key with an active chain role; submit authoring a
    proposal entry; and selfAccess.role from the folded grants.
    """
    for action in ("share_config", "set_share_config"):
        refused(sock, {"action": action, "database": database},
                400, "invalid_request", f"unknown action {action}")
    refused(sock, {"action": "grant", "database": database, "grantee": GRANTEE, "role": "propose"},
            400, "invalid_request", "role must be read, submit, or write")
    refused(sock, {"action": "grant", "database": database, "grantee": GRANTEE, "role": "revoked"},
            400, "invalid_request", "role must be read, submit, or write")
    refused(sock, {"action": "grant", "database": database, "grantee": "nobody", "role": "read"},
            400, "invalid_request", "grantee must be 64 lowercase hex characters or any")
    refused(sock, {"action": "grants", "database": database, "role": "read"},
            400, "invalid_request", "unknown field role")
    bodies = {
        "grant": {"grantee": GRANTEE, "role": "read"},
        "revoke": {"grantee": GRANTEE},
        "grants": {}, "heads": {}, "relays": {}, "bootstrap_relay": {},
    }
    for action in CHAIN_ACTIONS:
        refused(sock, {"action": action, "database": database, **bodies[action]},
                409, "no_cloud_account", NO_ACCOUNT)
    print("Databases extension grants: every chain action answered 409 "
          "no_cloud_account. The signed chain is not proven here.", flush=True)


def round_trip(sock, database, schema=SCHEMA, note=NOTE):
    item = create(schema, {"hash": "one"}, {"id": "one", "note": note}, database=database)
    status, reply = v1_post(sock, "/v1/write", {"mutations": [item]})
    require(status == 200 and reply.get("written") == 1, f"named write failed: {status} {reply.get('error')!r}")
    status, reply = v1_post(sock, "/v1/read", {"reads": [
        v1.point(schema, {"hash": "one"}, database=database),
    ]})
    rows = (reply.get("results") or [{}])[0].get("records") or []
    require(status == 200 and len(rows) == 1 and rows[0]["record"]["note"] == note, "named read missed the record")
    status, reply = v1_post(sock, "/v1/read", {"reads": [v1.point(schema, {"hash": "one"})]})
    personal = (reply.get("results") or [{}])[0]
    records = personal.get("records") or []
    require(status == 200 and all(row.get("record", {}).get("note") != note for row in records),
            "personal read returned the named database record")


def refused(sock, body, status, code, message):
    got, reply = app_post(sock, body)
    error = reply.get("error") or {}
    require(got == status and error.get("code") == code and error.get("message") == message,
            f"{body.get('action')} was {got} {error.get('code')!r}")


def app_post(sock, body):
    return post(sock, "/v1/x/databases", body)


def v1_post(sock, path, body):
    return post(sock, path, body)


def post(sock, path, body):
    status, raw = exchange(sock, "POST", path, body, {
        "Content-Type": "application/json", "X-LastDB-App": APP,
    })
    return status, decode(path, raw)


def owner_exchange(node, method, path, body):
    return exchange(node.socket, method, path, body, {"Content-Type": "application/json"})


def exchange(sock, method, path, body, headers):
    status, raw, _, _, _ = record_app_api_calls.send_unix_request(sock, method, path, body, headers)
    return status, raw


def decode(path, raw):
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{path} returned non-JSON") from error
