"""The databases extension on the app socket of a fresh generated home.

An app that did not declare databases is refused. An app that did lists and
creates a database, places one schema, reads the schema list, and reads and
writes the share configuration through the same handlers as the owner routes.
A following /v1/write and /v1/read round-trips one record in that database.

The write grant also names social/Note. declare_schema with app_id social
places that schema and a record round-trips. The same call for a schema
outside the grant is refused. Without app_id the owner handler refuses
social/Note because the namespace is not the calling app.

invite, accept, submit, and submission_status need two nodes that share one
database. No operation in this tree does that, so this proof does not call them.
"""
import json
import uuid
from pathlib import Path

import record_app_api_calls
import v1_records as v1
from record_app_api_calls import create
from record_runs import definition, require

APP = "databasesproof"
# The owner handler requires schema.name to be "{app_id}/{local}".
SCHEMA = "databasesproof/Note"
OTHER = "databasesproof/Other"
# A schema in another namespace. The write grant names it. app_id is that namespace.
FOREIGN = "social/Note"
FOREIGN_NAMESPACE = "social"
OUTSIDE = "social/Outside"
NAME = "dbext"
NOTE = "kept"
FOREIGN_NOTE = "kept-social"
ACTIONS = (
    "list", "create", "accept", "schemas", "declare_schema", "share_config",
    "set_share_config", "invite", "submit", "submission_status",
)
REQUEST_ID = "11111111-1111-4111-8111-111111111111"
SHARE = {
    "share_revision_conflict": "the share configuration is at another revision",
    "share_configuration_unconfirmed": "share configuration is not confirmed",
    "payment_required": "payment is required",
    "invalid_share_configuration": "invalid share configuration",
}
CODES = {
    400: "invalid_request", 403: "permission_denied", 404: "not_found",
    409: "conflict", 413: "too_large", 503: "busy",
}


def exercise(node):
    sock = Path(node.socket).with_name("apps.sock")
    register(node, [])
    without_extension(sock)
    register(node, ["databases"])
    database = create_database(node, sock)
    declare_note(sock, database)
    refused(sock, {"action": "declare_schema", "database": database, "schema": {"name": OTHER}},
            403, "permission_denied", f"the app {APP} did not declare that it writes {OTHER}")
    place_other_namespace(sock, database)
    refused(sock, {"action": "nope"}, 400, "invalid_request", "unknown action nope")
    same_schemas(node, sock, database)
    prove_share(node, sock, database)
    round_trip(sock, database)
    round_trip(sock, database, FOREIGN, FOREIGN_NOTE)
    print("Databases extension: list, create, declare_schema, a schema in another "
          "namespace, schemas, share_config, set_share_config, and a named read and "
          "write passed", flush=True)


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
    require(uuid.UUID(row["id"]).version == 4, "create did not return a v4 id")
    listed = same_list(node, sock)
    found = [item for item in listed["databases"] if item.get("name") == NAME]
    require(len(found) == 1 and found[0]["id"] == row["id"], "the owner list omitted the created database")
    return row["id"]


def same_list(node, sock):
    status, reply = app_post(sock, {"action": "list"})
    owner_status, raw = owner_exchange(node, "GET", "/api/databases", None)
    require(status == 200 and owner_status == 200 and reply == json.loads(raw),
            f"list differs from GET /api/databases: extension {status} owner {owner_status}")
    return reply


def declare_note(sock, database):
    schema = definition(SCHEMA, "Hash", ["id", "note"], description_prefix="Databases extension ")
    schema.pop("source", None)
    status, reply = app_post(sock, {"action": "declare_schema", "database": database, "schema": schema})
    require(status == 200 and reply.get("schema_name") == SCHEMA and reply.get("app_id") == APP
            and reply.get("resolution") == "register",
            f"declare_schema failed: {status} {reply.get('error')!r}")
    return schema


def place_other_namespace(sock, database):
    schema = definition(FOREIGN, "Hash", ["id", "note"], description_prefix="Databases extension ")
    schema.pop("source", None)
    status, reply = app_post(sock, {
        "action": "declare_schema", "database": database, "schema": schema,
        "app_id": FOREIGN_NAMESPACE,
    })
    require(status == 200 and reply.get("schema_name") == FOREIGN
            and reply.get("app_id") == FOREIGN_NAMESPACE and reply.get("resolution") == "register",
            f"declare_schema with app_id failed: {status} {reply.get('error')!r}")
    refused(sock, {
        "action": "declare_schema", "database": database,
        "schema": {"name": OUTSIDE}, "app_id": FOREIGN_NAMESPACE,
    }, 403, "permission_denied", f"the app {APP} did not declare that it writes {OUTSIDE}")
    refused(sock, {"action": "declare_schema", "database": database, "schema": schema},
            400, "invalid_request", "schema.name namespace must match request.app_id")


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
    require(SCHEMA in names and FOREIGN in names, "schemas omitted a placed schema")


def prove_share(node, sock, database):
    config = {"expectedRevision": "0", "requestId": REQUEST_ID, "members": []}
    get_owner, get_ext = compare(
        node, sock, "GET", f"/api/databases/{database}/share/config", None,
        {"action": "share_config", "database": database})
    put_owner, put_ext = compare(
        node, sock, "PUT", f"/api/databases/{database}/share/config", config,
        {"action": "set_share_config", "database": database, "config": config})
    print(f"Databases extension share: GET owner {get_owner} extension {get_ext}, "
          f"PUT owner {put_owner} extension {put_ext}", flush=True)


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


def compare(node, sock, method, path, owner_body, extension_body):
    owner_status, raw = owner_exchange(node, method, path, owner_body)
    ext_status, reply = app_post(sock, extension_body)
    if 200 <= owner_status < 300:
        require(ext_status == owner_status and reply == json.loads(raw),
                f"{method} share success differs: owner {owner_status} extension {ext_status}")
        return owner_status, ext_status
    expect = app_status(owner_status)
    message = "internal error" if expect == 500 else owner_message(raw)
    got = reply.get("error")
    require(ext_status == expect and got == {"code": error_code(expect), "message": message},
            f"{method} share extension {ext_status} {got!r} owner {owner_status}")
    return owner_status, ext_status


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


def app_status(status):
    if status == 402:
        status = 403
    status = {422: 400, 401: 403, 423: 503, 429: 503}.get(status, status)
    return status if status in CODES or status == 507 else 500


def error_code(status):
    return CODES.get(status, "internal")


def owner_message(raw):
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else (raw or "")
    try:
        value = json.loads(text) if text else None
    except json.JSONDecodeError:
        value = None
    if isinstance(value, dict):
        for key in ("message", "reason"):
            got = value.get(key)
            if isinstance(got, str) and got:
                return got
        code = value.get("error")
        if isinstance(code, str) and code:
            return SHARE.get(code, "request refused")
    return text.strip() or "request refused"
