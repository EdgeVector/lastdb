"""The databases extension on the app socket of a fresh generated home.

An app that did not declare databases is refused. An app that did lists and
creates a database, places schemas, reads the schema list, and reads and
writes the share configuration through the same handlers as the owner routes.
A following /v1/write and /v1/read round-trips one record of a schema the
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
share_link is not called: it needs a connected publisher. Share configuration
on this fresh home has no cloud account and answers 409 no_cloud_account at once.
"""
import json
import time
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
ACTIONS = (
    "list", "create", "accept", "schemas", "declare_schema", "share_config",
    "set_share_config", "invite", "share_link", "submit", "submission_status",
)
REQUEST_ID = "11111111-1111-4111-8111-111111111111"
NO_ACCOUNT = "this node has no cloud account"
SHARE = {
    "share_revision_conflict": "the share configuration is at another revision",
    "share_configuration_unconfirmed": "share configuration is not confirmed",
    "database_enrolling": "the database is still enrolling with the cloud; retry",
    "no_cloud_account": NO_ACCOUNT,
    "payment_required": "payment is required",
    "invalid_share_configuration": "invalid share configuration",
}
OWNER_CODES = {"no_cloud_account": "no_cloud_account", "database_enrolling": "enrolling"}
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
    place(sock, database, SCHEMA)
    place_ungranted(sock, database)
    place_other_namespace(sock, database)
    refused(sock, {"action": "nope"}, 400, "invalid_request", "unknown action nope")
    same_schemas(node, sock, database)
    prove_share(node, sock, database)
    round_trip(sock, database)
    round_trip(sock, database, FOREIGN, FOREIGN_NOTE)
    print("Databases extension: list, create, declare_schema without a write grant, "
          "a schema in another namespace, schemas, share_config, set_share_config, "
          "and a named read and write passed", flush=True)


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


def prove_share(node, sock, database):
    """No cloud account, so share configuration answers 409 at once.

    The owner body is ok false, error no_cloud_account, and the message
    "this node has no cloud account". The extension code is no_cloud_account with
    that message. A PUT that names read, write, or memberWrites has the same
    answer. A 400 would mean the body was refused before that check. The
    calls return in under 10 seconds: this node does not wait.

    Still enrolling cannot be shown here. A connected publisher creates a
    database and immediately PUTs /api/databases/{id}/share/config. That call
    must succeed after a short wait, not answer 503.

    The signed statement and the proposal check are not proven here. No CI
    operation runs a publisher and a member against the cloud. A two-node DEV
    proof must make these requests:

    1. Publisher GET /api/databases/{id}/share/config. No stored statement
       answers read "members", write "proposals", and memberWrites {}.
    2. Publisher PUT /api/databases/{id}/share/config with the same members,
       read "anyone", write "direct", and memberWrites
       {"social/Comment": "own_records"}. The node POSTs
       /api/cloud/backups/v2/share/members/settings/get, signs sequence 1, and
       POSTs /api/cloud/backups/v2/share/members/settings/set. Members are
       unchanged, so it does not POST /api/cloud/backups/v2/share/members/set.
       The reply is 200, the same member revision, committed true.
    3. Publisher GET /api/databases/{id}/share/config again. read is "anyone",
       write is "direct", and memberWrites is {"social/Comment": "own_records"}.
    4. Publisher repeats that PUT. The three are unchanged, so no settings
       statement and no member set. 200, same revision.
    5. Publisher PUT /api/databases/{id}/share/config with only write
       "proposals" (read and memberWrites omitted). The node signs sequence 2
       (anyone, proposals, the same map) and POSTs
       /api/cloud/backups/v2/share/members/settings/set. No member set.
    6. Publisher PUT /api/databases/{id}/share/config that changes a member and
       sets read back to "members". Settings set goes first (sequence 3,
       members and proposals, the same map), then the member set. If the
       member set fails, that member error is the reply and sequence 3 stays
       stored. A repeat sends no statement and retries the members while
       expectedRevision still matches.
    7. After the member node's sync refresh, GET /api/databases.
       selfAccess.read, selfAccess.write, and selfAccess.memberWrites come
       from POST /api/cloud/backups/v2/share/members/settings/get with the
       member LastDBNode token and X-LastDB-Self-Access: 1, in the same
       refresh as POST /api/cloud/backups/v2/share/members/read. No stored
       statement is members, proposals, and memberWrites {}.
    8. With write "proposals" and an active Submit member, memberWrites is
       {"social/Comment": "own_records"}. The member's create is published and
       created_by is that member. That member's replace and delete of the
       record are published. Another member's replace is discarded.
    9. The publisher sets social/Comment to "shared". A member's create is
       published. A member's replace or delete of another member's record is
       published.
    10. The publisher sets social/Comment to "owner_only", or leaves the schema
        out of the map. A member's create is discarded. The publisher's own
        writes are not on this check.
    """
    config = {"expectedRevision": "0", "requestId": REQUEST_ID, "members": []}
    path = f"/api/databases/{database}/share/config"
    started = time.monotonic()
    get_owner, get_ext = compare(
        node, sock, "GET", path, None,
        {"action": "share_config", "database": database})
    put_owner, put_ext = share_put(node, sock, database, path, config)
    both = dict(config, read="anyone", write="direct")
    both_owner, both_ext = share_put(node, sock, database, path, both)
    one = dict(config, write="proposals")
    one_owner, one_ext = share_put(node, sock, database, path, one)
    mapped = dict(config, memberWrites={
        "social/Comment": "own_records",
        "social/Album": "owner_only",
    })
    map_owner, map_ext = share_put(node, sock, database, path, mapped)
    require(get_owner == 409 and put_owner == 409 and both_owner == 409
            and one_owner == 409 and map_owner == 409,
            f"share shape was GET {get_owner} PUT {put_owner} "
            f"read/write {both_owner} write {one_owner} memberWrites {map_owner}")
    elapsed = time.monotonic() - started
    require(elapsed < 10, f"no-account share answers took {elapsed:.1f}s")
    print(f"Databases extension share: GET owner {get_owner} extension {get_ext}, "
          f"PUT owner {put_owner} extension {put_ext}, "
          f"read/write owner {both_owner} extension {both_ext}, "
          f"write owner {one_owner} extension {one_ext}, "
          f"memberWrites owner {map_owner} extension {map_ext}. "
          "The signed statement and the proposal check are not proven.", flush=True)


def share_put(node, sock, database, path, config):
    return compare(
        node, sock, "PUT", path, config,
        {"action": "set_share_config", "database": database, "config": config})


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
    body = parsed_owner(raw)
    require(owner_status == 409 and body.get("ok") is False
            and body.get("error") == "no_cloud_account" and body.get("message") == NO_ACCOUNT,
            f"{method} share owner {owner_status} {body!r}")
    expect = app_status(owner_status)
    message = "internal error" if expect == 500 else owner_message(raw)
    got = reply.get("error")
    code = OWNER_CODES.get(body.get("error"), error_code(expect))
    require(ext_status == expect and got == {"code": code, "message": message},
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


def parsed_owner(raw):
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else (raw or "")
    try:
        value = json.loads(text) if text else {}
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def owner_message(raw):
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else (raw or "")
    try:
        value = json.loads(text) if text else None
    except json.JSONDecodeError:
        value = None
    if isinstance(value, dict):
        detail = value.get("message")
        code = value.get("error")
        if (isinstance(detail, str) and detail and isinstance(code, str) and code
                and code not in SHARE):
            return f"{code}: {detail}"
        for key in ("message", "reason"):
            got = value.get(key)
            if isinstance(got, str) and got:
                return got
        if isinstance(code, str) and code:
            return SHARE.get(code, "request refused")
    return text.strip() or "request refused"
