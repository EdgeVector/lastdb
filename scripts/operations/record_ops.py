"""Owner request counts, checked after the app API proof's own calls."""
import json
from pathlib import Path

from record_runs import require
import record_app_api_calls
import v1_records as v1

APP = "opsproof"
BY_ID = record_app_api_calls.BY_ID
BY_BUCKET = record_app_api_calls.BY_BUCKET


def check(node):
    registered = node.call("/api/apps/register", {
        "app": APP,
        "access": {"read": [BY_ID, BY_BUCKET], "write": [BY_ID], "extensions": []},
    })[0]
    require(registered.get("app") == APP, "ops proof app was not registered: " + repr(registered))
    apps = Path(node.socket).with_name("apps.sock")
    status, reply = app_call(apps, "POST", "/v1/read", {"reads": [
        {"schema": BY_ID, "key": {"hash": "dup"}},
        {"schema": BY_ID, "key": {"hash": "one"}},
        {"schema": BY_BUCKET, "key": {"hash": "bk", "range": "a"}},
    ]})
    require(status == 200, f"ops proof read failed: {status} {reply!r}")
    status, reply = app_call(apps, "POST", "/v1/write", {"mutations": [
        record_app_api_calls.by_id("ops-1", "b", "v"),
        record_app_api_calls.by_id("ops-2", "b", "v"),
    ]})
    require(status == 200 and reply.get("written") == 2,
            f"ops proof write failed: {status} {reply!r}")
    status, reply = app_call(apps, "POST", "/v1/write", {"mutations": [
        v1.create_item(BY_BUCKET, {"hash": "bk", "range": "ops-no"},
                       {"id": "ops-no", "bucket": "bk", "value": "v"}),
    ]})
    require(status == 403 and reply.get("error", {}).get("code") == "permission_denied",
            f"ops proof refused write was not permission_denied: {status} {reply!r}")
    status, reply = app_call(apps, "GET", "/api/query", None)
    require(status == 404 and reply.get("error", {}).get("code") == "not_found",
            f"ops proof deleted route was not not_found: {status} {reply!r}")
    status, reply = app_call(apps, "POST", "/v1/x/databases", {"action": "list"})
    require(status == 403 and reply.get("error", {}).get("code") == "permission_denied",
            f"ops proof databases extension was not permission_denied: {status} {reply!r}")
    payload = node.call("/api/ops")[0]
    calls = [row for row in payload.get("calls") or [] if row.get("caller") == APP]
    schemas = [row for row in payload.get("schemas") or [] if row.get("caller") == APP]
    require(len(calls) == 4, f"ops proof call rows: {calls!r}")
    require(len(schemas) == 4, f"ops proof schema rows: {schemas!r}")
    read = one(calls, call="/v1/read")
    write = one(calls, call="/v1/write")
    query = one(calls, call="/api/query")
    databases = one(calls, call="/v1/x/databases")
    require(read["count"] == 1 and read["items"] == 3 and read["client"] is None
            and read["refused"] == {}, f"read counts: {read!r}")
    require(write["count"] == 2 and write["items"] == 3 and write["client"] is None
            and write["refused"] == {"permission_denied": 1}, f"write counts: {write!r}")
    require(query["count"] == 1 and query["items"] == 1 and query["client"] is None
            and query["refused"] == {"not_found": 1}, f"deleted route: {query!r}")
    require(databases["count"] == 1 and databases["items"] == 1 and databases["client"] is None
            and databases["refused"] == {"permission_denied": 1},
            f"databases extension: {databases!r}")
    schema(schemas, "read", BY_ID, 2)
    schema(schemas, "read", BY_BUCKET, 1)
    schema(schemas, "write", BY_ID, 2)
    schema(schemas, "write", BY_BUCKET, 1)
    uptime = payload.get("uptime_secs")
    require(isinstance(uptime, int) and not isinstance(uptime, bool), f"uptime: {uptime!r}")
    status, reply = app_call(apps, "GET", "/api/ops", None)
    require(status == 404 and reply.get("error", {}).get("code") == "not_found",
            f"the app socket served request counts: {status} {reply!r}")
    print("Request counts: the registered app's reads and writes, a refused write, "
          "a deleted route, and a refused databases extension are on the owner route, "
          "and the app socket does not serve the counts",
          flush=True)


def app_call(sock, method, path, body):
    # Look up the wrapped sender at call time, after the schema install.
    status, raw, _, _, _ = record_app_api_calls.send_unix_request(
        sock, method, path, body,
        {"Content-Type": "application/json", "X-LastDB-App": APP})
    try:
        reply = json.loads(raw) if raw else {}
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{method} {path} returned non-JSON: {raw[:300]!r}") from error
    return status, reply


def one(rows, call):
    found = [row for row in rows if row.get("call") == call]
    require(len(found) == 1, f"expected one {call} row in {rows!r}")
    return found[0]


def schema(rows, op, name, items):
    found = [row for row in rows if row.get("op") == op and row.get("schema") == name]
    require(len(found) == 1, f"expected one {op} {name} row in {rows!r}")
    got = found[0]
    require(got["items"] == items and got["client"] is None, f"{op} {name}: {got!r}")
