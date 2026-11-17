"""An app granted every schema: read without a name, and write without a name."""
import json
from pathlib import Path

from harness import send_unix_request
from record_app_api_calls import BY_ID, by_id, value_of
from record_runs import require


def all_schemas(node):
    """All-schemas read can read an unnamed schema and cannot write it.

    All-schemas write can write that schema.
    """
    sock = Path(node.socket).with_name("apps.sock")

    def register(access):
        reply = node.call(
            "/api/apps/register", {"app": "every", "access": access})[0]
        require(reply.get("app") == "every",
                "all-schemas registration did not return the app id: " + repr(reply))
        return reply["app"]

    def call(app_id, path, body):
        headers = {"Content-Type": "application/json", "X-LastDB-App": app_id}
        status, raw, _, _, _ = send_unix_request(sock, "POST", path, body, headers)
        return status, json.loads(raw)

    app_id = register({"read": "*", "write": [], "extensions": []})
    status, reply = call(app_id, "/v1/read", {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]})
    require(status == 200 and len(reply["results"][0].get("records") or []) == 1,
            f"all-schemas read could not read a schema it did not name: {status} {reply!r}")
    status, reply = call(
        app_id, "/v1/write", {"mutations": [by_id("all-read-denied", "b1", "v")]})
    require(status == 403 and reply["error"]["code"] == "permission_denied",
            f"all-schemas read wrote a schema: {status} {reply!r}")
    require(value_of(node, "all-read-denied") is None, "all-schemas read wrote a record")

    app_id = register({"read": [], "write": "*", "extensions": []})
    status, reply = call(
        app_id, "/v1/write", {"mutations": [by_id("all-write", "b1", "v")]})
    require(status == 200 and reply.get("written") == 1,
            f"all-schemas write could not write a schema it did not name: {status} {reply!r}")
    require(value_of(node, "all-write") == "v", "all-schemas write did not store the record")
    print("App socket: all-schemas read can read an unnamed schema and cannot write it; "
          "all-schemas write can write it", flush=True)
