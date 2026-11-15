"""Read a disposable Social database from ExeMem as its two DEV participants."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import time
import urllib.error
import urllib.request

from support import OperationError, decode_json, dev_endpoint, require

sys.path.append(str(Path(__file__).resolve().parents[1] / "mutation_log_load"))
from node_auth import NodeKey, authorization

ROOT = "/api/cloud/backups/v2"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise OperationError("circle_cloud_redirect")


def post(path, data, authentication, *, expect_denial=False):
    request = urllib.request.Request(dev_endpoint() + path, data=data, method="POST",
                                     headers={"Content-Type": "application/json", **authentication})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=15) as response:
            require(not expect_denial, "circle_cloud_receiver_inbox_access")
            require(response.status == 200, "circle_cloud_response_status")
            data = response.read(2_000_001)
    except urllib.error.HTTPError as error:
        if expect_denial and error.code in (401, 403):
            return None
        raise OperationError(f"circle_cloud_{path.rsplit('/', 1)[-1]}_http_{error.code}") from None
    except (OSError, urllib.error.URLError):
        raise OperationError("circle_cloud_unavailable") from None
    require(len(data) <= 2_000_000, "circle_cloud_response_size")
    return decode_json(data)


def scope_for(publisher, api_key):
    body = json.dumps({"version": 2, "account_id": publisher.account}).encode()
    scope = post(ROOT + "/context/read", body, {"X-API-Key": api_key.decode("ascii")})["scope"]
    require(scope["account_id"] == publisher.account, "circle_cloud_publisher_scope")
    return scope


def view(node, scope, database, action, revision, *, expect_denial=False):
    key = NodeKey.from_seed((node.home / "identity.key").read_bytes())
    path = f"{ROOT}/mutation-log/{action}"
    fields = {"database_id": database}
    if action == "list":
        fields.update(after="0", limit="32")
    if action == "inbox/settle":
        fields.update(through="0")
    body = json.dumps(fields).encode()
    header, _ = authorization(key, path, database, time.time_ns() // 1_000_000, body,
                              scope["account_generation"], account=scope["account_id"],
                              member_revision=str(revision))
    return key.public.hex(), post(path, body, {"Authorization": header}, expect_denial=expect_denial)


def inbox(publisher, scope, database, revision):
    return view(publisher, scope, database, "inbox/list", revision)[1]


def published_views(recipient, scope, database, revision):
    with ThreadPoolExecutor(max_workers=2) as pool:
        return list(pool.map(lambda action: view(recipient, scope, database, action, revision)[1], ("list", "head")))


def pending_positions(publisher, recipient, scope, database, revision, mutation_ids, published_before):
    calls = [(publisher, "list", False), (publisher, "inbox/list", False),
             (recipient, "inbox/list", True), (recipient, "inbox/settle", True)]
    with ThreadPoolExecutor(max_workers=len(calls)) as pool:
        owner, owner_inbox, reader_inbox, _ = list(pool.map(
            lambda call: view(call[0], scope, database, call[1], revision, expect_denial=call[2]), calls))
    publisher_key, published = owner
    _, page = owner_inbox
    recipient_key = reader_inbox[0]
    pending = {row["entry"]["device_id"]: int(row["delivery_seq"]) for row in page["entries"]
               if row["entry"]["node_public_key"] == recipient_key
               and row["entry"]["namespace"] == "lastdb-member-submission"}
    expected = {f"member-{mutation_id}" for mutation_id in mutation_ids}
    require(set(pending) == expected, "circle_publisher_sees_all_pending_submissions")
    published_after = published_views(recipient, scope, database, revision)
    require(published_after[0]["entries"], "circle_exemem_receiver_has_published_entries")
    require(published_after == published_before, "circle_inbox_did_not_advance_published_log")
    for view_body in (published, *published_after):
        require(all(row["entry"]["node_public_key"] == publisher_key for row in view_body["entries"]),
                "circle_exemem_receiver_only_publisher_entries")
    return max(pending.values())
