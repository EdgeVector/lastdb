"""Publish app-declared sharing through the owner UI in the signed DEV operation."""

from contextlib import closing
import json
from pathlib import Path
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from node import Connection
from social_app import call, sdk, window
from support import decode_json, require, summary


def configure_bindings(publisher, owner, circle, first, second, scopes):
    require(sdk(publisher, "circles") == [{"id": circle["id"], "name": circle["name"]}],
            "app_circle_discovery")
    decline_binding(publisher, circle["id"], first)
    requests = []
    for app, database, grant in (
        ("photo-journal", first, {"writeSchemas": scopes}),
        ("album-viewer", second, {"preset": "viewer"}),
    ):
        declaration = propose(publisher, app, {"operation": "setCircleBinding", "databaseId": database,
                                              "circleId": circle["id"], **grant})
        current = roster(publisher, database)
        require(current["revision"] == "0" and not current["members"], "app_request_needs_owner_approval")
        request_id = declaration["requestId"]
        requests.append(request_id)
        with window(publisher, sharing_request=request_id) as review_window:
            preview = preview_request(review_window, request_id)
            require(preview["status"] == "awaitingApproval", "app_request_visible_on_reopen")
            call(review_window, "/api/sharing-request/apply", {"requestId": request_id, "token": preview["token"]},
                 authorized=False, status=403)
            approve(review_window, request_id, preview)
    for request_id in requests:
        require(sdk(publisher, "sharing-status", requestId=request_id)["revision"] == "0",
                "app_request_confirmed_empty_circle")
    require(not call(owner, "/api/sharing-requests")["requests"], "app_confirmed_requests_leave_list")
    directory = call(owner, "/api/reusable-circles")
    links = next(row["links"] for row in directory["circles"] if row["id"] == circle["id"])
    require(links == {first: sorted(scopes), second: []}, "app_declarations_saved")
    summary("Two app callers proposed independent bindings through the packaged SDK. The owner approved "
            "both and their confirmed status survived reopening Social.")
    return links


def decline_binding(publisher, circle, database):
    change = {"operation": "setCircleBinding", "databaseId": database,
              "circleId": circle, "preset": "commenter"}
    declaration = propose(publisher, "photo-journal", change)
    legacy = save_legacy_request(publisher, change)
    legacy_id = legacy["requestId"]
    request = {"requestId": declaration["requestId"]}
    before = roster(publisher, database)
    with window(publisher) as review_window:
        call(review_window, "/api/sharing-requests", authorized=False, status=403)
        pending = call(review_window, "/api/sharing-requests")
        names = {row["requestId"]: (row["appId"], row["databaseId"]) for row in pending["requests"]}
        require(names == {declaration["requestId"]: ("photo-journal", database), legacy_id: ("old-gallery", database)}
                and pending["nextCursor"] is None and not pending["importing"], "app_request_found_on_regular_open")
        preview = preview_request(review_window, declaration["requestId"])
        call(review_window, "/api/sharing-request/decline", request, authorized=False, status=403)
        declined = call(review_window, "/api/sharing-request/decline", request)
        require(declined["status"] == "declined", "app_owner_declined")
        require(approve(review_window, declaration["requestId"], preview) == declined,
                "app_decline_cannot_be_approved")
        remaining = call(review_window, "/api/sharing-requests")["requests"]
        require([row["requestId"] for row in remaining] == [legacy_id], "app_declined_request_leaves_list")
        write_legacy_request(publisher, legacy, {"status": "declined"})
        require(not call(review_window, "/api/sharing-requests")["requests"]
                and sdk(publisher, "sharing-status", requestId=legacy_id)["status"] == "declined",
                "app_legacy_decision_leaves_list")
        discover_late_requests(publisher, review_window, change)
    with window(publisher, sharing_request=declaration["requestId"]) as reopened:
        require(call(reopened, "/api/sharing-request/decline", request) == declined,
                "app_decline_idempotent_after_reopen")
    status = sdk(publisher, "sharing-status", **request)
    require(status["status"] == "declined" and sdk(publisher, "request-sharing", **declaration) == status
            and roster(publisher, database) == before, "app_decline_returned_through_sdk")
    summary("A signed-in owner found a proposal in Requests in a regular Social window and declined it. "
            "It left the list, and its terminal status survived reopening, a repeated request, "
            "and an old approval preview without changing the roster. Social reconciled a decline saved in "
            "the legacy format and recovered two late legacy proposals through refresh and SDK retry.")


def discover_late_requests(publisher, window, change):
    late = save_legacy_request(publisher, change)
    call(window, "/api/sharing-requests/recheck", {}, authorized=False, status=403)
    page = call(window, "/api/sharing-requests/recheck", {})
    require([row["requestId"] for row in page["requests"]] == [late["requestId"]],
            "app_refresh_recovers_late_legacy_request")
    retried = save_legacy_request(publisher, change)
    require(sdk(publisher, "request-sharing", **retried)["status"] == "awaitingApproval",
            "app_retry_recovers_late_legacy_request")
    page = call(window, "/api/sharing-requests")
    request_ids = {late["requestId"], retried["requestId"]}
    require({row["requestId"] for row in page["requests"]} == request_ids, "app_late_requests_visible")
    for request_id in sorted(request_ids):
        call(window, "/api/sharing-request/decline", {"requestId": request_id})


def save_legacy_request(publisher, change):
    normalized = {key: value for key, value in change.items() if key != "preset"}
    normalized["writeSchemas"] = ["social/Comment"]
    declaration = {"requestId": str(uuid.uuid4()), "appId": "old-gallery", "change": normalized}
    write_legacy_request(publisher, declaration)
    return declaration


def write_legacy_request(publisher, declaration, receipt=None):
    manifest = Path(__file__).resolve().parents[2] / "lastdb-app.json"
    definition = next(row for row in json.loads(manifest.read_text())["schemas"] if row["name"] == "SharingRequest")
    definition = {name: value for name, value in definition.items() if name != "owner_app_id"}
    installed = publisher.call("/api/schemas/declare", {"namespace": "social-graph", "schema": definition})
    schema = "social-graph/SharingRequest"
    request_id = declaration["requestId"]
    saved, expected, mode = {"declaration": declaration}, {"type": "absent"}, "create"
    if receipt is not None:
        prior = publisher.call("/api/query", {"schema_name": schema, "filter": {"HashKey": request_id}})["results"][0]
        expected, mode = {"type": "revision", "revision": prior["revision"]}, "replace"
        saved["receipt"] = receipt
    publisher.call("/api/mutation", {
        "type": "mutation", "schema": schema, "definition_id": installed["definition_id"],
        "key_value": {"hash": request_id}, "alternates": {},
        "record": {"request": json.dumps(saved)}, "mutation_type": mode,
        "expected": expected, "durability": "durable",
    })


def propose(publisher, app, change):
    request = {"requestId": str(uuid.uuid4()), "appId": app, "change": change}
    original = sdk(publisher, "request-sharing", **request)
    require(original["status"] == "awaitingApproval" and original["requestId"] == request["requestId"],
            "app_declaration_enrolled")
    require(sdk(publisher, "request-sharing", **request) == original, "app_declaration_durable_retry")
    return request


def preview_request(owner, request_id, review=None):
    return call(owner, "/api/sharing-request/preview", {"requestId": request_id, "review": review})


def approve(owner, request_id, preview, review=None):
    return call(owner, "/api/sharing-request/apply", {"requestId": request_id, "review": review,
                                                    "token": preview["token"]})


def roster(node, database):
    return node.call(f"/api/databases/{database}/share/config")


def put_members(node, database, revision, members):
    update = {"expectedRevision": revision, "requestId": str(uuid.uuid4()), "members": members}
    with closing(Connection(node.socket)) as connection:
        connection.request("PUT", f"/api/databases/{database}/share/config",
                           json.dumps(update), {"Content-Type": "application/json"})
        with connection.getresponse() as response:
            require(response.status == 200, "publisher_roster_update")
            value = decode_json(response.read(128 * 1024))
    require(value["committed"] and value["requestId"] == update["requestId"], "publisher_roster_confirmed")
    return value


def external_changes(publisher, owner, circle, database, reader, schema):
    person = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
    current = roster(publisher, database)
    separate = {"nodeKey": person, "state": "active", "role": "read", "writeSchemas": []}
    put_members(publisher, database, current["revision"], [*current["members"], separate])
    change = {"operation": "setCircleBinding", "databaseId": database,
              "circleId": circle["id"], "preset": "commenter"}
    request_id = propose(publisher, "photo-journal", change)["requestId"]
    result = approve(owner, request_id, preview_request(owner, request_id))
    require(result["status"] == "confirmed" and int(result["revision"]) > int(current["revision"]) + 1,
            "unrelated_owner_grant_keeps_social_applicable")
    current = roster(publisher, database)
    require(separate in current["members"], "external_grant_preserved")
    outside = {"nodeKey": reader, "state": "active", "role": "submit", "writeSchemas": {"names": [schema]}}
    put_members(publisher, database, current["revision"], [outside, separate])
    request_id = propose(publisher, "photo-journal", {**change, "preset": "viewer"})["requestId"]
    review = preview_request(owner, request_id)
    conflict = review["databases"][0]["conflicts"]
    require(len(conflict) == 1 and conflict[0]["nodeKey"] == reader
            and conflict[0]["current"]["writeSchemas"] == outside["writeSchemas"], "outside_scope_requires_review")
    call(owner, "/api/sharing-request/apply", {"requestId": request_id, "token": review["token"]}, status=400)
    require(outside in roster(publisher, database)["members"], "outside_scope_preserved_before_review")
    choices = {database: {reader: "keepDirect"}}
    # Use a fresh owner process to pick up the saved proposal and approve the independent grant.
    with window(publisher, sharing_request=request_id) as reopened:
        preview = preview_request(reopened, request_id, choices)
        result = approve(reopened, request_id, preview, choices)
    require(result["status"] == "confirmed" and outside in roster(publisher, database)["members"],
            "outside_scope_retained_after_owner_review")
    summary("A separate publisher grant survived an app update. An overlapping write grant required review "
            "and remained intact when the owner kept it as a direct grant after reopening Social.")
    person_requests(publisher, owner, database, reader, separate)


def person_requests(publisher, owner, database, reader, separate):
    target = {"databaseId": database, "nodeKey": reader}
    for operation, extra, state in (
        ("revokePerson", {}, "revoked"),
        ("restorePerson", {"preset": "viewer"}, "active"),
        ("setPersonGrant", {"preset": "viewer"}, "active"),
    ):
        request = propose(publisher, "photo-journal", {**target, "operation": operation, **extra})
        request_id = request["requestId"]
        result = approve(owner, request_id, preview_request(owner, request_id))
        rows = roster(publisher, database)["members"]
        grant = next(row for row in rows if row["nodeKey"] == reader)
        require(result["status"] == "confirmed" and grant["state"] == state
                and grant["writeSchemas"] == [] and separate in rows, "app_direct_grant_lifecycle")
    summary("The app requested an explicit revoke and restore for one person, and the publisher kept "
            "the other independent grant in place.")


def independent_update(publisher, owner, circle_id, first, second, reader, schema):
    circle = next(row for row in call(owner, "/api/reusable-circles")["circles"] if row["id"] == circle_id)
    current = roster(publisher, first)
    outside = {"nodeKey": reader, "state": "active", "role": "submit", "writeSchemas": {"names": [schema]}}
    put_members(publisher, first, current["revision"], [
        outside if row["nodeKey"] == reader else row for row in current["members"]
    ])
    first_before = roster(publisher, first)
    edit = {"circle": {**circle, "links": {**circle["links"], first: [schema]}}}
    preview = call(owner, "/api/circles/preview", edit)
    require(preview["databases"][0]["conflicts"][0]["nodeKey"] == reader,
            "app_overlapping_owner_edit_needs_review")
    pending = call(owner, "/api/circles/apply", {**edit, "token": preview["token"]})
    require(pending["pending"] == [first], "app_first_database_pending")
    overlap = propose(publisher, "photo-journal", {
        "operation": "setCircleBinding", "databaseId": first, "circleId": circle_id, "preset": "viewer",
    })
    blocked = call(owner, "/api/sharing-request/preview", {"requestId": overlap["requestId"]}, status=400)
    require("finish the pending sharing update" in blocked["error"], "app_same_database_waits")
    call(owner, "/api/sharing-request/decline", {"requestId": overlap["requestId"]})

    request_id = propose(publisher, "album-viewer", {
        "operation": "restorePerson", "databaseId": second, "nodeKey": reader, "preset": "viewer",
    })["requestId"]
    with window(publisher, sharing_request=request_id) as reopened:
        result = approve(reopened, request_id, preview_request(reopened, request_id))
    active = next(row for row in roster(publisher, second)["members"] if row["nodeKey"] == reader)
    require(result["status"] == "confirmed" and active["state"] == "active"
            and call(owner, "/api/reusable-circles")["pending"] == [first]
            and roster(publisher, first) == first_before, "app_second_database_confirmed_while_first_pending")

    retry = {"databaseId": first, "review": {first: {reader: "keepDirect"}}}
    preview = call(owner, "/api/circles/preview", retry)
    result = call(owner, "/api/circles/apply", {**retry, "token": preview["token"]})
    require(not result["pending"] and roster(publisher, first) == first_before
            and sdk(publisher, "sharing-status", requestId=request_id)["status"] == "confirmed",
            "app_first_database_finished_without_disturbing_second")
    summary("A pending grant review blocked the same database while a second database confirmed its own "
            "owner-approved update. Resolving the first preserved the second outcome.")


def revoke_roster(node, database):
    current = roster(node, database)
    if any(row["state"] == "active" for row in current["members"]):
        members = [{**row, "state": "revoked", "writeSchemas": []} for row in current["members"]]
        put_members(node, database, current["revision"], members)
    require(all(row["state"] == "revoked" for row in roster(node, database)["members"]), "circle_cleanup_revoked")
