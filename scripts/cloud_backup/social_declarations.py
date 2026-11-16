"""Publish app-declared sharing through the owner's Social commands in the signed DEV operation."""

from contextlib import closing
import json
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from node import Connection
from social_app import sdk, social
from support import decode_json, require, summary


def configure_bindings(publisher, circle, first, second, scopes):
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
        # Each command is a new owner process, so this reads the saved proposal.
        preview = preview_request(publisher, request_id)
        require(preview["status"] == "awaitingApproval", "app_request_visible_on_reopen")
        approve(publisher, request_id, preview)
    for request_id in requests:
        require(sdk(publisher, "sharing-status", requestId=request_id)["revision"] == "0",
                "app_request_confirmed_empty_circle")
    require(not social(publisher, "request", "list")["requests"], "app_confirmed_requests_leave_list")
    links = next(row["links"] for row in social(publisher, "circle", "list") if row["id"] == circle["id"])
    require(links == {first: sorted(scopes), second: []}, "app_declarations_saved")
    summary("Two app callers proposed independent bindings through the packaged SDK. The owner approved "
            "both and their confirmed status survived a new Social process.")
    return links


def decline_binding(publisher, circle, database):
    change = {"operation": "setCircleBinding", "databaseId": database,
              "circleId": circle, "preset": "commenter"}
    declaration = propose(publisher, "photo-journal", change)
    request_id = declaration["requestId"]
    before = roster(publisher, database)
    pending = social(publisher, "request", "list")
    names = {row["requestId"]: (row["appId"], row["databaseId"]) for row in pending["requests"]}
    require(names == {request_id: ("photo-journal", database)} and pending["nextCursor"] is None,
            "app_request_found_on_regular_open")
    preview = preview_request(publisher, request_id)
    declined = social(publisher, "request", "decline", request_id)
    require(declined["status"] == "declined", "app_owner_declined")
    require(approve(publisher, request_id, preview) == declined, "app_decline_cannot_be_approved")
    require(not social(publisher, "request", "list")["requests"], "app_declined_request_leaves_list")
    require(social(publisher, "request", "decline", request_id) == declined,
            "app_decline_idempotent_after_reopen")
    status = sdk(publisher, "sharing-status", requestId=request_id)
    require(status["status"] == "declined" and sdk(publisher, "request-sharing", **declaration) == status
            and roster(publisher, database) == before, "app_decline_returned_through_sdk")
    summary("The owner found a proposal with `social request list` and declined it. It left the list, and its "
            "terminal status survived a repeated decline, a repeated request, and an old approval preview "
            "without changing the roster.")


def propose(publisher, app, change):
    request = {"requestId": str(uuid.uuid4()), "appId": app, "change": change}
    original = sdk(publisher, "request-sharing", **request)
    require(original["status"] == "awaitingApproval" and original["requestId"] == request["requestId"],
            "app_declaration_enrolled")
    require(sdk(publisher, "request-sharing", **request) == original, "app_declaration_durable_retry")
    return request


def resolutions(review):
    """Write `{database: {person: choice}}` as the --resolve flags of a Social command."""
    return [flag for database, people in (review or {}).items() for person, choice in people.items()
            for flag in ("--resolve", f"{database}:{person}={choice}")]


def preview_request(owner, request_id, review=None):
    return social(owner, "request", "show", request_id, *resolutions(review))


def approve(owner, request_id, preview, review=None, refusal=None):
    return social(owner, "request", "approve", request_id, "--token", preview["token"], *resolutions(review),
                  refusal=refusal)


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


def external_changes(publisher, circle, database, reader, schema):
    person = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw).hex()
    current = roster(publisher, database)
    separate = {"nodeKey": person, "state": "active", "role": "read", "writeSchemas": []}
    put_members(publisher, database, current["revision"], [*current["members"], separate])
    change = {"operation": "setCircleBinding", "databaseId": database,
              "circleId": circle["id"], "preset": "commenter"}
    request_id = propose(publisher, "photo-journal", change)["requestId"]
    result = approve(publisher, request_id, preview_request(publisher, request_id))
    require(result["status"] == "confirmed" and int(result["revision"]) > int(current["revision"]) + 1,
            "unrelated_owner_grant_keeps_social_applicable")
    current = roster(publisher, database)
    require(separate in current["members"], "external_grant_preserved")
    outside = {"nodeKey": reader, "state": "active", "role": "submit", "writeSchemas": {"names": [schema]}}
    put_members(publisher, database, current["revision"], [outside, separate])
    request_id = propose(publisher, "photo-journal", {**change, "preset": "viewer"})["requestId"]
    review = preview_request(publisher, request_id)
    conflict = review["databases"][0]["conflicts"]
    require(len(conflict) == 1 and conflict[0]["nodeKey"] == reader
            and conflict[0]["current"]["writeSchemas"] == outside["writeSchemas"], "outside_scope_requires_review")
    approve(publisher, request_id, review, refusal=b"grants changed outside Social")
    require(outside in roster(publisher, database)["members"], "outside_scope_preserved_before_review")
    choices = {database: {reader: "keep"}}
    preview = preview_request(publisher, request_id, choices)
    result = approve(publisher, request_id, preview, choices)
    require(result["status"] == "confirmed" and outside in roster(publisher, database)["members"],
            "outside_scope_retained_after_owner_review")
    summary("A separate publisher grant survived an app update. An overlapping write grant required review "
            "and remained intact when the owner kept it as a direct grant.")
    person_requests(publisher, database, reader, separate)


def person_requests(publisher, database, reader, separate):
    target = {"databaseId": database, "nodeKey": reader}
    for operation, extra, state in (
        ("revokePerson", {}, "revoked"),
        ("restorePerson", {"preset": "viewer"}, "active"),
        ("setPersonGrant", {"preset": "viewer"}, "active"),
    ):
        request = propose(publisher, "photo-journal", {**target, "operation": operation, **extra})
        request_id = request["requestId"]
        result = approve(publisher, request_id, preview_request(publisher, request_id))
        rows = roster(publisher, database)["members"]
        grant = next(row for row in rows if row["nodeKey"] == reader)
        require(result["status"] == "confirmed" and grant["state"] == state
                and grant["writeSchemas"] == [] and separate in rows, "app_direct_grant_lifecycle")
    summary("The app requested an explicit revoke and restore for one person, and the publisher kept "
            "the other independent grant in place.")


def independent_update(publisher, circle_id, first, second, reader, schema):
    current = roster(publisher, first)
    outside = {"nodeKey": reader, "state": "active", "role": "submit", "writeSchemas": {"names": [schema]}}
    put_members(publisher, first, current["revision"], [
        outside if row["nodeKey"] == reader else row for row in current["members"]
    ])
    first_before = roster(publisher, first)
    edit = ("circle", "set", circle_id, "--link", f"{first}={schema}")
    preview = social(publisher, *edit)
    require(preview["databases"][0]["conflicts"][0]["nodeKey"] == reader,
            "app_overlapping_owner_edit_needs_review")
    social(publisher, *edit, "--apply", "--token", preview["token"], refusal=b"grants changed outside Social")
    pending = social(publisher, *edit, "--apply", "--token", preview["token"], "--defer-conflicts")
    require(pending["pending"] == [first], "app_first_database_pending")
    overlap = propose(publisher, "photo-journal", {
        "operation": "setCircleBinding", "databaseId": first, "circleId": circle_id, "preset": "viewer",
    })
    social(publisher, "request", "show", overlap["requestId"], refusal=b"finish the pending sharing update")
    social(publisher, "request", "decline", overlap["requestId"])

    request_id = propose(publisher, "album-viewer", {
        "operation": "restorePerson", "databaseId": second, "nodeKey": reader, "preset": "viewer",
    })["requestId"]
    result = approve(publisher, request_id, preview_request(publisher, request_id))
    active = next(row for row in roster(publisher, second)["members"] if row["nodeKey"] == reader)
    require(result["status"] == "confirmed" and active["state"] == "active"
            and social(publisher, "circle", "show", circle_id)["pending"] == [first]
            and roster(publisher, first) == first_before, "app_second_database_confirmed_while_first_pending")

    result = social(publisher, "db", "retry", first, "--resolve", f"{first}:{reader}=keep")
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
