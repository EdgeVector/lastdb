"""Accept app sharing declarations and hand approval to the Social owner UI."""

from __future__ import annotations

import json
import re
from uuid import NAMESPACE_URL, UUID, uuid5

from lastdb import LastDBError, SCHEMAS, SocialError, record_mutation
from circles import DIGEST
from request_index import RequestIndex
from reusable_circles import confirm, encoded


SCHEMA = "social-graph/SharingRequest"
DEFINITION = SCHEMAS["SharingRequest"]
# An owner's own change has no saved proposal. Its ID comes from the change,
# so a preview and its apply agree.
OWNER_CHANGES = uuid5(NAMESPACE_URL, "social-graph/owner-change")
APP_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\Z")
PRESETS = {
    "viewer": [],
    "commenter": ["social/Comment"],
    "album-organizer": ["social/Album", "social/AlbumItem"],
}


def uuid_key(value):
    if isinstance(value, str):
        try:
            parsed = UUID(value)
            if parsed.int and str(parsed) == value:
                return value
        except ValueError:
            pass
    raise SocialError("use a canonical UUID for the sharing request and its selections")


class SharingRequests:
    def __init__(self, node, reusable):
        self.node, self.reusable = node, reusable
        self.index = RequestIndex(node, reusable)

    def circles(self):
        _, _, data = self.reusable.load()
        return [{"id": row["id"], "name": row["name"]} for row in data["circles"]]

    def awaiting(self, cursor=None):
        return self.index.awaiting(cursor)

    def submit(self, args):
        if args.keys() != {"requestId", "appId", "change"}:
            raise SocialError("provide a request ID, an app ID, and a sharing change")
        request_id, app = uuid_key(args["requestId"]), args["appId"]
        if not isinstance(app, str) or not APP_ID.fullmatch(app):
            raise SocialError("enter the proposing app's identifier")
        change = args["change"]
        if not isinstance(change, dict):
            raise SocialError("provide a sharing declaration")
        change = parse_change(change)
        database, circle = change["databaseId"], change.get("circleId")
        declaration = {"requestId": request_id, "appId": app, "change": change}
        definition, previous, prior = self.load(request_id)
        if previous:
            return self.replay(declaration, prior)
        if (circle and not any(row["id"] == circle for row in self.circles())) or database not in self.reusable.owned():
            raise SocialError("select an existing circle and an owned whole-database share")
        try:
            self.save(definition, request_id, {"declaration": declaration}, None, self.index.add(declaration))
        except LastDBError as error:
            if error.status != 409:
                raise
            _, _, prior = self.load(request_id)
            return self.replay(declaration, prior)
        return status(declaration)

    def replay(self, declaration, prior):
        if not prior or prior["declaration"] != declaration:
            raise SocialError("this request ID belongs to a different declaration")
        return self.status(declaration["requestId"])

    def load(self, request_id):
        definition = self.node.declare_private(DEFINITION)
        record = self.node.get_record(SCHEMA, uuid_key(request_id))
        return definition, record, json.loads(record["fields"]["request"]) if record else None

    def save(self, definition, request_id, data, previous, index_change=None):
        fields = {"request": encoded(data)}
        if index_change is None:
            self.node.write_record(SCHEMA, definition, request_id, fields, previous)
        else:
            update = record_mutation(SCHEMA, definition, {"hash": request_id, "range": None}, fields, previous)
            self.node.call("POST", "/api/mutations/batch", {"mutations": [update, index_change]})

    def status(self, request_id):
        _, _, data = self.load(request_id)
        if data is None:
            raise SocialError("sharing request does not exist")
        declaration = data["declaration"]
        if "receipt" in data:
            return status(declaration, data["receipt"])
        _, _, directory = self.reusable.load()
        active = directory["operations"].get(declaration["change"]["databaseId"])
        if active and active["requestId"] == request_id:
            return status(declaration, active)
        # A newer operation may have archived this receipt between the reads.
        if active:
            _, _, data = self.load(request_id)
        return status(declaration, data.get("receipt"))

    def preview(self, request_id, review=None):
        with self.reusable.circles.lock():
            return self.review(request_id, review)

    def approve(self, request_id, review=None, token=None):
        """Apply the proposal exactly as its preview shows it, under one lock."""
        with self.reusable.circles.lock():
            preview = self.review(request_id, review)
            if "token" not in preview:
                return preview
            confirm(preview, token)
            return self.review(request_id, review, preview["token"])

    def owner_change(self, change, review=None, *, apply=False, token=None):
        """Preview or apply a direct grant, revoke or restore that the owner asked for."""
        change = parse_change(change)
        request = {"requestId": str(uuid5(OWNER_CHANGES, encoded(change))), "change": change}
        with self.reusable.circles.lock():
            _, _, directory = self.reusable.load()
            edit, operation, label = self.edit(request, directory, review)
            preview = {**self.reusable.edit_locked(edit, operation=operation),
                       "subjectName": label, "change": change}
            if not apply:
                return {**preview, "applied": False}
            confirm(preview, token)
            self.archive(directory["operations"].get(change["databaseId"]))
            applied = self.reusable.edit_locked({**edit, "token": preview["token"]}, apply=True,
                                                operation=operation)
            return {**applied, "subjectName": label, "change": change, "applied": True}

    def review(self, request_id, review, token=None):
        _, _, data = self.load(request_id)
        if not data:
            raise SocialError("sharing request does not exist")
        request = data["declaration"]
        _, _, directory = self.reusable.load()
        active = directory["operations"].get(request["change"]["databaseId"])
        if "receipt" in data or (active and active["requestId"] == request_id):
            return self.status(request_id)
        edit, operation, label = self.edit(request, directory, review)
        if token is None:
            result = self.reusable.edit_locked(edit, operation=operation)
            return {**status(request), **result, "subjectName": label}
        self.archive(active)
        self.reusable.edit_locked({**edit, "token": token}, apply=True, operation=operation,
                                  remove_request=self.index.remove(request_id))
        return self.status(request_id)

    def decline(self, request_id):
        with self.reusable.circles.lock():
            definition, record, data = self.load(request_id)
            if data is None:
                raise SocialError("sharing request does not exist")
            request = data["declaration"]
            receipt = data.get("receipt")
            if receipt and receipt["status"] == "declined":
                return status(request, receipt)
            _, _, directory = self.reusable.load()
            active = directory["operations"].get(request["change"]["databaseId"])
            if receipt or (active and active["requestId"] == request_id):
                raise SocialError("only requests awaiting approval can be declined")
            receipt = {"status": "declined"}
            self.save(definition, request_id, {**data, "receipt": receipt}, record, self.index.remove(request_id))
            return status(request, receipt)

    def edit(self, request, directory, review):
        change = request["change"]
        database = change["databaseId"]
        edit = {"review": review or {}}
        operation = {"requestId": request["requestId"], "databaseId": database}
        if "nodeKey" in change:
            node = change["nodeKey"]
            people = self.reusable.circles.people.by_keys([node])
            if node not in people:
                raise SocialError("save this person's verified contact before sharing")
            return edit, {**operation, "direct": change}, people[node]["display_name"]
        circle = next((row for row in directory["circles"] if row["id"] == change["circleId"]), None)
        if not circle:
            raise SocialError("the circle no longer exists")
        links = dict(circle["links"])
        if change["operation"] == "setCircleBinding":
            links[database] = change["writeSchemas"]
        else:
            links.pop(database, None)
        return {**edit, "circle": {**circle, "links": links}}, operation, circle["name"]

    def archive(self, active):
        if not active or active["status"] != "confirmed":
            return
        definition, record, data = self.load(active["requestId"])
        if data and "receipt" not in data:
            self.save(definition, active["requestId"], {**data, "receipt": active}, record)


def parse_change(change):
    operation = change["operation"]
    database = uuid_key(change["databaseId"])
    if operation in ("setCircleBinding", "removeCircleBinding"):
        target = {"circleId": uuid_key(change["circleId"])}
    elif operation in ("setPersonGrant", "revokePerson", "restorePerson"):
        node = change["nodeKey"]
        if not isinstance(node, str) or not DIGEST.fullmatch(node):
            raise SocialError("choose a verified person")
        target = {"nodeKey": node}
    else:
        raise SocialError("choose a sharing declaration operation")
    result = {"operation": operation, "databaseId": database, **target}
    if operation in ("setCircleBinding", "setPersonGrant", "restorePerson"):
        return {**result, "writeSchemas": parse_scope(change, result.keys())}
    if change.keys() != result.keys():
        raise SocialError("remove a circle or revoke a person without a write scope")
    return result


def parse_scope(change, fields):
    if change.keys() == fields | {"preset"}:
        if isinstance(change["preset"], str) and change["preset"] in PRESETS:
            return PRESETS[change["preset"]]
        raise SocialError("choose Viewer, Commenter, or Album organizer")
    scope = change.get("writeSchemas")
    if change.keys() != fields | {"writeSchemas"}:
        raise SocialError("choose one write preset or a list of installed schema names")
    if scope == "all":
        return scope
    if not isinstance(scope, list) or any(not isinstance(name, str) for name in scope):
        raise SocialError("choose installed schema names")
    return sorted(set(scope))


def status(declaration, receipt=None):
    change = declaration["change"]
    result = {"requestId": declaration["requestId"], "appId": declaration["appId"],
              "databaseId": change["databaseId"], "change": change, "status": "awaitingApproval"}
    if receipt:
        result.update({key: receipt[key] for key in ("status", "revision", "error") if key in receipt})
    return result
