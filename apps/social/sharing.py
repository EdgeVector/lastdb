"""Change one member while preserving the rest of the applied configuration."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import time
import uuid

from lastdb import LastDBError, SocialError


SPEC = {"name": "social", "version": 1}
TRANSPORT_ERRORS = (OSError, http.client.HTTPException)
PENDING_SCHEMA = "social-graph/SharingUpdate"
UNREAD = object()


def can_manage_sharing(database):
    return database.get("kind") == "shareable" and (
        database.get("sharingMode") == "whole_database" or database.get("spec") == SPEC)


class Sharing:
    def __init__(self, node, people):
        self.node = node
        self.people = people

    def create(self, name, whole=False, *, request_id=None):
        policy = {"sharingMode": "whole_database"} if whole else {"spec": SPEC}
        return self.node.call("POST", "/api/databases", {
            "name": name, "kind": "shareable", **policy,
        }, request_id=request_id)["database"]

    def databases(self):
        return self.node.call("GET", "/api/databases")["databases"]

    def owned(self, selector):
        matches = [row for row in self.databases()
                   if selector in (row.get("id"), row.get("name"))]
        if len(matches) != 1 or not can_manage_sharing(matches[0]):
            raise SocialError(f"{selector} needs an owned database with a supported sharing format")
        return matches[0]

    def configuration(self, database_id):
        reply = self.node.call("GET", self.config_path(database_id))
        if reply["databaseId"] != database_id:
            raise SocialError("LastDB returned the configuration of a different database")
        return {key: reply[key] for key in ("databaseId", "revision", "members")}

    def all_configurations(self, selector):
        databases = [self.owned(selector)] if selector else [
            row for row in self.databases() if can_manage_sharing(row)
        ]
        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(self.configuration_row, databases))

    def configuration_row(self, database):
        database_id = database["id"]
        row = {"databaseId": database_id, "name": database["name"]}
        try:
            config = self.configuration(database_id)
        except LastDBError as error:
            if error.code == "share_update_pending":
                return {**row, "status": "pending"}
            return {**row, "status": "unavailable", "httpStatus": error.status}
        except TRANSPORT_ERRORS:
            return {**row, "status": "unavailable"}
        return {**row, **config}

    @staticmethod
    def config_path(database_id):
        return f"/api/databases/{database_id}/share/config"

    def update_member(self, database_id, node_key, active):
        definition = self.node.declare_private("SharingUpdate")
        starting_member = UNREAD
        for _ in range(3):
            previous_request = self.node.get_record(PENDING_SCHEMA, database_id)
            if previous_request and previous_request["fields"]["request"]:
                self.finish_update(database_id, definition, previous_request)
                continue
            current = self.configuration(database_id)
            members = {row["nodeKey"]: row for row in current["members"]}
            previous = members.get(node_key)
            desired = "active" if active else "revoked"
            if active and previous and previous["state"] == "active" and previous["role"] != "read":
                raise SocialError("this person already has contributor access; revoke it before granting reader access")
            if previous and previous["state"] == desired:
                return current["revision"]
            if starting_member is not UNREAD and previous != starting_member:
                raise SocialError("this person's access changed concurrently; review the current grant")
            starting_member = previous
            if previous is None and not active:
                raise SocialError("the recipient has never been granted access to this database")
            scoped = previous is not None and "writeSchemas" in previous
            member = {"nodeKey": node_key, "role": "read" if active else previous["role"],
                      "state": desired}
            if scoped:
                member["writeSchemas"] = []
            if previous and active:
                member["regrant"] = True
            members[node_key] = member
            update = {"expectedRevision": current["revision"], "requestId": str(uuid.uuid4()),
                      "members": list(members.values())}
            try:
                revision = self.save_update(database_id, definition, previous_request, update)
            except LastDBError as error:
                if error.status != 409:
                    raise
                continue
            if revision is not None:
                return revision
        raise SocialError("the sharing configuration changed concurrently; retry the operation")

    def apply_members(self, database_id, expected_revision, members, circles):
        definition = self.node.declare_private("SharingUpdate")
        previous = self.node.get_record(PENDING_SCHEMA, database_id)
        if previous and previous["fields"]["request"]:
            raise SocialError("finish the pending sharing change first")
        request = {"expectedRevision": expected_revision, "requestId": str(uuid.uuid4()),
                   "members": members, "circles": circles}
        revision = self.save_update(database_id, definition, previous, request)
        if revision is None:
            raise SocialError("sharing changed while you edited it; review the current roster")
        return revision

    def save_update(self, database_id, definition, previous, request):
        encoded = json.dumps(request, separators=(",", ":"))
        written = self.node.write_record(PENDING_SCHEMA, definition, database_id,
                                         {"request": encoded}, previous)
        saved = {"revision": written["record_revision"], "fields": {"request": encoded}}
        return self.finish_update(database_id, definition, saved)

    def resume_pending(self, database_id):
        definition = self.node.declare_private("SharingUpdate")
        saved = self.node.get_record(PENDING_SCHEMA, database_id)
        if saved and saved["fields"]["request"]:
            self.finish_update(database_id, definition, saved)

    def pending_edit(self, database_id):
        self.node.declare_private("SharingUpdate")
        saved = self.node.get_record(PENDING_SCHEMA, database_id)
        if not saved or not saved["fields"]["request"]:
            return None
        request = json.loads(saved["fields"]["request"])
        return {"expectedRevision": request["expectedRevision"], "members": request["members"]}

    def finish_update(self, database_id, definition, saved):
        request = json.loads(saved["fields"]["request"])
        if any(member.get("regrant") and member["role"] == "submit" and "writeSchemas" not in member
               for member in request["members"]):
            raise SocialError("a pending contributor regrant requires owner review")
        try:
            revision = self.apply(database_id, request)
        except LastDBError as error:
            if error.code != "share_revision_conflict" and error.status not in (400, 403, 404):
                raise
            self.clear_update(database_id, definition, saved)
            if error.code != "share_revision_conflict":
                raise
            return None
        if "circles" in request:
            from circles import store_policy
            store_policy(self.node, database_id, request["circles"])
        self.clear_update(database_id, definition, saved)
        return revision

    def clear_update(self, database_id, definition, saved):
        try:
            self.node.write_record(PENDING_SCHEMA, definition, database_id, {"request": ""}, saved)
        except LastDBError as error:
            if error.status != 409:
                raise

    def apply(self, database_id, update):
        path = self.config_path(database_id)
        wire = {key: update[key] for key in ("expectedRevision", "requestId", "members")}
        for attempt in range(3):
            try:
                ack = self.node.call("PUT", path, wire)
            except (LastDBError, *TRANSPORT_ERRORS) as error:
                if isinstance(error, LastDBError) and error.status != 503:
                    raise
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)
            else:
                if (ack["databaseId"] != database_id or ack["requestId"] != update["requestId"]
                        or ack["committed"] is not True
                        or int(ack["revision"]) != int(update["expectedRevision"]) + 1):
                    raise SocialError("LastDB returned a mismatched sharing acknowledgement")
                return ack["revision"]

    def invite(self, selector, display_name, node_key, contact_path=None):
        database_id = self.owned(selector)["id"]
        publisher = self.node.own_key()
        if publisher == node_key:
            raise SocialError("the database owner already has access")
        if contact_path is None:
            person = self.people.by_keys([node_key]).get(node_key)
            if not person or not person.get("contact_code", "").startswith("lastdb-share-contact-v1."):
                raise SocialError("provide the recipient's verified contact file with --contact")
            verified = self.node.verify_saved_contact(person["contact_code"], node_key)
        else:
            verified = self.node.verify_contact(contact_path, node_key)
        contact_code = verified["contactCode"]
        self.people.remember(node_key, display_name, contact_code)
        revision = self.update_member(database_id, node_key, True)
        return self.seal(database_id, node_key, contact_code, revision, publisher)

    def seal(self, database_id, node_key, contact_code, revision, publisher):
        response = self.node.call("POST", f"/api/databases/{database_id}/invitations", {
            "contactCode": contact_code, "expectedNodeKey": node_key,
        })
        if response["databaseId"] != database_id or not response.get("invitation"):
            raise SocialError("LastDB returned a mismatched invitation")
        return {"databaseId": database_id, "revision": revision, "publisher": publisher,
                "invitation": response["invitation"]}

    def accept(self, receipt, publisher, name):
        if receipt.get("publisher") != publisher:
            raise SocialError("invitation publisher differs from the trusted publisher")
        return self.node.call("POST", "/api/databases/accept", {
            "invitation": receipt["invitation"], "publisher": publisher,
            "database_id": receipt["databaseId"], "name": name,
        })["database"]


def read_invitation(stream):
    raw = stream.read(8193)
    if len(raw) > 8192:
        raise SocialError("invitation receipt exceeds its size limit")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SocialError("expected a Social invitation receipt")
    return value
