"""Change one member while preserving the rest of the applied configuration."""

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import time
import uuid

from lastdb import LastDBError, SocialError


SPEC = {"name": "social", "version": 1}
PENDING_SCHEMA = "social-graph/SharingUpdate"
PENDING = {
    "name": "SharingUpdate", "descriptive_name": "Sharing Update",
    "purpose_statement": "The last pending Social sharing update for one database.",
    "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
    "fields": ["request"], "field_types": {"request": "String"},
    "field_descriptions": {"request": "The exact pending revision and member configuration."},
}


def owns_social_database(database):
    return database.get("kind") == "shareable" and database.get("spec") == SPEC


class Sharing:
    def __init__(self, node, people):
        self.node = node
        self.people = people

    def create(self, name):
        return self.node.call("POST", "/api/databases", {
            "name": name, "kind": "shareable", "spec": SPEC,
        })["database"]

    def databases(self):
        return self.node.call("GET", "/api/databases")["databases"]

    def owned(self, selector):
        matches = [row for row in self.databases()
                   if selector in (row.get("id"), row.get("name"))]
        if len(matches) != 1 or not owns_social_database(matches[0]):
            raise SocialError(f"{selector} is not an owned social v1 database")
        return matches[0]

    def configuration(self, database_id):
        reply = self.node.call("GET", self.config_path(database_id))
        if reply["databaseId"] != database_id:
            raise SocialError("LastDB returned the configuration of a different database")
        return {key: reply[key] for key in ("databaseId", "revision", "members")}

    def all_configurations(self, selector):
        databases = [self.owned(selector)] if selector else [
            row for row in self.databases() if owns_social_database(row)
        ]
        with ThreadPoolExecutor(max_workers=4) as pool:
            configs = list(pool.map(self.configuration, [row["id"] for row in databases]))
        return [{**config, "name": database["name"]}
                for database, config in zip(databases, configs)]

    @staticmethod
    def config_path(database_id):
        return f"/api/databases/{database_id}/share/config"

    def update_member(self, database_id, node_key, active):
        definition = self.node.declare_private(PENDING)
        for _ in range(3):
            previous_request = self.node.get_record(PENDING_SCHEMA, database_id)
            if previous_request and previous_request["fields"]["request"]:
                self.finish_update(database_id, definition, previous_request)
                continue
            current = self.configuration(database_id)
            members = {row["nodeKey"]: row for row in current["members"]}
            previous = members.get(node_key)
            desired = "active" if active else "revoked"
            if previous and previous["state"] == desired:
                return current["revision"]
            if previous is None and not active:
                raise SocialError("the recipient has never been granted access to this database")
            member = {"nodeKey": node_key, "role": previous["role"] if previous else "read",
                      "state": desired}
            if previous and active:
                member["regrant"] = True
            members[node_key] = member
            update = {"expectedRevision": current["revision"], "requestId": str(uuid.uuid4()),
                      "members": list(members.values())}
            encoded = json.dumps(update, separators=(",", ":"))
            try:
                written = self.node.write_record(PENDING_SCHEMA, definition, database_id,
                                                {"request": encoded}, previous_request)
            except LastDBError as error:
                if error.status != 409:
                    raise
                continue
            saved = {"revision": written["record_revision"], "fields": {"request": encoded}}
            revision = self.finish_update(database_id, definition, saved)
            if revision is not None:
                return revision
        raise SocialError("the sharing configuration changed concurrently; retry the operation")

    def finish_update(self, database_id, definition, saved):
        request = json.loads(saved["fields"]["request"])
        try:
            revision = self.apply(database_id, request)
        except LastDBError as error:
            if error.code != "share_revision_conflict" and error.status not in (400, 403, 404):
                raise
            self.clear_update(database_id, definition, saved)
            if error.code != "share_revision_conflict":
                raise
            return None
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
        for attempt in range(3):
            try:
                ack = self.node.call("PUT", path, update)
            except (LastDBError, OSError, http.client.HTTPException) as error:
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

    def invite(self, selector, display_name, node_key, contact_path):
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
