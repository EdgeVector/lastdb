"""Private Social contacts stored as keyed Person records."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib

from lastdb import LastDBError, SocialError, record_mutation


PERSON_SCHEMA = "social-graph/Person"
ADDRESS_BOOK = "social-graph/AddressBookEntry"
BOOK_HASH = "contacts"


def query_for(person_id):
    return {"schema_name": PERSON_SCHEMA, "filter": {"HashKey": person_id}}


class People:
    def __init__(self, node):
        self.node = node

    def declare(self):
        return self.node.declare_private("Person")

    def remember(self, node_key, display_name, contact_code):
        definition = self.declare()
        person_id = hashlib.sha256(node_key.encode()).hexdigest()
        for _ in range(3):
            existing = self.node.get_record(PERSON_SCHEMA, person_id)
            fields = dict(existing["fields"]) if existing else {
                "person_id": person_id, "node_key": node_key, "avatar": "none",
                "added_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            if fields["node_key"] != node_key:
                raise SocialError("stored contact has a different node key")
            if fields.get("display_name") == display_name and fields.get("contact_code") == contact_code:
                self.index(person_id)
                return fields
            fields.update(display_name=display_name, contact_code=contact_code)
            try:
                self.node.write_record(PERSON_SCHEMA, definition, person_id, fields, existing)
            except LastDBError as error:
                if error.status != 409:
                    raise
            else:
                self.index(person_id)
                return fields
        raise SocialError("contact changed concurrently; retry the operation")

    def remove(self, node_key):
        """Delete the contact and its address book entry in one batch."""
        definition = self.declare()
        entry_definition = self.node.declare_private("AddressBookEntry")
        person_id = hashlib.sha256(node_key.encode()).hexdigest()
        entry_key = {"hash": BOOK_HASH, "range": person_id}
        reply = self.node.call("POST", "/api/queries/batch", {"queries": [
            query_for(person_id), {"schema_name": ADDRESS_BOOK, "filter": {"HashRangeKey": entry_key}},
        ]})["results"]
        if len(reply) != 2 or any(item["status"] != 200 for item in reply):
            raise SocialError("LastDB could not read the contact")
        person, entry = (item["response"]["results"] for item in reply)
        if not person:
            raise SocialError("this person is not in the address book")
        removals = [record_mutation(PERSON_SCHEMA, definition, {"hash": person_id, "range": None}, None, person[0])]
        if entry:
            removals.append(record_mutation(ADDRESS_BOOK, entry_definition, entry_key, None, entry[0]))
        self.node.call("POST", "/api/mutations/batch", {"mutations": removals})

    def by_keys(self, node_keys):
        if not node_keys:
            return {}
        self.declare()
        person_ids = [hashlib.sha256(key.encode()).hexdigest() for key in sorted(set(node_keys))]
        batches = [person_ids[index:index + 64] for index in range(0, len(person_ids), 64)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            return {person["node_key"]: person for batch in pool.map(self.read_batch, batches)
                    for person in batch}

    def read_batch(self, person_ids):
        reply = self.node.call("POST", "/api/queries/batch", {
            "queries": [query_for(person_id) for person_id in person_ids],
        })
        if len(reply["results"]) != len(person_ids):
            raise SocialError("LastDB returned an incomplete contact batch")
        people = []
        for item in reply["results"]:
            if item["status"] != 200:
                raise SocialError("LastDB could not read a contact")
            people.extend(row["fields"] for row in item["response"]["results"])
        return people

    def index(self, person_id, definition=None):
        if definition is None:
            definition = self.node.declare_private("AddressBookEntry")
        target = {"hash": BOOK_HASH, "range": person_id}
        try:
            self.node.call("POST", "/api/mutation", {
                "type": "mutation", "schema": ADDRESS_BOOK, "definition_id": definition,
                "key_value": target, "alternates": {}, "record": {"person_id": person_id},
                "mutation_type": "create", "expected": {"type": "absent"}, "durability": "durable",
            })
        except LastDBError as error:
            if error.status != 409:
                raise

    def address_book(self):
        self.declare()
        self.node.declare_private("AddressBookEntry")
        result = []
        cursor = None
        while True:
            request = {"schema_name": ADDRESS_BOOK, "filter": {"HashKey": BOOK_HASH}, "limit": 64}
            if cursor:
                request["cursor"] = cursor
            page = self.node.call("POST", "/api/query", request)
            keys = [row["fields"]["person_id"] for row in page["results"]]
            if keys:
                result.extend(self.read_batch(keys))
            if not page["has_more"]:
                return result
            cursor = page.get("next_cursor")
            if not cursor:
                raise SocialError("LastDB omitted the next address book cursor")
