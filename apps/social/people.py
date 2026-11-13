"""Private Social contacts stored as keyed Person records."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from urllib.parse import urlencode

from lastdb import LastDBError, SocialError


PERSON_SCHEMA = "social-graph/Person"
ADDRESS_BOOK = "social-graph/AddressBookEntry"
BOOK_HASH = "contacts"
BOOK = {
    "name": "AddressBookEntry", "descriptive_name": "Address Book Entry",
    "purpose_statement": "A keyed reference to a verified Social contact.",
    "schema_type": "HashRange",
    "record_keys": {"primary": {"kind": "hash_range"}, "alternates": {}},
    "fields": ["person_id"], "field_types": {"person_id": "String"},
    "field_descriptions": {"person_id": "Primary key of the private Social Person record."},
}
PERSON = {
    "name": "Person",
    "descriptive_name": "Person",
    "purpose_statement": "One person the owner has added. The hash coordinate person_id is the hash of node_key.",
    "schema_type": "Hash",
    "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
    "fields": ["person_id", "node_key", "contact_code", "display_name", "avatar", "added_at"],
    "field_types": {
        "person_id": "String",
        "node_key": "String",
        "contact_code": "String",
        "display_name": "String",
        "avatar": "String",
        "added_at": "DateTime",
    },
    "field_descriptions": {
        "person_id": "Stable id of this person: the hash of node_key.",
        "node_key": "Node key of this person. person_id is its hash.",
        "contact_code": "Code another person uses to add this contact.",
        "display_name": "Name shown for this person.",
        "avatar": "Inline thumbnail.",
        "added_at": "When this person was added, RFC 3339.",
    },
}


def query_for(person_id):
    return {"schema_name": PERSON_SCHEMA, "filter": {"HashKey": person_id}}


class People:
    def __init__(self, node):
        self.node = node

    def declare(self):
        return self.node.declare_private(PERSON)

    def import_contact(self, node_key, display_name, contact_code):
        if not isinstance(display_name, str) or not (1 <= len(display_name.strip()) <= 100):
            raise SocialError("enter a contact name of 1 to 100 characters")
        if not isinstance(contact_code, str) or not (1 <= len(contact_code.strip()) <= 512):
            raise SocialError("enter the LastDB contact code")
        if not isinstance(node_key, str) or len(node_key) != 64:
            raise SocialError("enter the verified node key")
        verified = self.node.verify_saved_contact(contact_code.strip(), node_key)
        return self.remember(node_key, display_name.strip(), verified["contactCode"])

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

    def all(self):
        self.declare()
        cursor = None
        while True:
            params = {"schema": PERSON_SCHEMA, "limit": 64}
            if cursor is not None:
                params["cursor"] = cursor
            page = self.node.call("GET", "/api/list?" + urlencode(params))["list"]
            if page["keys"]:
                yield from self.read_batch([key["hash"] for key in page["keys"]])
            if not page["has_more"]:
                return
            cursor = page.get("next_cursor")
            if not cursor:
                raise SocialError("LastDB omitted the next contact cursor")

    def index(self, person_id, definition=None):
        if definition is None:
            definition = self.node.declare_private(BOOK)
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
        self.node.declare_private(BOOK)
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
