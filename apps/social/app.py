"""Social operations used by the local app and its database picker."""

from concurrent.futures import ThreadPoolExecutor
import re

from lastdb import SocialError
from people import People
from sharing import Sharing, owns_social_database


SELECTOR = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")
NODE_KEY = re.compile(r"[0-9a-f]{64}\Z")


def public_person(person):
    return {"nodeKey": person["node_key"], "name": person["display_name"]}


class SocialApp:
    def __init__(self, node):
        self.node = node
        self.people = People(node)
        self.sharing = Sharing(node, self.people)

    def remember(self, args):
        require_node_key(args["nodeKey"])
        return public_person(self.people.import_contact(args["nodeKey"], args["name"], args["contactCode"]))

    def remember_file(self, node_key, name, path):
        require_node_key(node_key)
        name = checked_name(name)
        verified = self.node.verify_contact(path, node_key)
        return public_person(self.people.remember(node_key, name, verified["contactCode"]))

    def create(self, name):
        return self.sharing.create(checked_name(name))

    def accept(self, receipt, publisher, name):
        require_node_key(publisher)
        if not isinstance(receipt, dict):
            raise SocialError("enter the sealed invitation receipt")
        return self.sharing.accept(receipt, publisher, checked_name(name) if name else None)

    def home(self):
        databases = [row for row in self.sharing.databases() if row.get("spec")]
        contacts = [public_person(person) for person in self.people.address_book()]
        return {"people": sorted(contacts, key=lambda person: person["name"].casefold()),
                "databases": sorted(databases, key=lambda row: (row.get("name") or row["id"]).casefold())}

    def person(self, node_key):
        require_node_key(node_key)
        person = self.people.by_keys([node_key]).get(node_key)
        if person is None:
            raise SocialError("this person is not in the address book")
        databases = self.sharing.databases()
        owned = [row for row in databases if owns_social_database(row)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            configurations = list(pool.map(self.sharing.configuration_row, owned))
        outgoing = []
        for config in configurations:
            if "status" in config:
                outgoing.append({"databaseId": config["databaseId"], "name": config["name"],
                                 "status": config["status"]})
            elif member := next((item for item in config["members"] if item["nodeKey"] == node_key), None):
                outgoing.append({"databaseId": config["databaseId"], "name": config["name"], **member})
        incoming = [row for row in databases if row.get("kind") == "received" and row.get("publisher") == node_key]
        return {"person": public_person(person), "outgoing": outgoing, "incoming": incoming}

    def database(self, database_id):
        row = self.selected(database_id)
        if not owns_social_database(row):
            return {"database": row}
        config = self.sharing.configuration_row(row)
        contacts = self.people.by_keys([item["nodeKey"] for item in config.get("members", [])])
        for member in config.get("members", []):
            person = contacts.get(member["nodeKey"])
            member["name"] = person["display_name"] if person else None
        pending = self.sharing.pending_edit(row["id"])
        return {"database": row, "sharing": config, "pendingEdit": pending}

    def selected(self, database_id):
        if not isinstance(database_id, str) or not SELECTOR.fullmatch(database_id):
            raise SocialError("invalid database identifier")
        for database in self.sharing.databases():
            if database["id"] == database_id:
                return database
        raise SocialError("the database is not registered on this device")

    def picker(self, spec, intent):
        if (not isinstance(spec, dict) or not isinstance(spec.get("name"), str)
                or not SELECTOR.fullmatch(spec["name"]) or type(spec.get("version")) is not int
                or not 1 <= spec["version"] <= 4_294_967_295 or intent not in ("read", "write")):
            raise SocialError("select a database spec and a read or write intent")
        return [{"databaseId": row["id"], "name": row["name"], "spec": row["spec"],
                 "kind": row["kind"], "state": row["state"], "selfAccess": row.get("selfAccess")}
                for row in self.sharing.databases() if row.get("spec") == spec
                and (row["kind"] == "shareable" or (intent == "read" and row["kind"] == "received"
                     and row["state"] == "ready"))]

    def pick(self, spec, intent, database_id):
        if database_id is None:
            return None
        for choice in self.picker(spec, intent):
            if choice["databaseId"] == database_id:
                return {key: choice[key] for key in ("databaseId", "name", "spec")}
        raise SocialError("this database cannot be selected for the requested use")

    def approve_pick(self, spec, intent, database_id):
        selection = self.pick(spec, intent, database_id)
        if selection is None:
            return None
        result = self.node.call("POST", "/api/databases/session", {
            "databaseId": database_id, "intent": intent,
        })
        return {**selection, "databaseSession": result["session"],
                "expiresInSeconds": result["expiresInSeconds"]}

    def invite(self, args):
        key = require_node_key(args["nodeKey"])
        database_id = self.selected(args["databaseId"])["id"]
        person = self.people.by_keys([key]).get(key)
        if person is None:
            raise SocialError("save this person's verified contact first")
        return self.sharing.invite(database_id, person["display_name"], key)

    def revoke(self, args):
        database_id = self.selected(args["databaseId"])["id"]
        self.sharing.owned(database_id)
        return {"revision": self.sharing.update_member(database_id, require_node_key(args["nodeKey"]), False)}

    def retry(self, database_id):
        self.sharing.owned(self.selected(database_id)["id"])
        self.sharing.resume_pending(database_id)
        return self.database(database_id)


def require_node_key(key):
    if not isinstance(key, str) or not NODE_KEY.fullmatch(key):
        raise SocialError("enter the verified LastDB node key")
    return key


def checked_name(name):
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 100:
        raise SocialError("enter a name of 1 to 100 characters")
    return name.strip()
