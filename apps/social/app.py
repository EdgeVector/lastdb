"""Social's owner operations on one LastDB node."""

from concurrent.futures import ThreadPoolExecutor
import re

from lastdb import SocialError
from circles import Circles
from people import People
from reusable_circles import ReusableCircles
from sharing_requests import SharingRequests
from setups import Setups
from sharing import Sharing, can_manage_sharing


NODE_KEY = re.compile(r"[0-9a-f]{64}\Z")


def public_person(person):
    return {"nodeKey": person["node_key"], "name": person["display_name"]}


def can_submit_received(row):
    access = row.get("selfAccess") or {}
    return (row.get("sharingMode") == "whole_database" and access.get("state") == "active"
            and access.get("stale") is False)


class SocialApp:
    def __init__(self, node):
        self.node = node
        self.people = People(node)
        self.sharing = Sharing(node, self.people)
        self.circles = Circles(node, self.people, self.sharing)
        self.reusable_circles = ReusableCircles(node, self.sharing, self.circles)
        self.sharing_requests = SharingRequests(node, self.reusable_circles)
        self.setups = Setups(node, self.sharing)

    def remember_file(self, node_key, name, path):
        require_node_key(node_key)
        name = checked_name(name)
        verified = self.node.verify_contact(path, node_key)
        return public_person(self.people.remember(node_key, name, verified["contactCode"]))

    def rename_person(self, node_key, name):
        person = self.people.by_keys([node_key])[node_key]
        return public_person(self.people.remember(node_key, checked_name(name), person["contact_code"]))

    def remove_person(self, node_key):
        """Remove a contact that no circle lists and no owned database grants access to."""
        person = self.people.by_keys([node_key])[node_key]
        circles = [row["name"] for row in self.reusable_circles.load()[2]["circles"] if node_key in row["members"]]
        if circles:
            raise SocialError(f"remove this person from these circles first: {', '.join(circles)}")
        configs = self.sharing.all_configurations(None)
        unknown = [config["name"] for config in configs if "status" in config]
        if unknown:
            raise SocialError(f"could not confirm the roster of: {', '.join(unknown)}")
        active = [config["name"] for config in configs for member in config["members"]
                  if member["nodeKey"] == node_key and member["state"] == "active"]
        if active:
            raise SocialError(f"revoke this person's access first on: {', '.join(active)}")
        self.people.remove(node_key)
        return {**public_person(person), "removed": True}

    def people_list(self):
        contacts = [public_person(person) for person in self.people.address_book()]
        return sorted(contacts, key=lambda person: person["name"].casefold())

    def resolve_people(self, selectors, *, known=True):
        """Return the node key for each display name or node key, in order."""
        named = {item for item in selectors if not NODE_KEY.fullmatch(item)}
        by_name = {}
        if named:
            for person in self.people.address_book():
                by_name.setdefault(person["display_name"], []).append(person["node_key"])
        keys = []
        for item in selectors:
            matches = by_name.get(item, []) if item in named else [item]
            if len(matches) != 1:
                raise SocialError(f"expected one saved contact named {item}; use the node key")
            keys.append(matches[0])
        direct = sorted(set(selectors) - named)
        if known and direct and self.people.by_keys(direct).keys() != set(direct):
            raise SocialError("save this person's verified contact first")
        return keys

    def create(self, name, whole=True):
        return self.sharing.create(checked_name(name), whole=whole)

    def setup_database(self, args):
        return self.setups.open(args)

    def accept(self, receipt, publisher, name):
        require_node_key(publisher)
        if not isinstance(receipt, dict):
            raise SocialError("enter the sealed invitation receipt")
        return self.sharing.accept(receipt, publisher, checked_name(name) if name else None)

    def home(self):
        databases = [row for row in self.sharing.databases() if row.get("spec")]
        return {"people": self.people_list(),
                "databases": sorted(databases, key=lambda row: (row.get("name") or row["id"]).casefold())}

    def person(self, node_key):
        person = self.people.by_keys([node_key])[node_key]
        databases = self.sharing.databases()
        owned = [row for row in databases if can_manage_sharing(row)]
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

    def database(self, selector):
        row = self.selected(selector)
        if not can_manage_sharing(row):
            return {"database": row}
        config = self.sharing.configuration_row(row)
        members = config.get("members", [])
        contacts = self.people.by_keys([item["nodeKey"] for item in members])
        result = {"database": row, "sharing": config, "pendingEdit": self.sharing.pending_edit(row["id"])}
        circles = []
        if row.get("sharingMode") == "whole_database":
            circles = self.circles.describe(row["id"])
            result.update(circles=circles, schemas=self.circles.schemas(row["id"]))
        for member in members:
            person = contacts.get(member["nodeKey"])
            member["name"] = person["display_name"] if person else None
            member["circles"] = [circle["name"] for circle in circles if member["nodeKey"] in circle["members"]]
        return result

    def grants(self, selector=None):
        configs = self.sharing.all_configurations(selector)
        keys = [row["nodeKey"] for config in configs for row in config.get("members", [])]
        contacts = self.people.by_keys(keys)
        rows = []
        for config in configs:
            database = {"database": config["name"], "databaseId": config["databaseId"]}
            if "status" in config:
                rows.append({**database, "status": config["status"]})
                continue
            for member in config["members"]:
                if member["state"] == "active":
                    key = member["nodeKey"]
                    rows.append({**database, **member, "user": contacts.get(key, {}).get("display_name", key)})
        return rows

    def circle(self, selector):
        circles = self.reusable_circles.load()[2]["circles"]
        matches = [row for row in circles
                   if selector == row["id"] or selector.casefold() == row["name"].casefold()]
        if len(matches) != 1:
            raise SocialError(f"there is no circle named {selector}")
        return matches[0]

    def circle_view(self, selector):
        circle = self.circle(selector)
        pending = self.reusable_circles.load()[2]["pendingDatabases"]
        owned = self.reusable_circles.owned()
        contacts = self.people.by_keys(circle["members"])
        return {
            "id": circle["id"], "name": circle["name"],
            "members": [{"nodeKey": key, "name": contacts.get(key, {}).get("display_name")}
                        for key in circle["members"]],
            "links": [{"databaseId": database, "name": owned.get(database, {}).get("name"),
                       "writeSchemas": scope} for database, scope in sorted(circle["links"].items())],
            "pending": [database for database in pending if database in circle["links"]],
        }

    def seal(self, database, node_key):
        """Seal an invitation for an active grant on an owned whole-database share."""
        person = self.people.by_keys([node_key]).get(node_key)
        if person is None:
            raise SocialError("save this person's verified contact first")
        config = self.sharing.configuration(database["id"])
        if not any(row["nodeKey"] == node_key and row["state"] == "active" for row in config["members"]):
            raise SocialError("grant access with a circle or `social grant set` before sealing an invitation")
        return self.sharing.seal(database["id"], node_key, person["contact_code"], config["revision"],
                                 self.node.own_key())

    def selected(self, selector, rows=None):
        rows = self.sharing.databases() if rows is None else rows
        matches = [row for row in rows if selector in (row["id"], row.get("name"))]
        if len(matches) != 1:
            raise SocialError(f"{selector} is not one database registered on this device")
        return matches[0]

    def owned_whole(self, selector, rows=None):
        database = self.selected(selector, rows)
        if database.get("sharingMode") != "whole_database" or not can_manage_sharing(database):
            raise SocialError(f"{selector} needs an owned whole-database share")
        return database

    def picker(self, spec, intent):
        if (not isinstance(spec, dict) or not isinstance(spec.get("name"), str)
                or type(spec.get("version")) is not int
                or not 1 <= spec["version"] <= 4_294_967_295 or intent not in ("read", "write")):
            raise SocialError("select a database spec and a read or write intent")
        return [{"databaseId": row["id"], "name": row["name"], "spec": row["spec"],
                 "kind": row["kind"], "state": row["state"], "selfAccess": row.get("selfAccess")}
                for row in self.sharing.databases() if row.get("spec") == spec
                and (row["kind"] == "shareable" or (row["kind"] == "received"
                     and row["state"] == "ready" and (intent == "read" or can_submit_received(row))))]

    def pick(self, spec, intent, database_id):
        for choice in self.picker(spec, intent):
            if choice["databaseId"] == database_id:
                return {key: choice[key] for key in ("databaseId", "name", "spec")}
        raise SocialError("this database cannot be selected for the requested use")

    def approve_pick(self, spec, intent, database_id):
        selection = self.pick(spec, intent, database_id)
        result = self.node.call("POST", "/api/databases/session", {
            "databaseId": database_id, "intent": intent,
        })
        return {**selection, "databaseSession": result["session"],
                "expiresInSeconds": result["expiresInSeconds"]}


def require_node_key(key):
    if not isinstance(key, str) or not NODE_KEY.fullmatch(key):
        raise SocialError("enter the verified LastDB node key")
    return key


def checked_name(name):
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 100:
        raise SocialError("enter a name of 1 to 100 characters")
    return name.strip()
