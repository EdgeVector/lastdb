"""Store reusable circles and apply approved per-database sharing changes."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from http.client import HTTPException
import json
from uuid import UUID

from circles import normalize
from lastdb import SocialError
from sharing import can_manage_sharing


SCHEMA = "social-graph/CircleDirectory"
KEY = "directory"
DEFINITION = {
    "name": "CircleDirectory", "descriptive_name": "Circle directory",
    "purpose_statement": "Private reusable circles and their unfinished database updates.",
    "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
    "fields": ["directory"], "field_types": {"directory": "String"},
    "field_descriptions": {"directory": "Circle identities, members, linked database scopes and pending updates."},
}


def encoded(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class ReusableCircles:
    def __init__(self, node, sharing, circles):
        self.node, self.sharing, self.circles = node, sharing, circles

    def load(self):
        definition = self.node.declare_private(DEFINITION)
        record = self.node.get_record(SCHEMA, KEY)
        data = json.loads(record["fields"]["directory"]) if record else {"circles": [], "pending": []}
        return definition, record, data

    def save(self, definition, record, data):
        fields = {"directory": encoded(data)}
        ack = self.node.write_record(SCHEMA, definition, KEY, fields, record)
        return {"fields": fields, "revision": ack["record_revision"]}

    def owned(self):
        return {row["id"]: row for row in self.sharing.databases()
                if row.get("sharingMode") == "whole_database" and can_manage_sharing(row)}

    def overview(self):
        _, _, data = self.load()
        owned = self.owned()
        with ThreadPoolExecutor(max_workers=4) as pool:
            catalogs = list(pool.map(self.catalog, owned))
        databases = [{"id": key, "name": row["name"], "schemas": names}
                     for (key, row), names in zip(owned.items(), catalogs)]
        return {**data, "databases": databases}

    def catalog(self, database):
        try:
            return sorted(self.circles.schemas(database))
        except (SocialError, OSError, HTTPException):
            return None

    def edit(self, args, *, apply=False):
        with self.circles.lock():
            definition, record, data = self.load()
            prior = data["circles"]
            pending = data["pending"]
            if pending and "circle" in args:
                raise SocialError("finish the pending circle update before starting another")
            if "circle" in args:
                desired, databases = self.replacement(prior, args["circle"], args.get("remove") is True)
            else:
                if not pending:
                    raise SocialError("there is no pending circle update")
                desired, databases = prior, pending
            plans = self.prepare(desired, databases)
            fingerprint = sha256(encoded([record["revision"] if record else None,
                                          desired, plans]).encode()).hexdigest()
            result = {"token": fingerprint, "databases": [impact(plan) for plan in plans]}
            if not apply:
                return result
            if args.get("token") != fingerprint:
                raise SocialError("sharing changed since the preview; review the changes again")
            if not pending and any("error" in plan for plan in plans):
                raise SocialError("resolve the unavailable databases before applying this edit")
            data = {"circles": desired, "pending": sorted(databases)}
            record = self.save(definition, record, data)
            with ThreadPoolExecutor(max_workers=4) as pool:
                outcomes = list(pool.map(self.deliver, plans))
            data["pending"] = [row["databaseId"] for row in outcomes if "error" in row]
            self.save(definition, record, data)
            return {"databases": outcomes, "pending": data["pending"]}

    def replacement(self, prior, change, remove):
        identity = change["id"]
        if not isinstance(identity, str) or str(UUID(identity)) != identity:
            raise SocialError("select a circle by its ID")
        previous = next((row for row in prior if row["id"] == identity), None)
        desired = [row for row in prior if row["id"] != identity]
        databases = set(previous["links"]) if previous else set()
        if remove:
            if not previous:
                raise SocialError("this circle no longer exists")
            return desired, databases
        row = normalize([{**change, "writeSchemas": []}], {})[0]
        if any(other["name"].casefold() == row["name"].casefold() for other in desired):
            raise SocialError("use a unique circle name")
        keys = set(row["members"])
        if self.circles.people.by_keys(sorted(keys)).keys() != keys:
            raise SocialError("save verified contacts before adding circle members")
        if not isinstance(change["links"], dict):
            raise SocialError("select the databases linked to this circle")
        links = {}
        for database, scope in change["links"].items():
            if scope != "all":
                if not isinstance(scope, list) or any(not isinstance(name, str) for name in scope):
                    raise SocialError("choose schema names for the linked database")
                scope = sorted(set(scope))
            links[database] = scope
        desired.append({"id": identity, "name": row["name"], "members": row["members"], "links": links})
        return sorted(desired, key=lambda item: item["id"]), databases | links.keys()

    def prepare(self, desired, databases):
        owned = self.owned()

        def plan(database):
            row = owned.get(database)
            item = {"databaseId": database, "name": row["name"] if row else database}
            if not row:
                return {**item, "error": "This database is unavailable or you no longer own it."}
            try:
                definitions = self.circles.schemas(database)
                linked = [
                    {**normalize([{**circle, "writeSchemas": circle["links"][database]}], definitions)[0],
                     "id": circle["id"]} for circle in desired if database in circle["links"]
                ]
                plan = self.circles.plan(database, definitions, linked)
                return {**item, **plan}
            except (SocialError, OSError, HTTPException) as error:
                return {**item, "error": str(error)}

        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(plan, sorted(databases)))

    def deliver(self, plan):
        row = {key: plan[key] for key in ("databaseId", "name")}
        if "error" in plan:
            return {**row, "error": plan["error"]}
        try:
            result = self.circles.apply(row["databaseId"], plan)
            return {**row, "revision": result["revision"]}
        except (SocialError, OSError, HTTPException) as error:
            return {**row, "error": str(error)}


def impact(plan):
    row = {key: plan[key] for key in ("databaseId", "name")}
    if "error" in plan:
        return {**row, "error": plan["error"]}
    before = {member["nodeKey"]: member for member in plan["previous"]}
    return {**row, "revision": plan["revision"], "changes": [
        {"nodeKey": member["nodeKey"], "before": before.get(member["nodeKey"]), "after": member}
        for member in plan["members"] if member != before.get(member["nodeKey"])
    ]}
