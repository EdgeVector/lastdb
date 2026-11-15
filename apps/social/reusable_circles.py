"""Store reusable circles and apply approved per-database sharing changes."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from http.client import HTTPException
import json
from uuid import UUID

from circles import GrantReview, SavedSharingUpdate, normalize
from lastdb import SocialError
from sharing import can_manage_sharing


SCHEMA = "social-graph/CircleDirectory"
KEY = "directory"


def encoded(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class ReusableCircles:
    def __init__(self, node, sharing, circles):
        self.node, self.sharing, self.circles = node, sharing, circles

    def load(self):
        definition = self.node.declare_private("CircleDirectory")
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
        return {"circles": data["circles"], "pending": data["pending"], "databases": databases}

    def catalog(self, database):
        try:
            return sorted(self.circles.schemas(database))
        except (SocialError, OSError, HTTPException):
            return None

    def edit(self, args, *, apply=False):
        with self.circles.lock():
            return self.edit_locked(args, apply=apply)

    def edit_locked(self, args, *, apply=False, operation=None):
        if not self.node.has_named_grants():
            raise SocialError("reusable circles need a LastDB build with grants by schema name")
        definition, record, data = self.load()
        prior, pending = data["circles"], data["pending"]
        if pending and ("circle" in args or operation):
            raise SocialError("finish the pending circle update before starting another")
        if "circle" in args:
            desired, databases = self.replacement(prior, args["circle"], args.get("remove") is True)
            if operation:
                databases.add(operation["databaseId"])
        elif operation:
            desired, databases = prior, [operation["databaseId"]]
        else:
            if not pending:
                raise SocialError("there is no pending circle update")
            desired, databases = prior, pending
        active = operation or data.get("operation")
        plans = self.prepare(desired, databases, args.get("review", {}), self.direct_intent(active))
        fingerprint = sha256(encoded([record["revision"] if record else None, desired,
                                      plans, operation]).encode()).hexdigest()
        if not apply:
            return {"token": fingerprint, "databases": [impact(plan) for plan in plans]}
        if args.get("token") != fingerprint:
            raise SocialError("sharing changed since the preview; review the changes again")
        if not pending and any("error" in plan and "conflicts" not in plan for plan in plans):
            raise SocialError("resolve the unavailable databases before applying this edit")
        if operation:
            if any("error" in plan for plan in plans):
                raise SocialError("resolve the outside grants in the preview before approving this request")
            data["operation"] = {**operation, "status": "pending"}
        data.update(circles=desired, pending=sorted(databases))
        record = self.save(definition, record, data)
        if pending:
            plans = self.replay_pending(data, plans, args.get("review", {}))
        return self.finish(definition, record, data, plans)

    def resume(self):
        if not self.load()[2]["pending"]:
            return {"databases": [], "pending": []}
        with self.circles.lock():
            definition, record, data = self.load()
            if data["pending"]:
                plans = self.prepare(data["circles"], data["pending"],
                                     direct=self.direct_intent(data.get("operation")))
                return self.finish(definition, record, data, self.replay_pending(data, plans))
            return {"databases": [], "pending": []}

    @staticmethod
    def direct_intent(operation):
        return operation.get("direct") if operation and operation.get("status") != "confirmed" else None

    def replay_pending(self, data, plans, review=None):
        saved = [plan["databaseId"] for plan in plans if plan.get("savedRequest")]
        if not saved:
            return plans

        def resume(database):
            try:
                self.sharing.resume_pending(database)
            except (SocialError, OSError, HTTPException):
                return None
            return database

        with ThreadPoolExecutor(max_workers=4) as pool:
            reopened = [database for database in pool.map(resume, saved) if database]
        # A saved request can belong to another action in Social. Confirm the
        # approved circle or direct declaration against the resulting roster.
        refreshed = {plan["databaseId"]: plan for plan in self.prepare(
            data["circles"], reopened, review, self.direct_intent(data.get("operation"))
        )} if reopened else {}
        return [refreshed.get(plan["databaseId"], plan) for plan in plans]

    def finish(self, definition, record, data, plans):
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(self.deliver, plans))
        data["pending"] = [row["databaseId"] for row in outcomes if "error" in row]
        operation = data.get("operation")
        if operation and operation["status"] != "confirmed":
            outcome = next((row for row in outcomes if row["databaseId"] == operation["databaseId"]), None)
            if outcome:
                direct = self.direct_intent(operation) if outcome["status"] != "confirmed" else None
                receipt = {key: outcome[key] for key in ("databaseId", "status", "revision", "error")
                           if key in outcome}
                data["operation"] = {"requestId": operation["requestId"], "direct": direct, **receipt}
        self.save(definition, record, data)
        return {"databases": outcomes, "pending": data["pending"]}

    def replacement(self, prior, change, remove):
        identity = change["id"]
        if not isinstance(identity, str) or str(UUID(identity)) != identity:
            raise SocialError("select a circle by its ID")
        previous = next((row for row in prior if row["id"] == identity), None)
        desired = [row for row in prior if row["id"] != identity]
        old_links = previous["links"] if previous else {}
        if remove:
            if not previous:
                raise SocialError("this circle no longer exists")
            return desired, set(old_links)
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
        databases = old_links.keys() | links.keys()
        if previous and all(row[field] == previous[field] for field in ("name", "members")):
            databases = {database for database in databases if old_links.get(database) != links.get(database)}
        return sorted(desired, key=lambda item: item["id"]), databases

    def prepare(self, desired, databases, review=None, direct=None):
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
                change = direct if direct and direct["databaseId"] == database else None
                plan = self.circles.plan(database, definitions, linked, review=(review or {}).get(database),
                                         direct_change=change)
                return {**item, **plan}
            except GrantReview as conflict:
                return {**item, "error": str(conflict), "conflicts": conflict.changes}
            except SavedSharingUpdate as error:
                return {**item, "error": str(error), "savedRequest": True}
            except (SocialError, OSError, HTTPException) as error:
                return {**item, "error": str(error)}

        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(plan, sorted(databases)))

    def deliver(self, plan):
        row = {key: plan[key] for key in ("databaseId", "name")}
        if "conflicts" in plan:
            return {**row, "error": plan["error"], "conflicts": plan["conflicts"], "status": "needsReview"}
        if "error" in plan:
            return {**row, "error": plan["error"], "status": "pending"}
        try:
            result = self.circles.apply(row["databaseId"], plan)
            return {**row, "revision": result["revision"], "status": "confirmed"}
        except (SocialError, OSError, HTTPException) as error:
            return {**row, "error": str(error), "status": "pending"}


def impact(plan):
    row = {key: plan[key] for key in ("databaseId", "name")}
    if "error" in plan:
        return {**row, **{key: plan[key] for key in ("error", "conflicts") if key in plan}}
    before = {member["nodeKey"]: member for member in plan["previous"]}
    return {**row, "revision": plan["revision"], "changes": [
        {"nodeKey": member["nodeKey"], "before": before.get(member["nodeKey"]), "after": member}
        for member in plan["members"] if member != before.get(member["nodeKey"])
    ]}
