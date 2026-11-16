"""Store reusable circles and apply approved per-database sharing changes."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from http.client import HTTPException
import json
from uuid import UUID

from circles import GrantReview, SavedSharingUpdate, normalize
from lastdb import SocialError, record_mutation
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
        data = json.loads(record["fields"]["directory"]) if record else {"circles": []}
        data.setdefault("pendingDatabases", [])
        data.setdefault("operations", {})
        return definition, record, data

    def save(self, definition, record, data, remove_request=None):
        fields = {"directory": encoded(data)}
        if remove_request is None:
            revision = self.node.write_record(SCHEMA, definition, KEY, fields, record)["record_revision"]
        else:
            update = record_mutation(SCHEMA, definition, {"hash": KEY, "range": None}, fields, record)
            ack = self.node.call("POST", "/api/mutations/batch", {"mutations": [update, remove_request]})
            revision = ack["record_revisions"][ack["mutation_ids"][0]]
        return {"fields": fields, "revision": revision}

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
        return {"circles": data["circles"], "pending": data["pendingDatabases"], "databases": databases}

    def catalog(self, database):
        try:
            return sorted(self.circles.schemas(database))
        except (SocialError, OSError, HTTPException):
            return None

    def change(self, args, *, apply=False, token=None, defer=False):
        """Preview one edit under the lock, and apply that same preview when asked.

        With `defer`, a database whose grants changed outside Social stays pending
        for `social db retry --resolve`, and the other databases are applied.
        """
        with self.circles.lock():
            preview = self.edit_locked(args)
            if not apply:
                return {**preview, "applied": False}
            confirm(preview, token, defer=defer)
            return {**self.edit_locked({**args, "token": preview["token"]}, apply=True), "applied": True}

    def edit_locked(self, args, *, apply=False, operation=None, remove_request=None):
        self.node.require_named_grants()
        definition, record, data = self.load()
        prior, pending = data["circles"], set(data["pendingDatabases"])
        retrying = "circle" not in args and operation is None
        if "circle" in args:
            desired, databases = self.replacement(prior, args["circle"], args.get("remove") is True)
            if operation:
                databases.add(operation["databaseId"])
        elif operation:
            desired, databases = prior, {operation["databaseId"]}
        else:
            desired, databases = prior, pending
            if "databaseId" in args:
                databases = pending.intersection({args["databaseId"]})
            if not databases:
                raise SocialError("there is no pending circle update for the selection")
        if not retrying and pending.intersection(databases):
            raise SocialError("finish the pending sharing update for the affected database before changing it again")

        operations = dict(data["operations"])
        if operation:
            operations[operation["databaseId"]] = {**operation, "status": "pending"}
        review = args.get("review", {})
        plans = self.prepare(desired, databases, operations, review)
        fingerprint = sha256(encoded([record["revision"] if record else None, desired,
                                      plans, operation]).encode()).hexdigest()
        if not apply:
            return self.preview(fingerprint, plans)
        if args.get("token") != fingerprint:
            raise SocialError("sharing changed since the preview; review the changes again")
        if not retrying and any("error" in plan and "conflicts" not in plan for plan in plans):
            raise SocialError("resolve the unavailable databases before applying this edit")
        if operation and any("error" in plan for plan in plans):
            raise SocialError("resolve the outside grants in the preview before approving this request")
        data.update(circles=desired, pendingDatabases=sorted(pending | databases), operations=operations)
        record = self.save(definition, record, data, remove_request)
        if retrying:
            plans = self.replay_pending(data, plans, review)
        return self.finish(definition, record, data, plans)

    def preview(self, fingerprint, plans):
        databases = [impact(plan) for plan in plans]
        node_keys = {entry["nodeKey"] for item in databases for field in ("changes", "conflicts")
                     for entry in item.get(field, [])}
        try:
            people = self.circles.people.by_keys(node_keys)
        except (SocialError, OSError, HTTPException):
            people = {}
        return {"token": fingerprint, "databases": databases,
                "contactNames": {key: person["display_name"] for key, person in people.items()}}

    def resume(self, database=None):
        pending = self.load()[2]["pendingDatabases"]
        if not pending or (database is not None and database not in pending):
            return {"databases": [], "pending": pending}
        with self.circles.lock():
            definition, record, data = self.load()
            pending = data["pendingDatabases"]
            selected = pending if database is None else [item for item in pending if item == database]
            if selected:
                plans = self.prepare(data["circles"], selected, data["operations"])
                return self.finish(definition, record, data, self.replay_pending(data, plans))
            return {"databases": [], "pending": pending}

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
            data["circles"], reopened, data["operations"], review
        )} if reopened else {}
        return [refreshed.get(plan["databaseId"], plan) for plan in plans]

    def finish(self, definition, record, data, plans):
        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(self.deliver, plans))
        pending = set(data["pendingDatabases"])
        operations = data["operations"]
        for outcome in outcomes:
            database = outcome["databaseId"]
            if outcome["status"] == "confirmed":
                pending.discard(database)
            operation = operations.get(database)
            if operation and operation["status"] != "confirmed":
                receipt = {key: outcome[key] for key in ("databaseId", "status", "revision", "error")
                           if key in outcome}
                operations[database] = {"requestId": operation["requestId"], **receipt}
                if outcome["status"] != "confirmed" and "direct" in operation:
                    operations[database]["direct"] = operation["direct"]
        data["pendingDatabases"] = sorted(pending)
        self.save(definition, record, data)
        return {"databases": outcomes, "pending": data["pendingDatabases"]}

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

    def prepare(self, desired, databases, operations, review=None):
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
                change = self.direct_intent(operations.get(database))
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


def confirm(preview, token, *, defer=False):
    """Refuse to apply a preview that moved or that still has outside grants to resolve."""
    if token not in (None, preview["token"]):
        raise SocialError("sharing changed since the preview; review the changes again")
    names = preview.get("contactNames", {})
    unresolved = [f'--resolve {row["name"]}:{names.get(item["nodeKey"], item["nodeKey"])}=keep|social'
                  for row in preview["databases"] for item in row.get("conflicts", [])]
    if unresolved and not defer:
        raise SocialError("grants changed outside Social; rerun with " + " ".join(unresolved))
    blocked = [f'{row["name"]}: {row["error"]}' for row in preview["databases"]
               if "error" in row and not row.get("conflicts")]
    if blocked:
        raise SocialError("; ".join(blocked))


def impact(plan):
    row = {key: plan[key] for key in ("databaseId", "name")}
    if "error" in plan:
        return {**row, **{key: plan[key] for key in ("error", "conflicts") if key in plan}}
    before = {member["nodeKey"]: member for member in plan["previous"]}
    return {**row, "revision": plan["revision"], "changes": [
        {"nodeKey": member["nodeKey"], "before": before.get(member["nodeKey"]), "after": member}
        for member in plan["members"] if member != before.get(member["nodeKey"])
    ]}
