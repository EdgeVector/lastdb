"""Resolve private circle membership into one whole-database sharing spec."""

from contextlib import contextmanager
import fcntl
import json
import re

from lastdb import SocialError


SCHEMA = "social-graph/CirclePolicy"
DEFINITION = {
    "name": "CirclePolicy", "descriptive_name": "Circle policy",
    "purpose_statement": "Social circles granting access to one named database.",
    "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
    "fields": ["circles"], "field_types": {"circles": "String"},
    "field_descriptions": {"circles": "The circle names, member identities and schema write scopes."},
}
FULL = "all"
DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class Circles:
    def __init__(self, node, people, sharing):
        self.node = node
        self.people = people
        self.sharing = sharing

    def schemas(self, database):
        rows = self.node.call("GET", f"/api/databases/{database}/schemas?include_definition_ids=1")
        return {row["name"]: row["definition_id"] for row in rows["schemas"]
                if row.get("definition_id") and row.get("state") == "Available"}

    def load(self, database):
        self.node.declare_private(DEFINITION)
        previous = self.node.get_record(SCHEMA, database)
        saved = json.loads(previous["fields"]["circles"]) if previous else []
        return {"circles": saved, "direct": {}} if isinstance(saved, list) else saved

    def describe(self, database):
        return self.load(database)["circles"]

    def set(self, database, revision, requests, restore):
        if not isinstance(revision, str):
            raise SocialError("select the sharing revision you reviewed")
        if not isinstance(restore, list):
            raise SocialError("choose circle members to restore")
        definitions = self.schemas(database)
        circles = normalize(requests, definitions)
        plan = self.plan(database, definitions, circles, restore, revision=revision)
        return self.apply(database, plan)

    def plan(self, database, definitions, circles, restore=(), *, revision=None):
        if self.sharing.pending_edit(database):
            raise SocialError("finish the pending sharing change first")
        policy = self.load(database)
        prior_circles = policy["circles"]
        # Edits from the database view affect only its original local circles.
        linked = [circle for circle in prior_circles if "id" in circle]
        local = [circle for circle in prior_circles if "id" not in circle]
        circles = (circles + linked) if revision is not None else (local + circles)
        current = self.sharing.configuration(database)
        if revision is not None and revision != current["revision"]:
            raise SocialError("sharing changed while you edited it; review the current roster")
        before = resolve(prior_circles)
        desired = resolve(circles)
        keys = set(desired)
        if self.people.by_keys(sorted(keys)).keys() != keys:
            raise SocialError("save verified contacts before sharing with them")
        if any(key not in keys for key in restore):
            raise SocialError("choose circle members to restore")
        members = current["members"]
        direct = dict(policy["direct"])
        for member in members:
            key = member["nodeKey"]
            if key in desired and key not in before and member["state"] == "active":
                direct[key] = member.get("writeSchemas") or []
        replacement = merge(members, before, desired, set(restore), direct, definitions)
        next_policy = {"circles": circles, "direct": {key: direct[key] for key in desired if key in direct}}
        return {"revision": current["revision"], "previous": members, "members": replacement,
                "policy": next_policy}

    def apply(self, database, plan):
        revision, policy = plan["revision"], plan["policy"]
        if plan["members"] != plan["previous"]:
            revision = self.sharing.apply_members(database, revision, plan["members"], policy)
        else:
            store_policy(self.node, database, policy)
        return {"revision": revision, "circles": policy["circles"]}

    @contextmanager
    def lock(self):
        with (self.node.home / "social-circle-edit.lock").open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise SocialError("another circle edit is in progress; retry when it finishes") from error
            yield


def store_policy(node, database, circles):
    definition = node.declare_private(DEFINITION)
    previous = node.get_record(SCHEMA, database)
    encoded = json.dumps(circles, separators=(",", ":"))
    if not previous or previous["fields"]["circles"] != encoded:
        node.write_record(SCHEMA, definition, database, {"circles": encoded}, previous)


def normalize(requests, definitions):
    if not isinstance(requests, list) or len(requests) > 50:
        raise SocialError("provide up to 50 circles")
    result = {}
    for circle in requests:
        name, keys, scope = circle["name"], circle["members"], circle["writeSchemas"]
        if not isinstance(name, str) or not (1 <= len(name.strip()) <= 64) or name.strip() in result:
            raise SocialError("circle names must be unique and at most 64 characters")
        if not isinstance(keys, list) or any(not isinstance(key, str) or not DIGEST.fullmatch(key) for key in keys):
            raise SocialError("circle members must be verified node keys")
        if scope != FULL:
            if not isinstance(scope, list) or any(not isinstance(item, str) or item not in definitions for item in scope):
                raise SocialError("select writable schemas installed in this database")
            scope = sorted(set(scope))
        name = name.strip()
        result[name] = {"name": name, "members": sorted(set(keys)), "writeSchemas": scope}
    return list(result.values())


def resolve(circles):
    result = {}
    for circle in circles:
        scope = circle["writeSchemas"]
        for key in circle["members"]:
            previous = result.get(key, [])
            result[key] = FULL if FULL in (scope, previous) else sorted(set(previous) | set(scope))
    return result


def merge(current, before, desired, restore, direct, definitions):
    members = {row["nodeKey"]: row for row in current}
    for key in before.keys() - desired.keys():
        previous = members.get(key)
        if previous and previous["state"] == "active":
            if key in direct:
                scope = direct[key]
                members[key] = {"nodeKey": key, "state": "active", "role": "submit" if scope else "read",
                                "writeSchemas": scope}
            else:
                members[key] = {"nodeKey": key, "state": "revoked", "role": previous["role"], "writeSchemas": []}
    for key, scope in desired.items():
        previous = members.get(key)
        if previous and previous["state"] == "revoked" and key not in restore:
            continue
        scope = combine_scope(scope, direct.get(key), definitions)
        row = {"nodeKey": key, "state": "active", "role": "submit" if scope else "read", "writeSchemas": scope}
        if previous and previous["state"] == "revoked":
            row["regrant"] = True
        members[key] = row
    return [members[key] for key in sorted(members)]


def combine_scope(circle, direct, definitions):
    if FULL in (circle, direct):
        return FULL
    if isinstance(direct, dict):
        direct = direct["names"]
    elif direct:
        registered = {definition: name for name, definition in definitions.items()}
        if not circle:
            return direct
        if any(registered.get(item) not in circle for item in direct):
            raise SocialError("review the direct grant before combining it with a circle")
        direct = []
    names = sorted(set(circle) | set(direct or []))
    return {"names": names} if names else []
