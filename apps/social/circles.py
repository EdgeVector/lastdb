"""Resolve private circle membership into one whole-database sharing spec."""

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
                if row.get("definition_id")}

    def load(self, database):
        definition = self.node.declare_private(DEFINITION)
        previous = self.node.get_record(SCHEMA, database)
        circles = json.loads(previous["fields"]["circles"]) if previous else []
        return definition, previous, circles

    def describe(self, database):
        _, _, circles = self.load(database)
        return circles

    def set(self, database, revision, requests, restore):
        if self.sharing.pending_edit(database):
            raise SocialError("finish the pending sharing change first")
        _, _, prior_circles = self.load(database)
        current = self.sharing.configuration(database)
        if revision != current["revision"]:
            raise SocialError("sharing changed while you edited it; review the current roster")
        circles = normalize(requests, self.schemas(database))
        before = resolve(prior_circles)
        desired = resolve(circles)
        keys = set(desired)
        if self.people.by_keys(sorted(keys)).keys() != keys:
            raise SocialError("save verified contacts before sharing with them")
        if not isinstance(restore, list) or any(key not in keys for key in restore):
            raise SocialError("choose circle members to restore")
        members = current["members"]
        replacement = merge(members, before, desired, set(restore))
        if replacement != members:
            revision = self.sharing.apply_members(database, revision, replacement, circles)
        else:
            store_policy(self.node, database, circles)
        return {"revision": revision, "circles": circles}


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
        result[name] = {"name": name, "members": sorted(set(keys)), "writeSchemas": scope,
                        "definitions": FULL if scope == FULL else sorted({definitions[item] for item in scope})}
    return list(result.values())


def resolve(circles):
    result = {}
    for circle in circles:
        scope = circle["definitions"]
        for key in circle["members"]:
            previous = result.get(key, [])
            result[key] = FULL if FULL in (scope, previous) else sorted(set(previous) | set(scope))
    return result


def merge(current, before, desired, restore):
    members = {row["nodeKey"]: row for row in current}
    for key in before.keys() - desired.keys():
        previous = members.get(key)
        if previous and previous["state"] == "active":
            members[key] = {"nodeKey": key, "state": "revoked", "role": previous["role"], "writeSchemas": []}
    for key, scope in desired.items():
        previous = members.get(key)
        if previous and previous["state"] == "revoked" and key not in restore:
            continue
        row = {"nodeKey": key, "state": "active", "role": "submit" if scope else "read", "writeSchemas": scope}
        if previous and previous["state"] == "revoked":
            row["regrant"] = True
        members[key] = row
    return [members[key] for key in sorted(members)]
