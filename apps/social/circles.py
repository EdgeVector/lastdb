"""Resolve private circle membership into one whole-database sharing spec."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import re

from lastdb import SocialError


SCHEMA = "social-graph/CirclePolicy"
FULL = "all"
DIGEST = re.compile(r"[0-9a-f]{64}\Z")


class GrantReview(SocialError):
    def __init__(self, changes):
        super().__init__("Review grants changed outside Social before updating this database.")
        self.changes = changes


class SavedSharingUpdate(SocialError):
    pass


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
        self.node.declare_private("CirclePolicy")
        previous = self.node.get_record(SCHEMA, database)
        saved = json.loads(previous["fields"]["circles"]) if previous else []
        return {"circles": saved, "direct": {}} if isinstance(saved, list) else saved

    def describe(self, database):
        return self.load(database)["circles"]

    def plan(self, database, definitions, circles, restore=(), *, review=None, direct_change=None):
        if self.sharing.pending_edit(database):
            raise SavedSharingUpdate("A sharing request is waiting for confirmation.")
        policy = self.load(database)
        prior_circles = policy["circles"]
        current = self.sharing.configuration(database)
        before = resolve(prior_circles)
        desired = resolve(circles)
        before.update({key: before.get(key, []) for key in policy["direct"]})
        keys = set(desired)
        if self.people.by_keys(sorted(keys)).keys() != keys:
            raise SocialError("save verified contacts before sharing with them")
        if any(key not in keys for key in restore):
            raise SocialError("choose circle members to restore")
        members = current["members"]
        direct = dict(policy["direct"])
        member_by_key = {row["nodeKey"]: row for row in members}
        if direct_change:
            direct_key = direct_change["nodeKey"]
            if direct_key not in self.people.by_keys([direct_key]):
                raise SocialError("save this person's verified contact before sharing")
            target = member_by_key.get(direct_key)
            restore = apply_direct_intent(direct, direct_change, target, definitions, restore)
        conflicts = outside_changes(members, before, policy, definitions)
        if conflicts:
            choices = review or {}
            if set(choices) != {row["nodeKey"] for row in conflicts}:
                raise GrantReview(conflicts)
            for row in conflicts:
                key = row["nodeKey"]
                if choices[key] == "keepDirect":
                    scope = row["current"].get("writeSchemas")
                    if scope is None and row["current"]["role"] == "submit":
                        raise SocialError("Replace the old contributor grant with explicit schema names.")
                    direct[key] = scope or []
                elif choices[key] != "useSocial":
                    raise SocialError("Choose how to resolve each outside grant.")
        for member in members:
            key = member["nodeKey"]
            if key in desired and key not in before and member["state"] == "active":
                direct[key] = member.get("writeSchemas") or []
        for key in direct:
            desired.setdefault(key, [])
        replacement = merge(members, before, desired, set(restore), direct, definitions)
        if direct_change and direct_change["operation"] == "revokePerson":
            replacement = [revoke_row(target) if row["nodeKey"] == direct_key else row for row in replacement]
        if len(replacement) > 98:
            raise SocialError("This database allows 98 roster entries, including revoked people.")
        next_policy = {"circles": circles, "direct": direct,
                       "confirmed": {row["nodeKey"]: grant_value(row)
                                     for row in replacement if row["nodeKey"] in desired}}
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
    definition = node.declare_private("CirclePolicy")
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


def revoke_row(row):
    return {"nodeKey": row["nodeKey"], "role": row["role"], "state": "revoked", "writeSchemas": []}


def apply_direct_intent(direct, change, target, definitions, restore):
    key, operation = change["nodeKey"], change["operation"]
    if operation == "revokePerson":
        if target is None:
            raise SocialError("this person has no grant on the database")
        direct.pop(key, None)
        return restore
    scope = change["writeSchemas"]
    scope = normalize([{"name": "Direct grant", "members": [key], "writeSchemas": scope}],
                      definitions)[0]["writeSchemas"]
    scope = scope if scope == FULL else named_scope(scope, [])
    if operation == "restorePerson":
        if target and target["state"] == "revoked":
            restore = [*restore, key]
        elif not target or direct.get(key) != scope:
            raise SocialError("choose a person whose access is revoked")
    elif target and target["state"] == "revoked":
        raise SocialError("explicitly restore this person's access")
    direct[key] = scope
    return restore


def grant_value(member):
    scope = member.get("writeSchemas", [] if member["role"] == "read" else None)
    return {"state": member["state"], "role": member["role"], "writeSchemas": scope}


def outside_changes(members, before, policy, definitions):
    confirmed = policy.get("confirmed", {})
    changes = []
    for row in members:
        key = row["nodeKey"]
        if key not in before or row["state"] == "revoked":
            continue
        previous = confirmed.get(key)
        if previous is None:
            scope = combine_scope(before[key], policy["direct"].get(key), definitions)
            previous = {"state": "active", "role": "submit" if scope else "read", "writeSchemas": scope}
        current = grant_value(row)
        if previous != current:
            changes.append({"nodeKey": key, "confirmed": previous, "current": current})
    return changes


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
                members[key] = revoke_row(previous)
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
        return named_scope(circle, direct["names"])
    if direct:
        # A grant made outside Social can list definition IDs. Keep it as written.
        if not circle:
            return direct
        registered = {definition: name for name, definition in definitions.items()}
        if any(registered.get(item) not in circle for item in direct):
            raise SocialError("review the direct grant before combining it with a circle")
    return named_scope(circle, [])


def named_scope(circle, direct):
    names = sorted(set(circle) | set(direct))
    return {"names": names} if names else []
