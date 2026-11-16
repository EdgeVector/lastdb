"""Owner changes to sharing. Each one previews first and applies only on request."""

import json
import os
from uuid import NAMESPACE_URL, uuid5

from lastdb import SocialError


# A new circle's ID comes from its name, so a preview and its apply agree.
CIRCLE_IDS = uuid5(NAMESPACE_URL, "social-graph/circle")
RESOLUTIONS = {"keep": "keepDirect", "social": "useSocial"}


def same_circle(row, selector):
    return selector == row["id"] or selector.casefold() == row["name"].casefold()


def new_circle_id(name, taken):
    """Derive an unused ID from the name. A renamed circle keeps the ID its old name gave it."""
    attempt = 0
    while True:
        identity = str(uuid5(CIRCLE_IDS, f"{name.casefold()}#{attempt}"))
        if identity not in taken:
            return identity
        attempt += 1


def parse_scope(text):
    """Read `read`, `all`, or a comma-separated list of canonical schema names."""
    if text == "read":
        return []
    if text == "all":
        return "all"
    names = sorted({name.strip() for name in text.split(",") if name.strip()})
    if not names:
        raise SocialError("write a scope as read, all, or schema names such as social/Comment")
    return names


def parse_review(app, entries):
    """Turn `DATABASE:PERSON=keep|social` choices into a review keyed by database ID."""
    parsed = []
    for entry in entries or []:
        target, _, choice = entry.rpartition("=")
        database, _, person = target.partition(":")
        if choice not in RESOLUTIONS or not database or not person:
            raise SocialError("write --resolve as DATABASE:PERSON=keep or DATABASE:PERSON=social")
        parsed.append((database, person, RESOLUTIONS[choice]))
    if not parsed:
        return {}
    rows = app.sharing.databases()
    keys = app.resolve_people([person for _, person, _ in parsed], known=False)
    review = {}
    for (database, _, choice), key in zip(parsed, keys):
        review.setdefault(app.selected(database, rows)["id"], {})[key] = choice
    return review


def circle_set(app, name, *, members=None, links=(), unlink=(), rename=None, review=None,
               apply=False, token=None, defer=False):
    """Create a circle or replace the named parts of an existing one."""
    circles = app.reusable_circles.load()[2]["circles"]
    current = next((row for row in circles if same_circle(row, name)), None)
    rows = app.sharing.databases()
    scopes = dict(current["links"]) if current else {}
    for link in links:
        database, separator, scope = link.partition("=")
        if not separator:
            raise SocialError("write --link as DATABASE=read, DATABASE=all, or DATABASE=Schema,Schema")
        scopes[app.owned_whole(database, rows)["id"]] = parse_scope(scope)
    for database in unlink:
        scopes.pop(database if database in scopes else app.selected(database, rows)["id"], None)
    circle = {
        "id": current["id"] if current else new_circle_id(name, {row["id"] for row in circles}),
        "name": rename or (current["name"] if current else name),
        "members": (app.resolve_people(members) if members is not None
                    else current["members"] if current else []),
        "links": scopes,
    }
    result = app.reusable_circles.change({"circle": circle, "review": review}, apply=apply, token=token,
                                         defer=defer)
    return {**result, "circle": circle}


def circle_remove(app, selector, *, review=None, apply=False, token=None, defer=False):
    circle = app.circle(selector)
    result = app.reusable_circles.change({"circle": circle, "remove": True, "review": review},
                                         apply=apply, token=token, defer=defer)
    return {**result, "circle": circle}


def grant_revoke(app, database, person, *, apply=False):
    """Revoke one person on an owned database of either sharing format."""
    database = app.sharing.owned(database)
    key = app.resolve_people([person], known=False)[0]
    if not apply:
        app.sharing.resume_pending(database["id"])
        members = app.sharing.configuration(database["id"])["members"]
        member = next((row for row in members if row["nodeKey"] == key), None)
        if member is None:
            raise SocialError("the recipient has never been granted access to this database")
        return {"applied": False, "databaseId": database["id"], "name": database["name"], "nodeKey": key,
                "before": member, "after": {**member, "state": "revoked"}}
    revision = app.sharing.update_member(database["id"], key, False)
    return {"applied": True, "databaseId": database["id"], "revision": revision, "nodeKey": key,
            "state": "revoked"}


def grant_direct(app, operation, database, person, scope, *, review=None, apply=False, token=None):
    """Set or restore one person's own grant on an owned whole-database share."""
    change = {"operation": operation, "databaseId": app.owned_whole(database)["id"],
              "nodeKey": app.resolve_people([person])[0], "writeSchemas": parse_scope(scope)}
    return app.sharing_requests.owner_change(change, review, apply=apply, token=token)


def retry(app, database=None, review=None):
    """Resume saved sharing updates for one database, or for every database that has one."""
    database_id = app.sharing.owned(database)["id"] if database else None
    reusable = app.reusable_circles
    with app.circles.lock():
        if database_id:
            app.sharing.resume_pending(database_id)
        pending = reusable.load()[2]["pendingDatabases"]
        if not [item for item in pending if database_id in (None, item)]:
            return {"applied": True, "databases": [], "pending": pending}
        args = {"review": review, **({"databaseId": database_id} if database_id else {})}
        preview = reusable.edit_locked(args)
        return {**reusable.edit_locked({**args, "token": preview["token"]}, apply=True), "applied": True}


def invite(app, database, person, *, name=None, contact=None, out=None):
    """Seal an invitation. On a filtered share this also grants reader access."""
    receipt = seal(app, database, person, name, contact)
    if out is None:
        return receipt
    # The receipt lets its recipient open the database. Keep it readable by the owner only.
    descriptor = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        os.fchmod(descriptor, 0o600)
        json.dump(receipt, stream, sort_keys=True)
    return {"databaseId": receipt["databaseId"], "revision": receipt["revision"],
            "publisher": receipt["publisher"], "out": str(out)}


def seal(app, database, person, name, contact):
    database = app.sharing.owned(database)
    if contact is not None:
        if name is None:
            raise SocialError("give --name with --contact")
        key = app.remember_file(person, name, contact)["nodeKey"]
    else:
        key = app.resolve_people([person])[0]
    if database.get("sharingMode") == "whole_database":
        return app.seal(database, key)
    saved = app.people.by_keys([key])[key]
    return app.sharing.invite(database["id"], name or saved["display_name"], key)
