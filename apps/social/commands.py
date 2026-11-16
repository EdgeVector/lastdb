"""Map each `social NOUN VERB` command to its owner operation."""

import json
import sys

import changes
from lastdb import SocialError
from sharing import read_invitation


def person_add(app, args):
    return app.remember_file(args.node_key, args.name, args.contact)


def person_list(app, _args):
    return app.people_list()


def person_show(app, args):
    return app.person(app.resolve_people([args.person])[0])


def person_rename(app, args):
    return app.rename_person(app.resolve_people([args.person])[0], args.name)


def person_remove(app, args):
    return app.remove_person(app.resolve_people([args.person])[0])


def circle_list(app, _args):
    return app.reusable_circles.load()[2]["circles"]


def circle_show(app, args):
    return app.circle_view(args.circle)


def circle_set(app, args):
    members = None if args.members is None else [item for item in args.members.split(",") if item]
    return changes.circle_set(app, args.circle, members=members, links=args.link, unlink=args.unlink,
                              rename=args.rename, defer=args.defer_conflicts, **change_options(app, args))


def circle_remove(app, args):
    return changes.circle_remove(app, args.circle, defer=args.defer_conflicts, **change_options(app, args))


def db_create(app, args):
    if args.setup:
        return app.setup_database({"setupName": args.setup, "requiredVersion": args.version,
                                   "localName": args.name})
    return app.create(args.name, whole=not args.filtered)


def db_verify(app, args):
    return app.setup_database({"setupName": args.setup, "requiredVersion": args.version,
                               "databaseId": args.database, "finishExisting": args.finish})


def db_list(app, _args):
    return app.home()["databases"]


def db_show(app, args):
    return app.database(args.database)


def db_accept(app, args):
    if args.receipt in (None, "-"):
        receipt = read_invitation(sys.stdin.buffer)
    else:
        with open(args.receipt, "rb") as stream:
            receipt = read_invitation(stream)
    return app.accept(receipt, args.publisher, args.name)


def db_retry(app, args):
    return changes.retry(app, args.database, changes.parse_review(app, args.resolve))


def grant_list(app, args):
    return app.grants(args.database)


def grant_revoke(app, args):
    return changes.grant_revoke(app, args.database, args.person, apply=args.apply)


def grant_set(app, args):
    return changes.grant_direct(app, "setPersonGrant", args.database, args.person, args.write,
                                **change_options(app, args))


def grant_restore(app, args):
    return changes.grant_direct(app, "restorePerson", args.database, args.person, args.write,
                                **change_options(app, args))


def invite(app, args):
    return changes.invite(app, args.database, args.person, name=args.name, contact=args.contact, out=args.out)


def request_submit(app, _args):
    raw = sys.stdin.buffer.read(16385)
    if not 0 < len(raw) <= 16384:
        raise SocialError("provide a sharing declaration within 16 KiB")
    request = json.loads(raw)
    if not isinstance(request, dict):
        raise SocialError("provide a sharing declaration")
    return app.sharing_requests.submit(request)


def request_status(app, args):
    return app.sharing_requests.status(args.request_id)


def request_list(app, args):
    return app.sharing_requests.awaiting(args.cursor)


def request_show(app, args):
    return app.sharing_requests.preview(args.request_id, changes.parse_review(app, args.resolve))


def request_approve(app, args):
    return app.sharing_requests.approve(args.request_id, changes.parse_review(app, args.resolve), args.token)


def request_decline(app, args):
    return app.sharing_requests.decline(args.request_id)


def pick_database(app, args):
    spec = {"name": args.spec, "version": args.version}
    if args.session:
        if not args.database:
            raise SocialError("--session requires an explicit database selection")
        return app.approve_pick(spec, args.intent, args.database)
    return app.pick(spec, args.intent, args.database) if args.database else app.picker(spec, args.intent)


def check(app, _args):
    app.node.require_sessions()
    return {"ready": True}


def change_options(app, args):
    return {"review": changes.parse_review(app, args.resolve), "apply": args.apply, "token": args.token}


HANDLERS = {
    "person add": person_add, "person list": person_list, "person show": person_show,
    "person rename": person_rename, "person remove": person_remove,
    "circle list": circle_list, "circle show": circle_show, "circle set": circle_set,
    "circle remove": circle_remove,
    "db create": db_create, "db verify": db_verify, "db list": db_list, "db show": db_show,
    "db accept": db_accept, "db retry": db_retry,
    "grant list": grant_list, "grant set": grant_set, "grant restore": grant_restore,
    "grant revoke": grant_revoke,
    "invite": invite,
    "request submit": request_submit, "request status": request_status, "request list": request_list,
    "request show": request_show, "request approve": request_approve, "request decline": request_decline,
    "pick-database": pick_database, "check": check,
}
