#!/usr/bin/env python3
"""The change call on real nodes, run by lease_proof.py (no entry point of its own).

`POST /api/databases/{id}/change` (owner socket) and the `databases` action `change`
(app socket): the node decides, the app asks nothing. These run after step 4 of
lease_proof.py, while A is a writer under a live lease, B a submit member and C a
reader. 9c runs inside step 4, while A's lease has lapsed.
   9a  a write member under a live lease: applied, in its database at once,
       on the publisher after sync.
   9b  a submit member: pending, not in its database until the publisher
       accepts, then everywhere.
   9c  a write member whose lease has lapsed: pending.
   9d  the publisher, on its own database, over the owner socket: applied
       (a create and a replace).
   9e  a repeat with the same Idempotency-Key. A pending change answers
       pending with the same sequence. The direct path keeps no record of ids:
       a repeat of an applied create is refused 409 by its own condition and
       changes nothing.
   9f  a read member: 403.
"""
import json
import sys
import uuid
from pathlib import Path

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import send_unix_request  # noqa: E402
from record_runs import held, require  # noqa: E402
from shared_database_proof import SCHEMA, wait  # noqa: E402


def change(node, key, note, *, mid=None, mutation_type="create", expected=None, owner=False):
    """Call the change route. Returns the HTTP status, the reply and the Idempotency-Key.

    `owner` uses `POST /api/databases/{id}/change` on the owner socket. Otherwise the
    `databases` action `change` runs on the app socket.
    """
    mid = mid or str(uuid.uuid4())
    document = {"type": "mutation", "schema": SCHEMA, "key_value": {"hash": key}, "alternates": {},
                "mutation_type": mutation_type, "expected": expected or {"type": "absent"},
                "record": {"id": key, "note": note}, "file": None}
    if not owner:
        status, reply = node.post("/v1/x/databases", {
            "action": "change", "database": node.db, "mutation": document, "idempotency_key": mid})
        return status, reply, mid
    status, raw, _, _, _ = send_unix_request(
        node.node.socket, "POST", f"/api/databases/{node.db}/change", document,
        {"Content-Type": "application/json", "Idempotency-Key": mid})
    return status, (json.loads(raw) if raw else {}), mid


def prove_change(pub, a, b, c, pb, members, everywhere, outcomes):
    """Steps 9a, 9b, 9d, 9e and 9f. `pb` is B's relay proxy."""
    wait("A to hold a lease that covers it", lambda: (x := a.lease()) and x["directWrites"] and x)
    before = a.streams()
    status, reply, applied_id = change(a, "c-a", "a-applied")
    require(status == 200 and reply.get("status") == "applied" and reply.get("mutation_id") == applied_id,
            f"a change under a live lease was HTTP {status} {reply!r}")
    require(a.row("c-a"), "A does not hold the record it changed under its lease")
    after = a.streams()
    require(after["direct"] == before["direct"] + 1 and after["proposal"] == before["proposal"],
            f"A's entries {before} then {after}: the change was not one direct entry")
    everywhere(members, "c-a", "a-applied", a.key)
    outcomes["9a_write_member_live_lease"] = "applied, on every node"
    print("9a. write member under a live lease: change answered applied, record at once and on every node", flush=True)

    status, reply, _ = change(a, "c-a", "a-applied", mid=applied_id)
    require(status == 409, f"a repeat of an applied create was HTTP {status} {reply!r}")
    require({k: v for k, v in a.streams().items() if k != "incorporations"} ==
            {k: v for k, v in after.items() if k != "incorporations"}, "a repeat of an applied change signed an entry")
    require(held(a.row("c-a")["record"]).get("note") == "a-applied", "a repeat changed the record")
    outcomes["9e_repeat_applied"] = "409 by the create condition, nothing signed, record unchanged"
    print("9e. repeat of the applied change: 409 (direct path keeps no ids), nothing signed, record unchanged", flush=True)

    pb.block()
    before = b.streams()
    status, reply, pending_id = change(b, "c-b", "b-pending")
    require(status == 202 and reply.get("status") == "pending" and reply.get("mutation_id") == pending_id
            and reply.get("submission_sequence", 0) > 0, f"a submit member change was HTTP {status} {reply!r}")
    sequence = reply["submission_sequence"]
    require(b.row("c-b") is None, "B holds a change before the publisher accepted it")
    require(b.streams()["proposal"] == before["proposal"] + 1 and b.streams()["direct"] == before["direct"],
            "the change was not exactly one proposal")
    status, reply, _ = change(b, "c-b", "b-pending", mid=pending_id)
    require(status == 202 and reply.get("status") == "pending" and reply.get("submission_sequence") == sequence,
            f"a repeat of a pending change was HTTP {status} {reply!r}, expected pending {sequence}")
    require(b.streams()["proposal"] == before["proposal"] + 1, "a repeat of a pending change signed a second proposal")
    require(b.row("c-b") is None, "B holds a change before the publisher accepted it (after the repeat)")
    pb.unblock()
    wait("B's change to be published", lambda: b.status(pending_id) == "published")
    everywhere(members, "c-b", "b-pending", b.key)
    outcomes["9b_submit_member"] = "pending, absent until published, then on every node"
    outcomes["9e_repeat_pending"] = f"pending with the same sequence {sequence}, one proposal"
    print(f"9b. submit member: change answered pending (sequence {sequence}), absent from B until the publisher accepted it", flush=True)
    print("9e. repeat of the pending change: pending with the same sequence, one proposal signed", flush=True)

    status, reply, _ = puchange(b, "c-p", "p-applied", owner=True)
    require(status == 200 and reply.get("status") == "applied", f"the publisher's change was HTTP {status} {reply!r}")
    require(pub.row("c-p"), "the publisher does not hold its own change")
    revision = pub.row("c-p")["revision"]
    status, reply, _ = puchange(b, "c-p", "p-replaced", mutation_type="replace",
                                  expected={"type": "revision", "revision": revision}, owner=True)
    require(status == 200 and reply.get("status") == "applied", f"the publisher's replace was HTTP {status} {reply!r}")
    for member in members:
        wait(f"{member.name} to hold c-p=p-replaced",
             lambda m=member: (row := m.row("c-p")) and held(row["record"]).get("note") == "p-replaced")
    outcomes["9d_publisher_owner_socket"] = "create and replace applied, on every node"
    print("9d. publisher on its own database (owner socket): create and replace answered applied, on every node", flush=True)

    status, reply, _ = change(c, "c-f", "c-no")
    message = (reply.get("error") or {}).get("message")
    require(status == 403 and message == "this member cannot submit this shared mutation",
            f"a read member change was HTTP {status} {reply!r}")
    require(c.row("c-f") is None and pub.row("c-f") is None, "a read member changed a record")
    outcomes["9f_read_member"] = "403, nothing written"
    print("9f. read member: change refused 403, nothing written", flush=True)
