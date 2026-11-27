#!/usr/bin/env python3
"""Write leases across real nodes and the reference relay.

One publisher and three members run as real `lastdbd` processes. A and B go
through a small proxy that can cut them off from the relay, so a member can
write while it is offline. Leases are 60 seconds long (renewed at 30 seconds, a
margin of 6 seconds, closable 30 seconds after expiry). No clock is changed.

1. With no lease length set, a write to a received database is refused 403 and
   no lease exists: the database behaves as before.
2. The publisher sets a lease length. A (write) writes directly, and every node
   holds the record with no accept step. B (submit) can only propose.
3. A restarts with no relay and still writes directly, from the lease on disk.
4. The publisher stops. A's lease runs out by its own clock, A proposes, and
   after the publisher returns A writes directly again and the proposal is decided.
5. B becomes a writer. A new lease covers B right after the grant.
6. A writes directly while offline past the closing of its lease. The excluded
   entries are on every node, count nowhere, and come back as proposals.
7. A writes directly while offline and is reduced to submit before it uploads.
   The relay takes the entries, every node agrees they count, and A's later
   proposals go through.
8. B writes directly while offline and is reduced to read before it uploads. The
   relay refuses the entries: B's sync status says so, it does not retry in a
   storm, and B still reads this database and another one.
"""
import argparse
import json
import socket
import sys
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from record_runs import held, require  # noqa: E402
import v1_records as v1  # noqa: E402
from shared_database_proof import (  # noqa: E402
    APP, SCHEMA, Member, converge, dump, wait, write_credentials,
)
from record_runs import definition  # noqa: E402
from lease_change_proof import change_lapsed, check_blocked_upload_after_pull, lapsed_published, prove_change  # noqa: E402

LEASE_MS = 60_000
READ_ONLY = "a received database is read only"
OUTCOMES = {}


class Proxy:
    """A TCP forwarder to the relay that can be cut."""

    def __init__(self, relay_url):
        target = urlparse(relay_url)
        self.target = (target.hostname, target.port)
        self.blocked, self.conns = False, []
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(64)
        self.url = f"http://127.0.0.1:{self.listener.getsockname()[1]}"
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            if self.blocked:
                client.close()
                continue
            try:
                upstream = socket.create_connection(self.target, timeout=10)
            except OSError:
                client.close()
                continue
            with self.lock:
                self.conns += [client, upstream]
            for source, sink in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pump, args=(source, sink), daemon=True).start()

    @staticmethod
    def _pump(source, sink):
        try:
            while data := source.recv(65536):
                sink.sendall(data)
        except OSError:
            pass
        for end in (source, sink):
            try:
                end.close()
            except OSError:
                pass

    def block(self):
        self.blocked = True
        with self.lock:
            for end in self.conns:
                try:
                    end.close()
                except OSError:
                    pass
            self.conns = []

    def unblock(self):
        self.blocked = False


class Node(Member):
    def register(self):
        try:
            self.node.call("/api/apps/register", {"app": APP, "access": {
                "read": [SCHEMA], "write": [SCHEMA], "extensions": ["databases"]}})
        except RuntimeError:
            pass  # already registered in this home

    def stop(self):
        self.node.stop()

    def restart(self):
        self.node.start()
        self.register()

    def lease(self):
        """The lease this node holds for the database, from the database list."""
        reply = self.act(action="list")
        row = next((r for r in reply.get("databases", []) if r.get("id") == self.db), None)
        return ((row or {}).get("selfAccess") or {}).get("lease")

    def role(self):
        reply = self.act(action="list")
        row = next((r for r in reply.get("databases", []) if r.get("id") == self.db), None)
        return ((row or {}).get("selfAccess") or {}).get("role")

    def write_owned(self, key, note):
        self.write(key, note)

    def write(self, key, note, expect=200, database=None):
        item = v1.create_item(SCHEMA, {"hash": key}, {"id": key, "note": note},
                              database=database or self.db)
        status, reply = self.post("/v1/write", {"mutations": [item]})
        require(status == expect, f"{self.name} write {key}: HTTP {status} {reply!r}")
        return reply

    def row_in(self, database, key):
        status, reply = self.post("/v1/read", {"reads": [v1.point(SCHEMA, {"hash": key}, database=database)]})
        records = ((reply.get("results") or [{}])[0].get("records") or []) if status == 200 else []
        return records[0] if records else None

    def streams(self):
        """How many entries of each stream this node signed, and how many publisher incorporations."""
        counts = {"direct": 0, "proposal": 0, "incorporations": 0}
        try:
            log = json.loads((self.node.home / "log-sync" / str(self.db) / "authored.json").read_text())
        except (OSError, ValueError):
            return counts
        for entry in log:
            # The entry is flat: header and body fields sit side by side.
            if entry.get("incorporated_from"):
                counts["incorporations"] += 1
            elif entry.get("kind") == "data" and entry.get("stream") in counts:
                counts[entry["stream"]] += 1
        return counts

    def upload_error(self):
        status = self.node.call("/api/status")[0]["status"]["cloud_sync"]
        return (((status.get("databases") or {}).get(self.db) or {}).get("upload") or {}).get("last_error")

    def upload_settled(self, signed):
        """Every entry this node had signed when `signed` was counted is on the relay.

        The status keeps its old numbers until the next sync turn, so a head that
        equals the confirmed count proves nothing until the head has caught up."""
        status = self.node.call("/api/status")[0]["status"]["cloud_sync"]
        upload = ((status.get("databases") or {}).get(self.db) or {}).get("upload") or {}
        head = upload.get("spool_head")
        return head is not None and head >= signed and head == upload.get("spool_confirmed_through")

    def signed_count(self):
        try:
            return len(json.loads((self.node.home / "log-sync" / str(self.db) / "authored.json").read_text()))
        except (OSError, ValueError):
            return 0

    def log_count(self, text):
        try:
            return (self.out / "node.log").read_text().count(text)
        except OSError:
            return 0


def fresh_lease(member, seconds=60):
    """Wait until the member holds a lease newer than the one it holds now."""
    first = member.lease()
    before = first["authoredAtMs"] if first else 0
    return wait(f"{member.name} to hold a newer lease",
                lambda: (lease := member.lease()) and lease["authoredAtMs"] > before and lease, seconds)


def publisher_lease(pub):
    return pub.act(action="lease", database=pub.db)["lease"]


def everywhere(nodes, key, note, author):
    converge(nodes, key, note, author, author)


def prove(pub, a, b, c, proxies):
    pa, pb = proxies
    members = (pub, a, b, c)
    pub.db = pub.act(action="create", name="circles", sharingMode="whole_database")["database"]["id"]
    side_id = pub.act(action="create", name="side", sharingMode="whole_database")["database"]["id"]
    schema = definition(SCHEMA, "Hash", ["id", "note"], description_prefix="Lease proof ")
    schema.pop("source", None)
    for database in (pub.db, side_id):
        pub.act(action="declare_schema", database=database, schema=schema)
    status, reply = pub.post("/v1/write", {"mutations": [
        v1.create_item(SCHEMA, {"hash": "seed"}, {"id": "seed", "note": "seed"}, database=pub.db)]})
    require(status == 200, f"seed write: HTTP {status} {reply!r}")
    write_credentials(pub)
    link = pub.act(action="share_link", database=pub.db)["link"]
    side_link = pub.act(action="share_link", database=side_id)["link"]
    for member in (a, b, c):
        member.db = pub.db
        member.act(action="accept", link=link)
    b.act(action="accept", link=side_link)
    for member, role in ((a, "write"), (b, "submit"), (c, "read")):
        pub.act(action="grant", database=pub.db, grantee=member.key, role=role)
    pub.act(action="grant", database=side_id, grantee=b.key, role="read")
    for member in (a, b, c):
        wait(f"{member.name} to hold the seed record", lambda m=member: m.row("seed"))

    # 1. No lease length: a received write is refused, and no lease exists.
    status, reply = a.post("/v1/write", {"mutations": [v1.create_item(
        SCHEMA, {"hash": "no"}, {"id": "no", "note": "no"}, database=pub.db)]})
    require(status == 403 and (reply.get("error") or {}).get("message") == READ_ONLY,
            f"a write with no lease was HTTP {status} {reply!r}")
    require(a.lease() is None and publisher_lease(pub) is None, "a lease exists with no length set")
    print("1. no lease length: write refused 403, no lease", flush=True)

    # 2. A lease length: A writes directly, B only proposes.
    reply = pub.act(action="lease_length", database=pub.db, length_ms=LEASE_MS)
    require(reply.get("leaseLengthMs") == LEASE_MS, f"lease length reply {reply!r}")
    lease = wait("A to hold a lease that covers it", lambda: (x := a.lease()) and x["directWrites"] and x)
    require(lease["covers"] and lease["expiresAtMs"] > lease["authoredAtMs"] and
            lease["closesAtMs"] > lease["expiresAtMs"], f"lease fields {lease!r}")
    proposals_before = a.streams()["proposal"]
    a.write("k1", "a-direct")
    everywhere(members, "k1", "a-direct", a.key)
    require(a.streams()["proposal"] == proposals_before, "A proposed to write a covered record")
    b_lease = wait("B to hold the lease", b.lease)
    require(not b_lease["covers"] and not b_lease["directWrites"], f"a submit member was covered {b_lease!r}")
    status, _ = b.post("/v1/write", {"mutations": [v1.create_item(
        SCHEMA, {"hash": "bn"}, {"id": "bn", "note": "no"}, database=pub.db)]})
    require(status == 403, f"a submit member write was HTTP {status}")
    mid = b.propose("create", "k2", {"id": "k2", "note": "b-proposal"}, {"type": "absent"})
    wait("B's proposal to be published", lambda: b.status(mid) == "published")
    everywhere(members, "k2", "b-proposal", b.key)
    print("2. covered write counts everywhere with no accept; submit member proposes", flush=True)

    # 3. A restart with no relay keeps the lease and the right to write.
    fresh_lease(a)
    pa.block()
    a.stop()
    a.restart()
    lease = wait("A to hold its lease after the restart", lambda: (x := a.lease()) and x["directWrites"] and x, 60)
    a.write("k3", "written-offline")
    pa.unblock()
    everywhere(members, "k3", "written-offline", a.key)
    print("3. restart with no relay keeps the lease; offline direct write synced later", flush=True)

    # 4. The publisher stops: the lease runs out by A's own clock, A proposes.
    pub.stop()
    wait("A's lease to run out", lambda: (x := a.lease()) and not x["directWrites"], 90)
    status, reply = a.post("/v1/write", {"mutations": [v1.create_item(
        SCHEMA, {"hash": "late"}, {"id": "late", "note": "no"}, database=pub.db)]})
    require(status == 403 and (reply.get("error") or {}).get("message") == READ_ONLY,
            f"a write after the lease ran out was HTTP {status} {reply!r}")
    mid = a.propose("create", "k4", {"id": "k4", "note": "a-proposal"}, {"type": "absent"})
    lapsed_id = change_lapsed(a)
    pub.restart()
    wait("A to hold a renewed lease", lambda: (x := a.lease()) and x["directWrites"] and x, 90)
    wait("A's proposal to be decided", lambda: a.status(mid) == "published")
    a.write("k5", "direct-again")
    everywhere(members, "k4", "a-proposal", a.key)
    everywhere(members, "k5", "direct-again", a.key)
    print("4. lease lapsed at A's clock, A proposed, renewed after the publisher returned", flush=True)
    lapsed_published(a, lapsed_id, members, everywhere, OUTCOMES)
    prove_change(pub, a, b, c, pb, members, everywhere, OUTCOMES)
    # 5. A new writer is covered right after the membership change.
    seen = publisher_lease(pub)["authoredAtMs"]
    fresh = wait("the publisher to renew", lambda: (x := publisher_lease(pub)) and x["authoredAtMs"] > seen and x, 60)
    pub.act(action="grant", database=pub.db, grantee=b.key, role="write")
    covered = wait("B to hold a lease that covers it",
                   lambda: (x := b.lease()) and x["covers"] and x["directWrites"] and x, 60)
    gap = covered["authoredAtMs"] - fresh["authoredAtMs"]
    require(gap < LEASE_MS // 2 - 6_000, f"the lease for B came {gap} ms after a renewal; half-life is {LEASE_MS // 2}")
    b.write("k6", "b-direct")
    everywhere(members, "k6", "b-direct", b.key)
    print(f"5. B covered {gap} ms after the previous lease, by the change and not the half-life", flush=True)

    # 6. Direct entries written offline past the closing are excluded, then proposed again.
    fresh_lease(a)
    lease = a.lease()
    before, inc_before = a.streams(), pub.streams()["incorporations"]
    pa.block()
    a.write("x1", "offline-1")
    a.write("x2", "offline-2")
    require(a.row("x1") and a.row("x2"), "A does not hold its offline writes")
    require(pub.row("x1") is None, "the publisher holds an entry A has not uploaded")
    wait("a lease that closes A's lease", lambda: publisher_lease(pub)["authoredAtMs"] >= lease["closesAtMs"], 240)
    pa.unblock()
    everywhere(members, "x1", "offline-1", a.key)
    everywhere(members, "x2", "offline-2", a.key)
    after = a.streams()
    require(after["direct"] == before["direct"] + 2 and after["proposal"] >= before["proposal"] + 2,
            f"A's entries {before} then {after}: the excluded changes did not come back as proposals")
    require(pub.streams()["incorporations"] >= inc_before + 2, "the publisher did not incorporate the proposals")
    print("6. entries past the closing excluded on every node and signed again as proposals", flush=True)

    # 7. A writer reduced to submit uploads its unsent direct entries.
    fresh_lease(a)
    pa.block()
    a.write("g1", "before-reduction")
    pub.act(action="grant", database=pub.db, grantee=a.key, role="submit")
    pa.unblock()
    wait("A's role to be submit", lambda: a.role() == "submit", 60)
    everywhere(members, "g1", "before-reduction", a.key)
    status, _ = a.post("/v1/write", {"mutations": [v1.create_item(
        SCHEMA, {"hash": "g0"}, {"id": "g0", "note": "no"}, database=pub.db)]})
    require(status == 403, f"a reduced writer wrote directly: HTTP {status}")
    mid = a.propose("create", "g2", {"id": "g2", "note": "after-reduction"}, {"type": "absent"})
    wait("A's proposal after the reduction", lambda: a.status(mid) == "published")
    everywhere(members, "g2", "after-reduction", a.key)
    print("7. reduced writer: unsent direct entry uploaded and counted on every node; later proposal published", flush=True)

    # 8. A member reduced to read with unsent entries is refused, visibly and quietly.
    fresh_lease(b)
    pb.block()
    b.write("h1", "unsent")
    pub.act(action="grant", database=pub.db, grantee=b.key, role="read")
    # Wait for the reduction on the relay first so B's upload cannot win the race.
    signed = pub.signed_count()
    wait("the reduction to be on the relay", lambda: pub.upload_settled(signed))
    pb.unblock()
    # Whichever happens first, the relay refusing the entry or B learning that it
    # is read, the status says the entries cannot be deposited.
    message = wait("B's sync status to say its entries cannot be uploaded",
                   lambda: (m := b.upload_error()) and "read or removed" in m and m, 90)
    require("read or removed" in message, f"the refusal message is unclear: {message!r}")
    refusals = b.log_count("the relay refused this member's entries")
    time.sleep(12)
    storm = b.log_count("the relay refused this member's entries") - refusals
    require(storm <= 2, f"{storm} refused uploads in 12 seconds")
    pub.write_owned("h2", "after-refusal")
    wait("B to keep pulling", lambda: (row := b.row("h2")) and held(row["record"]).get("note") == "after-refusal")
    pub.write("s1", "side", database=side_id)
    wait("B to read the other database", lambda: b.row_in(side_id, "s1"))
    check_blocked_upload_after_pull(b)
    print(f"8. refused member: status says {message[:60]!r}...; {storm} retries in 12s; reads continue", flush=True)
    for key, note, author in (("k1", "a-direct", a.key), ("x1", "offline-1", a.key), ("g1", "before-reduction", a.key)):
        everywhere((pub, c), key, note, author)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("--daemon", "--cli", "--relay-url", "--revision"):
        parser.add_argument(name, required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    proxies = (Proxy(args.relay_url), Proxy(args.relay_url))
    nodes, failure = [], None
    for name, proxy in (("publisher", None), ("member-a", proxies[0]), ("member-b", proxies[1]), ("member-c", None)):
        own = argparse.Namespace(**{**vars(args), "relay_url": proxy.url if proxy else args.relay_url})
        nodes.append(Node(name, own, args.output))
    try:
        for node in nodes:
            node.start()
        prove(*nodes, proxies)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump(nodes, {})
    finally:
        for node in nodes:
            try:
                node.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{node.name}: cleanup failed: {error}", file=sys.stderr)
    if failure is not None:
        raise SystemExit(1)
    (args.output / "summary.json").write_text(json.dumps(OUTCOMES, indent=2))
    print("Write lease proof passed", flush=True)


if __name__ == "__main__":
    main()
