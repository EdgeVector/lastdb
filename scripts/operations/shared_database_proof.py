#!/usr/bin/env python3
"""Shared database proposals across real nodes and the reference relay.

One publisher and three member nodes (A and B hold `write`, C holds `read`) run
as real `lastdbd` processes. Each syncs through the relay at --relay-url. The
proof follows the People "direct" recipe:

1. The publisher creates a whole-database share, declares a schema, writes a
   seed record, and grants A write, B write, and C read. B signs nothing yet.
2. A /v1/write to the received database is refused with 403.
3. A proposes a create. It ends `published`, and the record is on every node
   with A as created_by and updated_by.
4. A and B both propose a create of one new key. One ends `published` and the
   other `not_accepted`. This is the publisher's decline path.
5. The publisher lowers A and B to `submit`. B proposes a replace of A's record
   and ends `not_accepted`. A replaces her own record and ends `published`.

A failure prints every proposal status and, for each node, the kind and
sequence number of every entry it holds for the database.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import uuid

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from benchmarks.node import Node  # noqa: E402
from harness import send_unix_request  # noqa: E402
from record_runs import definition, held, require  # noqa: E402
import v1_records as v1  # noqa: E402

APP = "relayproof"
SCHEMA = "relayproof/Note"
WAIT_SECONDS = 150
FINAL = ("published", "not_accepted")
OUTCOMES = {}


def wait(what, probe, seconds=WAIT_SECONDS):
    """Poll probe() until it returns a truthy value. Name `what` and the last value on timeout."""
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(1)
    raise RuntimeError(f"timed out after {seconds}s waiting for {what}; last value {last!r}")


class Member:
    """One real node. `role` is the role the publisher grants it."""

    def __init__(self, name, args, out):
        self.name, self.db, self.key, self.cli = name, None, None, Path(args.cli)
        out = out / name
        out.mkdir(parents=True)
        self.out = out
        self.node = Node(Path(args.daemon), args.revision, out,
                         environment={"LASTDB_RELAY_URL": args.relay_url})
        self.sock = Path(self.node.socket).with_name("apps.sock")

    def start(self):
        self.node.start()
        names = [SCHEMA]
        self.node.call("/api/apps/register", {"app": APP, "access": {
            "read": names, "write": names, "extensions": ["databases"]}})
        result = subprocess.run(
            [str(self.cli), "--data-dir", str(self.node.home), "share", "contact",
             "--out", str(self.node.root / "contact.txt")],
            capture_output=True, text=True, timeout=60, check=False,
            env={**os.environ, "LASTDB_HOME": str(self.node.home)})
        require(result.returncode == 0, f"{self.name}: share contact failed: {result.stderr}")
        self.key = json.loads(result.stdout)["recipient_public_key"]
        require(len(self.key) == 64, f"{self.name}: the node key is not 64 hex characters")

    def post(self, path, body):
        status, raw, _, _, _ = send_unix_request(
            self.sock, "POST", path, body, {"Content-Type": "application/json", "X-LastDB-App": APP})
        try:
            return status, (json.loads(raw) if raw else {})
        except json.JSONDecodeError:
            return status, {"raw": raw[:300].decode("utf-8", "replace")}

    def act(self, expect=200, **body):
        status, reply = self.post("/v1/x/databases", body)
        require(status == expect, f"{self.name} {body.get('action')}: HTTP {status} {reply!r}")
        return reply

    def row(self, key):
        status, reply = self.post("/v1/read", {"reads": [v1.point(SCHEMA, {"hash": key}, database=self.db)]})
        if status != 200:
            return None
        records = (reply.get("results") or [{}])[0].get("records") or []
        return records[0] if records else None

    def propose(self, mutation_type, key, record, expected):
        mutation = {"type": "mutation", "schema": SCHEMA, "key_value": {"hash": key},
                    "mutation_type": mutation_type, "expected": expected}
        if record is not None:
            mutation["record"] = record
        mid = str(uuid.uuid4())
        reply = self.act(expect=202, action="submit", database=self.db, mutation=mutation, idempotency_key=mid)
        require(reply.get("status") == "submitted", f"{self.name} submit: {reply!r}")
        return mid

    def status(self, mid):
        status, reply = self.post("/v1/x/databases", {
            "action": "submission_status", "database": self.db, "mutationId": mid})
        return reply.get("status") if status == 200 else f"HTTP {status}"

    def entries(self):
        """The kind and sequence of every entry this node holds for the database."""
        lines = []
        base = self.node.home / "log-sync" / str(self.db)
        for name in ("authored.json", "received.json"):
            try:
                data = json.loads((base / name).read_text())
            except (OSError, ValueError) as error:
                lines.append(f"  {name}: {error}")
                continue
            for entry in data if isinstance(data, list) else data.get("entries", []):
                head = entry.get("header", entry)
                body = entry.get("body") or {}
                lines.append("  {} {} seq={} node={} device={} {}".format(
                    name, entry.get("kind"), head.get("writer_seq"), str(head.get("node_key"))[:8],
                    str(head.get("device_id"))[:8],
                    body.get("stream") or ("incorporates" if body.get("incorporated_from") else "")))
        return lines


def dump(members, proposals):
    print("proposal statuses:", file=sys.stderr)
    for label, (member, mid) in proposals.items():
        print(f"  {label}: {member.name} {mid} {member.status(mid)}", file=sys.stderr)
    for member in members:
        print(f"{member.name} key={str(member.key)[:8]} entries:", file=sys.stderr)
        print("\n".join(member.entries()), file=sys.stderr)
        try:
            log = (member.out / "node.log").read_text().splitlines()
        except OSError:
            log = []
        print(f"{member.name} log (last 25 warnings):", file=sys.stderr)
        print("\n".join([line for line in log if "WARN" in line or "ERROR" in line][-25:]), file=sys.stderr)


def write_credentials(publisher):
    """Saved credentials make the grant and link routes available. No cloud call is made."""
    environments = json.loads((_OPS.parents[1] / "lastdb_node" / "environments.json").read_text())
    body = {"api_url": environments["environments"]["dev"]["exemem_api"], "api_key": "em_" + secrets.token_hex(24)}
    fd = os.open(publisher.node.home / "cloud-credentials.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(body, handle)


def settle(proposals, want=None):
    """Wait until each named proposal is final. `want` maps a label to its required status."""
    def done():
        found = {label: member.status(mid) for label, (member, mid) in proposals.items()}
        return found if all(value in FINAL for value in found.values()) else None
    found = wait("final proposal statuses " + ", ".join(proposals), done)
    for label, status in (want or {}).items():
        require(found[label] == status, f"{label} ended {found[label]!r}, expected {status!r}")
    OUTCOMES.update(found)
    return found


def converge(members, key, note, creator, updater):
    """Every node holds `key` with `note`, created by `creator` and last updated by `updater`."""
    for member in members:
        def probe(member=member):
            row = member.row(key)
            ok = row and held(row["record"]).get("note") == note
            return row if ok and (row.get("created_by"), row.get("updated_by")) == (creator, updater) else None
        wait(f"{member.name} to hold {key}={note} by {creator[:8]}/{updater[:8]}", probe)


def prove(pub, a, b, c, proposals):
    members = (pub, a, b, c)
    pub.db = pub.act(action="create", name="circles", sharingMode="whole_database")["database"]["id"]
    schema = definition(SCHEMA, "Hash", ["id", "note"], description_prefix="Shared proof ")
    schema.pop("source", None)
    pub.act(action="declare_schema", database=pub.db, schema=schema)
    seed = v1.create_item(SCHEMA, {"hash": "seed"}, {"id": "seed", "note": "seed"}, database=pub.db)
    status, reply = pub.post("/v1/write", {"mutations": [seed]})
    require(status == 200, f"seed write: HTTP {status} {reply!r}")
    write_credentials(pub)
    link = pub.act(action="share_link", database=pub.db)["link"]
    for member in (a, b, c):
        member.db = pub.db
        member.act(action="accept", link=link)
    for member, role in ((a, "write"), (b, "write"), (c, "read")):
        pub.act(action="grant", database=pub.db, grantee=member.key, role=role)
    for member in (a, b, c):
        wait(f"{member.name} to hold the seed record", lambda m=member: m.row("seed"))
    print("1. publisher database shared with A (write), B (write), C (read)", flush=True)

    # 2. A received database is read only, whatever the role.
    item = v1.create_item(SCHEMA, {"hash": "direct"}, {"id": "direct", "note": "no"}, database=pub.db)
    status, reply = a.post("/v1/write", {"mutations": [item]})
    error = reply.get("error") or {}
    require(status == 403 and error.get("message") == "a received database is read only",
            f"a received write was HTTP {status} {reply!r}")
    c_status, c_reply = c.post("/v1/x/databases", {
        "action": "submit", "database": pub.db, "idempotency_key": str(uuid.uuid4()),
        "mutation": {"type": "mutation", "schema": SCHEMA, "key_value": {"hash": "c"},
                     "mutation_type": "create", "expected": {"type": "absent"},
                     "record": {"id": "c", "note": "c"}}})
    require(c_status == 403, f"a read member submit was HTTP {c_status} {c_reply!r}")
    print("2. direct write refused 403; read member cannot submit", flush=True)

    # 3. The first proposal of the first writer is published. B has signed nothing.
    proposals["a-create-k1"] = (a, a.propose("create", "k1", {"id": "k1", "note": "a1"}, {"type": "absent"}))
    settle({"a-create-k1": proposals["a-create-k1"]}, {"a-create-k1": "published"})
    converge(members, "k1", "a1", a.key, a.key)
    print("3. A's first proposal published; k1 on every node, created by A", flush=True)

    # 4. Two writers create one key. One is published, the other not accepted.
    proposals["a-create-k2"] = (a, a.propose("create", "k2", {"id": "k2", "note": "from-a"}, {"type": "absent"}))
    proposals["b-create-k2"] = (b, b.propose("create", "k2", {"id": "k2", "note": "from-b"}, {"type": "absent"}))
    found = settle({label: proposals[label] for label in ("a-create-k2", "b-create-k2")})
    require(sorted(found.values()) == ["not_accepted", "published"], f"k2 proposals ended {found!r}")
    winner, note = (a, "from-a") if found["a-create-k2"] == "published" else (b, "from-b")
    converge(members, "k2", note, winner.key, winner.key)
    print(f"4. k2: {found}; the record names {winner.name}", flush=True)

    # 5. At `submit`, a member may change only a record it created.
    for member in (a, b):
        pub.act(action="grant", database=pub.db, grantee=member.key, role="submit")
    row = b.row("k1")
    proposals["b-replace-k1"] = (b, b.propose(
        "replace", "k1", {"id": "k1", "note": "from-b"}, {"type": "revision", "revision": row["revision"]}))
    settle({"b-replace-k1": proposals["b-replace-k1"]}, {"b-replace-k1": "not_accepted"})
    row = a.row("k1")
    proposals["a-replace-k1"] = (a, a.propose(
        "replace", "k1", {"id": "k1", "note": "a2"}, {"type": "revision", "revision": row["revision"]}))
    settle({"a-replace-k1": proposals["a-replace-k1"]}, {"a-replace-k1": "published"})
    converge(members, "k1", "a2", a.key, a.key)
    print("5. at submit: B's replace of A's record not accepted; A's own replace published", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--relay-url", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    members = [Member(name, args, args.output) for name in ("publisher", "member-a", "member-b", "member-c")]
    proposals, failure = {}, None
    try:
        for member in members:
            member.start()
        prove(*members, proposals)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump(members, proposals)
    finally:
        for member in members:
            try:
                member.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{member.name}: cleanup failed: {error}", file=sys.stderr)
    if failure is not None:
        raise SystemExit(1)
    (args.output / "summary.json").write_text(json.dumps(OUTCOMES, indent=2))
    print("Shared database proof passed", flush=True)


if __name__ == "__main__":
    main()
