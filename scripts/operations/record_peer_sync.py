#!/usr/bin/env python3
"""Exercise peer sync between two fresh format 7 (record model) daemons in CI.

Both daemons create a home with LASTDB_RECORD_HOME=1 and LASTDB_LOGICAL_CAPTURE=1.
Each enrolls the other, and the script moves the signed version 2 events through
/api/sync/logical-v1. It proves a two-field merge of one record, the convergence
of a concurrent write of one field with one kept conflict record, a replay that
changes nothing, the propagation of a delete, and the release of the conflict
record at the end of its retention window. It runs no test runner or benchmark.
The homes hold generated data only and never read or copy a primary.
"""
import argparse
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, RECORD_NODE_MARKER  # noqa: E402

SYNC = "/api/sync/logical-v1"
FIELDS = ["id", "x", "y", "note"]
RANGE_FIELDS = ["bucket", "id", "x"]
RETENTION_SECS = 60
ENVIRONMENT = {
    "LASTDB_RECORD_HOME": "1", "LASTDB_LOGICAL_CAPTURE": "1",
    "LASTDB_CONFLICT_RETENTION_SECS": str(RETENTION_SECS), "LASTDB_CONFLICT_JANITOR_MS": "1000",
    "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "EXEMEM_API_URL": "http://127.0.0.1:9",
    "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def definition(name, fields, key, schema_type, alternate=None):
    value = {
        "source": "system_seed", "name": name, "descriptive_name": name, "schema_type": schema_type,
        "key": key, "fields": fields, "field_types": {field: "String" for field in fields},
        "field_descriptions": {field: "Peer record " + field for field in fields},
        "field_versions": {field: 1 for field in fields},
    }
    if alternate:
        value["alternate_keys"] = alternate
    return value


def install(node):
    schemas = [
        definition("PeerRecord", FIELDS, {"hash_field": "id"}, "Hash",
                   {"by_note": {"hash_field": "note", "range_field": "id"}}),
        definition("PeerRange", RANGE_FIELDS, {"hash_field": "bucket", "range_field": "id"}, "HashRange"),
    ]
    for schema in schemas:
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})


def mutate(node, schema, key, values, operation="update"):
    body = {"type": "mutation", "schema": schema, "key_value": key, "fields_and_values": values,
            "mutation_type": operation, "durability": "durable", "convergence": "sync"}
    return node.call("/api/mutation", body)[0]


def query(node, schema, selector, fields=FIELDS, key_name=None):
    body = {"schema_name": schema, "fields": fields, "filter": selector}
    if key_name is not None:
        body["key_name"] = key_name
    page = node.call("/api/query", body)[0]
    require(not page.get("has_more"), f"{schema} {selector} was truncated")
    return page["results"]


def record(node, key):
    rows = query(node, "PeerRecord", {"HashKey": key})
    require(len(rows) <= 1, f"PeerRecord {key} returned {len(rows)} rows")
    if not rows:
        return None, {}
    values = {field: value for field, value in rows[0]["fields"].items() if value is not None}
    return values, rows[0].get("metadata") or {}


def head(node):
    status = node.call("/api/status")[0]["status"]["logical_capture"]
    require(status["enabled"] is True and status["position"], "logical capture is not enabled")
    return status["position"]


class Link:
    """One direction of sync: the source's events after `after` go to the target."""

    def __init__(self, source, target, name):
        self.source, self.target, self.name, self.after = source, target, name, 0

    def page(self, after):
        page = self.source.call(SYNC, {"action": "export", "after": after, "limit": 4})[0]
        for event in page["events"]:
            require(event["version"] == 2 and not event.get("outcomes") and event.get("records"),
                    "a format 7 home exported an event other than version 2 with records")
        return page["events"]

    def run(self):
        imported = outcomes = 0
        target_head = head(self.target)
        while True:
            events = self.page(self.after)
            if not events:
                break
            receipt = self.target.call(SYNC, {"action": "import", "events": events})[0]
            require(receipt["imported_events"] + receipt["duplicate_events"] == len(events),
                    "the import receipt does not count every event")
            imported += receipt["imported_events"]
            outcomes += receipt["imported_outcomes"]
            self.after = events[-1]["sequence"]
        require(head(self.target) == target_head, f"{self.name}: an import authored an event")
        return imported, outcomes

    def replay(self):
        after = duplicates = 0
        while after < self.after:
            events = self.page(after)
            require(events, "the source lost exported events")
            receipt = self.target.call(SYNC, {"action": "import", "events": events})[0]
            require(receipt["imported_events"] == 0 and receipt["duplicate_events"] == len(events),
                    f"{self.name}: a replay imported an event again: {receipt!r}")
            duplicates += len(events)
            after = events[-1]["sequence"]
        return duplicates


def enroll(a, b):
    anchors = [node.call(SYNC, {"action": "export", "after": 0, "limit": 1})[0]["anchor"] for node in (a, b)]
    require(anchors[0]["origin"] != anchors[1]["origin"], "the two fresh homes share an origin")
    a.call(SYNC, {"action": "enroll", "anchor": anchors[1]})
    b.call(SYNC, {"action": "enroll", "anchor": anchors[0]})


def both(links):
    return [link.run() for link in links]


def conflicts(node, key, fields):
    body = {"keys": [{"schema_name": "PeerRecord", "hash": key, "fields": fields}]}
    reply = node.call("/api/conflicts/keys", body)[0]
    require(reply["scope"] == "selected_record_fields" and reply["count"] == 1,
            "the conflict read is not a record read: " + repr(reply)[:400])
    return {field["field"]: field for field in reply["results"][0]["fields"]}


def field_merge(a, b, links):
    mutate(a, "PeerRecord", {"hash": "r1"}, {"id": "r1", "x": "x0"}, "create")
    links[0].run()
    require(record(b, "r1")[0] == {"id": "r1", "x": "x0"}, "B did not receive the created record")
    mutate(a, "PeerRecord", {"hash": "r1"}, {"x": "xa"})
    mutate(b, "PeerRecord", {"hash": "r1"}, {"y": "yb"})
    counts = both(links)
    for node in (a, b):
        require(record(node, "r1")[0] == {"id": "r1", "x": "xa", "y": "yb"},
                "a device did not keep both field changes: " + repr(record(node, "r1")[0]))
        found = conflicts(node, "r1", ["x", "y"])
        require(all(found[field]["state"] == "clean" for field in ["x", "y"]),
                "a merge of two fields made a conflict: " + repr(found))
    print(f"Field merge: A wrote x and B wrote y of one record; after {counts} (events, outcomes) "
          "imported each way, both devices hold x=xa and y=yb with no conflict", flush=True)


def concurrent_write(a, b, links):
    mutate(a, "PeerRecord", {"hash": "r1"}, {"note": "na"})
    time.sleep(0.05)
    mutate(b, "PeerRecord", {"hash": "r1"}, {"note": "nb"})
    both(links)
    kept = []
    for node in (a, b):
        values, metadata = record(node, "r1")
        require(values.get("note") == "nb", "a device did not converge to the newer note")
        found = conflicts(node, "r1", ["note", "x"])
        note = found["note"]
        require(note["state"] == "conflict" and len(note["conflicts"]) == 1
                and note["conflicts"][0]["value"] == "na" and found["x"]["state"] == "clean",
                "the losing note is not one conflict record: " + repr(found))
        entry = note["conflicts"][0]
        require(entry["loser"][2] != entry["winner"][2], "the conflict stamps name one device")
        kept.append((entry["mutation_id"], entry["value"], tuple(entry["loser"]), tuple(entry["winner"])))
        require((metadata.get("note") or {}).get("has_conflicts") is True,
                "the query did not flag the conflicted field: " + repr(metadata)[:400])
        require("has_conflicts" not in (metadata.get("x") or {}), "the query flagged a clean field")
        alternate = query(node, "PeerRecord", {"HashRangeKey": {"hash": "nb", "range": "r1"}},
                          key_name="by_note")
        stale = query(node, "PeerRecord", {"HashKey": "na"}, key_name="by_note")
        require(len(alternate) == 1 and stale == [], "the alternate entry did not follow the merged note")
    require(kept[0] == kept[1], "the two devices keep different conflict records")
    print("Concurrent write: both devices hold note=nb, one conflict record keeps the losing value na "
          f"(mutation {kept[0][0]}), the query flags only note, and the by_note alternate key follows",
          flush=True)


def replay(a, b, links):
    before = [(record(node, "r1")[0], conflicts(node, "r1", ["note", "x", "y"])) for node in (a, b)]
    duplicates = [link.replay() for link in links]
    after = [(record(node, "r1")[0], conflicts(node, "r1", ["note", "x", "y"])) for node in (a, b)]
    require(before == after, "a replay changed a record or a conflict record")
    print(f"Replay: {duplicates} duplicate events imported nothing and changed no record or conflict",
          flush=True)


def deletes(a, b, links):
    mutate(a, "PeerRecord", {"hash": "r2"}, {"id": "r2", "x": "gone", "note": "n2"}, "create")
    mutate(a, "PeerRange", {"hash": "bk", "range": "k1"}, {"bucket": "bk", "id": "k1", "x": "1"}, "create")
    mutate(a, "PeerRange", {"hash": "bk", "range": "k2"}, {"bucket": "bk", "id": "k2", "x": "2"}, "create")
    links[0].run()
    require(record(b, "r2")[0].get("x") == "gone", "B did not receive the record to delete")
    mutate(b, "PeerRecord", {"hash": "r2"}, {}, "delete")
    mutate(b, "PeerRange", {"hash": "bk", "range": "k1"}, {}, "delete")
    both(links)
    for node in (a, b):
        require(record(node, "r2")[0] is None, "a deleted record still reads on a device")
        require(query(node, "PeerRecord", {"HashKey": "n2"}, key_name="by_note") == [],
                "the alternate entry of a deleted record still resolves")
        rows = query(node, "PeerRange", {"HashKey": "bk"}, fields=RANGE_FIELDS)
        require([row["fields"]["id"] for row in rows] == ["k2"], "a HashRange delete did not propagate")
    mutate(a, "PeerRecord", {"hash": "r2"}, {"id": "r2", "x": "back"}, "create")
    both(links)
    for node in (a, b):
        require(record(node, "r2")[0] == {"id": "r2", "x": "back"},
                "a write after a delete brought back a deleted value")
    print("Delete: B deleted a Hash and a HashRange record; A no longer reads them or their alternate "
          "entry, and a later write makes a new record on both", flush=True)


def expiry(a, b):
    deadline = time.monotonic() + RETENTION_SECS * 3
    while time.monotonic() < deadline:
        states = [conflicts(node, "r1", ["note"])["note"] for node in (a, b)]
        flags = [record(node, "r1")[1].get("note", {}).get("has_conflicts") for node in (a, b)]
        if all(state["state"] == "clean" for state in states) and flags == [None, None]:
            print(f"Retention: the conflict record and its query flag left the reads on both devices "
                  f"after the {RETENTION_SECS} s window", flush=True)
            return
        time.sleep(2)
    raise RuntimeError("the conflict record outlived its retention window")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    nodes = []
    try:
        for label in ("a", "b"):
            folder = output / label
            folder.mkdir(mode=0o700)
            nodes.append(Node(args.daemon.resolve(), args.revision, folder, temporary_directory=root,
                              environment=dict(ENVIRONMENT)))
            nodes[-1].start()
            marker = (nodes[-1].home / "data" / "laststore-pack-node-v1").read_text()
            require(marker == RECORD_NODE_MARKER, "a new home is not format 7: " + repr(marker))
            install(nodes[-1])
        a, b = nodes
        enroll(a, b)
        links = [Link(a, b, "A to B"), Link(b, a, "B to A")]
        field_merge(a, b, links)
        concurrent_write(a, b, links)
        replay(a, b, links)
        deletes(a, b, links)
        expiry(a, b)
    finally:
        for node in reversed(nodes):
            node.close()


if __name__ == "__main__":
    main()
