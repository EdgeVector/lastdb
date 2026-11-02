"""Reads past a run of deleted records in a format 7 home, and a schema drop.

One hash holds more deleted records between two live records than one
request may examine (8,192 keys). A range page that meets the run ends early
with has_more and a cursor past the last key it examined, so the caller reads
the range to its end page by page. An unfiltered walk page does the same with
a record key cursor. A count needs the complete range and fails with a read
limit that names next_cursor. A sample that the run ends early says that it
is not complete and names a cursor for the walk. A Hash schema with no hash
key field walks and samples with opaque keys.

A share snapshot reads a keyed leg past the same run, page by page, and
delivers the records after it.

The TTL sweep lists the expired keys of a hash with the same range read: it
reads past a run of deleted records to the expired records after the run. It
keeps a floor for each hash after the run; a write below the floor removes the
floor, so the next passes erase the key, not a full pass 64 passes later. A
schema drop releases the records, field history entries and alternate key
entries of the schema, so the same schema installed again holds none of them,
and the records of other schemas stay. An installed schema refuses a change of
its alternate keys, so its current alternate revision is its only one. A drop
blocks the schema first: a write during the drop is refused, and no record of
it stays. record_home.py runs this module on its CI daemon.
"""
import base64
import os
import threading
import time
import uuid

FIELDS = ["id", "bucket", "value"]
# More deleted records under one hash than one request may examine.
RUN = 12_000
BATCH = 500
LIMIT = 10
# Generous page bounds: the run needs two pages of each kind.
MAX_PAGES = 50
# The expired records of the TTL proof are this old, against a policy of half that age.
TTL_SECONDS = 3_600
# Time for the TTL sweep (one pass from each 1 s sampler tick) to erase a few records.
SWEEP_SECONDS = 120
# More records than one release batch of a schema drop reads (256 rows).
DROP_RECORDS = 600
# A write below the floor of a hash must be erased within this time: a few passes at one pass
# a second. Without the write note the floor stays for up to 64 passes.
FLOOR_SECONDS = 20
# Passes after an erase that set the floor of the hash again before the next write below it.
FLOOR_SETTLE_SECONDS = 5
FLOOR_ROUNDS = 3
# Records of the schema that a drop releases while a writer writes to it: the release then
# takes many batches, and the writer meets the drop.
RACE_RECORDS = 3_000
RACE_BATCH = 20


def require(value, message):
    if not value:
        raise RuntimeError(message)


def ids(rows):
    return [row["fields"]["id"] for row in rows]


def refused(call, words):
    try:
        call()
    except RuntimeError as error:
        require(all(word in str(error) for word in words),
                "the call failed for another reason: " + str(error))
        return
    raise RuntimeError("the call did not fail: " + repr(words))


def definition(name, schema_type, key, fields, alternates=None):
    schema = {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": schema_type,
        "fields": fields,
        "field_types": {field: "String" for field in fields},
        "field_descriptions": {field: "Record home example " + field for field in fields},
        "field_versions": {field: 1 for field in fields},
    }
    if key:
        schema["key"] = key
    if alternates:
        schema["alternate_keys"] = alternates
    return schema


RANGED = {"hash_field": "bucket", "range_field": "id"}
DROPPED = definition("RecordDrop", "Hash", {"hash_field": "id"}, FIELDS, {"by_bucket": RANGED})
RACED = definition("RecordRace", "Hash", {"hash_field": "id"}, FIELDS, {"by_bucket": RANGED})


def seed(node, schema):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})


def install(node):
    # A Hash schema with no hash key field declares exactly one field.
    for schema in [definition("RecordRun", "HashRange", RANGED, FIELDS),
                   definition("RecordNoKey", "Hash", None, ["value"]),
                   definition("RecordAged", "HashRange", RANGED, FIELDS),
                   DROPPED, RACED]:
        seed(node, schema)


def mutation(schema, key, values, operation):
    return {"type": "mutation", "schema": schema, "key_value": key,
            "fields_and_values": values, "mutation_type": operation,
            "durability": "durable", "convergence": "sync"}


def write(node, mutations):
    for start in range(0, len(mutations), BATCH):
        node.call("/api/mutations/batch", {"mutations": mutations[start:start + BATCH]})


def deleted_run(node):
    """Two live records under one hash with RUN deleted records between them."""
    keys = ["a-first"] + [f"m{number:05d}" for number in range(RUN)] + ["z-last"]
    write(node, [mutation("RecordRun", {"hash": "run", "range": key},
                          {"id": key, "bucket": "run", "value": key}, "create") for key in keys])
    write(node, [mutation("RecordRun", {"hash": "run", "range": key}, {}, "delete")
                 for key in keys[1:-1]])


def drain(read, body, limit, cursor=None):
    """Every page of one read from `cursor`: its rows, its page count and its short pages."""
    rows, short = [], 0
    for count in range(1, MAX_PAGES + 1):
        page = read({**body, "limit": limit, **({"cursor": cursor} if cursor else {})})
        rows.extend(page["results"])
        if not page["has_more"]:
            require(page.get("next_cursor") is None, "the last page has a cursor")
            return rows, count, short
        short += len(page["results"]) < limit
        following = page.get("next_cursor")
        require(following and following != cursor,
                "a page before the last has no cursor or did not move it: " + repr(following))
        cursor = following
    raise RuntimeError(f"the read did not end in {MAX_PAGES} pages: {body}")


def range_reads(node):
    body = {"schema_name": "RecordRun", "fields": FIELDS, "filter": {"HashKey": "run"}}
    rows, pages, short = drain(lambda request: node.call("/api/query", request)[0], body, LIMIT)
    require(ids(rows) == ["a-first", "z-last"],
            "the range read past deleted records differs: " + repr(ids(rows)))
    require(short >= 1, "no range page ended early in the run of deleted records")
    for extra in [{"expected_total_count": 2}, {"response_format": "count"}]:
        request = {**body, **extra}
        refused(lambda request=request: node.call("/api/query", request),
                ["HTTP 503", "next_cursor"])
    return pages


def walks(node, scan):
    body = {"schema_name": "RecordRun", "fields": FIELDS, "filter": None}
    rows, pages, short = drain(lambda request: scan(node, request)[0], body, LIMIT)
    require(ids(rows) == ["a-first", "z-last"],
            "the walk past deleted records differs: " + repr(ids(rows)))
    require(short >= 1, "no walk page ended early in the run of deleted records")
    return pages


def incomplete_sample(node, scan):
    """A sample that the run of deleted records ends early says so, and the walk continues it."""
    page, _ = scan(node, {"schema_name": "RecordRun", "fields": FIELDS, "filter": {"SampleN": 5}})
    shown = {name: page.get(name) for name in ["has_more", "total_count", "next_cursor"]}
    require(ids(page["results"]) == ["a-first"],
            "the sample before the run of deleted records differs: " + repr(ids(page["results"])))
    require(page.get("has_more") is True and page.get("total_count") is None and page.get("next_cursor"),
            "a sample that a run of deleted records ended early reads as complete: " + repr(shown))
    body = {"schema_name": "RecordRun", "fields": FIELDS, "filter": None}
    rows, _, _ = drain(lambda request: scan(node, request)[0], body, LIMIT, page["next_cursor"])
    require(ids(rows) == ["z-last"], "the walk from the sample cursor differs: " + repr(ids(rows)))


def opaque_keys(node, scan):
    """A Hash schema with no hash key field keeps no key field in its records."""
    names = [f"n{number}" for number in range(5)]
    write(node, [mutation("RecordNoKey", {"hash": name}, {"value": name}, "create")
                 for name in names])
    write(node, [mutation("RecordNoKey", {"hash": "n2"}, {}, "delete")])
    body = {"schema_name": "RecordNoKey", "fields": ["value"], "filter": None}
    rows, _, _ = drain(lambda request: scan(node, request)[0], body, 2)
    values = sorted(row["fields"]["value"] for row in rows)
    require(values == ["n0", "n1", "n3", "n4"],
            "the walk of a schema with no key field differs: " + repr(values))
    require(all(row["key"]["hash"] not in names for row in rows),
            "a walk row of a schema with no key field claims an API key")
    page, _ = scan(node, {**body, "limit": 10})
    require(page.get("key_form") == "opaque", "the walk page key form differs: "
            + repr(page.get("key_form")))
    sample, _ = scan(node, {**body, "filter": {"SampleN": 3}})
    require(len(sample["results"]) == 3,
            f"a sample of 3 returned {len(sample['results'])} rows")


def share_snapshot(node):
    """A share snapshot reads its keyed legs past the run of deleted records."""
    body = {"recipient_pubkey": "record-home",
            "messaging_public_key": base64.b64encode(os.urandom(32)).decode(),
            "messaging_pseudonym": str(uuid.uuid4()),
            "legs": [{"schema_name": "RecordRun", "fields": FIELDS, "filter": {"HashKey": "run"}},
                     {"schema_name": "RecordRun", "fields": FIELDS,
                      "filter": {"HashRangeRange": {"hash": "run", "start": "a", "end": "zz"}}}]}
    reply = node.call("/api/sharing/snapshot", body)[0]
    require(reply["record_count"] == 2,
            f"the share snapshot over a run of {RUN} deleted records holds "
            f"{reply['record_count']} records, not 2")
    require(reply["encrypted_blob"], "the share snapshot has no sealed blob")


def wait_gone(node, keys, message, seconds=SWEEP_SECONDS):
    """Wait until the TTL sweep erased every one of `keys` under the hash "aged"."""
    body = {"schema_name": "RecordAged", "fields": FIELDS,
            "filter": {"HashRangeKeys": [["aged", key] for key in keys]}}
    deadline = time.time() + seconds
    while node.call("/api/query", body)[0]["results"]:
        require(time.time() < deadline, message)
        time.sleep(1)


def ttl_resume(node):
    """The TTL sweep reads past a run of deleted records to the expired records after it."""
    old = int(time.time()) - 2 * TTL_SECONDS
    run = [f"{old:020d}-m{number:05d}" for number in range(RUN)]
    # The run is written and deleted before the policy exists, so only the client deletes it.
    write(node, [mutation("RecordAged", {"hash": "aged", "range": key},
                          {"id": key, "bucket": "aged", "value": key}, "create") for key in run])
    write(node, [mutation("RecordAged", {"hash": "aged", "range": key}, {}, "delete") for key in run])
    reply = node.call("/api/db/schema-retention",
                      {"action": "set", "schema": "RecordAged", "ttl_seconds": TTL_SECONDS})[0]
    require(reply["retention_policy"]["ttl_seconds"] == TTL_SECONDS, "the retention policy differs")
    # A write after the policy registers the hash. The second group comes after the first is
    # erased, so a later pass that starts after the floor of the hash must find it too.
    erased = []
    for group in [["z0", "z1", "z2"], ["z7", "z8"]]:
        keys = [f"{old:020d}-{suffix}" for suffix in group]
        write(node, [mutation("RecordAged", {"hash": "aged", "range": key},
                              {"id": key, "bucket": "aged", "value": key}, "create") for key in keys])
        wait_gone(node, keys, "the TTL sweep did not erase the expired records after a run of "
                  f"{RUN} deleted records: {group}")
        erased += keys
    rows = [row for row in ledger(node) if row.get("schema") in {"RecordAged", reply.get("anchor")}
            and row["caller"] == "ttl-sweep-purge"]
    converged = sum(row["records_erased"] for row in rows)
    require(converged == len(erased), f"the TTL sweep audit rows name {converged} records, "
            f"not {len(erased)}: " + repr(rows))


def ttl_below_floor(node):
    """A write below the floor of a hash is erased by the next passes of the TTL sweep.

    After ttl_resume the floor of the hash "aged" is in the run of deleted records. A key
    older than the run sorts below it. Each round writes one such key and waits less than the
    64 passes after which a pass reads the hash from its start without the write note.
    """
    older = int(time.time()) - 3 * TTL_SECONDS
    for number in range(FLOOR_ROUNDS):
        # The passes after the last erase read the erased key as a deleted record and set the
        # floor of the hash again.
        time.sleep(FLOOR_SETTLE_SECONDS)
        key = f"{older:020d}-a{number}"
        write(node, [mutation("RecordAged", {"hash": "aged", "range": key},
                              {"id": key, "bucket": "aged", "value": key}, "create")])
        started = time.time()
        wait_gone(node, [key], f"the TTL sweep did not erase the expired record {key} below the "
                  f"floor of its hash within {FLOOR_SECONDS} s", FLOOR_SECONDS)
        print(f"TTL floor: round {number + 1} erased a write below the floor in "
              f"{time.time() - started:.1f} s", flush=True)


def ledger(node):
    return node.call("/api/db/delete-ledger")[0]["delete_ledger"]["entries"]


def drop_history(node, key, action):
    return node.call("/api/field-history", {
        "location": {"schema": "RecordDrop", "field": "value", "key": {"hash": key}},
        "action": action,
    })[0]


def dropped_rows(node, scan):
    """Every live record of RecordDrop, by walk and by its alternate key."""
    body = {"schema_name": "RecordDrop", "fields": FIELDS, "filter": None}
    walked, _, _ = drain(lambda request: scan(node, request)[0], body, 100)
    page = node.call("/api/query", {"schema_name": "RecordDrop", "fields": FIELDS,
                                    "filter": {"HashKey": "bd"}, "key_name": "by_bucket",
                                    "limit": 1_000})[0]
    require(not page.get("has_more"), "the alternate read of RecordDrop was truncated")
    return ids(walked), ids(page["results"])


def drop_release(node, scan):
    """A schema drop releases the rows of the schema and leaves the other schemas."""
    reply = drop_history(node, "d0000", {"op": "policy", "policy": {"mode": "keep"}})
    require(reply["policy"] == {"mode": "keep"}, "the field history policy was not set")
    names = [f"d{number:04d}" for number in range(DROP_RECORDS)]
    write(node, [mutation("RecordDrop", {"hash": name}, {"id": name, "bucket": "bd", "value": name},
                          "create") for name in names])
    write(node, [mutation("RecordDrop", {"hash": name}, {"value": name + "-2"}, "update")
                 for name in names[:10]])
    write(node, [mutation("RecordDrop", {"hash": name}, {}, "delete") for name in names[-10:]])
    walked, alternate = dropped_rows(node, scan)
    require(len(walked) == len(alternate) == DROP_RECORDS - 10,
            f"RecordDrop holds {len(walked)} walked and {len(alternate)} alternate records before the drop")
    require(len(drop_history(node, "d0000", {"op": "list"})["entries"]) == 1,
            "the overwrite before the drop kept no history entry")
    reply = node.call("/api/schemas/drop", {"schema": "RecordDrop", "must_exist": True})[0]
    require(reply["existed"], "the schema drop did not find RecordDrop: " + repr(reply))
    seed(node, DROPPED)
    walked, alternate = dropped_rows(node, scan)
    require(walked == [] and alternate == [],
            f"the schema installed again after its drop holds {len(walked)} walked and "
            f"{len(alternate)} alternate records")
    # The drop releases the entries of the current alternate revision. The schema has no other:
    # an installed schema refuses a change of its alternate keys.
    changed = definition("RecordDrop", "Hash", {"hash_field": "id"}, FIELDS,
                         {"by_value": {"hash_field": "value", "range_field": "id"}})
    refused(lambda: seed(node, changed), ["alternate"])
    require(drop_history(node, "d0000", {"op": "list"})["entries"] == [],
            "the schema installed again after its drop holds a field history entry")
    write(node, [mutation("RecordDrop", {"hash": "d0001"},
                          {"id": "d0001", "bucket": "fresh", "value": "new"}, "create")])
    walked, alternate = dropped_rows(node, scan)
    require(walked == ["d0001"] and alternate == [],
            f"a new record after the drop reads as {walked} by walk and {alternate} under the old bucket")
    body = {"schema_name": "RecordRun", "fields": FIELDS, "filter": {"HashKey": "run"}}
    rows, _, _ = drain(lambda request: node.call("/api/query", request)[0], body, LIMIT)
    require(ids(rows) == ["a-first", "z-last"], "the drop of RecordDrop changed RecordRun: " + repr(ids(rows)))


def race_writer(node, stop, outcome):
    """Write new records of RecordRace until `stop`, and count how each batch ended."""
    number = 0
    while not stop.is_set():
        names = [f"w{number + offset:06d}" for offset in range(RACE_BATCH)]
        number += RACE_BATCH
        batch = [mutation("RecordRace", {"hash": name}, {"id": name, "bucket": "race", "value": name},
                          "create") for name in names]
        try:
            node.call("/api/mutations/batch", {"mutations": batch})
            outcome["written"] += 1
        except RuntimeError as error:
            text = str(error)
            if "blocked" in text:
                outcome["blocked"] += 1
            elif "not found" in text.lower():
                outcome["missing"] += 1
            else:
                outcome["other"].append(text[:400])
                return


def race_rows(node, scan):
    """Every live record of RecordRace, by walk and by its alternate key."""
    body = {"schema_name": "RecordRace", "fields": FIELDS, "filter": None}
    walked, _, _ = drain(lambda request: scan(node, request)[0], body, 100)
    page = node.call("/api/query", {"schema_name": "RecordRace", "fields": FIELDS,
                                    "filter": {"HashKey": "race"}, "key_name": "by_bucket",
                                    "limit": 1_000})[0]
    return ids(walked), ids(page["results"])


def drop_race(node, scan):
    """A write during a schema drop is refused, and the drop leaves no record of the schema."""
    names = [f"r{number:05d}" for number in range(RACE_RECORDS)]
    write(node, [mutation("RecordRace", {"hash": name}, {"id": name, "bucket": "race", "value": name},
                          "create") for name in names])
    stop = threading.Event()
    outcome = {"written": 0, "blocked": 0, "missing": 0, "other": []}
    writer = threading.Thread(target=race_writer, args=(node, stop, outcome), daemon=True)
    writer.start()
    deadline = time.time() + SWEEP_SECONDS
    while outcome["written"] < 3 and not outcome["other"]:
        require(time.time() < deadline, "the race writer wrote nothing: " + repr(outcome))
        time.sleep(0.05)
    try:
        reply = node.call("/api/schemas/drop", {"schema": "RecordRace", "must_exist": True})[0]
    finally:
        stop.set()
        writer.join(SWEEP_SECONDS)
    require(not writer.is_alive(), "the race writer did not stop")
    require(not outcome["other"], "a write during the drop failed for another reason: "
            + repr(outcome["other"]))
    require(reply["existed"], "the schema drop did not find RecordRace: " + repr(reply))
    require(outcome["blocked"] >= 1, "no write met the drop while it ran: " + repr(outcome))
    seed(node, RACED)
    walked, alternate = race_rows(node, scan)
    require(walked == [] and alternate == [],
            f"the drop left {len(walked)} walked and {len(alternate)} alternate records of a "
            f"writer that wrote during it: {outcome}")
    return outcome


def check(node, scan):
    """`scan(node, body)` is one unfiltered /api/query with the full-scan opt-in."""
    install(node)
    deleted_run(node)
    range_pages = range_reads(node)
    walk_pages = walks(node, scan)
    incomplete_sample(node, scan)
    opaque_keys(node, scan)
    print(f"Deleted runs: {RUN} deleted records under one hash; the range read took {range_pages} "
          f"pages and the walk {walk_pages} pages of {LIMIT}, both complete and in key order; a "
          "count failed with a read limit that names next_cursor; a sample said it was not "
          "complete and the walk continued it; a schema with no key field walked and sampled "
          "with opaque keys", flush=True)
    share_snapshot(node)
    print(f"Share snapshot: two keyed legs read past {RUN} deleted records page by page and "
          "delivered the 2 records", flush=True)
    ttl_resume(node)
    print(f"TTL resume: the sweep read past {RUN} deleted records and erased the expired records "
          "after them, twice, and wrote their audit rows", flush=True)
    ttl_below_floor(node)
    print(f"TTL floor: {FLOOR_ROUNDS} writes below the floor of a hash were each erased within "
          f"{FLOOR_SECONDS} s", flush=True)
    drop_release(node, scan)
    print(f"Schema drop: the drop of a schema with {DROP_RECORDS} records, field history and an "
          "alternate key released them; the schema installed again held none of them and "
          "refused a change of its alternate keys; another schema kept its records", flush=True)
    outcome = drop_race(node, scan)
    print(f"Drop race: a writer wrote {outcome['written']} batches before the drop, the drop "
          f"refused {outcome['blocked']} batches as blocked and {outcome['missing']} found no "
          f"schema after it; the schema installed again held none of the {RACE_RECORDS} records "
          "or the writes", flush=True)
