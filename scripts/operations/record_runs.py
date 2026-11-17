"""Reads past a run of deleted records in a home, and a schema drop.

Draining range and walk cursors across a long deleted run returns live keys in order;
counts report exact totals or read-limit cursors; Hash schemas without key fields keep
opaque keys. TTL sweep reads past deleted runs and clears per-hash floors on writes
below the floor. Schema drop blocks concurrent writes and releases records and alternate
entries without affecting other schemas.
"""
from pathlib import Path
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import v1_records as v1  # noqa: E402

FIELDS = ["id", "bucket", "value"]
MISSING_DATABASE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
# More deleted records under one hash than one request may examine.
RUN = 12_000
BATCH = 500
LIMIT = 1
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
    return [row["record"]["id"] for row in rows]


def held(record):
    return {field: value for field, value in record.items() if value is not None}


def refused(call, words):
    expected = (words,) if isinstance(words, str) else tuple(words)
    try:
        call()
    except RuntimeError as error:
        require(all(word in str(error) for word in expected),
                "the call failed for another reason: " + str(error))
        return
    raise RuntimeError("the call did not fail: " + repr(words))


def definition(name, schema_type, fields, alternates=None, description_prefix="Record home example "):
    return {
        "source": "system_seed", "name": name, "descriptive_name": name, "schema_type": schema_type,
        "record_keys": {"primary": {"kind": "hash_range", "range_type": "String"} if schema_type == "HashRange" else {"kind": "hash"},
                        "alternates": alternates or {}},
        "fields": fields, "field_types": {f: "String" for f in fields},
        "field_descriptions": {f: description_prefix + f for f in fields}, "field_versions": {f: 1 for f in fields},
    }


RANGED = {"kind": "hash_range", "range_type": "String"}
DROPPED = definition("RecordDrop", "Hash", FIELDS, {"by_bucket": RANGED})
RACED = definition("RecordRace", "Hash", FIELDS, {"by_bucket": RANGED})


def system_seed(node, schema, database_id=None):
    path = "/api/schemas/seed-system" if database_id is None else f"/api/databases/{database_id}/schemas/seed-system"
    node.call(path, {"isolated_copy": True, "schema": schema})


def install(node):
    for schema in [definition("RecordRun", "HashRange", FIELDS), definition("RecordNoKey", "Hash", ["value"]),
                   definition("RecordAged", "HashRange", FIELDS), DROPPED, RACED]:
        system_seed(node, schema)


def mutation(schema, key, values, operation):
    """A /v1 write item. Replace and delete omit revision; write() reads it."""
    item_key = v1.key_input(key)
    alternates = None
    if schema in ("RecordDrop", "RecordRace") and operation not in ("delete", "purge") and values:
        alternates = {"by_bucket": {"hash": values["bucket"], "range": values["id"]}}
    if operation == "create":
        return v1.create_item(schema, item_key, values, alternates=alternates)
    if operation == "delete":
        return {"schema": schema, "op": "delete", "key": item_key}
    item = {"schema": schema, "op": "replace", "key": item_key, "record": values}
    if alternates:
        item["alternates"] = alternates
    return item


def write(node, mutations):
    for start in range(0, len(mutations), BATCH):
        v1.commit(node, mutations[start:start + BATCH])


def deleted_run(node):
    """Two live records under one hash with RUN deleted records between them."""
    keys = ["a-first"] + [f"m{number:05d}" for number in range(RUN)] + ["z-last"]
    write(node, [mutation("RecordRun", {"hash": "run", "range": key},
                          {"id": key, "bucket": "run", "value": key}, "create") for key in keys])
    write(node, [mutation("RecordRun", {"hash": "run", "range": key}, {}, "delete")
                 for key in keys[1:-1]])


def drain(node, schema, span, limit):
    """Every page of one range read: its rows, its page count and its short pages."""
    rows, short, cursor = [], 0, None
    for count in range(1, MAX_PAGES + 1):
        page = v1.read_items(node, [v1.range_read(schema, span, limit=limit, cursor=cursor)])[0]
        rows.extend(page["records"])
        following = page.get("next_cursor")
        if following is None:
            return rows, count, short
        short += len(page["records"]) < limit
        require(following != cursor,
                "a page before the last has no cursor or did not move it: " + repr(following))
        cursor = following
    raise RuntimeError(f"the read did not end in {MAX_PAGES} pages: {schema} {span}")


def range_reads(node):
    body = {"schema_name": "RecordRun", "filter": {"HashKey": "run"}}
    rows, pages, short = drain(node, "RecordRun", {"hash": "run"}, LIMIT)
    require(ids(rows) == ["a-first", "z-last"],
            "the range read past deleted records differs: " + repr(ids(rows)))
    require(pages >= 2, "the range proof did not exercise its cursor")
    for extra in [{"expected_total_count": 2}, {"response_format": "count"}]:
        request = {**body, **extra}
        try:
            result = node.call("/api/query", request)[0]
        except RuntimeError as error:
            require("HTTP 503" in str(error) and "next_cursor" in str(error),
                    "complete-count request failed for another reason: " + str(error))
        else:
            require(result.get("total_count") == 2,
                    "completed count differs from the two live keys")
    print(f"Range cursor crossed the erased keys with {short} short pages", flush=True)
    return pages


def listed_hashes(node, schema, limit=500):
    """Every live primary hash of `schema`, one keys_only page at a time."""
    keys, cursor = [], None
    for _ in range(MAX_PAGES):
        page = v1.read_items(node, [v1.key_list(schema, limit=limit, cursor=cursor)])[0]
        keys.extend(item["hash"] for item in page["keys"])
        cursor = page.get("next_cursor")
        if cursor is None:
            return keys
    raise RuntimeError(f"the key list for {schema} did not finish in {MAX_PAGES} pages")


def opaque_keys(node):
    """A Hash schema with no hash key field keeps its API key independently of its fields."""
    names = [f"n{number}" for number in range(5)]
    write(node, [mutation("RecordNoKey", {"hash": name}, {"value": name}, "create")
                 for name in names])
    write(node, [mutation("RecordNoKey", {"hash": "n2"}, {}, "delete")])
    listed = sorted(listed_hashes(node, "RecordNoKey"))
    require(listed == ["n0", "n1", "n3", "n4"],
            "the key list of a schema with no key field differs: " + repr(listed))
    rows = v1.flat_records(v1.read_all(node, [v1.point("RecordNoKey", {"hash": name}) for name in names]))
    values = sorted(row["record"]["value"] for row in rows)
    require(values == ["n0", "n1", "n3", "n4"],
            "the keyed batch read of a schema with no key field differs: " + repr(values))
    require(all(row["key"]["hash"] == row["record"]["value"] for row in rows),
            "a record lost its independently stored API key")


def wait_gone(node, keys, message, seconds=SWEEP_SECONDS):
    """Wait until the TTL sweep erased every one of `keys` under the hash "aged"."""
    reads = [v1.point("RecordAged", {"hash": "aged", "range": key}) for key in keys]
    deadline = time.time() + seconds
    while v1.flat_records(v1.read_all(node, reads)):
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
        # External coordinates alone determine age, including an empty payload.
        write(node, [mutation("RecordAged", {"hash": "aged", "range": key},
                              {} if number == 0 else {"value": key}, "create")
                     for number, key in enumerate(keys)])
        wait_gone(node, keys, "the TTL sweep did not erase the expired records after a run of "
                  f"{RUN} deleted records: {group}")
        erased += keys
    rows = [row for row in ledger(node) if row.get("schema") in {"RecordAged", reply.get("anchor")}
            and row["caller"] == "ttl-sweep-purge"]
    converged = sum(row["records_erased"] for row in rows)
    require(converged == len(erased), f"the TTL sweep audit rows name {converged} records, "
            f"not {len(erased)}: " + repr(rows))


def ttl_below_floor(node):
    """A write below the floor of a hash is erased by the next passes of the TTL sweep."""
    older = int(time.time()) - 3 * TTL_SECONDS
    for number in range(FLOOR_ROUNDS):
        time.sleep(FLOOR_SETTLE_SECONDS)
        key = f"{older:020d}-a{number}"
        write(node, [mutation("RecordAged", {"hash": "aged", "range": key}, {"id": key, "bucket": "aged", "value": key}, "create")])
        started = time.time()
        wait_gone(node, [key], f"the TTL sweep did not erase the expired record {key} below the floor of its hash within {FLOOR_SECONDS} s", FLOOR_SECONDS)
        print(f"TTL floor: round {number + 1} erased a write below the floor in {time.time() - started:.1f} s", flush=True)


def ledger(node):
    return node.call("/api/db/delete-ledger")[0]["delete_ledger"]["entries"]


def bucket_rows(node, schema, bucket):
    """Every live record of `schema`, by key list and by its `by_bucket` alternate key."""
    listed = listed_hashes(node, schema)
    page = v1.read_items(node, [v1.range_read(schema, {"hash": bucket}, via="by_bucket", limit=1_000)])[0]
    require(page.get("next_cursor") is None, f"the alternate read of {schema} was truncated")
    return listed, ids(page["records"])


def drop_release(node):
    """A schema drop releases the rows of the schema and leaves the other schemas."""
    names = [f"d{number:04d}" for number in range(DROP_RECORDS)]
    write(node, [mutation("RecordDrop", {"hash": name}, {"id": name, "bucket": "bd", "value": name},
                          "create") for name in names])
    write(node, [mutation("RecordDrop", {"hash": name}, {"id": name, "bucket": "bd", "value": name + "-2"}, "replace")
                 for name in names[:10]])
    write(node, [mutation("RecordDrop", {"hash": name}, {}, "delete") for name in names[-10:]])
    primary = v1.flat_records(v1.read_all(
        node, [v1.point("RecordDrop", {"hash": name}) for name in names]))
    require(len(primary) == DROP_RECORDS - 10,
            f"RecordDrop keyed read returned {len(primary)} records before the drop")
    listed, alternate = bucket_rows(node, "RecordDrop", "bd")
    require(len(listed) == len(alternate) == DROP_RECORDS - 10,
            f"RecordDrop holds {len(listed)} listed and {len(alternate)} alternate records before the drop")
    reply = node.call("/api/schemas/drop", {"schema": "RecordDrop", "must_exist": True})[0]
    require(reply["existed"], "the schema drop did not find RecordDrop: " + repr(reply))
    system_seed(node, DROPPED)
    listed, alternate = bucket_rows(node, "RecordDrop", "bd")
    require(listed == [] and alternate == [],
            f"the schema installed again after its drop holds {len(listed)} listed and "
            f"{len(alternate)} alternate records")
    # The drop releases the entries of the current alternate revision. The schema has no other:
    # an installed schema refuses a change of its alternate keys.
    changed = definition("RecordDrop", "Hash", FIELDS,
                         {"by_value": RANGED})
    refused(lambda: system_seed(node, changed), ["alternate"])
    write(node, [mutation("RecordDrop", {"hash": "d0001"},
                          {"id": "d0001", "bucket": "fresh", "value": "new"}, "create")])
    listed, alternate = bucket_rows(node, "RecordDrop", "bd")
    require(listed == ["d0001"] and alternate == [],
            f"a new record after the drop reads as {listed} by list and {alternate} under the old bucket")
    rows, _, _ = drain(node, "RecordRun", {"hash": "run"}, LIMIT)
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
            v1.write_items(node, batch)
            outcome["written"] += 1
        except RuntimeError as error:
            text = str(error)
            if "blocked" in text:
                outcome["blocked"] += 1
            elif "not found" in text.lower() or "no schema is named" in text.lower():
                outcome["missing"] += 1
            else:
                outcome["other"].append(text[:400])
                return


def drop_race(node):
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
        require(outcome["written"] >= 3 and not outcome["other"],
                "the race writer failed before the drop: " + repr(outcome))
        reply = node.call("/api/schemas/drop", {"schema": "RecordRace", "must_exist": True})[0]
    finally:
        stop.set()
        writer.join(SWEEP_SECONDS)
    require(not writer.is_alive(), "the race writer did not stop")
    require(not outcome["other"], "a write during the drop failed for another reason: "
            + repr(outcome["other"]))
    require(reply["existed"], "the schema drop did not find RecordRace: " + repr(reply))
    require(outcome["blocked"] >= 1, "no write met the drop while it ran: " + repr(outcome))
    system_seed(node, RACED)
    listed, alternate = bucket_rows(node, "RecordRace", "race")
    require(listed == [] and alternate == [],
            f"the drop left {len(listed)} listed and {len(alternate)} alternate records of a "
            f"writer that wrote during it: {outcome}")
    return outcome


def check(node):
    install(node)
    deleted_run(node)
    range_pages = range_reads(node)
    opaque_keys(node)
    print(f"Deleted runs: {RUN} deleted records under one hash; the range read took {range_pages} "
          f"pages of {LIMIT}, complete and in key order; count reported complete results or "
          "resumable reads; a schema with no key field listed and read by key", flush=True)
    ttl_resume(node)
    print(f"TTL resume: the sweep read past {RUN} deleted records and erased the expired records "
          "after them, twice, and wrote their audit rows", flush=True)
    ttl_below_floor(node)
    print(f"TTL floor: {FLOOR_ROUNDS} writes below the floor of a hash were each erased within "
          f"{FLOOR_SECONDS} s", flush=True)
    drop_release(node)
    print(f"Schema drop: the drop of a schema with {DROP_RECORDS} records and an "
          "alternate key released them; the schema installed again held none of them and "
          "refused a change of its alternate keys; another schema kept its records", flush=True)
    outcome = drop_race(node)
    print(f"Drop race: a writer wrote {outcome['written']} batches before the drop, the drop "
          f"refused {outcome['blocked']} batches as blocked and {outcome['missing']} found no "
          f"schema after it; the schema installed again held none of the {RACE_RECORDS} records "
          "or the writes", flush=True)
