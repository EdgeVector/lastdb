"""Reads past a run of deleted records in a format 7 home.

One hash holds more deleted records between two live records than one
request may examine (10,000 keys). A range page that meets the run ends early
with has_more and a cursor past the last key it examined, so the caller reads
the range to its end page by page. An unfiltered walk page does the same with
a record key cursor. A count needs the complete range and fails with a read
limit that names next_cursor. A Hash schema with no hash key field walks and
samples with opaque keys. record_home.py runs this module on its CI daemon.
"""

FIELDS = ["id", "bucket", "value"]
# More deleted records under one hash than one request may examine.
RUN = 12_000
BATCH = 500
LIMIT = 10
# Generous page bounds: the run needs two pages of each kind.
MAX_PAGES = 50


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


def install(node):
    # A Hash schema with no hash key field declares exactly one field.
    for name, key, fields in [("RecordRun", {"hash_field": "bucket", "range_field": "id"}, FIELDS),
                              ("RecordNoKey", None, ["value"])]:
        definition = {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange" if key else "Hash",
            "fields": fields,
            "field_types": {field: "String" for field in fields},
            "field_descriptions": {field: "Record home example " + field for field in fields},
            "field_versions": {field: 1 for field in fields},
        }
        if key:
            definition["key"] = key
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


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


def drain(read, body, limit):
    """Every page of one read: its rows, its page count and its short pages."""
    rows, short, cursor = [], 0, None
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


def check(node, scan):
    """`scan(node, body)` is one unfiltered /api/query with the full-scan opt-in."""
    install(node)
    deleted_run(node)
    range_pages = range_reads(node)
    walk_pages = walks(node, scan)
    opaque_keys(node, scan)
    print(f"Deleted runs: {RUN} deleted records under one hash; the range read took {range_pages} "
          f"pages and the walk {walk_pages} pages of {LIMIT}, both complete and in key order; a "
          "count failed with a read limit that names next_cursor; a schema with no key field "
          "walked and sampled with opaque keys", flush=True)
