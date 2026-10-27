"""Generated product operations through the isolated node's real schema and mutation routes."""
import hashlib
from urllib.parse import quote

from workload import encoded

FIELDS = ["bucket", "id", "alternate", "title", "body"]
BY_ID, BY_ALT, OTHER = "LifecycleById", "LifecycleByAlternate", "LifecycleUnrelated"
COUNT = 64


def definition(name, ranged, unrelated=False):
    prefix = "unrelated generated product" if unrelated else "schema lifecycle generated product"
    return {"source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": ranged},
            "fields": FIELDS, "field_descriptions": {f: prefix + " " + f for f in FIELDS}}


def values(number, title):
    return {"bucket": "generated", "id": f"id-{number:04d}", "alternate": f"alt-{number:04d}",
            "title": title, "body": "schema lifecycle generated body"}


def write(node, schema, ranged, revision, create=False):
    mutations = []
    for number in range(COUNT):
        value = values(number, revision + f"-{number}")
        mutations.append({"type": "mutation", "schema": schema,
                          "mutation_type": "create" if create else "update", "durability": "durable",
                          "key_value": {"hash": "generated", "range": value[ranged]},
                          "fields_and_values": value})
    reply = node.call("/api/mutations/batch", {"mutations": mutations})[0]
    if reply.get("count") != COUNT or reply.get("durability") != "durable":
        raise RuntimeError("A lifecycle write lacks its complete durable ack")


def read(node, schema, ranged, revision):
    expected = {values(n, "")[ranged]: values(n, revision + f"-{n}") for n in range(COUNT)}
    reply = node.call("/api/query", {"schema_name": schema, "fields": FIELDS,
                      "filter": {"HashRangeKeys": [["generated", k] for k in expected]}, "limit": COUNT})[0]
    records = reply.get("results", [])
    observed = {r["key"]["range"]: r["fields"] for r in records}
    if len(records) != COUNT or observed != expected:
        raise RuntimeError(f"The {schema} lifecycle values differ: {reply}")
    return hashlib.sha256(encoded(sorted(observed.items()))).hexdigest()


def metadata(node, name):
    return node.call("/api/schema/" + quote(name, safe=""))[0]["schema"]


def verify(node):
    return {"by-id": read(node, BY_ID, "id", "through-alternate"),
            "by-alternate": read(node, BY_ALT, "alternate", "through-alternate"),
            "unrelated": read(node, OTHER, "alternate", "unrelated")}


def operate(node):
    definitions = (definition(BY_ID, "id"), definition(BY_ALT, "alternate"),
                   definition(OTHER, "alternate", unrelated=True))
    for schema in definitions:
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})
    write(node, BY_ID, "id", "through-id", create=True)
    read(node, BY_ALT, "alternate", "through-id")
    write(node, BY_ALT, "alternate", "through-alternate")
    read(node, BY_ID, "id", "through-alternate")
    write(node, OTHER, "alternate", "unrelated", create=True)
    before = verify(node)
    maps = {name: metadata(node, name) for name in (BY_ID, BY_ALT, OTHER)}
    # Repeated wire definitions omit local molecules, like a catalog reload.
    for schema in definitions:
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})
    if before != verify(node) or maps != {name: metadata(node, name) for name in maps}:
        raise RuntimeError("A reload changed local molecules or generated data")
    return before
