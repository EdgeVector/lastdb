"""Named keyed condition requests and their expected generated values."""

import math

from condition_values import COUNT, EXTRA_FIELDS, HASH, JSON_NUMBERS

FIELDS = ["bucket", "id", "title", "body", *EXTRA_FIELDS]


def request(schema, fields, **options):
    return {"schema_name": schema, "fields": fields, "filter": {"HashKey": HASH},
            "limit": 256, **options}


def cases(schema):
    present = [{"present": {"field": "title"}}]
    ordered = {"order_by": {"field": "score", "order": "desc"}, "predicate_limit": 17}
    return [("predicate-overlap", request(schema, ["title", "body"], **{"where": present})),
        ("predicate-disjoint", request(schema, ["body"], **{"where": present})),
        ("primary-field-control", request(schema, ["bucket", "title"],
            **{"where": [{"eq": {"field": "bucket", "value": HASH}}]})),
        ("captured-absence", request(schema, ["never", "body"],
            **{"where": [{"absent": {"field": "never"}}]})),
        ("nullable-absence", request(schema, ["optional", "body"],
            **{"where": [{"absent": {"field": "optional"}}]})),
        ("numeric-overlap", request(schema, ["optional", "body"],
            value_filters=[{"GreaterThan": {"field": "optional", "value": 30}}])),
        ("ordered-overlap", request(schema, ["score", "body"], **ordered)),
        ("hidden-order-helper", request(schema, ["body"], **ordered))]


def selected(expected, spec):
    keys = sorted(expected, key=lambda key: (key[1], key[0]))
    for predicate in spec.get("where", []):
        kind, condition = next(iter(predicate.items()))
        name = condition["field"]
        if kind == "present":
            keys = [key for key in keys if expected[key].get(name) is not None]
        elif kind == "absent":
            keys = [key for key in keys if expected[key].get(name) is None]
        else:
            keys = [key for key in keys if expected[key].get(name) == condition["value"]]
    for numeric in spec.get("value_filters", []):
        condition = numeric["GreaterThan"]
        keys = [key for key in keys if condition["field"] not in expected[key]
            or (isinstance(expected[key][condition["field"]], (int, float))
                and expected[key][condition["field"]] > condition["value"])]
    if "order_by" in spec:
        # This recipe orders concrete numeric scores, with ascending key ties.
        name = spec["order_by"]["field"]
        keys.sort(key=lambda key: (-expected[key][name], key[1], key[0]))
    return keys[:spec.get("predicate_limit", len(keys))]


def validate_envelope(row):
    if (set(row) != {"key", "fields", "metadata", "author_pub_key"}
            or set(row["key"]) != {"hash", "range"}):
        raise RuntimeError("A condition row changes its four-key envelope or complete key coordinates")
    writers = {value["writer_pubkey"] for value in row["metadata"].values()
               if value.get("writer_pubkey")}
    # The product chooses a writer from its original randomized metadata map,
    # then filters an empty selection. Do not impose a new author tie order.
    if not writers or any(value.get("writer_pubkey") == "" for value in row["metadata"].values()):
        writers.add(None)
    if row["author_pub_key"] not in writers:
        raise RuntimeError("A condition row changes the author selected from its projected metadata")
    if "body" in row["fields"]:
        numbers = row["fields"]["body"]["numbers"]
        if (set(numbers) != set(JSON_NUMBERS) or any(type(numbers[name]) is not type(value)
                or numbers[name] != value for name, value in JSON_NUMBERS.items())
                or math.copysign(1, numbers["negative_zero"]) != -1):
            raise RuntimeError("A condition body changes an integer boundary, float type, or negative zero")


def validate(result, expected, spec, reference=None, *, conditioned=True):
    keys = selected(expected, spec)
    wanted = keys[spec.get("offset", 0):spec.get("offset", 0) + spec["limit"]]
    rows = result.get("results", [])
    actual = [(row["key"].get("hash"), row["key"].get("range")) for row in rows]
    if (actual != wanted or result.get("returned_count") != len(wanted)
            or result.get("total_count") != (len(keys) if conditioned else None)
            or result.get("has_more") != (spec.get("offset", 0) + len(wanted) < len(keys))
            or result.get("next_cursor") is not None
            or result.get("unresolved_rows") != 0 or result.get("tombstoned_rows") != 0):
        raise RuntimeError("A keyed condition response differs in membership, order, count, or offset/limit")
    fields = spec["fields"] or FIELDS
    for key, row in zip(wanted, rows):
        validate_envelope(row)
        projected = {name: expected[key][name] for name in ["bucket", *fields] if name in expected[key]}
        if row["fields"] != projected or set(row["metadata"]) != set(projected):
            raise RuntimeError("A keyed condition response changes its exact projected fields or metadata fields")
        if reference is None:
            marker = expected[key]["title"].split("-class-", 1)[0]
            if any(row["metadata"][name].get("source_file_name") != marker for name in ("title", "body")):
                raise RuntimeError("The condition metadata reference lacks the generated selected-source marker")
        if reference is not None:
            metadata = {name: reference[key]["metadata"][name] for name in projected}
            if row["metadata"] != metadata:
                raise RuntimeError("A keyed condition response changes record, field key, writer, or source metadata")
    return rows
