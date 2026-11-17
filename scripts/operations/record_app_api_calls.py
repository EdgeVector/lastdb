"""Requests and mutation shapes shared by the app API operation."""
import json

from harness import send_unix_request
from record_runs import held, require
import v1_records as v1

BY_ID, BY_BUCKET = "AppApiById", "AppApiByBucket"


def read(node, *reads):
    return v1.read_items(node, reads)


def write(node, *mutations):
    return v1.write_items(node, mutations)["revisions"]


def create(schema, key, record, alternates=None, database=None):
    return v1.create_item(schema, key, record, alternates=alternates, database=database)


def by_id(name, bucket, value, schema=BY_ID, database=None):
    return create(schema, {"hash": name}, {"id": name, "bucket": bucket, "value": value},
                  {"by_bucket": {"hash": bucket, "range": name}}, database)


def replace(name, bucket, value, revision):
    return {**by_id(name, bucket, value), "op": "replace", "revision": revision}


def delete(schema, key, revision):
    return v1.delete_item(schema, key, revision)


def stored(node, name, schema=BY_ID, database=None):
    records = read(node, v1.point(schema, {"hash": name}, database=database))[0]["records"]
    require(len(records) <= 1, f"{schema} {name} returned {len(records)} records")
    return records[0] if records else None


def value_of(node, name, **options):
    row = stored(node, name, **options)
    return held(row["record"]).get("value") if row else None


def error_body(node, path, body):
    status, raw, _, _, _ = send_unix_request(
        node.socket, "POST", path, body, {"Content-Type": "application/json"})
    return status, json.loads(raw)


def conflict(node, *mutations, index=None, reason=None):
    status, reply = error_body(node, "/v1/write", {"mutations": list(mutations)})
    error = reply["error"]
    require(status == 409 and error["code"] == "conflict",
            f"a write whose condition does not hold was not refused: {status} {reply!r}")
    if index is None:
        return
    sentence = {
        "exists": "a record already holds the key",
        "absent": "no record holds the key",
        "revision": "the record is at another revision",
    }[reason]
    require(error.get("index") == index and error.get("reason") == reason
            and error.get("message") == f"mutation {index}: {sentence}",
            f"a refused write named the wrong mutation: {reply!r}")
