"""Refuse every target except the DEV mutation-log table in us-west-2.

DEV and PROD share account 152335099025, so the region is the guard.
The only table is ``ExememMutationLog-dev``. The database id is a protocol
Id: a UUID whose first group is ``c1da11d0``, derived from the run id.
Snapshot bytes are rows in that table. This module has no object-store target.
"""

import hashlib
import json
import re


DEV_TABLE = "ExememMutationLog-dev"
DEV_REGION = "us-west-2"
DEV_ACCOUNT = "152335099025"
STREAM_ID = re.compile(
    r"c1da11d0-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
RUN_ID = re.compile(r"[1-9][0-9]{0,18}")
ROLE_ARN = re.compile(
    r"arn:aws:iam::" + DEV_ACCOUNT + r":role/[A-Za-z0-9+=,.@_-]{1,64}")
BATCH_LIMIT = 25


class GuardError(Exception):
    """The call would leave the DEV load target."""


def load_endpoints(path):
    value = json.loads(path.read_text(encoding="utf-8"))
    environments = value["environments"]
    dev = environments["dev"]["exemem_api"].rstrip("/")
    prod = environments["prod"]["exemem_api"].rstrip("/")
    if dev == prod or not dev.startswith("https://") or not prod.startswith("https://"):
        raise GuardError("endpoint")
    return dev, prod


def allow_endpoint(endpoint, dev, prod):
    chosen = endpoint.rstrip("/")
    if chosen == prod.rstrip("/") or chosen != dev.rstrip("/"):
        raise GuardError("endpoint")
    return chosen


def refuse_prod_environment(prod_endpoint, values):
    for value in values:
        if prod_endpoint and prod_endpoint in str(value):
            raise GuardError("endpoint")


def allow_region(region):
    if region != DEV_REGION:
        raise GuardError("region")
    return region


def allow_region_environment(environ):
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        value = environ.get(name)
        if value not in (None, "", DEV_REGION):
            raise GuardError("region")
    return DEV_REGION


def allow_table(name):
    if name != DEV_TABLE:
        raise GuardError("table")
    return name


def allow_table_arn(arn, table):
    expected = "arn:aws:dynamodb:{0}:{1}:table/{2}".format(
        allow_region(DEV_REGION), DEV_ACCOUNT, allow_table(table))
    if arn != expected:
        raise GuardError("table")
    return arn


def allow_role(arn):
    if not ROLE_ARN.fullmatch(arn) or "prod" in arn.lower():
        raise GuardError("role")
    return arn


def allow_stream(value):
    if not isinstance(value, str) or len(value) > 64 or not STREAM_ID.fullmatch(value):
        raise GuardError("database")
    return value


def database_id_for_run(run_id):
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise GuardError("database")
    digest = hashlib.sha256(run_id.encode("ascii")).hexdigest()
    return allow_stream("c1da11d0-{0}-{1}-{2}-{3}".format(
        digest[0:4], digest[4:8], digest[8:12], digest[12:24]))


def query_request(table, database_id):
    allow_table(table)
    allow_stream(database_id)
    return {
        "TableName": table,
        "KeyConditionExpression": "database_id = :id",
        "ExpressionAttributeValues": {":id": {"S": database_id}},
        "ConsistentRead": True,
    }


def delete_batch(table, database_id, record_ids):
    allow_table(table)
    allow_stream(database_id)
    if not record_ids or len(record_ids) > BATCH_LIMIT:
        raise GuardError("batch")
    deletes = []
    for record_id in record_ids:
        if not isinstance(record_id, str) or not record_id or len(record_id) > 1024:
            raise GuardError("record")
        deletes.append({"DeleteRequest": {"Key": {
            "database_id": {"S": database_id},
            "record_id": {"S": record_id},
        }}})
    return {"RequestItems": {table: deletes}}


def unprocessed_ids(response, table, database_id):
    """Ids still to delete. A key for another database is a hard failure."""
    allow_table(table)
    allow_stream(database_id)
    pending = response.get("UnprocessedItems", {})
    if not isinstance(pending, dict) or set(pending) - {table}:
        raise GuardError("table")
    ids = []
    for item in pending.get(table, []):
        key = item.get("DeleteRequest", {}).get("Key", {})
        if key.get("database_id", {}).get("S") != database_id:
            raise GuardError("database")
        record_id = key.get("record_id", {}).get("S")
        if not isinstance(record_id, str) or not record_id:
            raise GuardError("record")
        ids.append(record_id)
    return ids


def record_ids_from_query(response, database_id):
    allow_stream(database_id)
    ids = []
    for item in response.get("Items", []):
        if item.get("database_id", {}).get("S") != database_id:
            raise GuardError("database")
        record_id = item.get("record_id", {}).get("S")
        if not isinstance(record_id, str) or not record_id:
            raise GuardError("record")
        ids.append(record_id)
    return ids
