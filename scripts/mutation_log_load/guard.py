"""Refuse every target except the DEV mutation-log table in us-west-2.

DEV and PROD share account 152335099025, so the region is the guard.
The only table is ``ExememMutationLog-dev``. The database id is
``c1da11d0-0000-4000-8000-`` plus 12 hex characters from SHA-256 of the run
id. LastDB CEO approved LeadingKeys ``c1da11d0-0000-4000-8000-*``.
DescribeTable belongs in a separate IAM statement on the table ARN only.
This module does not change IAM. Snapshot rows live in the ``<id>#snap``
partition of the same table.
"""

import hashlib
import json
import re


DEV_TABLE = "ExememMutationLog-dev"
DEV_REGION = "us-west-2"
DEV_ACCOUNT = "152335099025"
STREAM_PREFIX = "c1da11d0-0000-4000-8000-"
STREAM_ID = re.compile(STREAM_PREFIX + r"[0-9a-f]{12}")
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
    return allow_stream(STREAM_PREFIX + digest[:12])


def snap_partition(database_id):
    """The snapshot partition is exactly the load id plus ``#snap``."""
    return allow_stream(database_id) + "#snap"


def sub_partition(database_id):
    """Submission records live in exactly the load id plus ``#sub``."""
    return allow_stream(database_id) + "#sub"


def writes_partition(database_id):
    """Accepted-write facts use exactly the load id plus ``#writes``."""
    return allow_stream(database_id) + "#writes"


def _record_label(value):
    if not isinstance(value, str) or not value or "#" in value:
        raise GuardError("record")
    return value


def submission_key(node, device, writer_seq):
    """Sort key on ``{id}#sub``: ``sub#{node}#{device}#{writer_seq:020}``."""
    _record_label(node)
    _record_label(device)
    if isinstance(writer_seq, bool) or not isinstance(writer_seq, int) or writer_seq < 0:
        raise GuardError("record")
    return "sub#{0}#{1}#{2:020d}".format(node, device, writer_seq)


def snap_manifest_key(reference):
    """Manifest sort key on ``{id}#snap``: ``snap#{reference}``."""
    return "snap#" + _record_label(reference)


def snap_part_key(reference, index):
    """Chunk sort key on ``{id}#snap``: ``snap#{reference}#{index:06}``."""
    _record_label(reference)
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise GuardError("record")
    return "snap#{0}#{1:06d}".format(reference, index)


def load_partitions(database_id):
    """The exact partitions owned by this one DEV load."""
    base = allow_stream(database_id)
    return (base, snap_partition(base), sub_partition(base), writes_partition(base))


def allow_partition(value):
    for suffix, builder in (("#snap", snap_partition), ("#sub", sub_partition),
                            ("#writes", writes_partition)):
        if isinstance(value, str) and value.endswith(suffix):
            base = value[:-len(suffix)]
            if value != builder(base):
                raise GuardError("database")
            return value
    return allow_stream(value)


def continue_page(start, previous):
    """True when DynamoDB returned a new continuation key."""
    if not start:
        return False
    if previous is not None and start == previous:
        raise GuardError("page")
    return True


def query_request(table, database_id):
    allow_table(table)
    allow_partition(database_id)
    return {
        "TableName": table,
        "KeyConditionExpression": "database_id = :id",
        "ExpressionAttributeValues": {":id": {"S": database_id}},
        "ConsistentRead": True,
    }


def delete_batch(table, database_id, record_ids):
    allow_table(table)
    allow_partition(database_id)
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
    allow_partition(database_id)
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
    allow_partition(database_id)
    ids = []
    for item in response.get("Items", []):
        if item.get("database_id", {}).get("S") != database_id:
            raise GuardError("database")
        record_id = item.get("record_id", {}).get("S")
        if not isinstance(record_id, str) or not record_id:
            raise GuardError("record")
        ids.append(record_id)
    return ids
