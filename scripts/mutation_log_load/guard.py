"""Refuse every target except the DEV mutation-log load prefix.

The only DynamoDB table is ``ExememMutationLog-dev``. The only object
prefix is ``mutation-log/c1da11d0-load-``. The only HTTP endpoint is the
DEV Exemem API from ``folddb_profile/environments.json``. A PROD
endpoint, any other table, or any other prefix is a hard failure.
"""

import json
import re

DEV_TABLE = "ExememMutationLog-dev"
LOAD_PREFIX = "mutation-log/c1da11d0-load-"
DEV_ACCOUNT = "152335099025"
ROLE_ARN = re.compile(
    r"arn:aws:iam::" + DEV_ACCOUNT + r":role/[A-Za-z0-9+=,.@_-]{1,64}")
DATABASE_ID = re.compile(r"c1da11d0-load-[A-Za-z0-9-]{1,80}")


class GuardError(Exception):
    """The call would leave the DEV load target."""


def load_endpoints(path):
    """Return ``(dev_endpoint, prod_endpoint)`` from the URL registry."""
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


def allow_table(name):
    if name != DEV_TABLE:
        raise GuardError("table")
    return name


def allow_table_arn(arn, table):
    expected = ":table/" + allow_table(table)
    if not arn.endswith(expected) or DEV_ACCOUNT not in arn or "prod" in arn.lower():
        raise GuardError("table")
    return arn


def refuse_prod_environment(prod_endpoint, values):
    """A PROD URL in the process environment is a hard failure."""
    for value in values:
        if prod_endpoint in str(value):
            raise GuardError("endpoint")


def allow_prefix(prefix):
    if not prefix.startswith(LOAD_PREFIX) or prefix != prefix.strip():
        raise GuardError("prefix")
    if ".." in prefix or "\\" in prefix or prefix.startswith("/"):
        raise GuardError("prefix")
    rest = prefix[len("mutation-log/"):]
    database = rest.split("/", 1)[0]
    if not DATABASE_ID.fullmatch(database):
        raise GuardError("prefix")
    return prefix


def allow_bucket(name):
    lowered = name.lower()
    if "prod" in lowered or not lowered.endswith("-dev"):
        raise GuardError("bucket")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", name):
        raise GuardError("bucket")
    return name


def allow_role(arn):
    if not ROLE_ARN.fullmatch(arn) or "prod" in arn.lower():
        raise GuardError("role")
    return arn


def database_id_for_run(run_id):
    if not re.fullmatch(r"[1-9][0-9]{0,18}", run_id):
        raise GuardError("database")
    database_id = "c1da11d0-load-" + run_id
    allow_prefix("mutation-log/" + database_id + "/")
    return database_id


def prefix_for(database_id):
    prefix = "mutation-log/" + database_id + "/"
    return allow_prefix(prefix)


def cleanup_plan(table, prefix, bucket):
    """Query and list shapes. Both stay inside the load target."""
    allow_table(table)
    allow_prefix(prefix)
    allow_bucket(bucket)
    database_id = prefix[len("mutation-log/"):].split("/", 1)[0]
    return {
        "dynamodb": {
            "operation": "Query",
            "TableName": table,
            "KeyConditionExpression": "database_id = :id",
            "ExpressionAttributeValues": {":id": {"S": database_id}},
        },
        "s3": {"operation": "ListObjectsV2", "Bucket": bucket, "Prefix": prefix},
    }


def retain_keys(keys, prefix):
    """Keep only keys under ``prefix``. A stranger key is a hard failure."""
    allow_prefix(prefix)
    chosen = []
    for key in keys:
        if not isinstance(key, str) or not key.startswith(prefix):
            raise GuardError("prefix")
        chosen.append(key)
    return chosen
