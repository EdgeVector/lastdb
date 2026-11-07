"""Bounded DEV-only control I/O; captured credential values never enter output."""

import json
import os
import re
import subprocess
import time
import uuid
from urllib.request import HTTPRedirectHandler, Request, build_opener

from shared_origin import owner
from support import OperationError, decode_json, require


ACCOUNT = "152335099025"
REGION = "us-west-2"
TABLE = "ExememCloudControl-dev"
LEDGER = "ci#lastdb-shared-daily-v1"
PREFIX = "c1da11d0-"
MAX_SCOPES = 12
GRACE_MS = 60 * 60 * 1000
MAX_AGE_MS = 26 * 60 * 60 * 1000
CASES = ("lifecycle", "conflicts", "retention", "automatic")


def now_ms():
    return time.time_ns() // 1000000


def aws(service, operation, payload=None, *, target=None, credentials=None):
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true",
                       AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="")
    region = target["region"] if target else REGION
    endpoint = target["endpoint"] if target else f"https://{service}.{REGION}.amazonaws.com"
    if credentials is not None:
        environment.update(AWS_ACCESS_KEY_ID=credentials[0], AWS_SECRET_ACCESS_KEY=credentials[1])
        environment.pop("AWS_SESSION_TOKEN", None)
        environment.pop("AWS_SECURITY_TOKEN", None)
    command = ["aws", service, operation, "--region", region, "--endpoint-url", endpoint,
               "--output", "json", "--no-cli-pager", "--no-cli-auto-prompt", "--no-paginate"]
    if payload is not None:
        encoded = json.dumps(payload, separators=(",", ":"))
        require(len(encoded) <= 256 * 1024, "shared_daily_request_bound")
        command += ["--cli-input-json", encoded]
    result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                            env=environment, timeout=40, check=False)
    if result.returncode:
        match = re.search(rb"An error occurred \(([A-Za-z0-9]+)\)", result.stderr)
        code = match[1].decode() if match else "refused"
        raise OperationError(f"shared_daily_{service}_{operation}_{code}")
    require(len(result.stdout) <= 2 * 1024 * 1024, "shared_daily_response_bound")
    return decode_json(result.stdout) if result.stdout.strip() else {}


def key(partition, record):
    return {"account_id": {"S": partition}, "record_id": {"S": record}}


def item(partition, record, value, revision=1):
    return {**key(partition, record), "revision": {"N": str(revision)},
            "json": {"S": json.dumps(value, separators=(",", ":"), sort_keys=True)}}


def get(partition, record):
    return aws("dynamodb", "get-item", {"TableName": TABLE, "Key": key(partition, record),
                                         "ConsistentRead": True}).get("Item")


def body(row):
    return decode_json(row["json"]["S"].encode())


def query(partition, limit=4096):
    rows, cursor = [], None
    for _ in range(33):
        request = {"TableName": TABLE, "KeyConditionExpression": "account_id = :p",
                   "ExpressionAttributeValues": {":p": {"S": partition}},
                   "ConsistentRead": True, "Limit": min(limit + 1, 128)}
        if cursor:
            request["ExclusiveStartKey"] = cursor
        response = aws("dynamodb", "query", request)
        rows.extend(response.get("Items", []))
        require(len(rows) <= limit, "shared_daily_control_directory_bound")
        cursor = response.get("LastEvaluatedKey")
        if not cursor:
            return rows
    raise OperationError("shared_daily_control_page_bound")


def database_id(value):
    parsed = uuid.UUID(value)
    require(str(parsed) == value and parsed.version == 4 and value.startswith(PREFIX),
            "shared_daily_owned_database")
    return value


def admission():
    identity = owner(os.environ.get("GITHUB_SHA", ""))
    require(identity["workflow"].endswith("/cloud-shared-daily.yml"), "shared_daily_workflow")
    caller = aws("sts", "get-caller-identity")
    require(caller["Account"] == ACCOUNT and caller["Arn"].startswith(
        f"arn:aws:sts::{ACCOUNT}:assumed-role/LastDBSharedDailyDev/"), "shared_daily_operator_role")
    table = aws("dynamodb", "describe-table", {"TableName": TABLE})["Table"]
    require(table["TableArn"] == f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{TABLE}"
            and table["TableStatus"] == "ACTIVE", "shared_daily_dev_table")
    return identity


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise OperationError("shared_daily_github_redirect_refused")


def github(path):
    require(re.fullmatch(r"(?:actions/(?:runs|artifacts)/[1-9][0-9]{0,19}|commits/[0-9a-f]{40}/check-runs\?per_page=100)",
                         path), "shared_daily_github_path")
    request = Request("https://api.github.com/repos/EdgeVector/lastdb/" + path,
                      headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"],
                               "Accept": "application/vnd.github+json"})
    with build_opener(NoRedirect()).open(request, timeout=20) as response:
        raw = response.read(2 * 1024 * 1024 + 1)
        require(response.status == 200 and len(raw) <= 2 * 1024 * 1024, "shared_daily_github_response")
        return decode_json(raw)


def terminal(identity):
    require(identity["repository"] == "EdgeVector/lastdb"
            and identity["workflow"].endswith("/cloud-shared-daily.yml")
            and re.fullmatch(r"[1-9][0-9]{0,19}", identity["run_id"]), "shared_daily_ledger_owner")
    value = github("actions/runs/" + identity["run_id"])
    require(value["head_sha"] == identity["source"] and value["head_branch"] == "main"
            and value["path"] == identity["workflow"], "shared_daily_owner_readback")
    return value["status"] == "completed"
