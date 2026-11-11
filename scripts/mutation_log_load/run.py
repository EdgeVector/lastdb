#!/usr/bin/env python3
"""DEV mutation-log load. Refuses every target except the load prefix.

Unit tests do not call this command. It sends signed requests only to the
DEV Exemem API from the URL registry, and it deletes only the
``c1da11d0-load-`` database it created.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import Ledger, format_estimate
from drive import drive
from guard import (DEV_TABLE, GuardError, allow_bucket, allow_endpoint,
                   allow_role, allow_table, allow_table_arn, cleanup_plan,
                   database_id_for_run, load_endpoints, prefix_for,
                   refuse_prod_environment, retain_keys)
from plan import scenario
from sign import headers
from stats import format_summary


REGISTRY = Path(__file__).resolve().parents[2] / "folddb_profile" / "environments.json"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise GuardError("endpoint")


def post(endpoint, secret, method, path, body, database_id):
    timestamp = time.time_ns() // 1_000_000
    signed = headers(secret, method, path, database_id, timestamp, body)
    request = urllib.request.Request(
        endpoint + path, data=body, method=method, headers=signed)
    opener = urllib.request.build_opener(NoRedirect)
    started = time.monotonic()
    try:
        with opener.open(request, timeout=20) as response:
            response.read(1024)
            status = response.status
    except urllib.error.HTTPError as error:
        error.read(1024)
        status = error.code
    return status, (time.monotonic() - started) * 1000


def aws(service, operation, payload):
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true",
                       AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="")
    command = ["aws", service, operation, "--region", "us-west-2",
               "--output", "json", "--no-cli-pager", "--cli-input-json",
               json.dumps(payload, separators=(",", ":"))]
    result = subprocess.run(command, capture_output=True, env=environment,
                            timeout=40, check=False)
    if result.returncode:
        raise GuardError("aws")
    if not result.stdout.strip():
        return {}
    return json.loads(result.stdout)


def cleanup(table, prefix, bucket, database_id, ledger):
    plan = cleanup_plan(table, prefix, bucket)
    query = aws("dynamodb", "query", plan["dynamodb"])
    ledger.record()
    for item in query.get("Items", []):
        found = item.get("database_id", {}).get("S")
        if found != database_id or "sk" not in item:
            raise GuardError("table")
        aws("dynamodb", "delete-item", {
            "TableName": table,
            "Key": {"database_id": {"S": database_id}, "sk": item["sk"]},
        })
        ledger.record()
    listed = aws("s3api", "list-objects-v2", plan["s3"])
    ledger.record()
    keys = retain_keys([row["Key"] for row in listed.get("Contents", [])], prefix)
    for key in keys:
        aws("s3api", "delete-object", {"Bucket": bucket, "Key": key})
        ledger.record()


def main(argv):
    parser = argparse.ArgumentParser(description="DEV mutation-log load")
    parser.add_argument("--level", required=True, choices=("5", "20", "100", "trim"))
    parser.add_argument("--confirm-deployed", required=True)
    args = parser.parse_args(argv)
    if args.confirm_deployed != "yes":
        raise GuardError("deployed")
    dev, prod = load_endpoints(REGISTRY)
    refuse_prod_environment(prod, os.environ.values())
    endpoint = allow_endpoint(dev, dev, prod)
    table = allow_table(os.environ.get("LASTDB_MUTATION_LOG_DEV_TABLE", DEV_TABLE))
    bucket = allow_bucket(os.environ["LASTDB_MUTATION_LOG_DEV_BUCKET"])
    allow_role(os.environ["LASTDB_MUTATION_LOG_DEV_ROLE_ARN"])
    secret = os.environ["LASTDB_MUTATION_LOG_DEV_SIGNING_KEY"].encode("utf-8")
    database_id = database_id_for_run(os.environ["GITHUB_RUN_ID"])
    prefix = prefix_for(database_id)
    described = aws("dynamodb", "describe-table", {"TableName": table})
    allow_table_arn(described.get("Table", {}).get("TableArn", ""), table)
    ledger = Ledger()
    report = {"stopped": "error", "summary": None}
    try:
        def transport(method, path, body):
            return post(endpoint, secret, method, path, body, database_id)

        report = drive(scenario(args.level), database_id, transport, ledger,
                       lambda: time.monotonic() * 1000)
    finally:
        cleanup(table, prefix, bucket, database_id, ledger)
        text = format_estimate(ledger.estimate())
        if report.get("summary") is not None:
            text = text + "\n" + format_summary(report["summary"])
        print(text)
    return 2 if report["stopped"] == "cap" else 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except (GuardError, KeyError) as error:
        print("mutation-log load refused: {0}".format(error), file=sys.stderr)
        sys.exit(1)
