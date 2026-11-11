#!/usr/bin/env python3
"""DEV mutation-log load. Unit tests do not call this command.

One throwaway Ed25519 key signs every request as a LastDBNode token.
The run creates the load database, drives the writers, and leaves cleanup
to a later step so a cleanup failure cannot replace the load's status.
"""

import argparse
import json
import os
import random
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import Ledger, format_estimate
from drive import LoadError, Session, drive
from faults import MAX_CONTENTION_TRIES, backoff_seconds, worst_exit
from guard import (DEV_REGION, DEV_TABLE, GuardError, allow_endpoint,
                   allow_region_environment, allow_role, allow_stream,
                   allow_table, allow_table_arn, continue_page, database_id_for_run,
                   delete_batch, load_endpoints, query_request, record_ids_from_query,
                   load_partitions, refuse_prod_environment, unprocessed_ids)
from plan import phases
from stats import format_summary
from node_auth import NodeKey, TokenError, account_id, authorization


REGISTRY = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise GuardError("endpoint")


def post(endpoint, key, database_id, method, path, body):
    if method != "POST":
        raise GuardError("method")
    issued = time.time_ns() // 1_000_000
    header, _encoded = authorization(key, path, database_id, issued, body)
    request = urllib.request.Request(
        endpoint + path, data=body, method=method,
        headers={"Authorization": header, "Content-Type": "application/json"})
    opener = urllib.request.build_opener(NoRedirect)
    started = time.monotonic()
    try:
        with opener.open(request, timeout=20) as response:
            payload = response.read(65536)
            status = response.status
    except urllib.error.HTTPError as error:
        payload = error.read(65536)
        status = error.code
    elapsed = (time.monotonic() - started) * 1000
    try:
        parsed = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        parsed = None
    return status, parsed, elapsed


def aws(service, operation, payload):
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true",
                       AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="",
                       AWS_REGION=DEV_REGION, AWS_DEFAULT_REGION=DEV_REGION)
    command = ["aws", service, operation, "--region", DEV_REGION, "--output", "json",
               "--no-cli-pager", "--cli-input-json",
               json.dumps(payload, separators=(",", ":"))]
    result = subprocess.run(command, capture_output=True, env=environment,
                            timeout=40, check=False)
    if result.returncode:
        raise GuardError("aws")
    if not result.stdout.strip():
        return {}
    return json.loads(result.stdout)


def cleanup(table, database_id, ledger):
    for partition in load_partitions(database_id):
        purge(table, partition, ledger)


def keep_going(stopped):
    """A scaling result does not cancel the tiers that follow it."""
    return stopped not in ("error", "cap")


def batch_retry(misses, pause, draw):
    """Backoff while BatchWriteItem returns the same unprocessed chunk."""
    misses += 1
    if misses > MAX_CONTENTION_TRIES:
        raise GuardError("batch")
    pause(backoff_seconds(misses, draw))
    return misses


def purge(table, partition, ledger, pause=None):
    pause = pause or time.sleep
    pending, start = [], None
    while True:
        request = query_request(table, partition)
        if start is not None:
            if start.get("database_id", {}).get("S") != partition:
                raise GuardError("database")
            request["ExclusiveStartKey"] = start
        found = aws("dynamodb", "query", request)
        ledger.record("read")
        pending.extend(record_ids_from_query(found, partition))
        nxt = found.get("LastEvaluatedKey")
        if not continue_page(nxt, start):
            break
        start = nxt
    misses = 0
    while pending:
        chunk = pending[:25]
        response = aws("dynamodb", "batch-write-item",
                       delete_batch(table, partition, chunk))
        ledger.record("write", 64 * len(chunk))
        left = unprocessed_ids(response, table, partition)
        if left == chunk:
            misses = batch_retry(misses, pause, random.randrange)
        else:
            misses = 0
        pending = left + pending[len(chunk):]


def target():
    dev, prod = load_endpoints(REGISTRY)
    refuse_prod_environment(prod, os.environ.values())
    endpoint = allow_endpoint(dev, dev, prod)
    table = allow_table(os.environ.get("LASTDB_MUTATION_LOG_DEV_TABLE", DEV_TABLE))
    allow_region_environment(os.environ)
    allow_role(os.environ["LASTDB_MUTATION_LOG_DEV_ROLE_ARN"])
    database_id = database_id_for_run(os.environ["GITHUB_RUN_ID"])
    allow_stream(database_id)
    described = aws("dynamodb", "describe-table", {"TableName": table})
    allow_table_arn(described.get("Table", {}).get("TableArn", ""), table)
    return endpoint, table, database_id


def main(argv):
    parser = argparse.ArgumentParser(description="DEV mutation-log load")
    parser.add_argument("--phase", required=True, choices=("load", "cleanup"))
    parser.add_argument("--level", choices=("5", "20", "100", "trim", "all"))
    parser.add_argument("--confirm-deployed", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args(argv)
    if args.confirm_deployed != "yes":
        raise GuardError("deployed")
    if args.phase == "load" and args.level is None:
        raise GuardError("level")
    endpoint, table, database_id = target()
    ledger = Ledger()
    summary_path = Path(args.summary)
    if args.phase == "cleanup":
        saved = json.loads(Path(args.ledger).read_text(encoding="utf-8"))
        ledger.restore(saved)
        cleanup(table, database_id, ledger)
        text = format_estimate(ledger.snapshot())
        if summary_path.is_file():
            text += "\n" + format_summary(json.loads(summary_path.read_text(encoding="utf-8")))
        print(text)
        return 0
    key = NodeKey.generate()
    ident = account_id(key.public)
    reports = []

    def transport(method, path, body):
        return post(endpoint, key, database_id, method, path, body)

    session = Session(database_id, ident, key.public.hex(), transport, ledger,
                      lambda: time.time_ns() // 1_000_000)
    try:
        for spec in phases(args.level):
            report = drive(spec, session)
            reports.append(report)
            print("tier {0} writers={1}".format(spec.kind, spec.writers))
            print(format_summary(report["summary"]))
            if report.get("message"):
                print(report["message"], file=sys.stderr)
            if not keep_going(report["stopped"]):
                break
    finally:
        Path(args.ledger).write_text(json.dumps(ledger.snapshot()), encoding="utf-8")
        if reports:
            summary_path.write_text(json.dumps(reports[-1]["summary"]), encoding="utf-8")
            print(format_estimate(ledger.snapshot()))
    return worst_exit(report["stopped"] for report in reports)


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except (GuardError, LoadError, TokenError, KeyError, OSError) as error:
        print("mutation-log load refused: {0}".format(error), file=sys.stderr)
        sys.exit(1)
