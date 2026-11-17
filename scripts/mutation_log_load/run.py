#!/usr/bin/env python3
"""Scoped DEV mutation-log load and private recovery.

The fixed DEV CI account signs every request as a LastDBNode token.
Create also sends that account's API key. The run creates the load
database, drives the writers, and leaves cleanup to a later step so a
cleanup failure cannot replace the load's status.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import random
import sys
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
_HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(_HERE), str(_HERE.parent)]

from cost import Ledger, format_estimate  # noqa: E402
from drive import LoadError, Session, drive  # noqa: E402
from faults import (MAX_CONTENTION_TRIES, backoff_seconds, format_summary, is_contention,  # noqa: E402
                    is_throttle, render_summaries, worst_exit, write_summaries)
from guard import (DEV_TABLE, GuardError, allow_endpoint, allow_region_environment,  # noqa: E402
                   allow_role, allow_stream, allow_table, allow_table_arn, aws_dynamodb,
                   batch_delete_keys, database_id_for_run, iter_partition_keys,
                   load_endpoints, load_partitions, refuse_prod_environment, writes_partition)
from harness import NoRedirect  # noqa: E402


REGISTRY = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"
DEV_ENVIRONMENT = "dev"
MEMBER_LIST_REVISION = "0"
AUTHORITY_DOMAIN = b"lastdb:share:node-read-authority:v1"
HEADER_LIMIT = 4096
STEADY_MS = 3 * 60 * 1000
WRITER_LEVELS = ("5", "20", "100")
LEVELS = (*WRITER_LEVELS, "all", "private")
PRELIMINARY_LABEL = "finished 10-append run"
SCALE_LABEL = "3-minute scale run"


class TokenError(Exception):
    """The token does not match the server rule."""


class NodeKey:
    def __init__(self, private):
        self.private = private
        self.public = private.public_key().public_bytes_raw()

    @classmethod
    def generate(cls):
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        return cls(Ed25519PrivateKey.generate())

    @classmethod
    def from_seed(cls, seed):
        if len(seed) != 32:
            raise TokenError("key")
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        return cls(Ed25519PrivateKey.from_private_bytes(seed))

    def sign(self, message):
        return self.private.sign(message)


def account_id(public_key):
    if len(public_key) != 32:
        raise TokenError("key")
    return hashlib.sha256(public_key).digest()[:16].hex()


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _canonical(value):
    """RFC 8785 for the strings, integers, arrays, and objects this digest uses."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, list):
            return b"[" + b",".join(_canonical(item) for item in value) + b"]"
        if isinstance(value, dict):
            return b"{" + b",".join(_canonical(k) + b":" + _canonical(value[k]) for k in sorted(value)) + b"}"
        raise TokenError("canonical")
    return str(value).encode("ascii")


def _authority_digest(covered):
    canonical = _canonical(covered)
    framed = (
        len(AUTHORITY_DOMAIN).to_bytes(4, "big") + AUTHORITY_DOMAIN
        + len(canonical).to_bytes(8, "big") + canonical
    )
    return hashlib.sha256(framed).hexdigest()


def authorization(key, path, database_id, issued_at_ms, body, account_generation):
    """Sign a live DEV LastDBNode bearer token for an owned stream."""
    nonce = os.urandom(32).hex()
    if not isinstance(body, bytes) or not isinstance(account_generation, str):
        raise TokenError("body")
    if not account_generation.isdigit() or (account_generation.startswith("0") and account_generation != "0"):
        raise TokenError("account")
    issued = str(int(issued_at_ms))
    public = key.public.hex()
    stream = {
        "scope": {"account_generation": account_generation, "account_id": account_id(key.public)},
        "stream_id": database_id,
    }
    covered = [
        2, database_id, stream, "POST", path, hashlib.sha256(body).hexdigest(),
        issued, nonce, MEMBER_LIST_REVISION, public, DEV_ENVIRONMENT,
    ]
    digest = _authority_digest(covered)
    token_json = json.dumps({
        "version": 2, "environment": DEV_ENVIRONMENT, "database_id": database_id,
        "stream": stream, "method": "POST", "path": path, "issued_at_ms": issued,
        "nonce": nonce, "member_list_revision": MEMBER_LIST_REVISION, "node_public_key": public,
    }, separators=(",", ":")).encode("utf-8")
    header = "LastDBNode " + _b64url(token_json) + "." + _b64url(key.sign(digest.encode("ascii")))
    if len(header.encode("ascii")) > HEADER_LIMIT:
        raise TokenError("header")
    return header


class Scenario:
    def __init__(self, kind, writers, steady_ms=None):
        self.kind = kind
        self.writers = writers
        self.steady_ms = steady_ms


def scenario(level, scale=False):
    if level not in WRITER_LEVELS:
        raise ValueError("level")
    if level == "100" and scale:
        return Scenario("steady", 100, STEADY_MS)
    return Scenario("full", int(level))


def phases(level, scale=False):
    levels = WRITER_LEVELS if level == "all" else (level,)
    return [scenario(name, scale) for name in levels]


def _note(status, path, parsed):
    """One stderr line for a failure that is not a retried head conflict."""
    if status < 400:
        return
    if isinstance(parsed, dict) and (is_contention(status, parsed) or is_throttle(status, parsed)):
        return
    code = field = "-"
    if isinstance(parsed, dict):
        code = str(parsed.get("code") or "-")[:80]
        field = str(parsed.get("field") or "-")[:80]
    print("http {0} {1} {2} {3}".format(
        status, path.rsplit("/", 1)[-1], code, field), file=sys.stderr)


def post(endpoint, key, database_id, method, path, body, api_key, account_generation):
    if method != "POST":
        raise GuardError("method")
    issued = time.time_ns() // 1_000_000
    header = authorization(
        key, path, database_id, issued, body, account_generation)
    headers = {"Authorization": header, "Content-Type": "application/json"}
    if path.endswith("/create-database"):
        headers["X-API-Key"] = api_key
    request = urllib.request.Request(
        endpoint + path, data=body, method=method, headers=headers)
    opener = urllib.request.build_opener(NoRedirect(GuardError("endpoint")))
    started = time.monotonic()
    try:
        with opener.open(request, timeout=60) as response:
            payload = response.read(2_000_000)
            status = response.status
    except urllib.error.HTTPError as error:
        payload = error.read(2_000_000)
        status = error.code
    elapsed = (time.monotonic() - started) * 1000
    try:
        parsed = json.loads(payload) if payload else {}
    except json.JSONDecodeError:
        parsed = None
    _note(status, path, parsed)
    return status, parsed, elapsed


def load_ci_key():
    """The enrolled DEV CI publisher. The phrase and the API key stay in process."""
    cloud = str(Path(__file__).resolve().parents[1] / "cloud_backup")
    if cloud not in sys.path:
        sys.path.insert(0, cloud)
    from support import ACCOUNT, OperationError, load_dev_owner

    try:
        key, api_key, generation, _phrase = load_dev_owner(ACCOUNT)
    except OperationError as error:
        raise TokenError(str(error)) from None
    return key, api_key, generation


def cleanup(table, database_id, ledger):
    for partition in load_partitions(database_id):
        purge(table, partition, ledger)


def keep_going(stopped):
    """A scaling result does not cancel the tiers that follow it."""
    return stopped not in ("error", "cap")


def partition_records(table, partition, ledger):
    return iter_partition_keys(table, partition, on_page=lambda: ledger.record("read"))


def verify_private_storage(table, database_id, ledger):
    """Inspect only keys for this run-owned database and its accepted-write facts."""
    if any(key.startswith(("tip#", "ack#"))
           for key in partition_records(table, database_id, ledger)):
        raise LoadError("private cloud stored a per-device progress row")
    records = list(partition_records(table, writes_partition(database_id), ledger))
    if not records or any(not record.startswith("write#") for record in records):
        raise LoadError("private cloud accepted-entry facts were absent or invalid")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write("No cloud upload-tip or download-ack rows were created. "
                      "The cloud retained immutable accepted-entry facts.\n")


def purge(table, partition, ledger):
    def stall(misses):
        if misses > MAX_CONTENTION_TRIES:
            raise GuardError("batch")
        time.sleep(backoff_seconds(misses, random.randrange))

    batch_delete_keys(
        table,
        partition,
        partition_records(table, partition, ledger),
        on_batch=lambda count: ledger.record("write", 64 * count),
        on_stall=stall,
    )


def target():
    dev, prod = load_endpoints(REGISTRY)
    refuse_prod_environment(prod, os.environ.values())
    endpoint = allow_endpoint(dev, dev, prod)
    table = allow_table(os.environ.get("LASTDB_MUTATION_LOG_DEV_TABLE", DEV_TABLE))
    allow_region_environment(os.environ)
    allow_role(os.environ["LASTDB_MUTATION_LOG_DEV_ROLE_ARN"])
    database_id = database_id_for_run(os.environ["GITHUB_RUN_ID"])
    allow_stream(database_id)
    described = aws_dynamodb("describe-table", {"TableName": table})
    allow_table_arn(described.get("Table", {}).get("TableArn", ""), table)
    return endpoint, table, database_id


def main(argv):
    parser = argparse.ArgumentParser(description="DEV mutation-log load")
    parser.add_argument("--phase", required=True, choices=("load", "cleanup"))
    parser.add_argument("--level", choices=LEVELS)
    parser.add_argument("--scale", action="store_true",
                        help="3-minute 100-writer clock. The default 100-writer tier appends 10 times and finishes.")
    parser.add_argument("--confirm-deployed", required=True)
    parser.add_argument("--ledger", required=True)
    parser.add_argument("--summary", required=True)
    args = parser.parse_args(argv)
    if args.confirm_deployed != "yes":
        raise GuardError("deployed")
    if args.phase == "load" and args.level is None:
        raise GuardError("level")
    endpoint, table, database_id = target()
    print("database {0}".format(database_id))
    ledger = Ledger()
    summary_path = Path(args.summary)
    if args.phase == "cleanup":
        saved = json.loads(Path(args.ledger).read_text(encoding="utf-8"))
        ledger.restore(saved)
        cleanup(table, database_id, ledger)
        text = format_estimate(ledger.snapshot())
        if summary_path.is_file():
            text += "\n" + render_summaries(json.loads(summary_path.read_text(encoding="utf-8")))
        print(text)
        return 0
    key, api_key, account_generation = load_ci_key()
    if args.level == "private":
        from private_recovery import recover as recover_private
        try:
            recover_private(post, endpoint, database_id, key, api_key, account_generation, ledger)
            verify_private_storage(table, database_id, ledger)
        finally:
            Path(args.ledger).write_text(json.dumps(ledger.snapshot()), encoding="utf-8")
        return 0
    reports = []

    def transport(method, path, body):
        return post(endpoint, key, database_id, method, path, body, api_key, account_generation)

    session = Session(database_id, transport, ledger, lambda: time.time_ns() // 1_000_000)
    label = SCALE_LABEL if args.scale else PRELIMINARY_LABEL
    print(label)
    try:
        for spec in phases(args.level, args.scale):
            report = drive(spec, session)
            reports.append(report)
            report["summary"]["run"] = label
            print("tier {0} writers={1}".format(spec.kind, spec.writers))
            print(format_summary(report["summary"]))
            if report.get("message"):
                print(report["message"], file=sys.stderr)
            if not keep_going(report["stopped"]):
                break
    finally:
        Path(args.ledger).write_text(json.dumps(ledger.snapshot()), encoding="utf-8")
        if reports:
            write_summaries(summary_path, reports)
            print(format_estimate(ledger.snapshot()))
    return worst_exit(report["stopped"] for report in reports)


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except (GuardError, LoadError, TokenError, KeyError, OSError) as error:
        print("mutation-log load refused: {0}".format(error), file=sys.stderr)
        sys.exit(1)
