#!/usr/bin/env python3
"""DEV round trip for one owner signed-database snapshot.

Trusted GitHub Actions on main runs this. Pull requests do not. The records
are generated from a public label, and the script never prints an API key,
recovery phrase, or invite code.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

import baseline_data
from node import OwnedNode
from programs import Programs, execute
from support import OperationError, dev_endpoint, failure, require, write_private


RECORDS = 300_000
BODY_CHARS = 1024
BATCH = 100
QUERY_BATCH = 64
MAX_LOG_ENTRIES = 8
MAX_REPLAYED = 8
BUDGET_SECONDS = 60
SETTLE_FILES = 48
SCHEMA = "SnapshotGate"
LABEL = b"snapshot-gate-v1"
NAME = re.compile(r"[a-z][a-z0-9-]{0,30}")
LOAD_PREFIX = "c1da11d0-0000-4000-8000-"


def record_body(index):
    """1024 hex characters from SHA-256(label || index || block). Incompressible."""
    prefix = LABEL + index.to_bytes(8, "big")
    text = "".join(hashlib.sha256(prefix + bytes([block])).hexdigest() for block in range(16))
    require(len(text) == BODY_CHARS, "record_body_size")
    return text


def content_digest(count):
    hashed = hashlib.sha256()
    for index in range(count):
        hashed.update(record_body(index).encode("ascii"))
    return hashed.hexdigest()


def summary(line):
    print(line, flush=True)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def note(numbers, key, value):
    numbers[key] = value
    summary(f"{key}={value}")


def database_name():
    name = os.environ.get("SNAPSHOT_GATE_DATABASE", "gate")
    require(isinstance(name, str) and NAME.fullmatch(name) and name != "personal", "database_name")
    return name


def approved_id(digest, start):
    """An id in the DEV load prefix the cleanup role is allowed to delete."""
    text = LOAD_PREFIX + digest[start:start + 12]
    require(len(text) == 36, "database_id")
    return text


def gate_ids():
    material = os.environ["GITHUB_RUN_ID"] + ":" + os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    require(re.fullmatch(r"[1-9][0-9]{0,18}:[1-9][0-9]{0,4}", material), "run_id")
    digest = hashlib.sha256(material.encode("ascii")).hexdigest()
    return approved_id(digest, 0), approved_id(digest, 12)


def install_registry(home, gate_id, owner_id, name):
    folder = home / "databases"
    folder.mkdir(mode=0o700)
    write_private(folder / "registry.json", json.dumps({
        "format": 1,
        "owner": {"id": owner_id, "name": "personal", "kind": "personal", "spec": None},
        "owned": [{"id": gate_id, "name": name, "kind": "personal", "spec": None}],
    }).encode("ascii"))


def mutation(index):
    return {
        "type": "mutation", "schema": SCHEMA, "record": {"body": record_body(index)},
        "key_value": {"hash": f"{index:06d}", "range": None}, "alternates": {},
        "mutation_type": "create", "source_file_name": None, "metadata": None, "file": None,
        "expected": {"type": "absent"}, "durability": "durable", "convergence": "sync",
    }


def install_schema(node, database_id):
    node.call(f"/api/databases/{database_id}/schemas/seed-system", {
        "isolated_copy": True,
        "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "Hash",
            "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": ["body"], "field_types": {"body": "String"},
            "field_descriptions": {"body": "snapshot gate payload"},
            "field_versions": {"body": 1},
        },
    })


def write_records(node, database_id):
    route = f"/api/databases/{database_id}/mutations/batch"
    written = hashlib.sha256()
    for start in range(0, RECORDS, BATCH):
        indexes = range(start, min(start + BATCH, RECORDS))
        bodies = [record_body(index).encode("ascii") for index in indexes]
        for body in bodies:
            written.update(body)
        reply = node.call(route, {"mutations": [mutation(index) for index in indexes]})
        require(isinstance(reply, dict) and reply.get("count") == len(bodies), "record_write")
    return written.hexdigest()


def run_file_count(home, database_id):
    base = home / "databases" / database_id / "data"
    if not (base / "disk-run" / "runs").is_dir():
        return None
    count = 0
    for root in ("disk-run", "disk-cas"):
        folder = base / root / "runs"
        if folder.is_dir():
            count += sum(1 for path in folder.iterdir() if path.name.endswith(".ldrun"))
    return count


def wait_until_settled(home, database_id):
    previous = None
    deadline = time.monotonic() + 40 * 60
    while time.monotonic() < deadline:
        count = run_file_count(home, database_id)
        if count is not None and count <= SETTLE_FILES and count == previous:
            return count
        previous = count
        time.sleep(3)
    raise OperationError("snapshot_files_not_settled")


def cli(programs, home, arguments, timeout, input_bytes=None):
    command = [str(programs.directory / "lastdb"), "--data-dir", str(home), *arguments]
    started = time.monotonic()
    output = execute(command, programs.environment, input_bytes, timeout)
    return time.monotonic() - started, output


def parse_wait(output):
    lines = output.splitlines()
    require(len(lines) == 3, "snapshot_wait_output")
    through = prefixed(lines[0], b"snapshot_through=")
    head = prefixed(lines[1], b"log_head=")
    entries = prefixed(lines[2], b"log_entries=")
    require(through == head, "snapshot_through_head")
    require(entries <= MAX_LOG_ENTRIES, "snapshot_log_entries")
    return through, entries


def prefixed(line, prefix):
    require(line.startswith(prefix), "snapshot_wait_output")
    return int(line[len(prefix):])


def parse_restore(output):
    match = re.search(rb"^snapshot_through=([0-9]+) replayed=([0-9]+)$", output, re.M)
    require(match is not None, "restore_line")
    replayed = int(match.group(2))
    require(replayed <= MAX_REPLAYED, "restore_replayed")
    return int(match.group(1)), replayed


def rows_from_batch(reply):
    require(isinstance(reply, dict) and isinstance(reply.get("results"), list), "record_reply")
    found = {}
    for item in reply["results"]:
        require(isinstance(item, dict) and item.get("status") == 200, "record_status")
        response = item.get("response")
        require(isinstance(response, dict), "record_reply")
        page = response.get("data", response)
        require(isinstance(page, dict) and isinstance(page.get("results"), list), "record_reply")
        for row in page["results"]:
            key = row.get("key", {}).get("hash")
            body = row.get("fields", {}).get("body")
            require(isinstance(key, str) and isinstance(body, str), "record_reply")
            found[key] = body
    return found


def verify_records(node, database_id, digest):
    route = f"/api/databases/{database_id}/queries/batch"
    checked = hashlib.sha256()
    count = 0
    for start in range(0, RECORDS, QUERY_BATCH):
        indexes = range(start, min(start + QUERY_BATCH, RECORDS))
        queries = [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": f"{index:06d}"}}
                   for index in indexes]
        found = rows_from_batch(node.call(route, {"queries": queries}))
        for index in indexes:
            body = found.get(f"{index:06d}")
            require(body == record_body(index), "record_mismatch")
            checked.update(body.encode("ascii"))
            count += 1
    require(count == RECORDS and checked.hexdigest() == digest, "record_digest")
    return count


def live_write(node, database_id):
    route = f"/api/databases/{database_id}/mutations/batch"
    reply = node.call(route, {"mutations": [{
        "type": "mutation", "schema": SCHEMA, "record": {"body": "live"},
        "key_value": {"hash": "live", "range": None}, "alternates": {},
        "mutation_type": "create", "source_file_name": None, "metadata": None, "file": None,
        "expected": {"type": "absent"}, "durability": "durable", "convergence": "sync",
    }]})
    require(isinstance(reply, dict) and reply.get("count") == 1, "live_write")
    found = rows_from_batch(node.call(
        f"/api/databases/{database_id}/queries/batch",
        {"queries": [{"schema_name": SCHEMA, "limit": 1, "filter": {"HashKey": "live"}}]},
    ))
    require(found.get("live") == "live", "live_read")


def seed(programs, node, api_key, name, numbers):
    started = time.monotonic()
    connected = programs.command(node.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", baseline_data.ACCOUNT, "--json",
    ], input_bytes=api_key, timeout=60)
    require(connected.get("ok") is True and connected.get("credentials_saved") is True
            and connected.get("account_id") == baseline_data.ACCOUNT, "dev_connection")
    listed = programs.command(node.home, ["db", "list", "--json"], timeout=30)
    rows = listed.get("databases") if isinstance(listed, dict) else None
    require(isinstance(rows, list), "database_list")
    matched = [row for row in rows if row.get("name") == name and row.get("id") == numbers["database_id"]]
    require(len(matched) == 1 and matched[0].get("kind") == "personal", "database_registered")
    database_id = numbers["database_id"]
    install_schema(node, database_id)
    digest = write_records(node, database_id)
    require(digest == content_digest(RECORDS), "seed_digest")
    files = wait_until_settled(node.home, database_id)
    note(numbers, "database_id", database_id)
    note(numbers, "records", RECORDS)
    note(numbers, "digest", digest)
    note(numbers, "run_files", files)
    note(numbers, "seed_seconds", round(time.monotonic() - started, 3))
    return database_id, digest


def upload(programs, node, name, numbers):
    try:
        elapsed, output = cli(programs, node.home, [
            "backup", "snapshot", "--database", name, "--wait",
        ], BUDGET_SECONDS)
    except OperationError as error:
        if str(error) == "cli_deadline":
            raise OperationError("upload_budget") from None
        raise
    require(elapsed < BUDGET_SECONDS, "upload_budget")
    through, entries = parse_wait(output)
    note(numbers, "upload_seconds", round(elapsed, 3))
    note(numbers, "snapshot_through", through)
    note(numbers, "log_entries", entries)


def restore(programs, source, destination, database_id, phrase, numbers):
    try:
        elapsed, output = cli(programs, source, [
            "backup", "restore", "--database", database_id, "--recovery-phrase-stdin",
            "--into", str(destination),
        ], BUDGET_SECONDS, phrase)
    except OperationError as error:
        if str(error) == "cli_deadline":
            raise OperationError("restore_budget") from None
        raise
    require(elapsed < BUDGET_SECONDS, "restore_budget")
    through, replayed = parse_restore(output)
    require(through == numbers["snapshot_through"], "restore_through")
    note(numbers, "restore_seconds", round(elapsed, 3))
    note(numbers, "replayed", replayed)


def check_restored(programs, destination, database_id, digest, numbers):
    started = time.monotonic()
    restored = OwnedNode(destination, programs)
    restored.start(sign_in=False)
    try:
        count = verify_records(restored, database_id, digest)
        live_write(restored, database_id)
    finally:
        restored.stop()
        restored.cleanup_stop()
    note(numbers, "verified_records", count)
    note(numbers, "verify_seconds", round(time.monotonic() - started, 3))
    note(numbers, "live_write", "ok")


def run_gate(programs, root, phrase, api_key):
    name = database_name()
    numbers = {}
    gate_id, owner_id = gate_ids()
    require(gate_id != owner_id, "database_id")
    note(numbers, "database_id", gate_id)
    note(numbers, "owner_id", owner_id)
    home = root / "source"
    home.mkdir(mode=0o700)
    node = OwnedNode(home, programs)
    write_private(home / "identity.key", baseline_data.seed(phrase))
    install_registry(home, gate_id, owner_id, name)
    node.start(sign_in=False)
    destination = root / "restored"
    try:
        database_id, digest = seed(programs, node, api_key, name, numbers)
        upload(programs, node, name, numbers)
        node.stop()
        restore(programs, home, destination, database_id, phrase, numbers)
        check_restored(programs, destination, database_id, digest, numbers)
    finally:
        node.cleanup_stop()
    summary("snapshot gate passed")
    return numbers


def load_guard():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "mutation_log_load"))
    import guard
    return guard


def aws(guard, operation, payload):
    environment = dict(os.environ, AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true",
                       AWS_EC2_METADATA_DISABLED="true", AWS_PAGER="",
                       AWS_REGION=guard.DEV_REGION, AWS_DEFAULT_REGION=guard.DEV_REGION)
    command = ["aws", "dynamodb", operation, "--region", guard.DEV_REGION, "--output", "json",
               "--no-cli-pager", "--cli-input-json",
               json.dumps(payload, separators=(",", ":"))]
    result = subprocess.run(command, capture_output=True, env=environment, timeout=40, check=False)
    if result.returncode:
        raise OperationError("cleanup_aws")
    if not result.stdout.strip():
        return {}
    return json.loads(result.stdout)


def purge_partition(guard, partition):
    pending, start, deleted = [], None, 0
    while True:
        request = guard.query_request(guard.DEV_TABLE, partition)
        if start is not None:
            request["ExclusiveStartKey"] = start
        found = aws(guard, "query", request)
        pending.extend(guard.record_ids_from_query(found, partition))
        nxt = found.get("LastEvaluatedKey")
        if not guard.continue_page(nxt, start):
            break
        start = nxt
    while pending:
        chunk = pending[:25]
        response = aws(guard, "batch-write-item", guard.delete_batch(guard.DEV_TABLE, partition, chunk))
        left = guard.unprocessed_ids(response, guard.DEV_TABLE, partition)
        if left == chunk:
            raise OperationError("cleanup_aws")
        pending = left + pending[len(chunk):]
        deleted += len(chunk) - len(left)
    return deleted


def cleanup():
    guard = load_guard()
    guard.allow_region_environment(os.environ)
    described = aws(guard, "describe-table", {"TableName": guard.DEV_TABLE})
    guard.allow_table_arn(described.get("Table", {}).get("TableArn", ""), guard.DEV_TABLE)
    deleted = 0
    for database_id in gate_ids():
        guard.allow_stream(database_id)
        for partition in guard.load_partitions(database_id):
            deleted += purge_partition(guard, partition)
    summary(f"cleanup_rows={deleted}")
    # snapshot/bytes accepts read, prepare, download, and publish. It has no
    # owner delete, and the DEV load role cannot reach the R2 or B2 objects.
    summary("provider_objects=left_behind missing=snapshot/bytes delete")


def main():
    command = sys.argv[1] if len(sys.argv) == 2 else ""
    if command == "cleanup":
        cleanup()
        return
    require(command == "run", "command")
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_only")
    dev_endpoint()
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE").strip().encode()
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY").encode()
    require(0 < len(api_key) <= 4096, "dev_api_key")
    parser_args = sys.argv[2:]
    require(len(parser_args) == 4 and parser_args[0] == "--bin-dir"
            and parser_args[2] == "--version", "arguments")
    programs = Programs(Path(parser_args[1]).resolve(strict=True), parser_args[3], dict(os.environ))
    with tempfile.TemporaryDirectory(prefix="ldb-snapshot-", dir="/tmp") as temporary:
        try:
            run_gate(programs, Path(temporary), phrase, api_key)
        except OperationError as error:
            summary("snapshot gate failed: " + str(failure(error)["failure"]))
            if error.cli_refusal:
                summary("cli=" + str(error.cli_refusal.get("reason")))
            raise
        finally:
            del phrase
            del api_key


if __name__ == "__main__":
    main()
