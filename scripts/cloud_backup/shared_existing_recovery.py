"""Restore an exact retained DEV shared cut with its compatible CI client."""

import argparse
import base64
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True

from baseline import private_write
from node import OwnedNode
from programs import Programs
from shared_daily_admit import wait_for_gates
import shared_daily_io as io
from support import (ACCOUNT, CREDENTIALS_FILE, OperationError, canonical_uuid, child_environment,
                     decode_json, dev_endpoint, private_directory, read_json, require, save_json)

sys.path.append(str(Path(__file__).resolve().parents[1] / "release"))
from nightly_probe import unpack  # noqa: E402


WORKFLOW = ".github/workflows/cloud-shared-existing-recovery.yml"
REPOSITORY_ID = 1412311495
TARGET = "x86_64-unknown-linux-gnu"
PINS = {
    "3fd7146a-a626-44fc-b58d-25c319566816": {
        "snapshot_id": "0c2d2182-dde0-4217-ba38-bcd59653b3c5",
        "manifest_sha256": "c3eecb093eb5019542078d5a7baef02087844b8481f1c68b6e4dfbe8ce21ab5b",
        "client_source": "a86d7b414ea690fc399e761c00d55481e082a46a",
        "client_run": 42293197007, "artifact": 13540254064,
        "artifact_sha256": "1eaf85e0c803ca349becbb524d102ea30091a1895a409da2e53c279d8276e73d",
        "seed_input": "LASTDB_CLOUD_DEV_EXISTING_MULTIWRITER_SEEDS",
    },
    "7c7344e8-795c-441c-8d07-0a05668cc987": {
        "snapshot_id": "2c457cf0-6403-4629-8011-65e0c43a46eb",
        "manifest_sha256": "76b83a6ac5322ab82c06234ed88f2268350700581fd1fb3afec814250f5b2e97",
        "client_source": "423ccd2f80ccfde65885d5e09ab36392f1b0da29",
        "client_run": 42268290995, "artifact": 13528766291,
        "artifact_sha256": "fe30443aaab67940e53b74a555bdb70d453fffa56eb9f2d2dbcffa868707651b",
        "seed_input": "LASTDB_CLOUD_DEV_EXISTING_RETENTION_SEEDS",
    },
}


def origin():
    source = os.environ.get("GITHUB_SHA", "")
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == "EdgeVector/lastdb"
            and os.environ.get("GITHUB_REPOSITORY_ID") == str(REPOSITORY_ID)
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"EdgeVector/lastdb/{WORKFLOW}@refs/heads/main"
            and re.fullmatch(r"[0-9a-f]{40}", source), "shared_existing_recovery_origin")
    run_id, attempt = (os.environ.get(name, "") for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"))
    require(all(re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in (run_id, attempt)),
            "shared_existing_recovery_run_identity")
    return {"source": source, "run_id": int(run_id), "attempt": int(attempt)}


def admit(pin, identity):
    run = io.github(f"actions/runs/{pin['client_run']}")
    require(run["id"] == pin["client_run"] and run["head_sha"] == pin["client_source"]
            and run["head_branch"] == "main" and run["event"] == "workflow_dispatch"
            and run["run_attempt"] == 1 and run["status"] == "completed" and run["conclusion"] == "success"
            and run["path"] == ".github/workflows/cloud-backup-recovery.yml"
            and run["repository"]["id"] == run["head_repository"]["id"] == REPOSITORY_ID,
            "shared_existing_recovery_client_run")
    artifact = io.github(f"actions/artifacts/{pin['artifact']}")
    require(artifact["id"] == pin["artifact"] and artifact["expired"] is False
            and artifact["name"] == f"cloud-backup-client-{pin['client_source']}-1"
            and artifact["digest"] == "sha256:" + pin["artifact_sha256"]
            and artifact["workflow_run"]["id"] == pin["client_run"]
            and artifact["workflow_run"]["head_sha"] == pin["client_source"]
            and artifact["workflow_run"]["head_repository_id"] == REPOSITORY_ID,
            "shared_existing_recovery_client_artifact")
    wait_for_gates(identity["source"])
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"client_run={pin['client_run']}\nartifact_id={pin['artifact']}\n")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"Historical format-13 client `{pin['client_source']}` from successful run "
                      f"`{pin['client_run']}`. Artifact `{pin['artifact']}`, ZIP SHA-256 "
                      f"`{pin['artifact_sha256']}`. Expires `{artifact['expires_at']}`.\n")


def read_scope(partition, keys):
    response = io.aws("dynamodb", "batch-get-item", {"RequestItems": {io.TABLE: {
        "Keys": [io.key(partition, key) for key in keys], "ConsistentRead": True,
    }}})
    require(not response.get("UnprocessedKeys"), "shared_existing_recovery_control_read_incomplete")
    rows = response["Responses"][io.TABLE]
    require(all(row["account_id"]["S"] == partition and row["record_id"]["S"] in keys for row in rows)
            and len({row["record_id"]["S"] for row in rows}) == len(rows),
            "shared_existing_recovery_control_scope")
    return {row["record_id"]["S"]: io.body(row) for row in rows}


def head(value):
    if value is not None:
        require(set(value) == {"sequence", "prefix_sha256"}
                and re.fullmatch(r"[1-9][0-9]{0,19}", value["sequence"])
                and int(value["sequence"]) < 2**64
                and re.fullmatch(r"[0-9a-f]{64}", value["prefix_sha256"]),
                "shared_existing_recovery_writer_head")
    return value


def cut(database, pin, devices=()):
    partition = "shared#" + database
    snapshot = {"database_id": database, "snapshot_id": pin["snapshot_id"],
                "manifest_sha256": pin["manifest_sha256"]}
    saved = io.get(partition, "database")
    require(saved is not None and io.body(saved)["snapshot"] == snapshot,
            "shared_existing_recovery_expected_snapshot")
    state = io.body(saved)
    writers = list(map(canonical_uuid, state["writers"]))
    require(0 < len(writers) <= 16 and writers == sorted(set(writers)),
            "shared_existing_recovery_writer_directory")
    snapshot_key = "snapshot#" + snapshot["snapshot_id"]
    rows = read_scope(partition, [snapshot_key, *["writer#" + key for key in writers],
                                 *["device#" + key for key in devices], *["hold#" + key for key in devices]])
    accepted = rows[snapshot_key]
    manifest = accepted["manifest"]
    require(accepted["receipt"] == snapshot and manifest["database_id"] == database
            and manifest["snapshot_id"] == snapshot["snapshot_id"] and manifest["disk_format"] == 13
            and io.get(partition, "database") == saved, "shared_existing_recovery_stable_cut")
    writer_heads = {}
    for stream in writers:
        writer = rows["writer#" + stream]
        require(writer["stream"] == {"database_id": database, "source_stream": stream},
                "shared_existing_recovery_exact_writer")
        writer_heads[stream] = head(writer["head"])
    return snapshot, writer_heads, state["publisher_device"], rows


def protected_inputs(pin):
    seed_inputs = {row["seed_input"] for row in PINS.values()}
    values = {name: os.environ.pop(name, "") for name in seed_inputs}
    seeds = values[pin["seed_input"]]
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "")
    require(0 < len(seeds) <= 4096 and 0 < len(phrase.encode()) <= 4096
            and 0 < len(api_key) <= 4096 and all(32 <= ord(char) < 127 for char in api_key)
            and api_key == api_key.strip(), "shared_existing_recovery_protected_input")
    values = decode_json(seeds.encode())
    require(isinstance(values, dict) and 0 < len(values) <= 16, "shared_existing_recovery_device_map")
    selected = {}
    for device, encoded in values.items():
        require(isinstance(encoded, str) and re.fullmatch(r"[A-Za-z0-9+/]{43}=", encoded),
                "shared_existing_recovery_device_seed")
        selected[canonical_uuid(device)] = base64.b64decode(encoded, validate=True)
    return selected, api_key, phrase.encode()


def public_key(seed, environment):
    der = bytes.fromhex("302e020100300506032b657004220420") + seed
    result = subprocess.run(["openssl", "pkey", "-inform", "DER", "-pubout", "-outform", "DER"],
                            input=der, capture_output=True, env=environment, timeout=10, check=False)
    require(result.returncode == 0 and len(result.stdout) == 44
            and result.stdout[:12] == bytes.fromhex("302a300506032b6570032100"),
            "shared_existing_recovery_device_key")
    return list(result.stdout[12:])


def reader(seeds, publisher, rows, environment):
    for device in sorted(seeds, key=lambda key: (key == publisher, key)):
        owner = rows.get("device#" + device)
        hold = rows.get("hold#" + device)
        if owner is not None and (hold is None or int(hold["view"]["expires_at_ms"]) <= io.now_ms()):
            require(owner["account_id"] == ACCOUNT and owner["public_key"] == public_key(seeds[device], environment),
                    "shared_existing_recovery_device_identity")
            return device, hold
    raise OperationError("shared_existing_recovery_no_idle_reader")


def verify_hold(database, device, prior, snapshot, heads):
    item = io.get("shared#" + database, "hold#" + device)
    require(item is not None, "shared_existing_recovery_hold_missing")
    hold = io.body(item)
    view = hold["view"]
    hold_id = canonical_uuid(view["hold_id"])
    streams = {canonical_uuid(stream["source_stream"]): head(stream["head"]) for stream in view["streams"]}
    require(hold["owner_device"] == device and view["snapshot"] == snapshot and streams == heads
            and len(streams) == len(view["streams"]) and int(view["expires_at_ms"]) > io.now_ms()
            and (prior is None or hold_id != prior["view"]["hold_id"]), "shared_existing_recovery_exact_hold")


def package_paths(args):
    runner = Path(os.environ["RUNNER_TEMP"])
    require(args.package == runner / "shared-existing-recovery-client"
            and args.binaries == runner / "shared-existing-recovery-bin", "shared_existing_recovery_package_paths")
    bundle = args.package / f"lastdb-{TARGET}.tar.gz"
    manifest = read_json(args.package / f"lastdb-{TARGET}.manifest.json", 65536)
    return bundle, manifest


def unpack_client(args, pin):
    bundle, manifest = package_paths(args)
    version = manifest["version"]
    require(manifest["target"] == TARGET and version.endswith(".g" + pin["client_source"][:9]),
            "shared_existing_recovery_client_version")
    args.binaries.mkdir(mode=0o700)
    for name in list(os.environ):
        if name.startswith(("AWS_", "ACTIONS_ID_TOKEN_")) or name in ("GH_TOKEN", "GITHUB_TOKEN"):
            os.environ.pop(name)
    unpack(bundle, manifest, args.binaries, version, pin["client_source"])


def restore(args, pin, identity):
    seeds, api_key, phrase = protected_inputs(pin)
    caller = io.aws("sts", "get-caller-identity")
    require(caller["Account"] == io.ACCOUNT and caller["Arn"].startswith(
        f"arn:aws:sts::{io.ACCOUNT}:assumed-role/LastDBSharedExistingRecoveryDev/"),
        "shared_existing_recovery_reader_role")
    runner = Path(os.environ["RUNNER_TEMP"])
    require(args.output == runner / "shared-existing-recovery/summary.json" and not args.output.parent.exists(),
            "shared_existing_recovery_output")
    _, manifest = package_paths(args)
    snapshot, heads, publisher, rows = cut(args.database_id, pin, seeds)
    root = Path(tempfile.mkdtemp(prefix="ldb-x-", dir="/tmp")).resolve()
    owned = private_directory(root)
    node = None
    try:
        for name in ("credentials", "os-home", "tmp"):
            (root / name).mkdir(mode=0o700)
        programs = Programs(args.binaries, manifest["version"], child_environment(root))
        expected = {binary["name"]: {"sha256": binary["sha256"], "bytes": binary["size"]}
                    for binary in manifest["binaries"]}
        require(programs.identities == expected, "shared_existing_recovery_client_changed")
        device, prior = reader(seeds, publisher, rows, programs.environment)
        key = root / "credentials" / "device-key"
        private_write(key, seeds[device])
        save_json(root / "credentials" / CREDENTIALS_FILE, {"api_url": dev_endpoint(), "api_key": api_key})
        destination = root / "restored"
        receipt = programs.command(root / "credentials", ["cloud", "restore-shared", "--env", "dev",
            "--database-id", args.database_id, "--snapshot-id", snapshot["snapshot_id"],
            "--device-id", device, "--device-key-file", str(key), "--into", str(destination),
            "--recovery-phrase-stdin", "--json"], input_bytes=phrase, timeout=300)
        require(receipt == {"activated": True, "destination": str(destination)},
                "shared_existing_recovery_activation")
        verify_hold(args.database_id, device, prior, snapshot, heads)
        node = OwnedNode(destination, programs)
        for _ in range(2):
            node.start()
            node.health()
            node.stop()
        require(node.clean_stops == 2 and not node.forced, "shared_existing_recovery_clean_boots")
        require(cut(args.database_id, pin)[:2] == (snapshot, heads), "shared_existing_recovery_cut_changed")
    finally:
        if node is not None:
            node.cleanup_stop()
        saved = private_directory(root)
        require((saved.st_dev, saved.st_ino, saved.st_uid) == (owned.st_dev, owned.st_ino, owned.st_uid),
                "shared_existing_recovery_local_owner_changed")
        shutil.rmtree(root)
    args.output.parent.mkdir(mode=0o700)
    save_json(args.output, {**identity, "environment": "dev", "database_id": args.database_id,
                            "account_id": ACCOUNT, "snapshot": snapshot, "writer_heads": heads,
                            "source_free": True, "activated": True, "restored_clean_boots": 2,
                            "checkpoint_and_tails_verified": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("admit", "unpack", "restore"))
    parser.add_argument("--database-id", choices=PINS, required=True)
    parser.add_argument("--package", type=Path)
    parser.add_argument("--binaries", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    identity = origin()
    if args.action == "admit":
        admit(PINS[args.database_id], identity)
    elif args.action == "unpack":
        unpack_client(args, PINS[args.database_id])
    else:
        restore(args, PINS[args.database_id], identity)


if __name__ == "__main__":
    try:
        main()
    except OperationError as error:
        raise SystemExit(str(error)) from None
    except (KeyError, OSError, ValueError, TypeError):
        raise SystemExit("shared_existing_recovery_input_or_io_failed") from None
