"""Observe original-file holds during a real DEV snapshot upload."""

from pathlib import PurePosixPath
from itertools import islice
import re
import time

from programs import execute
from support import (CREDENTIALS_FILE, canonical_uuid, decode_json, hash_file, private_directory,
                     read_json, require, save_json, stable)


CHECKPOINT = "checkpoint"
INTENT = "shared-snapshot-intent.json"
CUT = "shared-checkpoint-cut.json"
NODE_MARKER = "laststore-pack-node-v1"
RUN_DESCRIPTOR = "laststore-run-home-v2"
RUN_HOMES = (("main", "disk-run"), ("cas", "disk-cas"))
MAX_SOURCE_FILES = 8192
RUN_PATH = re.compile(r"(?:CURRENT|laststore-run-home-v2|runs/[0-9a-f-]{36}\.ldrun)")
HELD_PATH = re.compile(r"(?:current|descriptor|[0-9a-f-]{36}\.ldrun)")


def arguments(operation, node, writer, workspace):
    args = ["snapshot", "--device-id", writer["device_id"], "--device-key-file",
            str(operation.device_file(writer["device_id"])), "--workspace", str(workspace), "--json"]
    return [str(operation.programs.directory / "lastdb"), "--data-dir", str(node.home),
            *operation.programs.cli_arguments(args)]


def provider_counts(receipt):
    require(isinstance(receipt, dict) and set(receipt) == {
        "snapshot_id", "writer_uploaded_through", "r2_files", "b2_files",
    }, "shared_snapshot_receipt")
    counts = {"r2": receipt["r2_files"], "b2": receipt["b2_files"]}
    require(all(type(count) is int and count > 0 for count in counts.values()),
            "shared_snapshot_receipt_providers")
    return counts


def hold_path(data, snapshot_id):
    return data / "disk-run" / "cloud-shared" / "holds" / canonical_uuid(snapshot_id)


def check_receipt(node, receipt, through):
    provider_counts(receipt)
    require(type(receipt["writer_uploaded_through"]) is int
            and receipt["writer_uploaded_through"] == through, "shared_snapshot_command_completed_tail")
    require(not hold_path(node.home / "data", receipt["snapshot_id"]).exists(),
            "shared_snapshot_released_originals")
    return receipt


def invoke(operation, node, writer, workspace, through):
    operation.programs.verify()
    path = node.home / CREDENTIALS_FILE
    save_json(path, operation.credentials)
    try:
        raw = execute(arguments(operation, node, writer, workspace),
                      operation.programs.environment, b"", 120)
        return check_receipt(node, decode_json(raw), through)
    finally:
        path.unlink()


def upload_pending(workspace):
    try:
        return read_json(workspace / CHECKPOINT / INTENT)["phase"]["phase"] == "prepared"
    except FileNotFoundError:
        return False


def wait_for_upload(workspace, future):
    deadline = time.monotonic() + 30
    while not future.done() and time.monotonic() < deadline:
        if upload_pending(workspace):
            return
        time.sleep(0.01)
    if future.done():
        future.result()
    require(False, "shared_snapshot_command_upload_window")


def held_files(hold):
    files = {NODE_MARKER: (hold / "node", None)}
    for directory, disk in RUN_HOMES:
        private_directory(hold / directory)
        capture = read_json(hold / directory / "capture.json", 1024 * 1024)
        for file in capture["files"]:
            path, held = file["path"], file["held"]
            require(RUN_PATH.fullmatch(path) and HELD_PATH.fullmatch(held),
                    "shared_snapshot_held_file_path")
            name = f"{disk}/{path}"
            require(name not in files, "shared_snapshot_duplicate_held_path")
            files[name] = (hold / directory / held, file["bytes"])
    return files


def present_file(path):
    try:
        return path.lstat()
    except FileNotFoundError:
        return None


def source_file_identities(node):
    # Background merges can retire a source name during the upload. Keep the
    # identities that existed before the command as well as the live names.
    data = node.home / "data"
    paths = [data / NODE_MARKER]
    for _hold, disk in RUN_HOMES:
        root = data / disk
        paths.extend([root / "CURRENT", root / RUN_DESCRIPTOR])
        runs = list(islice((root / "runs").iterdir(), MAX_SOURCE_FILES + 1))
        require(len(runs) <= MAX_SOURCE_FILES, "shared_snapshot_source_file_count")
        paths.extend(path for path in runs if RUN_PATH.fullmatch(f"runs/{path.name}"))
    return {path: present_file(path) for path in paths}


def observe_originals(node, workspace, before_upload):
    cut = read_json(workspace / CHECKPOINT / CUT, 16 * 1024 * 1024)
    data = node.home / "data"
    hold = hold_path(data, cut["snapshot_id"])
    private_directory(hold)
    require((hold / "pair-ready").is_file(), "shared_snapshot_durable_freeze")
    files = held_files(hold)
    inventory = {file["path"]: file for file in cut["files"]}
    require(files.keys() == inventory.keys() and len(inventory) == len(cut["files"]),
            "shared_snapshot_exact_held_inventory")
    originals = {}
    counts = {"r2": 0, "b2": 0}
    for name, (held, captured_bytes) in files.items():
        original = data / PurePosixPath(name)
        retained = held.lstat()
        identities = (before_upload.get(original), present_file(original))
        file = inventory[name]
        size = int(file["bytes"])
        require(any(original_stat is not None and stable(original_stat, retained)
                    for original_stat in identities)
                and (captured_bytes is None or captured_bytes == size),
                "shared_snapshot_original_inode")
        require(hash_file(held, size) == file["sha256"], "shared_snapshot_original_digest")
        originals[held] = (retained, file["sha256"])
        destination = "b2" if name.startswith("disk-cas/runs/") else "r2"
        require(file["destination"] == destination, "shared_snapshot_frozen_file_destination")
        counts[destination] += 1
    return originals, counts


def check_frozen(originals):
    for path, (before, digest) in originals.items():
        require(stable(before, path.lstat()) and hash_file(path, before.st_size) == digest,
                "shared_snapshot_frozen_file_changed")
