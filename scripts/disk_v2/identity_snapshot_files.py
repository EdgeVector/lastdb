"""Bounded opaque-file transport between generated, stopped CI homes."""

import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from logical_capture import file_hash, require
from logical_snapshot import SOURCE, canonical, private_file
from node import RUN_NODE_MARKER

MAX_BYTES = 64 * 1024 * 1024
MAX_FILES = 512


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def owned(node):
    root = node.root
    require(node.process is None and not root.is_symlink() and not node.home.is_symlink()
            and root.stat().st_uid == os.getuid() and root.stat().st_ino == node.owner["inode"]
            and json.loads((root / "owner.json").read_text()) == node.owner,
            "refuse a running or unowned generated home")
    require(node.home.resolve(strict=True).parent == root and node.owner["generated_data_only"] is True,
            "refuse a home outside its generated root")
    require(node.home.stat().st_uid == os.getuid() and node.home.stat().st_mode & 0o077 == 0,
            "generated home is not private")


def mode_snapshot(node):
    paths = (".", "identity.key", "data", "data/disk-run", "data/disk-run/runs", "data/disk-run/LOCK",
             "data/disk-run/CURRENT", "data/disk-run/laststore-run-home-v2", "data/disk-cas",
             "data/disk-cas/runs", "data/disk-cas/CURRENT", "data/disk-cas/laststore-run-home-v2",
             "data/laststore-pack-node-v1", "data/logical-device-capture-v1")
    result = {}
    for name in paths:
        try:
            info = (node.home / name).lstat()
            result[name] = {"mode": oct(stat.S_IMODE(info.st_mode)), "owned": info.st_uid == os.getuid(),
                            "directory": stat.S_ISDIR(info.st_mode), "regular": stat.S_ISREG(info.st_mode),
                            "links": info.st_nlink}
        except FileNotFoundError:
            result[name] = {"exists": False}
    return result


def regular(path, root, maximum, private=False):
    info = path.lstat()
    require(path.resolve(strict=True).is_relative_to(root.resolve(strict=True))
            and stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
            and info.st_size <= maximum,
            "refuse an unexpected source file")
    require(not path.is_symlink() and (not private or (info.st_nlink == 1 and info.st_mode & 0o077 == 0)),
            "refuse a linked private control")
    return info


def copy_file(source, destination, root, length):
    before = regular(source, root, MAX_BYTES)
    require(before.st_size == length and not destination.exists(), "opaque file size or destination changed")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    digest, total = hashlib.sha256(), 0
    with source.open("rb") as original, os.fdopen(descriptor, "wb") as target:
        while data := original.read(64 * 1024):
            total += len(data)
            require(total <= length, "opaque source grew during transport")
            target.write(data)
            digest.update(data)
        target.flush()
        os.fsync(target.fileno())
    after = source.stat()
    require(total == length and (before.st_dev, before.st_ino, before.st_size)
            == (after.st_dev, after.st_ino, after.st_size), "opaque source changed during transport")
    require(file_hash(destination) == digest.hexdigest(), "opaque transport changed file bytes")
    return digest.hexdigest()


def copy_account(source, target):
    owned(source)
    owned(target)
    origin = source.home / "identity.key"
    require(regular(origin, source.home, 32, private=True).st_size == 32,
            "generated account key is not an exact private seed")
    require(not any(target.home.iterdir()), "account injection requires a new empty generated home")
    copy_file(origin, target.home / "identity.key", source.home, 32)
    sync_directory(target.home)
    return {"generated_account_only": True, "copied_device_signer": False}


def saved_intent(node, request_id):
    path = private_file(node, SOURCE + "/run-intent.json")
    record = json.loads(path.read_bytes())
    value = record["value"]
    require(record["sha256"] == hashlib.sha256(canonical(value)).hexdigest()
            and value["begin"]["job"]["commit_id"] == request_id,
            "capture intent differs from the owner request")
    reference = value["reference_id"]
    require(str(uuid.UUID(reference)) == reference and uuid.UUID(reference).int != 0,
            "capture reference is not canonical")
    return node.home / "data/disk-run/cloud-byte-holds-v2" / reference


def inventory(path, profile):
    metadata = path / "capture.json"
    regular(metadata, path, 1024 * 1024, private=True)
    value = json.loads(metadata.read_bytes())
    files = value["files"]
    require(value["home_id"] == profile["home_id"]
            and str(value["generation"]) == profile["generation"]
            and str(value["frontier"]) == profile["frontier"]
            and 2 <= len(files) <= MAX_FILES and len(files) == profile["files"]
            and sum(entry["bytes"] for entry in files) == profile["bytes"],
            "capture inventory differs from its authenticated profile")
    seen = set()
    for entry in files:
        relative, held, kind = entry["path"], entry["held"], entry["kind"]
        expected = ((relative, held, kind) == ("CURRENT", "current", "Current")
                    or (relative, held, kind) == ("laststore-run-home-v2", "descriptor", "Descriptor")
                    or (kind == "Run" and relative == "runs/" + held
                        and held.endswith(".ldrun") and str(uuid.UUID(held[:-6])) == held[:-6]))
        require(expected and relative not in seen and Path(held).name == held
                and type(entry["bytes"]) is int and 0 < entry["bytes"] <= MAX_BYTES,
                "capture contains an unexpected native file")
        seen.add(relative)
    require("CURRENT" in seen and "laststore-run-home-v2" in seen,
            "capture lacks its declared selector or descriptor")
    require(file_hash(path / "current") == profile["current_sha256"]
            and file_hash(path / "descriptor") == profile["descriptor_sha256"],
            "capture selector or descriptor checksum differs")
    return files


def materialize(donor, target, capture):
    owned(donor)
    owned(target)
    require(not (target.home / "data").exists(), "detached target already has native data")
    held = saved_intent(donor, capture["request_id"])
    require(held.is_dir() and not held.is_symlink() and held.stat().st_uid == os.getuid()
            and held.resolve(strict=True).is_relative_to(donor.home.resolve(strict=True))
            and held.stat().st_mode & 0o077 == 0, "capture hold is not a private owned directory")
    groups = [(prefix, inventory(held / name, capture["run_profile"][part]))
              for prefix, name, part in [("disk-run", "main", "main"), ("disk-cas", "cas", "cas")]]
    require(sum(len(files) for _, files in groups) + 1 <= MAX_FILES
            and sum(entry["bytes"] for _, files in groups for entry in files) <= MAX_BYTES,
            "generated snapshot exceeds its transport bound")
    (target.home / "data").mkdir(mode=0o700)
    for prefix in ("disk-run", "disk-cas"):
        home = target.home / "data" / prefix
        home.mkdir(mode=0o700)
        (home / "runs").mkdir(mode=0o700)
        descriptor = os.open(home / "LOCK", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
    for (prefix, files), folder in zip(groups, ["main", "cas"]):
        for entry in files:
            copy_file(held / folder / entry["held"], target.home / "data" / prefix / entry["path"],
                      held, entry["bytes"])
    regular(held / "node", held, 4096)
    require((held / "node").read_bytes() == RUN_NODE_MARKER.encode(), "capture node marker differs")
    copy_file(held / "node", target.home / "data/laststore-pack-node-v1", held, len(RUN_NODE_MARKER))
    for prefix in ("disk-run", "disk-cas"):
        sync_directory(target.home / "data" / prefix / "runs")
        sync_directory(target.home / "data" / prefix)
    sync_directory(target.home / "data")
    sync_directory(target.home)
    require(not (target.home / "data/logical-device-capture-v1").exists(), "donor signer reached the target")
    return {"native_files": sum(len(files) for _, files in groups) + 1,
            "native_bytes": sum(entry["bytes"] for _, files in groups for entry in files),
            "source": "actual_retained_capture_inventory", "local_lock_files": 2, "donor_signer_copied": False}


def tree_digest(node):
    owned(node)
    records, pending, total, visited = {}, [(node.home, 0)], 0, 0
    while pending:
        folder, depth = pending.pop()
        require(depth <= 6, "generated home exceeds the observation depth")
        entries = list(folder.iterdir())
        visited += len(entries)
        require(visited <= MAX_FILES, "generated home exceeds the observation count")
        for path in entries:
            require(not path.is_symlink(), "generated home contains a symlink")
            if path.is_dir():
                pending.append((path, depth + 1))
            else:
                info = regular(path, node.home, MAX_BYTES)
                total += info.st_size
                require(len(records) < MAX_FILES and total <= MAX_BYTES,
                        "generated home exceeds the observation bound")
                records[str(path.relative_to(node.home))] = {"bytes": info.st_size, "sha256": file_hash(path)}
    return {"files": len(records), "bytes": total,
            "sha256": hashlib.sha256(canonical(records)).hexdigest()}
