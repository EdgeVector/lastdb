"""Bounded deletion of exact stopped, owned build roots; never follow links."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import time

ROOT = Path(__file__).resolve().parent


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def load(path):
    if path.is_symlink() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("A build ownership receipt is linked or oversized")
    return json.loads(path.read_text())


def validate(owner, binaries, has_target):
    root = Path(owner["root"])
    info = root.stat()
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or owner.get("primary_data") is not False
            or (info.st_dev, info.st_ino, info.st_uid) != (owner["device"], owner["inode"], os.getuid())
            or owner["owner_uid"] != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or load(root / "owner.json") != owner):
        raise ValueError("The exact private temporary build-root identity differs")
    expected = {"owner.json", "bin", "target"} if has_target else {"owner.json", "bin"}
    directory = root / "bin"
    if ({path.name for path in root.iterdir()} != expected or directory.is_symlink()
            or set(directory.iterdir()) != {Path(row["path"]) for row in binaries}):
        raise ValueError("An owned build root contains an unexpected path")
    if has_target and ((root / "target").is_symlink() or not (root / "target").is_dir()):
        raise ValueError("The exact owned Cargo target must remain an unlinked directory")
    processes = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    if str(root) in processes:
        raise ValueError("An owned build root appears in a current process command")
    for row in binaries:
        path = Path(row["path"])
        if (path.parent != directory or path.is_symlink() or not path.is_file()
                or path.stat().st_uid != os.getuid() or digest(path) != row["sha256"]):
            raise ValueError("An exact recorded copied binary differs")
    return root


def target_inventory(root, label):
    target = root / "target"
    pending, rows, size = [(target, 0)], [], 0
    deadline = time.monotonic() + 120
    while pending:
        parent, depth = pending.pop()
        if depth > 32 or time.monotonic() > deadline:
            raise ValueError("The owned target exceeds its finite depth or time bound")
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError("An inventoried target directory changed type before its read")
        entries = [parent] if parent == target else []
        entries += list(parent.iterdir())
        for path in entries:
            info = path.lstat()
            mode = info.st_mode
            if info.st_uid != os.getuid() or info.st_dev != root.stat().st_dev:
                raise ValueError("The exact owned target contains a foreign owner or device")
            if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode) or stat.S_ISLNK(mode)):
                raise ValueError("The owned target contains an unsupported filesystem object")
            relative = str(path.relative_to(root))
            rows.append([relative, info.st_dev, info.st_ino, info.st_uid, mode, info.st_size,
                         info.st_mtime_ns, os.readlink(path) if stat.S_ISLNK(mode) else None])
            size += info.st_size if stat.S_ISREG(mode) else 0
            if len(rows) > 200000 or size > 64 * 1024 ** 3 or time.monotonic() > deadline:
                raise ValueError("The owned target exceeds two hundred thousand entries or sixty-four GiB")
            if path != target and stat.S_ISDIR(mode):
                pending.append((path, depth + 1))
    output = ROOT / (label + "-target-cleanup-inventory.jsonl.gz")
    if output.exists():
        raise ValueError("Do not replace an original target cleanup inventory")
    checksum = hashlib.sha256()
    with output.open("xb") as sink:
        with gzip.GzipFile(filename="", fileobj=sink, mode="wb", mtime=0) as packed:
            for row in sorted(rows):
                raw = (json.dumps(row, separators=(",", ":")) + "\n").encode()
                checksum.update(raw)
                packed.write(raw)
    with gzip.open(output, "rb") as stream:
        actual = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            actual.update(block)
    if actual.hexdigest() != checksum.hexdigest() or output.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("The target cleanup inventory fails its exact-byte or repository blob bound")
    return rows, {"path": output.name, "sha256": digest(output), "entries": len(rows), "file_bytes": size,
                  "uncompressed_sha256": checksum.hexdigest(), "round_trip_bytes_match": True}


def remove(owner, binaries, label, has_target):
    root = validate(owner, binaries, has_target)
    rows, inventory = target_inventory(root, label) if has_target else ([], None)
    result = {"owner": owner, "binaries": binaries, "target_inventory": inventory,
              "removed": False, "at_unix_ns": time.time_ns()}
    receipt = ROOT / (label + "-owned-build-cleanup.json")
    if receipt.exists():
        raise ValueError("Do not replace an original owned build cleanup receipt")
    receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    for row in sorted(rows, key=lambda value: value[0].count("/"), reverse=True):
        relative, device, inode, uid, mode, size, modified, link = row
        path = root / relative
        info = path.lstat()
        if (info.st_dev, info.st_ino, info.st_uid, info.st_mode) != (device, inode, uid, mode):
            raise ValueError("An exact target object changed identity before cleanup")
        if stat.S_ISDIR(mode):
            path.rmdir()
        else:
            if info.st_size != size or info.st_mtime_ns != modified:
                raise ValueError("A target file changed before cleanup")
            if stat.S_ISLNK(mode) and os.readlink(path) != link:
                raise ValueError("An owned target link changed before cleanup")
            path.unlink()
    for row in binaries:
        path = Path(row["path"])
        if digest(path) != row["sha256"]:
            raise ValueError("A copied binary changed after target cleanup")
        path.unlink()
    (root / "bin").rmdir()
    if load(root / "owner.json") != owner:
        raise ValueError("The original ownership marker changed")
    (root / "owner.json").unlink()
    root.rmdir()
    result["removed"] = not root.exists()
    receipt.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return {"label": label, "root": str(root), "removed": result["removed"], "receipt_sha256": digest(receipt)}
