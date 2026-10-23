#!/usr/bin/env python3
"""Copy only the exact previously accepted draft binaries into new owned scratch."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
PRIOR = ROOT.parent / "receipt-part-pruning-benchmark-20261022"
REVISION = "a1941e3a8742d4da4743a02db99153119bdf034b"
DRAFT = Path("/Users/tomtang/.fkanban/worktrees/lastdb-receipt-cache-draft-20261022")


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def output(*argv):
    return subprocess.check_output(argv, cwd=DRAFT, text=True).strip()


def accepted():
    if (DRAFT.parent != Path("/Users/tomtang/.fkanban/worktrees") or DRAFT.is_symlink()
            or DRAFT.resolve(strict=True) != DRAFT or output("git", "rev-parse", "HEAD") != REVISION
            or output("git", "status", "--porcelain", "--untracked-files=no")):
        raise ValueError("Use the exact clean source-only draft DEV worktree")
    if json.loads((PRIOR / "final-build-checks.json").read_text()).get("complete") is not True:
        raise ValueError("The original draft lacks its accepted build guard")
    node = json.loads((PRIOR / "after-build.json").read_text())
    cli = json.loads((PRIOR / "after-cli-build.json").read_text())
    physical = json.loads((PRIOR / "after-disk-build.json").read_text())
    if any(item["revision"] != REVISION or item["product_source_clean"] is not True for item in (node, cli, physical)):
        raise ValueError("An original draft receipt differs from its accepted source")
    return node, cli, physical


def main():
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive host Cargo lock")
    if (ROOT / "draft-copy.json").exists() or (ROOT / "build-scratch-owner.json").exists():
        raise ValueError("Do not replace earlier draft copies or scratch ownership")
    node, cli, physical = accepted()
    items = [("lastdbd", node), ("lastdb", cli), *physical["binaries"].items()]
    verified = []
    for name, receipt in items:
        source = REPO / "target" / "release" / name
        if source.is_symlink() or not source.is_file() or digest(source) != receipt["binary_sha256"]:
            raise ValueError("A cached release binary differs from its exact accepted hash; build it instead")
        if name in ("lastdbd", "lastdb"):
            version = subprocess.check_output([str(source), "--version"], text=True).strip()
            if version != receipt["version"] or REVISION[:9] not in version:
                raise ValueError("The cached node or CLI reports a different source version")
        verified.append((name, source, receipt))
    root = Path(tempfile.mkdtemp(prefix="rcache.", dir=os.environ["TMPDIR"])).resolve(strict=True)
    info = root.stat()
    if root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True) or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("The new build scratch is not the exact private temporary directory")
    owner = {"version": 1, "root": str(root), "device": info.st_dev, "inode": info.st_ino,
             "owner_uid": info.st_uid, "primary_data": False, "run_id": uuid.uuid4().hex}
    save(root / "owner.json", owner)
    save(ROOT / "build-scratch-owner.json", owner)
    (root / "bin").mkdir(mode=0o700)
    copied = {}
    for name, source, receipt in verified:
        target = root / "bin" / ("draft-" + name)
        shutil.copy2(source, target)
        if digest(target) != receipt["binary_sha256"]:
            raise RuntimeError("The exact accepted draft copy differs")
        copied[name] = {"binary": str(target), "binary_sha256": receipt["binary_sha256"],
                        "bytes": target.stat().st_size, "cached_source": str(source)}
    value = {"complete": True, "revision": REVISION, "verified_source_worktree": str(DRAFT),
             "copied_at_unix_ns": time.time_ns(), "owner": owner, "binaries": copied,
             "driver_sha256": digest(Path(__file__)), "original_build_not_repeated": True,
             "original_receipts": {name: digest(PRIOR / name) for name in
                                   ("after-build.json", "after-cli-build.json", "after-disk-build.json", "final-build-checks.json")}}
    save(ROOT / "draft-copy.json", value)
    for name in ("after-build.json", "after-cli-build.json", "after-disk-build.json", "final-build-checks.json"):
        (ROOT / ("draft-original-" + name)).write_bytes((PRIOR / name).read_bytes())
    save(ROOT / "draft-build.json", {**node, **copied["lastdbd"], "copy_receipt": "draft-copy.json"})
    save(ROOT / "draft-cli-build.json", {**cli, **copied["lastdb"], "copy_receipt": "draft-copy.json"})
    save(ROOT / "draft-disk-build.json", {**physical, "binaries": {name: copied[name] for name in physical["binaries"]},
                                         "copy_receipt": "draft-copy.json"})
    print(json.dumps({"draft_ready": True, "scratch": str(root), "copied_binaries": len(copied)}), flush=True)


if __name__ == "__main__":
    main()
