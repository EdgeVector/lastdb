#!/usr/bin/env python3
"""Build one exact source revision under the standard external host lock."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
DEV = Path("/Users/tomtang/.fkanban/worktrees")
BUILD = ["cargo", "build", "--locked", "--release", "-p", "lastdb_node", "--bin", "lastdbd", "--bin", "lastdb"]


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def output(repo, *args):
    return subprocess.check_output(args, cwd=repo, text=True).strip()


def frozen(repo, revision):
    if (output(repo, "git", "rev-parse", "HEAD") != revision
            or output(repo, "git", "status", "--porcelain", "--untracked-files=no")):
        raise RuntimeError("The exact product source must stay committed and unchanged")


def owner(scratch):
    root = scratch.resolve(strict=True)
    info = root.stat()
    if (root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True) or scratch.is_symlink()
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("Use an exact fresh owned temporary directory")
    marker = root / "owner.json"
    if marker.exists():
        value = json.loads(marker.read_text())
        expected = (str(root), info.st_dev, info.st_ino, info.st_uid, False)
        actual = tuple(value[key] for key in ("root", "device", "inode", "owner_uid", "primary_data"))
        if actual != expected:
            raise ValueError("The build-scratch owner differs")
        return root, value
    if any(root.iterdir()):
        raise ValueError("A new build scratch must be empty")
    value = {"version": 1, "root": str(root), "device": info.st_dev, "inode": info.st_ino,
             "owner_uid": info.st_uid, "primary_data": False, "run_id": uuid.uuid4().hex}
    save(marker, value)
    (root / "bin").mkdir(mode=0o700)
    save(ROOT / "build-scratch-owner.json", value)
    return root, value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"), required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--disk-tools", action="store_true")
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    if repo.parent != DEV or args.repo.is_symlink() or len(args.revision) != 40:
        raise ValueError("Use the exact source-only DEV worktree and full revision")
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock")
    scratch, ownership = owner(args.scratch)
    frozen(repo, args.revision)
    for name in ("lastdbd", "lastdb"):
        if (scratch / "bin" / f"{args.variant}-{name}").exists():
            raise ValueError("Do not replace a recorded candidate binary")
    environment = dict(os.environ)
    for key in ("FOLDDB_BUILD_VERSION_OVERRIDE", "GITHUB_REF_NAME"):
        environment.pop(key, None)
    target = ROOT.parents[2] / "target"
    environment["CARGO_TARGET_DIR"] = str(target)
    started = time.time_ns()
    subprocess.run(BUILD, cwd=repo, env=environment, check=True)
    ended = time.time_ns()
    frozen(repo, args.revision)
    shared = {"revision": args.revision, "product_source_clean": True, "source_worktree": str(repo),
              "build_command": " ".join(BUILD), "build_argv": BUILD,
              "build_start_unix_ns": started, "build_end_unix_ns": ended,
              "rustc": output(repo, "rustc", "--version"), "cargo": output(repo, "cargo", "--version"),
              "build_scratch_owner": ownership, "driver_sha256": digest(Path(__file__)),
              "build_environment": {"CARGO_TARGET_DIR": str(target), "profile": "release",
                  "package_features": "lastdb_node defaults: sentry-telemetry,purging-allocator"}}
    for name in ("lastdbd", "lastdb"):
        source = target / "release" / name
        copied = scratch / "bin" / f"{args.variant}-{name}"
        shutil.copy2(source, copied)
        version = subprocess.check_output([str(copied), "--version"], env=environment, text=True).strip()
        if name == "lastdbd" and args.revision[:9] not in version:
            raise RuntimeError("The node version differs from the frozen source")
        suffix = "build.json" if name == "lastdbd" else "cli-build.json"
        save(ROOT / f"{args.variant}-{suffix}", {**shared, "binary": str(copied),
             "binary_sha256": digest(copied), "version": version})
    if args.disk_tools:
        build_disk(repo, args, scratch, environment, target, shared)
    print(json.dumps({"candidate_ready": True, "variant": args.variant, "revision": args.revision,
                      "scratch": str(scratch)}), flush=True)


def build_disk(repo, args, scratch, environment, target, shared):
    names = ("lastdb-disk-home", "lastdb-disk-recovery", "lastdb-pack-maintain")
    command = ["cargo", "build", "--locked", "--release", "-p", "laststore"]
    for name in names:
        if (scratch / "bin" / f"{args.variant}-{name}").exists():
            raise ValueError("Do not replace a recorded physical command")
        command += ["--bin", name]
    frozen(repo, args.revision)
    started = time.time_ns()
    subprocess.run(command, cwd=repo, env=environment, check=True)
    frozen(repo, args.revision)
    receipts = {}
    for name in names:
        copied = scratch / "bin" / f"{args.variant}-{name}"
        shutil.copy2(target / "release" / name, copied)
        receipts[name] = {"binary": str(copied), "binary_sha256": digest(copied)}
    save(ROOT / f"{args.variant}-disk-build.json", {**shared, "build_command": " ".join(command),
         "build_start_unix_ns": started, "build_end_unix_ns": time.time_ns(), "binaries": receipts})


if __name__ == "__main__":
    main()
