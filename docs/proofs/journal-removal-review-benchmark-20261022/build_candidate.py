#!/usr/bin/env python3
"""Build one frozen candidate under the external host lock and record its binaries."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]


def output(*args):
    return subprocess.check_output(args, cwd=REPO, text=True).strip()


def save(name, value):
    (ROOT / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def frozen(revision):
    if output("git", "rev-parse", "HEAD") != revision or output("git", "status", "--porcelain", "--untracked-files=no"):
        raise RuntimeError("The exact candidate source must stay committed and unchanged")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", required=True, type=Path)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("Use the standard external host Cargo lock")
    scratch = args.scratch.resolve(strict=True)
    owner = json.loads((scratch / "owner.json").read_text())
    info = scratch.stat()
    if (owner["root"] != str(scratch) or owner["owner_uid"] != os.getuid()
            or owner["device"] != info.st_dev or owner["inode"] != info.st_ino or owner["primary_data"] is not False):
        raise RuntimeError("The exact build-scratch owner differs")
    frozen(args.revision)
    commands = [
        ["cargo", "clippy", "--workspace", "--lib", "--bins", "--locked", "--exclude",
         "generate_validated_schema_org_seeds", "--exclude", "schema_service_worker", "--", "-D", "warnings"],
        ["cargo", "build", "--locked", "--release", "-p", "lastdb_node", "--bin", "lastdbd", "--bin", "lastdb"],
    ]
    environment = dict(os.environ)
    for key in ("FOLDDB_BUILD_VERSION_OVERRIDE", "GITHUB_REF_NAME"):
        environment.pop(key, None)
    steps = []
    for command in commands:
        frozen(args.revision)
        started = time.time_ns()
        subprocess.run(command, cwd=REPO, env=environment, check=True)
        steps.append({"argv": command, "start_unix_ns": started, "end_unix_ns": time.time_ns()})
    frozen(args.revision)
    shared = {"revision": args.revision, "product_source_clean": True,
              "build_command": " ".join(commands[-1]), "build_steps": steps,
              "rustc": output("rustc", "--version"), "cargo": output("cargo", "--version"),
              "build_scratch_owner": owner, "build_environment": {
                  "CARGO_TARGET_DIR": str(REPO / "target"), "profile": "release",
                  "package_features": "lastdb_node defaults: sentry-telemetry,purging-allocator"}}
    for name, receipt in (("lastdbd", "after-build.json"), ("lastdb", "cli-build.json")):
        source = REPO / "target" / "release" / name
        target = scratch / "bin" / f"after-{name}"
        if target.exists():
            raise RuntimeError("Do not overwrite a recorded candidate")
        shutil.copy2(source, target)
        version = subprocess.check_output([str(target), "--version"], env=environment, text=True).strip()
        if name == "lastdbd" and args.revision[:9] not in version:
            raise RuntimeError("The compiled node stamp differs from its frozen source")
        save(receipt, {**shared, "binary": str(target), "binary_sha256": digest(target), "version": version})
    save("final-build-checks.json", {**shared, "complete": True, "driver_sha256": digest(Path(__file__))})
    print(json.dumps({"candidate_ready": True, "revision": args.revision}), flush=True)


if __name__ == "__main__":
    main()
