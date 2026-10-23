#!/usr/bin/env python3
"""Run four actual generated-data checks before the measured product pairs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from reuse import ROOT, original, validate_revisions


def load(path):
    return json.loads(path.read_text())


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def disk_build(label, revision):
    value = load(ROOT / f"{label}-disk-build.json")
    if value["revision"] != revision or value["product_source_clean"] is not True:
        raise ValueError("The physical tools differ from their required clean source commit")
    result = {}
    for name, item in value["binaries"].items():
        path = Path(item["binary"]).resolve(strict=True)
        if digest(path) != item["binary_sha256"]:
            raise ValueError("An exact recorded physical command changed")
        result[name] = str(path)
    return result


def command(manifest, argv):
    started = time.time_ns()
    process = subprocess.run(argv, stdin=subprocess.DEVNULL, check=False)
    item = {"argv": argv, "start_unix_ns": started, "end_unix_ns": time.time_ns(),
            "exit_code": process.returncode}
    manifest["commands"].append(item)
    (ROOT / "operational-runs.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    if process.returncode != 0:
        raise RuntimeError("A generated product operation failed; preserve its original receipts and home")
    return item


def run_item(name, script, base, extra, manifest, older=None):
    output = ROOT / name
    if output.exists():
        raise ValueError("Do not replace a preceding generated product capture")
    scratch = Path(tempfile.mkdtemp(prefix="rop.", dir=os.environ["TMPDIR"]))
    argv = [sys.executable, str(ROOT / script), *base, *extra,
            "--scratch", str(scratch), "--output", str(output)]
    try:
        command(manifest, argv)
        summary = load(output / "summary.json")
        if summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True:
            raise RuntimeError("The generated capture is incomplete or still active")
        if older:
            command(manifest, [sys.executable, str(ROOT / "older_refusal.py"),
                               "--run", str(output), "--binary", older])
            if load(output / "older-program-refusal.json").get("complete") is not True:
                raise RuntimeError("The older-program refusal lacks its complete receipt")
        original("run_pairs").cleanup(output.resolve(strict=True))
        print(json.dumps({"operational_capture": name, "complete": True,
                          "scratch_removed": not scratch.exists()}), flush=True)
    except BaseException:
        manifest["retained_scratch"] = str(scratch.resolve())
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-revision", required=True)
    parser.add_argument("--after-revision", required=True)
    args = parser.parse_args()
    if (ROOT / "operational-runs.json").exists():
        raise ValueError("Do not replace a preceding operational run manifest")
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock")
    builds = validate_revisions(ROOT, args.before_revision, args.after_revision)
    for item in builds.values():
        if digest(Path(item["binary"])) != item["binary_sha256"]:
            raise ValueError("A recorded measured node binary changed")
    before = disk_build("before", args.before_revision)
    after = disk_build("after", args.after_revision)
    base = ["--binary", builds["after"]["binary"], "--source-revision", args.after_revision,
            "--disk-binary", after["lastdb-disk-home"], "--maintenance-binary", after["lastdb-pack-maintain"]]
    manifest = {"complete": False, "commands": [], "before_revision": args.before_revision,
                "after_revision": args.after_revision, "start_unix_ns": time.time_ns(),
                "scope": "One owned generated home at a time; no primary, archive, or cloud access"}
    try:
        run_item("retention-operational", "retention_capture.py", base,
                 ["--recovery-binary", after["lastdb-disk-recovery"]], manifest)
        run_item("edge-operational", "edge_capture.py", base,
                 ["--before-binary", builds["before"]["binary"]], manifest,
                 older=before["lastdb-disk-home"])
        run_item("overflow-operational", "overflow_capture.py", base, [], manifest)
        manifest["complete"] = True
    except BaseException as failure:
        manifest["error"] = str(failure)
        raise
    finally:
        manifest["end_unix_ns"] = time.time_ns()
        (ROOT / "operational-runs.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"operational_checks_complete": True, "checks": 4}), flush=True)


if __name__ == "__main__":
    main()
