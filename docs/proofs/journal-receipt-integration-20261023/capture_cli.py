#!/usr/bin/env python3
"""Capture real native CLI operations on one fresh generated home; start no node."""

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

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
PROOFS = HERE.parent
sys.path.insert(0, str(PROOFS / "snapshot-commits-20261022"))
from capture import Capture, file_hash, save
from cleanup import inventory, no_active_sockets
from command_capture import parts
sys.path.insert(0, str(PROOFS / "receipt-cache-followup-20261022"))
from retention_capture import COLLECTION, PREFIX, body, key, mutation


def source(repo, revision):
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=repo, text=True)
    if actual != revision or dirty:
        raise ValueError("The parent source differs from its exact clean build revision")


def invoke(capture, binary, label, verb, request=None, good=True, **flags):
    if time.monotonic() > capture.deadline:
        raise ValueError("The finite CLI capture deadline expired")
    home = capture.home / "data/disk-pack"
    argv = [str(binary), verb, "--home", str(home)]
    if request is not None:
        path = capture.output / (label + (".jsonl" if verb in ("build", "write", "plan") else ".json"))
        if verb in ("build", "write", "plan"):
            path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in request))
        else:
            save(path, request)
        flags["input"] = path
    for name, value in flags.items():
        argv += ["--" + name.replace("_", "-"), str(value)]
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
    started = time.monotonic_ns()
    process = subprocess.Popen(argv, env=environment, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    capture.cli_pids.append(process.pid)
    capture.cli_processes.append(process)
    capture.remember("owned-cli-start", label=label, pid=process.pid, argv=argv)
    try:
        stdout, stderr = process.communicate(timeout=min(60, max(1, capture.deadline - time.monotonic())))
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=10)
    except BaseException:
        process.kill()
        process.wait(timeout=10)
        raise
    if len(stdout) > 4 * 1024 * 1024 or len(stderr) > 65536:
        raise ValueError("A finite CLI command output exceeds its saved byte bound")
    (capture.output / (label + "-stdout.txt")).write_bytes(stdout)
    (capture.output / (label + "-stderr.txt")).write_bytes(stderr)
    capture.remember("owned-cli-complete", label=label, pid=process.pid, argv=argv,
                     exit_code=process.returncode, elapsed_nanos=time.monotonic_ns() - started,
                     stdout_sha256=hashlib.sha256(stdout).hexdigest(),
                     stderr_sha256=hashlib.sha256(stderr).hexdigest())
    if good:
        if process.returncode or stderr:
            raise RuntimeError("A real CLI success command failed or wrote stderr: " + label)
        value = json.loads(stdout)
        if not isinstance(value, dict) or not {"disk", "elapsed_nanos"}.issubset(value):
            raise ValueError("The typed success response lacks its common fields")
        save(capture.output / (label + "-response.json"), value)
        return value
    if not process.returncode or stdout or len(stderr) > 256:
        raise ValueError("A real refused command lacks bounded failure-only output")
    if capture.marker.encode() in stderr or str(capture.root).encode() in stderr:
        raise ValueError("A refused command discloses generated caller or path text")
    return stderr.decode()


def fingerprint(capture, label):
    home = capture.home / "data/disk-pack"
    files = parts(home)
    if sum(files.values()) > 64 * 1024 * 1024:
        raise ValueError("The generated CLI home exceeds sixty-four MiB")
    value = {name: {"bytes": size, "sha256": file_hash(home / name)} for name, size in files.items()}
    save(capture.output / (label + "-files.json"), value)
    return value


def readback(capture, binary, label, expected, versions):
    coordinates = [{"collection": COLLECTION, "key": key(number)} for number in range(65)]
    value = invoke(capture, binary, label + "-exact", "read", {"coordinates": coordinates})
    if value["results"] != [{"body_hex": expected.get(key(number))} for number in range(65)]:
        raise ValueError("Batched keyed reads lose a body, empty slot, or input order")
    ranged = invoke(capture, binary, label + "-range", "range", {
        "collection": COLLECTION, "prefix": PREFIX, "start": "key-000000", "end": "key-000064",
        "page_size": 7, "max_pages": 16})
    records = [row for page in ranged["pages"] for row in page["records"]]
    if (ranged["exhausted"] is not True or any(len(page["records"]) > 7 for page in ranged["pages"])
            or [row["key"] for row in records] != sorted(expected)
            or {row["key"]: row["body_hex"] for row in records} != expected):
        raise ValueError("The ordered range loses keys, bodies, or its page bound")
    for row in records:
        if set(row) != {"key", "body_hex", "batch", "operation"} or (row["batch"], row["operation"]) != versions[row["key"]]:
            raise ValueError("A typed range record loses its public mutation version")
    if (value["generation"], value["frontier"]) != (ranged["generation"], ranged["frontier"]):
        raise ValueError("Fresh keyed and range processes disagree about the complete root")
    return value


def run(capture, binaries):
    disk = binaries["disk"]
    initial = [mutation(number, 0) for number in range(64)]
    initial[0]["body_hex"] = ("intentional-read-" + capture.marker).encode().hex()
    built = invoke(capture, disk, "build", "build", initial, fanout=1)
    if built["built"] is not True or built["records"] != 64 or built["required_features"] != 509:
        raise ValueError("The parent builder does not expose the expected complete snapshot")
    expected = {row["key"]: row["body_hex"] for row in initial}
    versions = {row["key"]: (0, 0) for row in initial}
    readback(capture, disk, "built-reopen", expected, versions)
    changes = [mutation(number, 1) for number in range(8)] + [mutation(number) for number in range(8, 12)]
    before = fingerprint(capture, "before-plan")
    planned = invoke(capture, disk, "plan", "plan", changes)
    if (planned["operations"] != 12 or planned["disk_reserved"] is not False
            or planned["logical_records_reserved"] is not False or planned["memory_ack_allowed"] is not False
            or planned["snapshot_quote"]["scope"] != "complete_snapshot"):
        raise ValueError("The plan response changes its public no-admission contract")
    if fingerprint(capture, "after-plan") != before:
        raise ValueError("The plan changed a generated native file")
    written = invoke(capture, disk, "write", "write", changes)
    if (written["published"] is not True or written["operations"] != 12
            or written["previous_frontier"] != built["frontier"]
            or written["frontier"] != built["frontier"] + 1
            or written["snapshot_quote"] != planned["snapshot_quote"]):
        raise ValueError("The real write differs from its quote or complete publication")
    for number in range(8):
        expected[key(number)] = body(number, 1)
        versions[key(number)] = (written["frontier"], number)
    for number in range(8, 12):
        expected.pop(key(number))
        versions.pop(key(number))
    readback(capture, disk, "written-reopen", expected, versions)
    shared(capture, disk, expected, versions)
    errors(capture, disk)
    final = fingerprint(capture, "final")
    return {"complete": True, "initial_records": 64, "remaining_original_records": 60,
            "shared_new_group_records": 4, "plan_files_unchanged": True,
            "fresh_process_keyed_range_versions_verified": True,
            "intentional_body_preserved": True, "bounded_errors_verified": True,
            "final_files": len(final), "cli_processes": len(capture.cli_pids)}


def shared(capture, disk, expected, versions):
    rows = [{"collection": COLLECTION + "-new", "key": "new\0" + f"key-{number:06d}",
             "body_hex": body(number, 2)} for number in range(4)]
    written = invoke(capture, disk, "shared-new-group", "write", rows)
    exact = invoke(capture, disk, "shared-batched-reopen", "read", {"coordinates": [
        {"collection": row["collection"], "key": row["key"]} for row in rows] + [
        {"collection": COLLECTION, "key": key(63)}]})
    if exact["results"] != [{"body_hex": row["body_hex"]} for row in rows] + [{"body_hex": expected[key(63)]}]:
        raise ValueError("The new shared group or an existing group loses its payload")
    ranged = invoke(capture, disk, "shared-range-reopen", "range", {
        "collection": COLLECTION + "-new", "prefix": "new\0", "page_size": 2, "max_pages": 4})
    records = [row for page in ranged["pages"] for row in page["records"]]
    wanted = [{"key": row["key"], "body_hex": row["body_hex"], "batch": written["frontier"], "operation": number}
              for number, row in enumerate(rows)]
    if ranged["exhausted"] is not True or records != wanted:
        raise ValueError("The new shared group's range differs from its offered write")
    readback(capture, disk, "shared-original-reopen", expected, versions)


def errors(capture, disk):
    invalid = capture.output / (capture.marker + "-invalid.json")
    invalid.write_text(capture.marker)
    first = invoke(capture, disk, "invalid-json", "read", good=False, input=invalid)
    missing = capture.root / (capture.marker + "-missing.json")
    second = invoke(capture, disk, "missing-input", "read", good=False, input=missing)
    if "invalid JSON input" not in first or "required file was not found" not in second:
        raise ValueError("The refused commands lose their static public category")
    limited = invoke(capture, disk, "per-key-limit", "read", {"coordinates": [
        {"collection": COLLECTION, "key": key(63)}], "budget": {"pages": 1, "blocks": 1, "bytes": 8192, "keys": 1}})
    if limited["results"] != [{"error": "disk-pack v1: read resource limit exceeded", "resource_exhausted": True}]:
        raise ValueError("A per-key resource error loses its bounded category or flag")


def cleanup(capture, summary):
    owner = json.loads((capture.output / "ownership.json").read_text())
    root = capture.root
    info = root.stat()
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or owner["owner_uid"] != info.st_uid
            or (info.st_dev, info.st_ino) != (owner["device"], owner["inode"])
            or owner["primary_data"] is not False or owner["home"] != str(root / "home")
            or json.loads((root / ".snapshot-capture-owner.json").read_text()) != owner
            or summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True):
        raise ValueError("Refuse any cleanup except the exact complete owned CLI capture")
    for pid in capture.cli_pids:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            continue
        raise ValueError("A recorded owned CLI process still exists")
    no_active_sockets(root)
    before = inventory(root)
    if before["logical_file_bytes"] > 64 * 1024 * 1024:
        raise ValueError("The exact cleanup subtree exceeds sixty-four MiB")
    receipt = {"removed": False, "ownership": owner, "scratch": str(root), "pre_cleanup": before,
               "stopped_cli_pids": capture.cli_pids, "node_processes_started": 0,
               "scope": "CLI-only cleanup; reuse bounded inventory/socket guards, not the node-only PID predicate"}
    save(capture.output / "cleanup.json", receipt)
    shutil.rmtree(root)
    receipt["removed"] = not root.exists()
    save(capture.output / "cleanup.json", receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "binary", "maintenance-binary", "recovery-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for the real CLI capture")
    source(args.source, args.source_revision)
    capture = Capture(args)
    capture.deadline, capture.cli_pids, capture.cli_processes = time.monotonic() + 180, [], []
    capture.marker = "generated-cli-" + capture.identity
    binaries = {"disk": args.binary.resolve(strict=True), "maintenance": args.maintenance_binary.resolve(strict=True),
                "recovery": args.recovery_binary.resolve(strict=True)}
    hashes = {name: file_hash(path) for name, path in binaries.items()}
    helpers = {Path(__file__).resolve()}
    for module in tuple(sys.modules.values()):
        name = getattr(module, "__file__", None)
        if name and Path(name).resolve().is_relative_to(PROOFS):
            helpers.add(Path(name).resolve())
    if len(helpers) > 100:
        raise ValueError("The exact imported proof-helper inventory exceeds one hundred files")
    helper_hashes = {str(path): file_hash(path) for path in sorted(helpers)}
    save(capture.output / "inputs.json", {"source_revision": args.source_revision, "source": str(args.source),
         "binary_sha256": hashes, "binaries": {name: str(path) for name, path in binaries.items()},
         "helper_sha256": helper_hashes, "node_processes": 0, "executed_binaries": ["disk"],
         "initial_records": 64, "hash_fanout": 1, "purpose": "Real native CLI output contract on generated data",
         "limits": {"seconds": 180, "home_bytes": 64 * 1024 * 1024, "command_output_bytes": 4 * 1024 * 1024}})
    summary = {"complete": False}
    try:
        summary.update(run(capture, binaries))
        source(args.source, args.source_revision)
        if {name: file_hash(path) for name, path in binaries.items()} != hashes:
            raise ValueError("A binary changed during its exact command capture")
        if {str(path): file_hash(path) for path in sorted(helpers)} != helper_hashes:
            raise ValueError("An imported proof helper changed during its exact command capture")
    except BaseException as error:
        summary.update(complete=False, error=str(error))
        raise
    finally:
        summary.update(scratch=str(capture.root),
                       all_owned_processes_stopped=all(process.poll() is not None for process in capture.cli_processes),
                       node_processes_started=0)
        save(capture.output / "summary.json", summary)
    cleanup(capture, summary)
    print(json.dumps({"complete": True, "home_removed": True, "node_processes": 0}))


if __name__ == "__main__":
    main()
