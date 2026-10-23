#!/usr/bin/env python3
"""Remove only the preserved parser-failure scratch after the replacement proof."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time

sys.dont_write_bytecode = True
from reuse import ROOT, original
original("benchmark")
import cleanup as guard

FAILED = ROOT / "failed-retention-operational-envelope-1"
SCRATCH = Path("/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/rop.ocpgkdzu")


def digest(path):
    if path.stat().st_size > 32 * 1024 ** 2:
        raise ValueError("The failure evidence exceeds its finite byte bound")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replacement_proof():
    paths = {}
    for name in ("retention-operational", "edge-operational", "overflow-operational", "auto-reclaim-operational",
                 "orphan-repeat-operational"):
        run = ROOT / name
        summary, cleanup = guard.load(run / "summary.json"), guard.load(run / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(summary["scratch"]).exists()):
            raise ValueError("A required replacement product proof or its owned cleanup is incomplete")
        paths[name] = {"summary_sha256": digest(run / "summary.json"),
                       "cleanup_sha256": digest(run / "cleanup.json")}
    older = ROOT / "edge-operational" / "older-program-refusal.json"
    if guard.load(older).get("complete") is not True:
        raise ValueError("The older program refusal proof is incomplete")
    paths["older-program-refusal"] = digest(older)
    return paths


def failure_evidence():
    summary = guard.load(FAILED / "summary.json")
    diagnosis = guard.load(FAILED / "failure-diagnosis.json")
    if (summary != {"all_owned_processes_stopped": True, "complete": False, "error": "'files'",
                    "home": str(SCRATCH / "home"), "scratch": str(SCRATCH), "scratch_retained": True}
            or diagnosis.get("product_failure_established") is not False
            or diagnosis.get("failure_kind") != "Operational proof parser error"
            or diagnosis.get("benchmark_runs_started") is not False):
        raise ValueError("The narrow cleanup does not match the original parser failure")
    inventory = FAILED / "retained-root-response.json"
    if digest(inventory) != diagnosis["original_inventory_sha256"]:
        raise ValueError("The original successful inventory evidence changed")
    names = ("summary.json", "failure-diagnosis.json", "ownership.json", "receipts.jsonl",
             "retained-root-response.json", "seed-response.json", "inputs.json", "node.log")
    return {name: digest(FAILED / name) for name in names}


def ownership():
    owner = guard.load(FAILED / "ownership.json")
    root = Path(owner["scratch"])
    if (root != SCRATCH or root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or owner.get("primary_data") is not False or owner.get("home") != str(root / "home")):
        raise ValueError("The failed scratch differs from its exact owned generated home")
    info = root.stat()
    if (info.st_uid != os.getuid() or info.st_uid != owner["owner_uid"]
            or stat.S_IMODE(info.st_mode) != 0o700
            or info.st_dev != owner["device"] or info.st_ino != owner["inode"]
            or guard.load(root / ".snapshot-capture-owner.json") != owner):
        raise ValueError("The failed scratch ownership differs")
    return owner


def main():
    target = FAILED / "failed-scratch-cleanup.json"
    if target.exists():
        raise ValueError("Do not replace an earlier narrow cleanup receipt")
    replacements = replacement_proof()
    original_hashes = failure_evidence()
    owner = ownership()
    stopped = guard.owned_processes(FAILED)
    guard.no_active_sockets(SCRATCH)
    listing = guard.inventory(SCRATCH)
    if listing["logical_file_bytes"] > 64 * 1024 ** 2:
        raise ValueError("The failed scratch exceeds the narrow sixty-four MiB bound")
    receipt = {"scratch": str(SCRATCH), "ownership": owner, "stopped_pids": stopped,
               "pre_cleanup": listing, "failure_evidence_sha256": original_hashes,
               "replacement_proof_sha256": replacements, "at_unix_ns": time.time_ns(),
               "original_failed_summary_remains_false": True, "removed": False}
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(SCRATCH)
    if SCRATCH.exists() or failure_evidence() != original_hashes:
        raise RuntimeError("The failed scratch remains or its preserved failure evidence changed")
    receipt["removed"] = True
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"failed_scratch_removed": True, "original_failure_unchanged": True}), flush=True)


if __name__ == "__main__":
    main()
