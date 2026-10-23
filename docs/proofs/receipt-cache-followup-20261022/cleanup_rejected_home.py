#!/usr/bin/env python3
"""Remove only the stale-engine capture home after genuine replacement proof."""

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

FAILED = ROOT / "rejected-build-1/retention-operational"
SCRATCH = Path("/private/var/folders/8n/hvjkb3pd0xddwxg89qp5ymgc0000gn/T/rop.9wuprvsp")


def digest(path):
    if path.stat().st_size > 32 * 1024 ** 2:
        raise ValueError("The narrow failure evidence exceeds its finite byte bound")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replacements():
    result = {}
    for name in ("retention-operational", "edge-operational", "overflow-operational",
                 "auto-reclaim-operational", "orphan-repeat-operational", "before-writer-capability", "after-writer-capability"):
        folder = ROOT / name
        summary, cleanup = guard.load(folder / "summary.json"), guard.load(folder / "cleanup.json")
        if (summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True
                or cleanup.get("removed") is not True or Path(summary["scratch"]).exists()):
            raise ValueError("The genuine replacement proof and owned cleanup must finish first")
        result[name] = {"summary_sha256": digest(folder / "summary.json"), "cleanup_sha256": digest(folder / "cleanup.json")}
    if guard.load(ROOT / "edge-operational/older-program-refusal.json").get("complete") is not True:
        raise ValueError("The genuine older-reader refusal proof is incomplete")
    return result


def preserved():
    summary = guard.load(FAILED / "summary.json")
    rejected = guard.load(ROOT / "rejected-cache-build-1.json")
    expected = {"all_owned_processes_stopped": True, "complete": False,
                "error": "The operational proof requires precise current-part accounting",
                "home": str(SCRATCH / "home"), "scratch": str(SCRATCH), "scratch_retained": True}
    if (summary != expected or rejected.get("rejected_for_measurement") is not True
            or rejected.get("product_failure_established") is not False):
        raise ValueError("The original stale-engine failure distinction changed")
    names = ("summary.json", "ownership.json", "receipts.jsonl", "inputs.json", "node.log",
             "retained-root-verified.json", "seed-response.json")
    result = {name: digest(FAILED / name) for name in names}
    for name in ("summary.json", "ownership.json", "retained-root-verified.json", "seed-response.json"):
        if result[name] != rejected["rejected_evidence_sha256"]["retention-operational/" + name]:
            raise ValueError("Original failure evidence differs from the rejection manifest")
    return result


def main():
    target = FAILED / "failed-scratch-cleanup.json"
    if target.exists():
        raise ValueError("Do not replace a narrow failed-home cleanup receipt")
    proofs, hashes = replacements(), preserved()
    owner = guard.load(FAILED / "ownership.json")
    root = Path(owner["scratch"])
    if (root != SCRATCH or root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or owner.get("primary_data") is not False or owner.get("home") != str(root / "home")):
        raise ValueError("The failed root differs from its exact owned generated home")
    info = root.stat()
    if (info.st_uid != os.getuid() or info.st_uid != owner["owner_uid"] or stat.S_IMODE(info.st_mode) != 0o700
            or (info.st_dev, info.st_ino) != (owner["device"], owner["inode"])
            or guard.load(root / ".snapshot-capture-owner.json") != owner):
        raise ValueError("The failed root ownership marker or directory identity changed")
    current = root / "home/data/disk-pack/CURRENT"
    if digest(current) != guard.load(FAILED / "retained-root-verified.json")["current_sha256"]:
        raise ValueError("The generated failed root changed after its preserved inventory")
    stopped = guard.owned_processes(FAILED)
    guard.no_active_sockets(root)
    listing = guard.inventory(root)
    if listing["logical_file_bytes"] > 64 * 1024 ** 2:
        raise ValueError("The exact failed root exceeds the narrow sixty-four MiB bound")
    receipt = {"scratch": str(root), "owner": owner, "stopped_pids": stopped, "pre_cleanup": listing,
               "original_evidence_sha256": hashes, "replacement_proof_sha256": proofs,
               "original_false_summary_unchanged": True, "removed": False, "at_unix_ns": time.time_ns()}
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(root)
    if root.exists() or preserved() != hashes:
        raise ValueError("The failed root remains or original failure evidence changed")
    receipt["removed"] = True
    target.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"rejected_owned_home_removed": True, "original_summary_stays_false": True}))


if __name__ == "__main__":
    main()
