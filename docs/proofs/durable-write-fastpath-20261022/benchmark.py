#!/usr/bin/env python3
"""Capture actual before/after daemon writes and notification recovery on fresh homes."""

import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
PROOF = Path(__file__).resolve().parent
ROOT = PROOF.parents[2]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


capture = module("timing_capture", PROOF.parent / "snapshot-write-timing-20261022/capture.py")
capture.PROOF = PROOF
original_recovery = capture.base.schemas.recovery
original_payload = capture.base.schemas.payload
ALIASES = False


def alias_payload(schema, number, batch_size, operation):
    body, values = original_payload(schema, number, batch_size, operation)
    for mutation in body["mutations"]:
        mutation["schema"] = " " + schema + " "
    return body, values


def recovery_with_notifications(node, args, expected):
    original_recovery(node, args, expected)
    if args.output.name.startswith("before"):
        return
    required = {identity: (sample, position)
                for sample in json.loads((args.output / "writes.json").read_text())["samples"]
                for position, identity in enumerate(sample["receipt"]["mutation_ids"])}
    sentinel = json.loads((args.output / "forced-stop.json").read_text())["receipt"]["mutation_id"]
    cursor, events = None, []
    for _ in range(8):
        page = node.call("/api/app/changes", {"since_cursor": cursor, "limit": 500})
        events.extend(page["changes"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    else:
        raise RuntimeError("notification replay exceeded the bounded generated workload")
    counts = Counter(e["mutation_id"] for e in events)
    if any(counts[identity] != 1 for identity in [*required, sentinel]):
        raise RuntimeError("an acknowledged mutation notification is absent or duplicated after forced stop")
    for event in events:
        selected = required.get(event["mutation_id"])
        sample, position = selected if selected else (None, None)
        expected_schema = " " + sample["schema"] + " " if sample and ALIASES else sample["schema"] if sample else None
        if sample and (event["schema_name"] != expected_schema
                       or event["operation"] != sample["mutation_type"]
                       or event["key"] != {"hash": sample["schema"], "range": f"record-{position:03}"}):
            raise RuntimeError("notification schema, key, or operation differs from the submitted write")
    capture.save(args.output / "notification-recovery.json", {
        "measured_mutations": len(required), "all_measured_once": True,
        "sentinel_notification_survives_immediate_sigkill": True,
        "event_count": len(events), "events": events})


def main():
    global ALIASES
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--number", type=int, required=True)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--no-profile", action="store_true")
    parser.add_argument("--aliases", action="store_true")
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise RuntimeError("hold the compiler exclusion for the complete measurement")
    scratch = args.scratch.resolve()
    if scratch.stat().st_uid != os.getuid() or not (scratch / "before").is_file():
        raise RuntimeError("use the owned scratch root with its pinned baseline")
    binary = scratch / args.name
    if binary.exists():
        raise RuntimeError("preserve prior binaries; use a fresh candidate name")
    shutil.copyfile(ROOT / "target/dev-release/lastdbd", binary)
    binary.chmod(0o755)
    if hashlib.sha256(binary.read_bytes()).digest() == hashlib.sha256((scratch / "before").read_bytes()).digest():
        raise RuntimeError("candidate build is unchanged from the pinned baseline")
    capture.save(PROOF / (args.name + "-identity.json"), {
        "source": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "baseline_sha256": hashlib.sha256((scratch / "before").read_bytes()).hexdigest(),
        "scratch": str(scratch), "uid": os.getuid(), "load_before": os.getloadavg(),
        "product_source_diff": subprocess.check_output(["git", "diff", "HEAD", "--", "fold_db", "vendor", "lastdb_node"],
                                                       cwd=ROOT, text=True),
        "compiler_exclusion": True, "other_host_work_controlled": False})
    capture.base.schemas.recovery = recovery_with_notifications
    ALIASES = args.aliases
    if ALIASES:
        capture.base.schemas.payload = alias_payload
    if args.baseline:
        print("baseline", flush=True)
        capture.capture(scratch / "before", scratch, "before", False, True, 0)
    print(args.name, flush=True)
    capture.capture(binary, scratch, args.name, not args.no_profile, True, args.number)
    if ALIASES:
        inputs_path = PROOF / args.name / "inputs.json"
        inputs = json.loads(inputs_path.read_text())
        inputs["schema_aliases"] = "whitespace aliases; notifications preserve caller spelling"
        capture.save(inputs_path, inputs)


if __name__ == "__main__":
    main()
