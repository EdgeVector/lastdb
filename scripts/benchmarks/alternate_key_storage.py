#!/usr/bin/env python3
"""CI storage comparison with fixed source records and complete keyed reads."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import sys

from alternate_key_compare import FIELDS, check_rows, declare, mutation
from node import Node, save
from alternate_key_workload import ENVIRONMENT, require

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "disk_v2"))
import cleanup

COUNT = 6144
GROUPS = 24
# The primary launchd job settings the node reads: record size, memory budget, and admin timeout.
STORAGE_SETTINGS = {
    "LASTDB_MAX_FIELD_VALUE_BYTES": "524288",
    "LASTDB_RESIDENT_MAX_DEFERRED_BYTES": "67108864",
    "LASTDB_RSS_BUDGET_MULTIPLIER": "1.75",
    "LASTDBD_RSS_LIMIT_MB": "16384",
    "LASTDB_UDS_ADMIN_TIMEOUT_SECS": "3600",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def record(number, version=0, moved=False):
    key = f"space-{number:05}"
    payload = base64.b64encode(hashlib.shake_256(f"{key}:{version}".encode()).digest(3072)).decode()
    return {"record_id": key, "group_id": f"g{(number + moved) % GROUPS:02}",
            "owner_id": f"o{(number + 3 * moved) % GROUPS:02}", "payload": payload}


def model_facts(model):
    ordered = sorted(model.items())
    return {"records": len(model), "source_bytes": sum(len(canonical(fields)) for _, fields in ordered),
            "digest": hashlib.sha256(canonical(ordered)).hexdigest()}


def write(node, mutations):
    for offset in range(0, len(mutations), 32):
        chunk = mutations[offset:offset + 32]
        reply = node.call("/api/mutations/batch", chunk)[0]
        require(reply.get("count") == len(chunk) and reply.get("durability") == "durable",
                "storage comparison lacks a complete durable batch acknowledgement")
    require(node.call("/api/db/flush", {})[0].get("durable") is True, "storage flush is not durable")


def verify(node, model, deleted):
    keys = sorted(set(model) | deleted)
    requests = 0
    for offset in range(0, len(keys), 64):
        chunk = keys[offset:offset + 64]
        body = {"schema_name": "Task",
                "filter": {"HashRangeKeys": [[key, ""] for key in chunk]}, "limit": len(chunk)}
        check_rows(node.call("/api/query", body)[0], {key: model[key] for key in chunk if key in model})
        requests += 1
    for name, field in (("by_group", "group_id"), ("by_owner", "owner_id")):
        seen = set()
        # Include all declared hash values, even if a change leaves one empty.
        for number in range(GROUPS):
            value = f"{'g' if field == 'group_id' else 'o'}{number:02}"
            body = {"schema_name": "Task", "key_name": name,
                    "filter": {"HashKey": value}, "limit": 1000}
            reply = node.call("/api/query", body)[0]
            expected = {key: fields for key, fields in model.items() if fields[field] == value}
            require(not reply.get("has_more"), "storage comparison truncated an alternate result")
            check_rows(reply, expected)
            require(not seen.intersection(expected), "one record occupies two hashes of the same key")
            seen.update(expected)
            requests += 1
        require(seen == set(model), "alternate key does not cover every source record")
    return {**model_facts(model), "deleted_absent": len(deleted), "read_requests": requests,
            "primary_and_both_alternates_equal": True}


def checkpoint(node, model, deleted, output, name):
    verified = verify(node, model, deleted)
    # Qualification uses caller-owned records, independent of stored records or compression.
    require(verified["source_bytes"] >= 16 * 1024 * 1024, "source corpus is below 16 MiB")
    report = node.call("/api/db/schemas")[0]["schema_storage"]
    receipt = cleanup.request_and_wait(node)
    node.stop()
    disk = node.disk_breakdown()
    result = {"verified": verified, "schema_storage": report, "disk": disk, "cleanup": receipt}
    save(output / f"{name}.json", result)
    node.start()
    require(verify(node, model, deleted) == verified, "reopen changed the source records")
    return result


def storage_arm(args, label, pair, output):
    environment = {**STORAGE_SETTINGS, **ENVIRONMENT}
    node = Node(getattr(args, label), getattr(args, label + "_revision"), output,
                environment=environment, temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve(), record_protocol=True)
    model = {f"space-{i:05}": record(i) for i in range(COUNT)}
    try:
        node.start()
        declare(node, records=0)
        write(node, [mutation(key, fields, "create") for key, fields in model.items()])
        seeded = checkpoint(node, model, set(), output, "seed")
        changes = []
        for number in range(512):
            fields = record(number, version=1, moved=True)
            model[fields["record_id"]] = fields
            changes.append(mutation(fields["record_id"], fields))
        deleted = {f"space-{i:05}" for i in range(512, 768)}
        for key in sorted(deleted):
            del model[key]
            changes.append(mutation(key, operation="delete"))
        write(node, changes)
        changed = checkpoint(node, model, deleted, output, "changes")
        return {"label": label, "pair": pair, "seed": seeded, "changes": changed}
    finally:
        node.close()


def report(args, pairs):
    lines = ["## Alternate-key storage comparison", "",
             f"Baseline: `{args.baseline_revision}`. Candidate: `{args.candidate_revision}`.",
             "Two fresh-home pairs use orders AB and BA. All source records and operations match.",
             "Each checkpoint verifies every primary key and both alternate paths, then repeats after reopen.",
             "The fixed input corpus must exceed 16 MiB. A smaller output is allowed.",
             "Disk sizes follow explicit native cleanup and a successful daemon stop; they are not peak-write sizes.", "",
             "| Pair | Phase | Records | Source MiB | Base disk MiB | New disk MiB | Change |",
             "|---:|---|---:|---:|---:|---:|---:|"]
    for pair in range(2):
        arms = {arm["label"]: arm for arm in pairs if arm["pair"] == pair}
        for phase in ("seed", "changes"):
            base, new = (arms[label][phase] for label in ("baseline", "candidate"))
            require(base["verified"] == new["verified"], "storage arms differ in their source records")
            # Match the usual conservative file size: max(apparent length, allocated bytes).
            bsize = max(base["disk"]["database"].values())
            nsize = max(new["disk"]["database"].values())
            facts = base["verified"]
            require(bsize > 0 and nsize > 0, "storage comparison lacks database files")
            lines.append(f"| {pair} | {phase} | {facts['records']} | {facts['source_bytes'] / 2**20:.2f} | "
                         f"{bsize / 2**20:.2f} | {nsize / 2**20:.2f} | {(nsize / bsize - 1) * 100:+.1f}% |")
    lines += ["", "The native record report is retained separately. It counts each serialized record body once,",
              "including provenance; it excludes alternate entries. It is not the fixed input size.", ""]
    text = "\n".join(lines)
    save(args.output / "summary.json", {"baseline": args.baseline_revision, "candidate": args.candidate_revision,
                                       "pairs": pairs})
    (args.output / "summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument("--" + label, type=Path, required=True)
        parser.add_argument("--" + label + "-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "CI only")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(not args.output.exists() and args.output.parent.resolve().is_relative_to(runner),
            "fresh output under RUNNER_TEMP required")
    args.output.mkdir(mode=0o700)
    pairs = []
    for pair in range(2):
        for label in (("baseline", "candidate") if pair == 0 else ("candidate", "baseline")):
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir(mode=0o700)
            print(f"Storage pair {pair}: {label}", flush=True)
            pairs.append(storage_arm(args, label, pair, output))
    report(args, pairs)


if __name__ == "__main__":
    main()
