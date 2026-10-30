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
from node_proof import Case, MINIMUM_LOGICAL, PRIMARY_SETTINGS, SCHEMAS, schema_request
import cleanup

COUNT = 6144
GROUPS = 24
ORIGINAL_SEED = {"base": 3000, "creates": 1000, "updates": 4000, "deletes": 250}


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


def verify(node, alternate, model, deleted):
    keys = sorted(set(model) | deleted)
    requests = 0
    for offset in range(0, len(keys), 64):
        chunk = keys[offset:offset + 64]
        body = {"schema_name": "Task", "fields": FIELDS,
                "filter": {"HashRangeKeys": [[key, ""] for key in chunk]}, "limit": len(chunk)}
        check_rows(node.call("/api/query", body)[0], {key: model[key] for key in chunk if key in model})
        requests += 1
    for name, field, peer in (("by_group", "group_id", "ByGroup"), ("by_owner", "owner_id", "ByOwner")):
        seen = set()
        # Include all declared hash values, even if a change leaves one empty.
        for number in range(GROUPS):
            value = f"{'g' if field == 'group_id' else 'o'}{number:02}"
            body = {"schema_name": "Task" if alternate else peer, "fields": FIELDS,
                    "filter": {"HashKey": value}, "limit": 1000}
            if alternate:
                body["key_name"] = name
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


def checkpoint(node, alternate, model, deleted, output, name):
    verified = verify(node, alternate, model, deleted)
    # Qualification uses caller-owned records, independent of atoms, tips, or compression.
    require(verified["source_bytes"] >= 16 * 1024 * 1024, "source corpus is below 16 MiB")
    report = node.call("/api/db/schemas")[0]["schema_storage"]
    receipt = cleanup.request_and_wait(node)
    node.stop()
    disk = node.disk_breakdown()
    result = {"verified": verified, "schema_storage": report, "disk": disk, "cleanup": receipt}
    save(output / f"{name}.json", result)
    node.start()
    require(verify(node, alternate, model, deleted) == verified, "reopen changed the source records")
    return result


def storage_arm(args, label, pair, output):
    alternate = label == "candidate"
    environment = {**PRIMARY_SETTINGS, **ENVIRONMENT}
    node = Node(getattr(args, label), getattr(args, label + "_revision"), output,
                environment=environment, temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
    model = {f"space-{i:05}": record(i) for i in range(COUNT)}
    try:
        node.start()
        declare(node, alternate, records=0)
        write(node, [mutation(key, fields, "create") for key, fields in model.items()])
        seeded = checkpoint(node, alternate, model, set(), output, "seed")
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
        changed = checkpoint(node, alternate, model, deleted, output, "changes")
        return {"label": label, "pair": pair, "seed": seeded, "changes": changed}
    finally:
        node.close()


def original_seed_arm(args, label, output):
    case = Case((getattr(args, label), getattr(args, label + "_revision")), output)
    try:
        case.node.start()
        for schema in SCHEMAS:
            case.node.call("/api/schemas/seed-system", schema_request(schema))
        writes = case.write(case.seed.phase(**ORIGINAL_SEED))
        reads = case.reads()
        listed = case.listed("original seed audit")
        require(not case.failures, "original seed audit has different key membership")
        logical = case.logical()
        receipt = cleanup.request_and_wait(case.node)
        case.node.stop()
        result = {"label": label, "profile": ORIGINAL_SEED, "writes": writes, "reads": reads,
                  "listed": listed, "logical": logical, "disk": case.disk(), "cleanup": receipt,
                  "qualifies_for_legacy_atom_volume_gate": logical["bytes"] >= MINIMUM_LOGICAL,
                  "alternate_keys_declared": False}
        save(output / "original-seed.json", result)
        return result
    finally:
        case.node.close()


def report(args, pairs, audits):
    require(audits[0]["reads"] == audits[1]["reads"], "original seed differs across revisions")
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
    lines += ["", "The native atom report is retained separately. It counts each serialized atom body once,",
              "including provenance; it excludes alternate entries and field tips. It is not the fixed input size.",
              "The existing disk ratio gates retain their thresholds.", "", "### Original disk-proof seed audit", "",
              "This audit uses the original 3,000-base seed and declares no alternate keys.",
              "Its atom-volume qualification is reported separately from data correctness.", "",
              "| Source | Logical atoms MiB | Atoms | Live records verified | Meets old 16 MiB minimum |",
              "|---|---:|---:|---:|---|"]
    for audit in audits:
        logical = audit["logical"]
        lines.append(f"| {audit['label']} | {logical['bytes'] / 2**20:.2f} | {logical['atoms']} | "
                     f"{audit['reads']['live']} | {audit['qualifies_for_legacy_atom_volume_gate']} |")
    lines += ["", "The original audit is not the alternate-key space comparison.", ""]
    text = "\n".join(lines)
    save(args.output / "summary.json", {"baseline": args.baseline_revision, "candidate": args.candidate_revision,
                                       "pairs": pairs, "original_seed": audits})
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
    pairs, audits = [], []
    for pair in range(2):
        for label in (("baseline", "candidate") if pair == 0 else ("candidate", "baseline")):
            output = args.output / f"pair-{pair}-{label}"
            output.mkdir(mode=0o700)
            print(f"Storage pair {pair}: {label}", flush=True)
            pairs.append(storage_arm(args, label, pair, output))
    for label in ("baseline", "candidate"):
        output = args.output / f"original-seed-{label}"
        output.mkdir(mode=0o700)
        print(f"Original seed audit: {label}", flush=True)
        audits.append(original_seed_arm(args, label, output))
    report(args, pairs, audits)


if __name__ == "__main__":
    main()
