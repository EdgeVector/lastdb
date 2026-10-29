#!/usr/bin/env python3
"""CI-only upgrade of a generated database with existing field proteins."""

import argparse
import os
from pathlib import Path

from node_ops import (ENVIRONMENT, Node, file_hash, install, molecule, protein, read,
                      require, same_atom, save, schema, write)


def upgrade(node, candidate, revision):
    definitions = [schema("OldField" + str(number), "upgrade shared", cross=number % 2 == 1, declared=False)
                   for number in range(4)]
    # Two independent old proteins share only the payload field. Their other
    # field identities differ by family, so the old schema-overlap rule refuses
    # to join them. Each pair internally shares all fields and different keys.
    for number in (2, 3):
        other = schema(definitions[number]["name"], "upgrade other", cross=number % 2 == 1, declared=False)
        for mapping in ("field_descriptions",):
            other[mapping]["payload"] = definitions[number][mapping]["payload"]
        definitions[number] = other
    names = [install(node, definition) for definition in definitions]
    keys = [{"hash": "record"}, {"hash": "bucket", "range": "record"}]
    values = [{"record_id": "record", "group_id": "bucket", "payload": "first family"},
              {"record_id": "record", "group_id": "bucket", "payload": "second family"}]
    for index in (0, 2):
        write(node, names[index], keys[0], values[index // 2])
    molecules = [molecule(node, name) for name in names]
    before_ids = [protein(node, member) for member in molecules]
    require(all(before_ids), "baseline did not create field proteins")
    before = [read(node, name, keys[index % 2], values[index // 2])
              for index, name in enumerate(names)]
    node.stop()
    node.binary, node.revision = candidate, revision
    node.start()
    # The first candidate request is an ordinary write. No schema reload or
    # candidate read may establish membership before sibling preparation.
    write(node, names[0], keys[0], {"payload": "after upgrade"}, "update")
    after_ids = [protein(node, member) for member in molecules]
    require(len(set(after_ids)) == 1, "existing equal fields remain on separate proteins")
    expected = [dict(value, payload="after upgrade") for value in values]
    after = [read(node, name, keys[index % 2], expected[index // 2])
             for index, name in enumerate(names)]
    require([[record["metadata"][field]["atom_uuid"] for field in ("record_id", "group_id")]
             for record in before]
            == [[record["metadata"][field]["atom_uuid"] for field in ("record_id", "group_id")]
                for record in after], "membership upgrade changed untouched field atoms")
    shared = same_atom(*(read(node, name, keys[index % 2], expected[index // 2])
                         for index, name in enumerate(names)))
    node.stop()
    node.start()
    require(all(protein(node, member) == after_ids[0] for member in molecules),
            "upgraded back-references differ after reopen")
    for index, name in enumerate(names):
        read(node, name, keys[index % 2], expected[index // 2])
    return {"original_proteins": before_ids, "original_protein_count": len(set(before_ids)),
            "final_protein": after_ids[0], "first_candidate_request": "ordinary update without schema reload",
            "shared_atom_after_new_write": shared, "reopen_verified": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "candidate", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("baseline-revision", "candidate-revision"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "CI only")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    require(args.output.is_absolute() and not args.output.exists()
            and args.output.parent.resolve(strict=True).is_relative_to(root), "fresh CI output required")
    args.output.mkdir(mode=0o700)
    node = Node(args.baseline, args.baseline_revision, args.output, environment=ENVIRONMENT,
                start_seconds=120, temporary_directory=root)
    result = {"baseline": args.baseline_revision, "candidate": args.candidate_revision,
              "baseline_binary_sha256": file_hash(args.baseline),
              "candidate_binary_sha256": file_hash(args.candidate), "status": "failed"}
    try:
        node.start()
        result["observed"] = upgrade(node, args.candidate, args.candidate_revision)
        result["status"] = "passed"
    finally:
        node.close()
        save(args.output / "protein-upgrade.json", result)
    print("Field protein upgrade: " + result["status"], flush=True)


if __name__ == "__main__":
    main()
