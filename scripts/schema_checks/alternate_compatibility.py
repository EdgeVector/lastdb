#!/usr/bin/env python3
"""Hosted-CI product proof for existing homes, alternate changes, and signed peers."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from alternate_keys import task_definition
from node_ops import ENVIRONMENT, install, require, schema, write
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from alternate_key_compare import FIELDS, check_rows, declare, mutation, query, values
from node import Node, file_hash, save
from alternate_changes import change, converted, owned_values, primary_rows, refused
from alternate_peers import peers

FOUR = "format=4\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\n"
FIVE = "format=5\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\nalternate-keys=v1\n"


def native_files(home):
    files = {}
    for name in ("disk-run", "disk-cas"):
        root = home / "data" / name
        paths = [root / "CURRENT", root / "laststore-run-home-v2"]
        runs = list((root / "runs").iterdir())
        require(len(runs) <= 10000, "native file proof exceeds its file bound")
        paths.extend(path for path in runs if path.suffix == ".ldrun")
        for path in paths:
            require(path.is_file() and not path.is_symlink(), "native proof encountered an invalid file")
            files[str(path.relative_to(home))] = file_hash(path)
    return files


def cli(args, node):
    command = [str(args.candidate_cli), "--data-dir", str(node.home), "db", "enable-alternate-keys", "--json"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    require(result.returncode == 0, f"offline alternate activation failed: {result.stderr[:800]}")
    return json.loads(result.stdout)


def refuse_old_writer(args, node, output):
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
    environment.update(ENVIRONMENT, LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1")
    with (output / "old-writer-refusal.log").open("w") as log:
        result = subprocess.run([str(args.baseline), "--data-dir", str(node.home),
                                 "--socket-path", str(node.socket), "--full-socket-path", str(node.root / "f.sock")],
                                env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=30)
    require(result.returncode != 0, "older writer accepted the enabled home")
    text = (output / "old-writer-refusal.log").read_text().lower()
    require("format" in text and ("unsupported" in text or "only format 4" in text or "not a format 4" in text),
            "older writer failed for a reason other than its format fence")


def legacy_home(args, output):
    node = Node(args.baseline, args.baseline_revision, output, environment=ENVIRONMENT,
                temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
    model = {f"existing-{i:04}": owned_values(i) for i in range(192)}
    result = {}
    try:
        node.start()
        require((node.home / "data/laststore-pack-node-v1").read_text() == FOUR, "baseline did not create format 4")
        declare(node, False)
        install(node, schema("ExistingTask", "existing compatibility", fields=FIELDS))
        for offset in range(0, 192, 32):
            batch = [dict(mutation(key, fields, "create"), schema="ExistingTask")
                     for key, fields in list(model.items())[offset:offset + 32]]
            require(node.call("/api/mutations/batch", batch)[0].get("durability") == "durable", "legacy write is not durable")
        before = primary_rows(node, "ExistingTask", model)
        node.stop()
        node.binary, node.revision = args.candidate, args.candidate_revision
        node.start()
        require(primary_rows(node, "ExistingTask", model) == before, "format 4 open changed fields or atom metadata")
        refused(lambda: install(node, task_definition("NeedsUpgrade")), "enable-alternate-keys")
        node.stop()
        original = native_files(node.home)
        result["activation"] = cli(args, node)
        require(result["activation"] == {"previous_format": 4, "current_format": 5, "changed": True,
                                         "encrypted_files_unchanged": True}, "offline activation receipt differs")
        require(native_files(node.home) == original, "format activation changed an encrypted native file")
        result["repeat_activation"] = cli(args, node)
        require(result["repeat_activation"]["changed"] is False, "format activation repeat changed the home")
        refuse_old_writer(args, node, output)
        require(native_files(node.home) == original, "refused older writer changed native data")
        node.start()
        require((node.home / "data/laststore-pack-node-v1").read_text() == FIVE, "activation lacks the format 5 fence")
        require(primary_rows(node, "ExistingTask", model) == before, "activation changed stored fields or atom metadata")
        check_rows(node.call("/api/query", query(False, "by_group", {"HashKey": "g0"}))[0],
                   {f"t{i:04}": values(f"t{i:04}") for i in range(256)})
        write(node, "Task", {"hash": "legacy-after"}, values("legacy-after"))
        check_rows(node.call("/api/query", query(False, "by_group",
                   {"HashRangeKey": {"hash": "g0", "range": "legacy-after"}}))[0],
                   {"legacy-after": values("legacy-after")})
        result["conversion"] = converted(node, model, before)
        result["legacy_protein_read_write"] = True
        result["encrypted_file_count_unchanged_by_activation"] = len(original)
        result["baseline"] = args.baseline_revision
        result["candidate"] = args.candidate_revision
        save(output / "result.json", result)
        return result
    finally:
        node.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument("--" + label, type=Path, required=True)
        parser.add_argument("--" + label + "-revision", required=True)
    parser.add_argument("--candidate-cli", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "hosted CI only")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(not args.output.exists() and args.output.parent.resolve().is_relative_to(runner), "fresh CI output is required")
    args.output.mkdir(mode=0o700)
    result = {"baseline": args.baseline_revision, "candidate": args.candidate_revision, "complete": False}
    try:
        home = args.output / "existing-home"
        home.mkdir(mode=0o700)
        result["existing_home"] = legacy_home(args, home)
        result["signed_peers"] = peers(args, args.output / "signed-peers")
        result["complete"] = True
    finally:
        save(args.output / "summary.json", result)
    text = ("## Alternate-key compatibility\n\n"
            f"Sources: `{args.baseline_revision}` → `{args.candidate_revision}`.\n\n"
            "Existing format 4 data opens unchanged. Explicit format activation preserves native file hashes.\n"
            "The older writer refuses the enabled format 5 home. Legacy protein reads and writes remain valid.\n"
            "Declaration backfill resumes after reopen; early publication and blocked writes receive errors.\n"
            "Primary values, atom identities, and metadata survive activation. Counts and named key lists match.\n"
            "Changes retire old entries; abort restores the old schema. Field reuse stays separate after removal.\n"
            "Signed peers check declaration revisions, retries, older field outcomes, restart, and retry after backfill.\n")
    (args.output / "summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text, flush=True)


if __name__ == "__main__":
    main()
