#!/usr/bin/env python3
"""Measure finite lifecycle operations on generated GitHub Actions homes."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "range_continuation"))
from node import Node, file_hash, save

FIELDS = ["bucket", "id", "body"]


def environment(node, **settings):
    value = {key: val for key, val in os.environ.items()
             if not key.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))
             and key not in ("GH_TOKEN", "GITHUB_TOKEN")}
    value.update(node.settings, LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                 LASTDB_HOME=str(node.home), FOLDDB_HOME=str(node.home), RUST_MIN_STACK="16777216")
    value.update(settings)
    return value


def command(node, binary, arguments, label, success=True, **settings):
    started = time.perf_counter()
    result = subprocess.run([str(binary), *arguments], env=environment(node, **settings),
                            stdin=subprocess.DEVNULL, capture_output=True, timeout=240)
    (node.output / (label + ".stdout")).write_bytes(result.stdout)
    (node.output / (label + ".stderr")).write_bytes(result.stderr)
    row = {"operation": label, "exit_code": result.returncode,
           "elapsed_ms": (time.perf_counter() - started) * 1000}
    node.remember(row)
    if (result.returncode == 0) != success:
        raise RuntimeError(label + " returned an unexpected exit code; inspect its output")
    if not success:
        return row
    value = json.loads(result.stdout.decode().strip().splitlines()[-1]) if binary.name == "lifecycle_measure" else json.loads(result.stdout)
    row["result"] = value
    node.remember(row)
    return value


def helper(node, binary, phase, window):
    node.stop()
    value = command(node, binary, [str(node.home), phase], phase,
                    LASTDB_CONFLICT_RETENTION_SECS=str(window),
                    LASTDB_TTL_SWEEP="1" if phase == "ttl" else "0",
                    LASTDB_BLOB_DWELL_SECS="120")
    node.start()
    return value


def schema(node, name, hashed=False):
    fields = ["bucket", "body"] if hashed else FIELDS
    key = {"hash_field": "bucket"}
    if not hashed:
        key["range_field"] = "id"
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "Hash" if hashed else "HashRange", "key": key,
        "fields": fields, "field_descriptions": {field: name + " generated " + field for field in fields}}})


def mutate(node, name, coordinates, kind="create", hashed=False, body="seed"):
    mutations = []
    for hashed_key, ranged in coordinates:
        fields = {} if kind in ("delete", "purge") else {"bucket": hashed_key, "body": body}
        key = {"hash": hashed_key}
        if not hashed:
            key["range"] = ranged
            if fields:
                fields["id"] = ranged
        mutations.append({"type": "mutation", "schema": name, "key_value": key,
                          "fields_and_values": fields, "mutation_type": kind, "durability": "durable"})
    value, _ = node.call("/api/mutations/batch", {"mutations": mutations})
    if value.get("count") != len(mutations) or value.get("durability") != "durable":
        raise RuntimeError("The generated batch lacks a complete durable acknowledgement")


def read(node, name, coordinates, hashed=False):
    value, _ = node.call("/api/query", {"schema_name": name, "fields": ["body"],
        "limit": max(1, len(coordinates)), "filter": {"HashRangeKeys": [list(key) for key in coordinates]}})
    if value.get("has_more"):
        raise RuntimeError("A complete known-key read unexpectedly continues")
    return {(row["key"]["hash"], row["key"].get("range", "")): row["fields"]["body"]
            for row in value["results"]}


def backfill(node, name):
    cursor, pages, tracked = None, 0, 0
    first = None
    for _ in range(32):
        request = {"action": "backfill", "schema": name, "limit": 7,
                   "confirm_expire_existing": True, "cursor": cursor}
        value, _ = node.call("/api/db/schema-retention", request)
        pages += 1
        tracked += value["tracked"]
        if first is None:
            first = request
            node.call("/api/db/schema-retention", first)
        if not value["has_more"]:
            return {"pages": pages, "tracked": tracked, "first_page_repeated": True}
        next_cursor = value["next_cursor"]
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("The backfill cursor does not advance")
        cursor = next_cursor
    raise RuntimeError("The generated backfill exceeds its finite page bound")


def retention(node):
    hashed = [(f"hash-{number:04}", "") for number in range(32)]
    old_time = int(time.time()) - 1000
    ranged = [("old-untracked", f"{old_time:020}-{number:04}") for number in range(32)]
    mutate(node, "LifecycleHash", hashed, hashed=True, body="old-hash")
    mutate(node, "LifecycleRange", ranged, body="old-range")
    # Age real writes before a policy exists. No fake clock or source-row injection.
    time.sleep(17)
    mutate(node, "LifecycleHash", hashed[:2], kind="update", hashed=True, body="young-hash")
    young = [("young-untracked", f"{int(time.time()):020}-young")]
    mutate(node, "LifecycleRange", young, body="young-range")
    node.call("/api/db/schema-retention", {"action": "backfill", "schema": "LifecycleHash"},
              expected=(409,))
    for name in ("LifecycleHash", "LifecycleRange"):
        node.call("/api/db/schema-retention", {"action": "set", "schema": name, "ttl_seconds": 15})
    values = {name: backfill(node, name) for name in ("LifecycleHash", "LifecycleRange")}
    return hashed, ranged, young, values


def recount(node, cli, label, execute=False, cutoff=None, success=True):
    arguments = ["--data-dir", str(node.home), "db", "recount", "--all-scopes", "--molecules", "--shards", "4", "--json"]
    if execute:
        arguments.append("--execute")
    if cutoff is not None:
        arguments.extend(["--delete-cutoff-unix-secs", str(cutoff)])
    return command(node, cli, arguments, label, success=success)


def run(node, args):
    node.settings.update(LASTDB_TTL_SWEEP="0", LASTDB_CONFLICT_JANITOR_MS="0",
                         LASTDB_MOLECULE_DRAIN_MS="0", LASTDB_CONFLICT_RETENTION_SECS="3600")
    node.start()
    (node.home / ".lifecycle-ci").write_text("generated CI home only\n")
    for name in ("LifecycleHistory", "LifecycleReplay", "LifecycleRange", "LifecycleBlob"):
        schema(node, name)
    schema(node, "LifecycleHash", hashed=True)
    results = {"conflicts": helper(node, args.helper, "conflicts", 3600),
               "purge": helper(node, args.helper, "purge", 1)}
    # The marker's original one-second window passes while the home is stopped.
    node.stop()
    time.sleep(2)
    node.start()
    results["drain"] = helper(node, args.helper, "drain", 3600)
    history_keys = [("generated", f"key-{index:04}") for index in range(64)]
    expected_history = {history_keys[0]: "reused-tip"}
    if read(node, "LifecycleHistory", history_keys) != expected_history:
        raise RuntimeError("The reopened drain changed the reused tip or restored an erased tip")
    hashed, ranged, young, results["backfill"] = retention(node)
    results["ttl"] = helper(node, args.helper, "ttl", 3600)
    if read(node, "LifecycleHash", hashed, True) != {key: "young-hash" for key in hashed[:2]}:
        raise RuntimeError("TTL did not remove original-age Hash records and preserve new writes")
    if read(node, "LifecycleRange", ranged + young) != {young[0]: "young-range"}:
        raise RuntimeError("TTL did not cover the old untracked hash and preserve its young key")
    # Stop expiry while the owner performs maintenance.
    for name in ("LifecycleHash", "LifecycleRange"):
        node.call("/api/db/schema-retention", {"action": "clear", "schema": name})
    results["running_home_refusal"] = recount(node, args.cli, "running-home-refusal", success=False)
    replay_keys = [("generated", f"key-{index:04}") for index in range(16)]
    mutate(node, "LifecycleReplay", replay_keys)
    mutate(node, "LifecycleReplay", replay_keys, kind="delete")
    if read(node, "LifecycleReplay", replay_keys):
        raise RuntimeError("A deleted record remains visible")
    node.stop()
    results["molecule_plan"] = recount(node, args.cli, "molecule-plan")
    results["molecule_execute"] = recount(node, args.cli, "molecule-execute", execute=True)
    repeated = recount(node, args.cli, "molecule-repeat", execute=True)
    if any(value for key, value in repeated["recount"]["molecules"].items()
           if key in ("headers_rewritten", "bindings_rewritten", "bindings_deleted", "drains_queued")):
        raise RuntimeError("The second molecule repair changes an already repaired home")
    results["molecule_repeat"] = repeated
    time.sleep(2)
    cutoff = int(time.time())
    results["delete_plan"] = recount(node, args.cli, "delete-plan", cutoff=cutoff)
    results["delete_execute"] = recount(node, args.cli, "delete-execute", execute=True, cutoff=cutoff)
    if results["delete_execute"]["recount"]["delete_prune"]["pruned"] == 0:
        raise RuntimeError("The explicit cleanup did not prune any actual Delete barriers")
    node.start()
    results["replay"] = helper(node, args.helper, "replay", 3600)
    expected_replay = {replay_keys[0]: "post-cutoff"}
    if read(node, "LifecycleReplay", replay_keys) != expected_replay:
        raise RuntimeError("Replay restored old data or lost the new write")
    results["shared_atom_and_blob"] = helper(node, args.helper, "blob", 3600)
    results["compaction"], _ = node.call("/api/db/compact", {"dry_run": False})
    node.stop()
    node.start()
    if (read(node, "LifecycleHistory", history_keys) != expected_history
            or read(node, "LifecycleReplay", replay_keys) != expected_replay):
        raise RuntimeError("Readback after compaction and restart differs")
    results["final_status"] = node.status("after-compaction-and-restart")
    results["scope"] = ("Generated real operations; healthy counter repair, no fabricated corruption, "
                        "no power-loss, cloud transport, or physical media erasure claim")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "cli", "helper", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("Lifecycle operations run only on GitHub Actions")
    args.output = args.output.resolve()
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    if not args.output.is_relative_to(temporary) or args.output.is_relative_to(Path(os.environ["GITHUB_WORKSPACE"]).resolve()):
        parser.error("Evidence must stay outside Git under RUNNER_TEMP")
    args.output.mkdir(exist_ok=True)
    manifest = {"complete": False, "revision": args.revision, "runs": [],
                "binaries": {name: {"sha256": file_hash(getattr(args, name)), "path": str(getattr(args, name))}
                             for name in ("binary", "cli", "helper")}}
    save(args.output / "lifecycle-run.json", manifest)
    for mode in ("plain", "encrypted"):
        output = args.output / mode
        output.mkdir()
        node = Node(args.binary, args.revision, mode, output)
        error, results = None, {}
        try:
            results = run(node, args)
        except BaseException as failure:
            error = type(failure).__name__ + ": " + str(failure)
        finally:
            try:
                node.close()
            except BaseException as failure:
                error = (error or "") + "; cleanup: " + str(failure)
        save(output / "result.json", {"complete": error is None, "error": error, "operations": results})
        manifest["runs"].append({"mode": mode, "complete": error is None, "error": error})
        save(args.output / "lifecycle-run.json", manifest)
        print(json.dumps(manifest["runs"][-1]), flush=True)
        if error:
            return 1
    manifest["complete"] = True
    save(args.output / "lifecycle-run.json", manifest)
    return 0


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt("CI interruption")))
    raise SystemExit(main())

