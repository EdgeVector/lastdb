#!/usr/bin/env python3
"""Capture real erase, meter, reopen, and retired-command results on generated data."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from reuse import original
from benchmark_support import FIELDS, SCHEMA, UnixConnection, bounded_inventory, file_hash, save

Benchmark = original("benchmark").Benchmark

PEER = "DiskGrowthMeasureById"
COUNT = 256


class EraseCapture(Benchmark):
    def install(self):
        for schema, layout, key in [(SCHEMA, "HashRange", {"hash_field": "bucket", "range_field": "id"}),
                                    (PEER, "Hash", {"hash_field": "id"})]:
            self.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
                "source": "system_seed", "name": schema, "descriptive_name": schema,
                "schema_type": layout, "key": key, "fields": FIELDS,
                "field_descriptions": {field: f"generated erase shared {field}" for field in FIELDS}}})

    def expected_http(self, path, value, status):
        connection = UnixConnection(self.full_socket)
        try:
            connection.request("POST", path, json.dumps(value), {"Content-Type": "application/json",
                               "X-LastDB-Client": "owned-erase-capture"})
            response = connection.getresponse()
            raw = response.read()
            result = json.loads(raw)
            self.remember("removed-route", path=path, http_status=response.status, response=result)
            if response.status != status:
                raise RuntimeError("The removed route returned an unexpected status")
        finally:
            connection.close()

    def erase(self, numbers):
        numbers = list(numbers)
        mutations = [{"type": "mutation", "schema": SCHEMA, "mutation_type": "delete",
                      "durability": "durable", "key_value": {"hash": "measure", "range": f"key-{n:06d}"},
                      "fields_and_values": {}} for n in numbers]
        reply = self.call("/api/mutations/batch", {"mutations": mutations})
        if reply.get("count") != len(numbers) or reply.get("durability") != "durable":
            raise RuntimeError("The erase lacks its complete durable acknowledgement")
        for number in numbers:
            self.expected.pop(f"key-{number:06d}")
        self.flush(self.phase)

    def verify(self, label):
        all_keys = [f"key-{n:06d}" for n in range(COUNT)]
        reply = self.call("/api/query", {"schema_name": SCHEMA, "fields": FIELDS,
                          "filter": {"HashRangeKeys": [["measure", key] for key in all_keys]}, "limit": COUNT})
        actual = {row["fields"]["id"]: row["fields"] for row in reply.get("results", [])}
        if actual != self.expected or reply.get("has_more") is not False:
            raise RuntimeError(f"Known-key read differs for {SCHEMA} after {label}")
        batches = [{"queries": [{"schema_name": PEER, "fields": FIELDS,
                    "filter": {"HashKey": key}, "limit": 1} for key in all_keys[start:start + 64]]}
                   for start in range(0, COUNT, 64)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            replies = list(pool.map(lambda request: self.call("/api/queries/batch", request), batches))
        actual = {}
        for batch in replies:
            for item in batch["results"]:
                if item["status"] != 200:
                    raise RuntimeError("A known-key peer read failed")
                reply = item["response"].get("data", item["response"])
                actual.update({row["fields"]["id"]: row["fields"] for row in reply.get("results", [])})
        if actual != self.expected:
            raise RuntimeError(f"Known-key read differs for {PEER} after {label}")
        result, cursor = {}, None
        for _ in range(8):
            request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 37,
                       "filter": {"HashRangeRange": {"hash": "measure", "start": "key-000000",
                                                       "end": "key-000256"}}}
            if cursor:
                request["cursor"] = cursor
            reply = self.call("/api/query", request)
            result.update({row["fields"]["id"]: row["fields"] for row in reply.get("results", [])})
            if not reply.get("has_more"):
                break
            next_cursor = reply.get("next_cursor")
            if not next_cursor or next_cursor == cursor:
                raise RuntimeError("The range cursor did not advance")
            cursor = next_cursor
        else:
            raise RuntimeError("The generated range exceeds its eight-page bound")
        if result != self.expected:
            raise RuntimeError("The range does not match the expected generated values")
        self.remember("known-key-readback", label=label, live_records=len(self.expected),
                      both_protein_schemas_verified=True)
        return self.meters(label)

    def meters(self, label):
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(self.call, "/api/storage/schema", {"schema": schema})
                       for schema in (SCHEMA, PEER)]
            app = pool.submit(self.call, "/api/storage/app")
            meters = {schema: future.result()["schema_storage"]
                      for schema, future in zip((SCHEMA, PEER), futures)}
            app = app.result()["app_storage"]
        for schema, meter in meters.items():
            if meter["active_slot_count"] != len(self.expected) * len(FIELDS):
                raise RuntimeError(f"The molecule slot count differs for {schema}")
            if not self.expected and meter["logical_value_bytes"] != 0:
                raise RuntimeError("The empty schema retains logical value bytes")
        if not self.expected and any(owner["atom_count"] != 0 for owner in app["owners"]):
            raise RuntimeError("The empty generated home retains aggregate atom counts")
        result = {"live_records": len(self.expected), "schemas": meters, "app": app}
        save(self.output / f"meters-{label}.json", result)
        return result


def command(capture, binary, arguments, label, expected=0):
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
    environment.update(LASTDB_HOME=str(capture.home), FOLDDB_HOME=str(capture.home),
                       LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1")
    result = subprocess.run([str(binary), *arguments], env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=min(45, capture.remaining()))
    if len(result.stdout) + len(result.stderr) > 2 * 1024 * 1024:
        raise RuntimeError("The product command exceeded its two MiB receipt bound")
    (capture.output / f"{label}.stdout").write_bytes(result.stdout)
    (capture.output / f"{label}.stderr").write_bytes(result.stderr)
    capture.remember("product-command", label=label, binary=str(binary), arguments=arguments,
                     exit_code=result.returncode, stdout_sha256=hashlib.sha256(result.stdout).hexdigest(),
                     stderr_sha256=hashlib.sha256(result.stderr).hexdigest())
    if (expected == "nonzero" and result.returncode == 0) or (expected != "nonzero" and result.returncode != expected):
        raise RuntimeError(f"Unexpected product command exit for {label}")
    return result


def inspect_meters(capture, disk_binary, label):
    if capture.process is not None:
        raise RuntimeError("Native inspection requires the owned node to stop")
    keys = ["keep_small:meters"]
    keys += ["keep_small:meters:schema:" + schema.encode().hex() for schema in (SCHEMA, PEER)]
    keys += ["keep_small:meters:unattributed", "keep_small:hard_erase_totals"]
    request = {"coordinates": [{"collection": "keep_small", "key": key} for key in keys],
               "budget": {"pages": 1024, "blocks": 1024, "bytes": 8 * 1024 * 1024, "keys": 512}}
    request_path = capture.output / f"{label}-native-input.json"
    save(request_path, request)
    output = command(capture, disk_binary, ["read", "--home", str(capture.home / "data" / "disk-pack"),
                     "--input", str(request_path)], f"{label}-native")
    listing = json.loads(output.stdout)
    if len(listing["results"]) != len(keys) or any("error" in row for row in listing["results"]):
        raise RuntimeError("The exact native meter read did not complete")
    rows = {key: bytes.fromhex(row["body_hex"]) if row.get("body_hex") is not None else None
            for key, row in zip(keys, listing["results"])}
    if any(rows[key] is None for key in keys[:3]):
        raise RuntimeError("The native snapshot lacks an aggregate header or schema slice")
    if rows["keep_small:hard_erase_totals"] is not None:
        raise RuntimeError("The retired aggregate recovery key exists in the generated home")
    if any(body is not None and not body.startswith(b"ENC:") for body in rows.values()):
        raise RuntimeError("A generated meter value lacks its encrypted envelope")
    inventory = bounded_inventory(capture.home)
    if any("journal" in name.lower() or "JOURNAL_TAIL" in name for name in inventory["_files"]):
        raise RuntimeError("The generated native home contains a recovery journal file")
    save(capture.output / f"{label}-stored-meters.json", {
         key: {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(), "encrypted": True}
         if body is not None else None for key, body in rows.items()})
    save(capture.output / f"{label}-native-inventory.json", inventory)
    return {"generation": listing["generation"], "frontier": listing["frontier"],
            "exact_meter_coordinates": keys, "retired_aggregate_key_absent": True,
            "no_recovery_files": True, "stored_values_encrypted": True,
            "format_version_2_is_source_guarantee": True,
            "key_absence_scope": "Exact retired aggregate key only; random journal-key absence is a zero-writer source fact",
            "numeric_restore_verification": "Ordinary owner APIs before and after reopen; no identity or key read"}


def protected_metadata(home):
    roots = [home / "data" / "disk-pack", home / "cloud-purge-resync", home / "identity.key"]
    pending, entries, result, deadline = [(path, 0) for path in roots], 0, {}, time.monotonic() + 15
    while pending:
        path, depth = pending.pop()
        entries += 1
        info = path.lstat()
        if (depth > 8 or entries > 40000 or time.monotonic() > deadline
                or stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid()):
            raise RuntimeError("The owned protected metadata capture exceeded its bounds")
        result[str(path.relative_to(home))] = [info.st_mode, info.st_size, info.st_mtime_ns, info.st_ino]
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as children:
                for child in children:
                    if len(pending) + entries >= 40000 or time.monotonic() > deadline:
                        raise RuntimeError("The protected metadata queue exceeded its bounds")
                    pending.append((Path(child.path), depth + 1))
        elif not stat.S_ISREG(info.st_mode):
            raise RuntimeError("Protected snapshot, retired state, and identity must use ordinary files")
    return result


def retired_state_refusal(capture):
    marker = capture.home / "cloud-purge-resync"
    marker.mkdir(mode=0o700)
    (marker / "owned-marker.json").write_text('{"generated_owned_marker":true}\n')
    before = protected_metadata(capture.home)
    log_start = (capture.output / "node.log").stat().st_size
    try:
        try:
            capture.start()
        except RuntimeError:
            if capture.process is None or capture.process.poll() in (None, 0):
                raise
        else:
            raise RuntimeError("The candidate accepted retired repair state")
        capture.stop(force=True)
        log_path = capture.output / "node.log"
        if log_path.stat().st_size > 8 * 1024 * 1024:
            raise RuntimeError("The owned node log exceeds its eight MiB bound")
        if b"unsupported retired cloud repair state" not in log_path.read_bytes()[log_start:]:
            raise RuntimeError("The failed boot lacks the expected retired-state refusal")
        after = protected_metadata(capture.home)
        if before != after:
            raise RuntimeError("The refusal changed the native snapshot, retired records, or identity metadata")
        save(capture.output / "retired-state-refusal.json", {
             "retired_records_and_snapshot_unchanged": True, "identity_metadata_unchanged": True,
             "scope": "Native snapshot files, retired repair tree, and identity file metadata; normal diagnostics may change",
             "before": before, "after": after, "protected_file_contents_were_not_read_by_capture": True})
    finally:
        if capture.process is not None:
            capture.stop(force=True)
        (marker / "owned-marker.json").unlink()
        marker.rmdir()


def run(capture, args):
    capture.phase = "generated-protein-seed"
    capture.start()
    capture.install()
    capture.write(range(COUNT), 0, "durable", create=True)
    capture.flush("seed")
    seeded = capture.verify("seed")
    capture.stop()
    seed_disk = inspect_meters(capture, args.disk_binary, "seed")
    capture.start()
    capture.phase = "three-chunk-protein-erase"
    capture.erase(range(129))
    erased = capture.verify("after-129")
    capture.stop(force=True)
    erased_disk = inspect_meters(capture, args.disk_binary, "after-129")
    capture.phase = "direct-snapshot-reopen"
    capture.start()
    reopened = capture.meters("reopened-before-any-value-fetch")
    if reopened["app"].get("incomplete_reason") != "stale_after_unclean_stop":
        raise RuntimeError("The interrupted reopen lacks its expected stale meter verdict")
    for schema in (SCHEMA, PEER):
        for field in ("active_slot_count", "logical_value_bytes", "schema_structure_bytes", "counter_epoch"):
            if reopened["schemas"][schema][field] != erased["schemas"][schema][field]:
                raise RuntimeError("Direct aggregate restore changed a molecule counter")
    for field in ("live_total_bytes", "system_bytes", "unattributed_bytes"):
        if reopened["app"][field] != erased["app"][field]:
            raise RuntimeError("Direct aggregate restore changed an app byte total")
    if reopened["app"]["owners"] != erased["app"]["owners"]:
        raise RuntimeError("Direct aggregate restore changed owner counts or bytes")
    capture.verify("reopened-129")
    capture.phase = "erase-remainder"
    capture.erase(range(129, COUNT))
    empty = capture.verify("empty")
    capture.expected_http("/api/sync/cloud-purge-resync", {"action": "status"}, 404)
    for label, arguments in [("removed-purge-command", ["cloud", "purge-resync", "status", "--json"]),
                             ("removed-rewrite-command", ["cloud", "rewrite-deleted-atoms", "--help"])]:
        refused = command(capture, args.cli_binary, arguments, label, expected="nonzero")
        if b"unrecognized subcommand" not in refused.stderr or arguments[1].encode() not in refused.stderr:
            raise RuntimeError("The CLI did not explicitly refuse the removed subcommand")
    capture.stop()
    empty_disk = inspect_meters(capture, args.disk_binary, "empty")
    retired_state_refusal(capture)
    capture.phase = "final-graceful-reopen"
    capture.start()
    capture.meters("final-empty-before-any-value-fetch")
    capture.verify("final-empty-reopen")
    return {"complete": True, "records": COUNT, "first_erase": 129, "first_erase_chunks_per_schema": 3,
            "shared_field_schemas": [SCHEMA, PEER], "seed": seeded, "after_129": erased,
            "reopened_129": reopened, "empty": empty, "persisted": [seed_disk, erased_disk, empty_disk],
            "retired_route_absent": True, "retired_commands_absent": True,
            "retired_records_and_snapshot_unchanged": True,
            "scope": "Generated product data; interruption follows explicit flush; no power-loss or primary claim"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("binary", "cli-binary", "disk-binary", "scratch", "output"):
        parser.add_argument(f"--{flag}", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    args.label = "erase-aggregate"
    args.cli_binary, args.disk_binary = args.cli_binary.resolve(strict=True), args.disk_binary.resolve(strict=True)
    capture = EraseCapture(args)
    metadata = json.loads((capture.output / "inputs.json").read_text())
    metadata.update(records=COUNT, shared_field_schemas=[SCHEMA, PEER], erasures=[129, COUNT - 129],
                    erase_chunks_per_schema=[3, 2], driver_sha256=file_hash(Path(__file__)),
                    cli_binary=str(args.cli_binary), cli_binary_sha256=file_hash(args.cli_binary),
                    disk_binary=str(args.disk_binary), disk_binary_sha256=file_hash(args.disk_binary))
    save(capture.output / "inputs.json", metadata)
    summary = {"complete": False, "source_revision": args.source_revision}
    try:
        summary.update(run(capture, args))
    except BaseException as error:
        summary.update(complete=False, error=str(error))
        raise
    finally:
        try:
            capture.stop()
        except BaseException as error:
            summary.update(complete=False, shutdown_error=str(error))
            raise
        finally:
            capture.save_receipts()
            summary.update(all_owned_processes_stopped=capture.process is None, scratch_retained=True,
                           scratch=str(capture.root), home=str(capture.home))
            save(capture.output / "summary.json", summary)
            print(json.dumps({key: summary[key] for key in ("complete", "all_owned_processes_stopped", "scratch")}), flush=True)


if __name__ == "__main__":
    main()
