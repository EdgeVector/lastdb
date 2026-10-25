#!/usr/bin/env python3
"""Approved one-time lifecycle operations through an actual v2 owner socket."""

import argparse
import json
import os
from pathlib import Path
import time

from node import Node, save
from ttl_latency import seed, mutations, validate_live, LIVE, OLD, OLD_HASHES, FIELDS
from ttl_maintenance import markers
from workload import Workload


def check_removed(node, schema, coordinates):
    queries = [{"schema_name": schema, "fields": FIELDS,
                "filter": {"HashRangeKeys": [coordinate]}} for coordinate in coordinates]
    for start in range(0, len(queries), 64):
        response = node.call("/api/queries/batch", {"queries": queries[start:start + 64]})[0]
        replies = response["results"]
        if len(replies) != len(queries[start:start + 64]):
            raise RuntimeError("Incomplete deleted-key verification")
        for reply in replies:
            data = reply["response"].get("data", reply["response"])
            if reply["status"] != 200 or data.get("results") != []:
                raise RuntimeError("A deleted generated key still resolves")


def erase_cases(node):
    # Separate keys leave all 4096 timed-workload values available for exact reads.
    result = node.call("/api/mutations/batch", mutations(LIVE, range(5000, 5128)))[0]
    Workload.validate_write(result, 128)
    for verb, numbers in (("delete", range(5000, 5064)), ("purge", range(5064, 5128))):
        body = {"mutations": [{"type": "mutation", "schema": LIVE, "mutation_type": verb,
            "durability": "durable", "key_value": {"hash": f"live-{n:09d}"},
            "fields_and_values": {}} for n in numbers]}
        Workload.validate_write(node.call("/api/mutations/batch", body)[0], 64)
    # HashRangeKeys is for the old HashRange schema. Hash reads use the real
    # HashKey selector; all calls here are batched at the HTTP boundary.
    queries = [{"schema_name": LIVE, "fields": FIELDS,
                "filter": {"HashKey": f"live-{n:09d}"}} for n in range(5000, 5128)]
    for start in range(0, len(queries), 64):
        replies = node.call("/api/queries/batch", {"queries": queries[start:start + 64]})[0]["results"]
        if len(replies) != 64 or any(reply["status"] != 200 or
            reply["response"].get("data", reply["response"]).get("results") != [] for reply in replies):
            raise RuntimeError("Delete/Purge left a generated Hash key visible")


def verifier(node, output):
    cursor, pages = None, []
    for _ in range(1024):
        report = node.call("/api/db/verify-refs", {"cursor": cursor, "max_pages": 64,
            "max_ms": 3000, "pause_ms": 0})[0]["verify_refs"]
        pages.append(report)
        save(output / "reference-verifier.json", pages)
        if report["done"]:
            if report["mismatches_total"] or not report["tally_complete"] or report["touched_atoms"]:
                raise RuntimeError("Reference verification is incomplete or reports faults")
            return report
        cursor = report["next_cursor"]
        if not cursor:
            raise RuntimeError("Reference verifier lost its cursor")
    raise RuntimeError("Reference verifier exceeds its generated-home bound")


def exercise(args):
    args.output.mkdir(parents=True)
    node = Node(args.binary, args.revision, args.output, environment={
        "LASTDB_DISK_FORMAT": "2", "LASTDB_TTL_SWEEP": "0",
        "LASTDB_SELF_METRICS_SAMPLE_SECS": "1", "LASTDB_TTL_SWEEP_BATCH": "64",
        "LASTDB_TTL_SWEEP_DUTY_PCT": "100", "LASTDB_TTL_SWEEP_MIN_PAUSE_MS": "1"})
    result = {"complete": False, "source": args.revision,
              "limit": "Operational evidence uses CI-only fast TTL pacing, not latency measurements. "
                       "V2 supports background run merge; explicit v1 compaction must refuse."}
    try:
        node.start()
        seed(node, "active-sweep")
        for start in range(0, 4096, 64):
            Workload.validate_write(node.call("/api/mutations/batch", mutations(LIVE, range(start, start + 64)))[0], 64)
        deadline, settled, peak = time.monotonic() + 1200, None, 0
        while time.monotonic() < deadline:
            status = node.call("/api/status")[0]["status"]
            save(args.output / "last-status.json", status)
            retention = status["local_retention"]
            peak = max(peak, retention["last_pass"]["peak_buffered_keys"])
            if retention["rows_reaped"] >= 8192 and retention["last_pass"]["finished_backlog"]:
                settled = retention
                break
            time.sleep(0.25)
        if not settled or not 0 < peak <= 64:
            raise RuntimeError("TTL did not complete with a bounded expired-key page")
        validate_live(node)
        check_removed(node, OLD, [(OLD_HASHES[n // 512], f"{1:020d}-{n:09d}") for n in range(8192)])
        erase_cases(node)
        node.stop()
        node.environment["LASTDB_TTL_SWEEP"] = "0"
        node.start()
        definition = node.call(f"/api/schema/{OLD}")[0]["schema"]
        cleanup = [markers(node, molecule) for molecule in sorted(set(definition["field_molecule_uuids"].values()))]
        try:
            node.call("/api/db/compact", {"dry_run": False, "full_copy": True})
        except RuntimeError as error:
            if "this operation is not in format v2" not in str(error):
                raise
            compact = {"v1_compaction_refused": str(error)}
        else:
            raise RuntimeError("V2 accepted unsupported v1 pack compaction")
        node.stop()
        node.start()
        validate_live(node)
        verify = verifier(node, args.output)
        merge_lines = [line for line in (args.output / "node.log").read_text().splitlines()
                       if "CI v2 run merge complete" in line]
        if not merge_lines:
            raise RuntimeError("No actual v2 background run merge completed")
        result.update(complete=True, expired_absent=8192, fresh_after_restart=4096,
                      deleted_absent=64, purged_absent=64, observed_peak_buffered_keys=peak, ttl=settled, cleanup=cleanup,
                      compaction=compact, merge_events=merge_lines, verifier=verify)
    except BaseException as failure:
        result["error"] = type(failure).__name__ + ": " + str(failure)
        raise
    finally:
        try:
            node.close()
        finally:
            save(args.output / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        parser.error("Lifecycle operations require GitHub-hosted CI")
    result = exercise(args)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n## One-time v2 lifecycle evidence\n\n```json\n" + json.dumps(result, indent=2) + "\n```\n")


if __name__ == "__main__":
    main()
