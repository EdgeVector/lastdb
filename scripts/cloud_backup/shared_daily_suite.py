#!/usr/bin/env python3
"""Run one real shared DEV scenario, or one bounded retirement pass."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True

import shared_daily_cleanup as cleanup
import shared_daily_io as io
import shared_daily_scopes as scopes
from shared_origin import owner
from support import OperationError, failure, require, save_json


def verify_case(case, proof, source, scope):
    require(proof["result"] == "passed" and proof["stage"] == "complete"
            and proof["source"] == source and proof["scope"] == scope
            and proof["primary_access"] is False and proof["provider_deletions"] == 0
            and proof["manual_push_pull_commands"] == 0
            and proof["cleanup"] == "owned runner data removed" and proof["restored_boots"] == 2,
            "shared_daily_complete_case")
    flags = {
        "lifecycle": ("live_write_during_upload", "live_write_after_restart", "local_only_excluded",
                      "returning_writer_private_file", "returning_writer_previous_data"),
        "conflicts": ("both_authored_before_either_upload", "opposite_replay_orders_converged",
                      "original_writers_converged", "exact_original_revisions_preserved",
                      "restart_convergence", "replay_retry_idle", "upload_retry_idle"),
        "retention": ("displaced_pending_file_uploaded", "live_write_during_upload", "local_only_excluded"),
        "automatic": ("automatic_upload", "automatic_download_apply", "automatic_bound_schema_alias",
                      "automatic_delete_applied", "automatic_replacement_file_applied",
                      "automatic_policy_paused_both_directions", "automatic_policy_resume_caught_up",
                      "automatic_policy_survived_restart", "automatic_download_after_restart",
                      "automatic_upload_after_restart", "automatic_snapshot_shorter_than_sync",
                      "automatic_snapshot", "automatic_uncovered_peer_tail", "source_homes_removed"),
    }
    require(all(proof.get(name) is True for name in flags[case]), "shared_daily_case_coverage")
    if case == "lifecycle":
        require(proof["returning_writer_next_through"] > proof["returning_writer_pending_through"] > 0
                and proof["delete_notes_reclaimed"] == 1, "shared_daily_returning_and_delete_coverage")
    if case == "retention":
        require(proof["shared_retention_overwrites"] == 24 and len(proof["retention_measurements"]) == 3,
                "shared_daily_retention_coverage")
    if case == "automatic":
        require(proof["automatic_running_writers"] == 6,
                "shared_daily_automatic_coverage")


def case(args):
    identity = io.admission()
    scope, seeds = scopes.create(identity, args.case, cleanup.configured_targets())
    root = Path(os.environ["RUNNER_TEMP"]).resolve() / ("shared-daily-work-" + args.case)
    root.mkdir(mode=0o700)
    environment = dict(os.environ, RUNNER_TEMP=str(root),
                       LASTDB_CLOUD_DEV_SHARED_SCOPE=json.dumps(scope),
                       LASTDB_CLOUD_DEV_SHARED_DEVICE_SEEDS=json.dumps(seeds))
    for name in list(environment):
        if name.startswith("AWS_") or name.startswith("ACTIONS_ID_TOKEN_") or name in (
                "GH_TOKEN", "GITHUB_TOKEN", "GITHUB_STEP_SUMMARY"):
            environment.pop(name)
    arguments = [sys.executable, str(Path(__file__).with_name("shared_recovery.py")),
                 "--bin-dir", str(args.bin_dir), "--source", args.source, "--version", args.version,
                 "--output", str(root / "cloud-shared-recovery"), "--multiwriter"]
    if args.case == "conflicts":
        arguments.append("--conflicts")
    if args.case == "retention":
        arguments.append("--retention")
    if args.case == "automatic":
        arguments.append("--automatic-sync")
    result = subprocess.run(arguments, env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, timeout=440, check=False)
    path = root / "cloud-shared-recovery/summary.json"
    require(path.is_file() and path.stat().st_size < 1024 * 1024, "shared_daily_case_summary")
    proof = json.loads(path.read_text())
    summary = {"source": args.source, "owner": identity, "case": args.case, "proof": proof,
               "phase": "active", "result": "failed"}
    try:
        # Only a confirmed stopped/removed source permits immediate authority fencing.
        require(proof.get("cleanup") == "owned runner data removed", "shared_daily_owned_case_cleanup")
        fenced = scopes.fence(scope["database_id"])
        summary.update(phase=fenced["phase"], fenced_at_ms=fenced["fenced_at_ms"])
        scopes.remember_schema(args.case, proof)
        require(result.returncode == 0, "shared_daily_product_case_failed")
        verify_case(args.case, proof, args.source, scope)
        summary["result"] = "passed"
    except OperationError as error:
        summary.update(failure(error))
    if proof.get("cleanup") == "owned runner data removed":
        shutil.rmtree(root)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--case", choices=io.CASES)
    selection.add_argument("--cleanup", action="store_true")
    parser.add_argument("--source", required=True)
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--version")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    expected = "cleanup" if args.cleanup else args.case
    require(args.output == Path(os.environ["RUNNER_TEMP"]) / f"shared-daily-{expected}.json"
            and not args.output.exists(), "shared_daily_output_path")
    summary = {"source": args.source, "result": "failed", "case": args.case or "cleanup"}
    try:
        identity = owner(args.source)
        require(identity["workflow"].endswith("/cloud-shared-daily.yml"), "shared_daily_workflow")
        if args.cleanup:
            cleanup.run(summary)
        else:
            summary = case(args)
    except OperationError as error:
        summary.update(failure(error), result="failed")
    except Exception:
        summary.update(result="failed", failure="shared_daily_unexpected_operation_error")
    save_json(args.output, summary)
    print(json.dumps({k: summary.get(k) for k in ("case", "result", "failure", "source")}))
    return 0 if summary["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
