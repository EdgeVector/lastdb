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

import shared_abandoned
import shared_daily_cleanup as cleanup
import shared_daily_io as io
import shared_daily_scopes as scopes
import shared_reclamation_observer
from shared_origin import owner
from support import OperationError, failure, require, save_json


def verify_case(case, proof, source, scope):
    require(proof["result"] == "passed" and proof["stage"] == "complete"
            and proof["source"] == source and proof["scope"] == scope
            and proof["primary_access"] is False and proof["provider_deletions"] == 0
            and proof["manual_push_pull_commands"] == 0
            and proof["cleanup"] == "owned runner data removed" and proof["restored_boots"] == 2,
            "shared_daily_complete_case")
    if case == io.RECLAMATION:
        evidence = proof["automatic_reclamation"]
        require(evidence["old_b2_versions_absent"] > 0 and evidence["old_r2_objects_absent"] > 0
                and evidence["old_objects_preserved_during_hold"] is True
                and evidence["uncovered_peer_through"] > 0
                and proof["source_homes_removed"] is True and proof["exact_absent_records"] == 1,
                "shared_daily_reclamation_coverage")
        return
    if case == io.ABANDONED:
        evidence = proof["abandoned_reclamation"]
        require(evidence["released_objects"] == 3 and evidence["released_bytes"] == 3 * shared_abandoned.FILE_BYTES
                and evidence["pruned_pages"] == 1 and evidence["old_b2_versions_absent"] == 1
                and evidence["old_r2_objects_absent"] == 1 and evidence["old_b2_multipart_absent"] is True
                and evidence["active_b2_multipart_preserved"] is True
                and evidence["provider_bytes_preserved_during_drain"] is True
                and all(proof[name] is True for name in ("source_homes_removed", "expired_staging_refused",
                        "accepted_receipt_replayed", "active_upload_completed", "fresh_upload_verified")),
                "shared_daily_abandoned_coverage")
        return
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
                      "automatic_snapshot", "automatic_uncovered_peer_tail", "source_homes_removed",
                      "automatic_later_schema_replicated", "automatic_unadmitted_write_refused",
                      "schema_service_unreachable", "later_definition_admitted",
                      "incompatible_definition_refused", "branch_definitions_converged",
                      "earlier_definition_writer_kept_writing"),
    }
    require(all(proof.get(name) is True for name in flags[case]), "shared_daily_case_coverage")
    if case == "lifecycle":
        require(proof["returning_writer_next_through"] > proof["returning_writer_pending_through"] > 0
                and proof["delete_notes_reclaimed"] == 1, "shared_daily_returning_and_delete_coverage")
    if case == "retention":
        require(proof["shared_retention_overwrites"] == 24 and len(proof["retention_measurements"]) == 3,
                "shared_daily_retention_coverage")
    if case == "automatic":
        # The replace that omits a later field is recorded, not decided.
        require(proof["automatic_running_writers"] == 6
                and isinstance(proof.get("earlier_app_replace_kept_later_field"), bool),
                "shared_daily_automatic_coverage")


def case(args):
    identity = io.admission()
    targets = cleanup.configured_targets()
    scope, seeds = scopes.create(identity, args.case, targets)
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
    observation_failure = None
    if args.case in io.MANUAL_CASES:
        arguments.append("--" + args.case)
        mailbox = root / "provider-observation"
        mailbox.mkdir(mode=0o700)
        observer = shared_abandoned if args.case == io.ABANDONED else shared_reclamation_observer
        with subprocess.Popen(arguments, env=environment, stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
            try:
                observer.observe(mailbox, scope, targets, process)
                exit_code = process.wait(timeout=240 if args.case == io.ABANDONED else 140)
            except Exception as error:
                observation_failure = (str(error) if isinstance(error, OperationError)
                                       else "shared_" + args.case + "_observer_failed")
                if process.poll() is None:
                    process.terminate()
                exit_code = process.wait(timeout=30)
    else:
        exit_code = subprocess.run(arguments, env=environment, stdin=subprocess.DEVNULL,
                                   capture_output=True, timeout=440, check=False).returncode
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
        require(observation_failure is None, observation_failure)
        require(exit_code == 0, "shared_daily_product_case_failed")
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
    selection.add_argument("--case", choices=(*io.CASES, *io.MANUAL_CASES))
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
