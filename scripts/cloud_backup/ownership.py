"""Record exact CI stream ownership after real recovery succeeds.

This receipt supplies provenance, not provider deletion authority. A retirement
service must still freeze and validate the complete stream and shared objects.
"""

import os
import re

from support import ACCOUNT, canonical_uuid, require, save_json


WORKFLOW = ".github/workflows/cloud-backup-recovery.yml"
REPOSITORY = "EdgeVector/lastdb"


def run_identity(source):
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_SHA") == source
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "ci_ownership_origin")
    values = {name: os.environ.get(variable, "") for name, variable in (
        ("run_id", "GITHUB_RUN_ID"), ("run_attempt", "GITHUB_RUN_ATTEMPT"))}
    require(all(re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in values.values()),
            "ci_ownership_run_identity")
    return {"repository": REPOSITORY, "workflow": WORKFLOW, "source": source, **values}


def accepted_point(accepted, baseline):
    job = accepted["begin"]["job"]
    stream = job["stream"]
    scope = stream["scope"]
    require(scope["account_id"] == ACCOUNT
            and str(scope["account_generation"]) == baseline["account_generation"],
            "ci_ownership_dev_scope")
    stream_id = canonical_uuid(stream["stream_id"])
    require(stream_id != baseline["stream_id"], "ci_ownership_excludes_pinned_stream")
    commit_id = canonical_uuid(job["commit_id"])
    digest = accepted["commit_sha256"]
    require(re.fullmatch(r"[0-9a-f]{64}", digest), "ci_ownership_accepted_hash")
    return {"stream_id": stream_id, "commit_id": commit_id, "commit_sha256": digest}


def complete(operation, source, recovered):
    require(source.node.process is None and source.node.clean_stops >= 2 and not source.node.forced,
            "ci_ownership_source_stopped")
    observed = [accepted_point(value, operation.baseline) for value in source.completed.values()]
    require(2 <= len(observed) <= 8
            and len({point["stream_id"] for point in observed}) == 1,
            "ci_ownership_single_bounded_stream")
    selected = [accepted_point(value, operation.baseline) for value in recovered]
    require(2 <= len(selected) <= 8 and all(point in observed for point in selected),
            "ci_ownership_restored_points_observed")
    receipt = {
        "schema": "lastdb.cloud-ci-stream.v1", "owner": run_identity(operation.args.source),
        "account": ACCOUNT, "account_generation": operation.baseline["account_generation"],
        "stream_id": observed[0]["stream_id"], "observed_accepted_points": observed,
        "verified_recovery_points": selected, "source_stopped": True,
        "log_and_snapshot_recovery_passed": True,
        "complete_frozen_directory": False, "provider_deletion_authority": False,
    }
    save_json(operation.args.output / "ci-stream.json", receipt)
    return {"stream_id": receipt["stream_id"], "observed_accepted_points": len(observed),
            "provider_deletion_authority": False}
