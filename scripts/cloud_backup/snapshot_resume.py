"""Verify the exact completed GitHub Actions attempt before resuming cleanup."""

import http.client
import os
import re

from snapshot_cleanup import RunAttempt
from support import OperationError, decode_json, require


REPOSITORY = "EdgeVector/lastdb"
WORKFLOW = ".github/workflows/snapshot-roundtrip-dev.yml"
PROOF_STEPS = {
    "snapshot-roundtrip-dev": "Seed, snapshot, and restore the gate database",
    "snapshot-roundtrip-published-nightly": "Seed, snapshot, and restore the gate database",
    "snapshot-native-cutoff-dev": "Recover a lagging device across the native snapshot cutoff",
}
MAX_REPLY_BYTES = 2_000_000


def github_json(path, token):
    connection = http.client.HTTPSConnection("api.github.com", timeout=20)
    try:
        connection.request("GET", path, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "LastDB-snapshot-cleanup",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        response = connection.getresponse()
        require(response.status == 200, "snapshot_resume_github_status")
        body = response.read(MAX_REPLY_BYTES + 1)
    except (OSError, http.client.HTTPException):
        raise OperationError("snapshot_resume_github_transport") from None
    finally:
        connection.close()
    require(len(body) <= MAX_REPLY_BYTES, "snapshot_resume_github_size")
    parsed = decode_json(body)
    require(isinstance(parsed, dict), "snapshot_resume_github_reply")
    return parsed


def verified_attempt(run_id, attempt_number):
    require(os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "snapshot_resume_protected_workflow")
    target = RunAttempt(run_id, attempt_number)
    token = os.environ.pop("SNAPSHOT_GITHUB_TOKEN", "")
    require(isinstance(token, str) and re.fullmatch(r"[!-~]{1,1024}", token),
            "snapshot_resume_github_token")
    path = f"/repos/{REPOSITORY}/actions/runs/{target.run_id}/attempts/{target.attempt}"
    run = github_json(path, token)
    require(run.get("id") == int(target.run_id)
            and run.get("run_attempt") == int(target.attempt)
            and run.get("path") == WORKFLOW
            and run.get("head_branch") == "main"
            and run.get("event") in ("push", "schedule", "workflow_dispatch")
            and isinstance(run.get("repository"), dict) and run["repository"].get("full_name") == REPOSITORY
            and isinstance(run.get("head_repository"), dict)
            and run["head_repository"].get("full_name") == REPOSITORY,
            "snapshot_resume_original_scope")
    require(run.get("status") == "completed" and bool(run.get("conclusion")),
            "snapshot_resume_original_not_terminal")
    revision = run.get("head_sha")
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision),
            "snapshot_resume_original_revision")
    return target, revision, proof_result(github_json(f"{path}/jobs?per_page=50", token), target)


def proof_result(reply, target):
    jobs = reply.get("jobs")
    require(isinstance(jobs, list) and len(jobs) == reply.get("total_count") and len(jobs) <= 50,
            "snapshot_resume_original_jobs")
    proofs = [job for job in jobs if isinstance(job, dict) and job.get("name") in PROOF_STEPS]
    require(len(proofs) == 1, "snapshot_resume_original_proof")
    job = proofs[0]
    require(job.get("run_id") == int(target.run_id) and job.get("run_attempt") == int(target.attempt)
            and job.get("status") == "completed" and isinstance(job.get("steps"), list),
            "snapshot_resume_original_job_scope")
    steps = [step for step in job["steps"]
             if isinstance(step, dict) and step.get("name") == PROOF_STEPS[job["name"]]]
    require(len(steps) <= 1, "snapshot_resume_original_proof_step")
    return steps[0].get("conclusion") if steps else None
